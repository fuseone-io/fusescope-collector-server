// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// Of equal keys labels keep the generated, resource, scope, then point value,
// while the fingerprint keeps series apart that differ only in a shadowed value
// (metrics §3, §4; observed: probe.collide_*).
func TestLabelPrecedence(t *testing.T) {
	histogram := func(attrs func(r, s, p pcommon.Map)) pmetric.Metrics {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("probe")
		m := sm.Metrics().AppendEmpty()
		m.SetName("h")
		h := m.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		p := h.DataPoints().AppendEmpty()
		p.ExplicitBounds().FromRaw([]float64{1, 2})
		p.BucketCounts().FromRaw([]uint64{1, 1, 1})
		p.SetCount(3)
		attrs(rm.Resource().Attributes(), sm.Scope().Attributes(), p.Attributes())
		return md
	}
	summary := func(attrs func(p pcommon.Map)) pmetric.Metrics {
		md := pmetric.NewMetrics()
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName("s")
		p := m.SetEmptySummary().DataPoints().AppendEmpty()
		for _, q := range []float64{0.5, 0.9} {
			p.QuantileValues().AppendEmpty().SetQuantile(q)
		}
		attrs(p.Attributes())
		return md
	}
	// gauges has two points whose attribute key is also set on the resource or the scope.
	gauges := func(key string, onResource bool) pmetric.Metrics {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		sm := rm.ScopeMetrics().AppendEmpty()
		if onResource {
			rm.Resource().Attributes().PutStr(key, "custom")
		} else {
			sm.Scope().Attributes().PutStr(key, "custom")
		}
		m := sm.Metrics().AppendEmpty()
		m.SetName("g")
		g := m.SetEmptyGauge()
		for _, v := range []string{"a", "b"} {
			g.DataPoints().AppendEmpty().Attributes().PutStr(key, v)
		}
		return md
	}

	tests := []struct {
		name   string
		md     pmetric.Metrics
		metric string
		key    string
		want   []string
		// wantMeta: metadata keeps the incoming "custom" value, as its attribute map does.
		wantMeta bool
	}{
		{"point le", histogram(func(_, _, p pcommon.Map) { p.PutStr("le", "custom") }), "h.bucket", "le", []string{"+Inf", "1", "2"}, false},
		{"resource le", histogram(func(r, _, _ pcommon.Map) { r.PutStr("le", "custom") }), "h.bucket", "le", []string{"+Inf", "1", "2"}, true},
		{"point quantile", summary(func(p pcommon.Map) { p.PutStr("quantile", "custom") }), "s.quantile", "quantile", []string{"0.5", "0.9"}, false},
		{"point __name__", histogram(func(_, _, p pcommon.Map) { p.PutStr("__name__", "custom") }), "h.count", "__name__", []string{"h.count"}, true},
		{"point __temporality__", histogram(func(_, _, p pcommon.Map) { p.PutStr("__temporality__", "custom") }), "h.count", "__temporality__", []string{"Cumulative"}, false},
		{"scope __scope.name__", histogram(func(_, s, _ pcommon.Map) { s.PutStr("__scope.name__", "custom") }), "h.count", "__scope.name__", []string{"probe"}, false},
		{"resource over point", gauges("host.name", true), "g", "host.name", []string{"custom", "custom"}, true},
		{"scope over point", gauges("region", false), "g", "region", []string{"custom", "custom"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := newMetricsEncoder()
			if err := enc.encode(tt.md); err != nil {
				t.Fatal(err)
			}
			c := enc.series
			var got []string
			fingerprints := map[uint64]bool{}
			for i := range c.labels.Rows() {
				assertUniqueKeys(t, "attrs", c.attrs, i)
				assertUniqueKeys(t, "scope_attrs", c.scopeAttrs, i)
				if c.metricName.Row(i) != tt.metric {
					continue
				}
				var labels map[string]string
				if err := json.Unmarshal([]byte(c.labels.Row(i)), &labels); err != nil {
					t.Fatal(err)
				}
				got = append(got, labels[tt.key])
				fingerprints[c.fingerprint.Row(i)] = true
			}
			slices.Sort(got)
			if !slices.Equal(got, tt.want) {
				t.Errorf("%s values of %s = %q, want %q", tt.key, tt.metric, got, tt.want)
			}
			if len(fingerprints) != len(tt.want) {
				t.Errorf("%d fingerprints for %d series", len(fingerprints), len(tt.want))
			}
			meta := false
			for _, r := range enc.metaRows {
				meta = meta || r.key.metricName == tt.metric && r.key.attrName == tt.key && r.key.value == "custom"
			}
			if meta != tt.wantMeta {
				t.Errorf("metadata has %s=custom: %v, want %v", tt.key, meta, tt.wantMeta)
			}
		})
	}
}

func assertUniqueKeys(t *testing.T, name string, m *strMap, row int) {
	t.Helper()
	start := 0
	if row > 0 {
		start = int(m.Offsets[row-1])
	}
	seen := map[string]bool{}
	for j := start; j < int(m.Offsets[row]); j++ {
		k := m.Keys.(*proto.ColLowCardinality[string]).Row(j)
		if seen[k] {
			t.Errorf("%s of row %d has key %q twice", name, row, k)
		}
		seen[k] = true
	}
}

// expansionMetrics has one resource with a resource attribute of resourceBytes
// and calls add for its metric slice.
func expansionMetrics(resourceBytes int, add func(pmetric.MetricSlice)) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("big", strings.Repeat("x", resourceBytes))
	add(rm.ScopeMetrics().AppendEmpty().Metrics())
	return md
}

func addHistogram(ms pmetric.MetricSlice, name string, bounds int) {
	m := ms.AppendEmpty()
	m.SetName(name)
	p := m.SetEmptyHistogram().DataPoints().AppendEmpty()
	for i := range bounds {
		p.ExplicitBounds().Append(float64(i))
	}
	p.SetCount(1)
}

func addGauges(ms pmetric.MetricSlice, name string, series, points int, value float64) {
	m := ms.AppendEmpty()
	m.SetName(name)
	g := m.SetEmptyGauge()
	for i := range points {
		p := g.DataPoints().AppendEmpty()
		p.Attributes().PutInt("series", int64(i%series))
		p.SetTimestamp(pcommon.Timestamp(i))
		p.SetDoubleValue(value)
	}
}

// A point whose derived series would exceed the per-point limit is dropped,
// the rest of the batch is kept, and the batch limit is not reached.
func TestMetricsExpansionLimits(t *testing.T) {
	tests := []struct {
		name        string
		md          pmetric.Metrics
		wantDropped int
		wantErr     bool
		wantSeries  int
	}{
		// The review's case: 10,000 bounds would copy the resource into 10,005 series.
		{"histogram over the point limit", expansionMetrics(1<<10, func(ms pmetric.MetricSlice) {
			addHistogram(ms, "huge", 10_000)
			addHistogram(ms, "small", 2)
		}), 1, false, 3 + 1},
		// The name is copied into every series, and JSON escaping can grow it sixfold.
		{"long name over the point limit", expansionMetrics(1<<10, func(ms pmetric.MetricSlice) {
			addHistogram(ms, strings.Repeat("\x01", 4<<10), 10)
		}), 1, false, 0},
		{"histogram under the point limit", expansionMetrics(1<<10, func(ms pmetric.MetricSlice) {
			addHistogram(ms, "h", 20)
		}), 0, false, 21 + 1},
		// Each point is small, but distinct series multiply the resource.
		{"distinct series over the batch limit", expansionMetrics(1<<10, func(ms pmetric.MetricSlice) {
			addGauges(ms, "g", 1_000, 1_000, 1)
		}), 0, true, 0},
		// Repeated points of a few series write one series row each.
		{"repeated series under the batch limit", expansionMetrics(1<<10, func(ms pmetric.MetricSlice) {
			addGauges(ms, "g", 10, 2_000, 1)
		}), 0, false, 10},
		// Metadata copies the resource for every metric name, also for NaN
		// points, which write no series.
		{"metric names over the batch limit", expansionMetrics(1<<10, func(ms pmetric.MetricSlice) {
			for i := range 1_000 {
				addGauges(ms, fmt.Sprint("g", i), 1, 1, math.NaN())
			}
		}), 0, true, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			core, logs := observer.New(zap.WarnLevel)
			enc := newMetricsEncoder()
			enc.logger = zap.New(core)
			enc.maxPointBytes, enc.maxBatchBytes = 256<<10, 1<<20
			err := enc.encode(tt.md)
			if (err != nil) != tt.wantErr {
				t.Fatalf("encode error = %v, want error %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got := enc.series.labels.Rows(); got != tt.wantSeries {
				t.Errorf("%d series rows, want %d", got, tt.wantSeries)
			}
			var dropped int64
			for _, e := range logs.All() {
				dropped += e.ContextMap()["dropped_points"].(int64)
			}
			if dropped != int64(tt.wantDropped) {
				t.Errorf("warned about %d dropped points, want %d", dropped, tt.wantDropped)
			}
		})
	}
}

func TestOverBudgetBatchIsPermanent(t *testing.T) {
	x := newMetricsExporter(createDefaultConfig(), zap.NewNop())
	x.encoders.New = func() any {
		enc := newMetricsEncoder()
		enc.maxBatchBytes = 1 << 10
		return enc
	}
	err := x.push(context.Background(), expansionMetrics(4<<10, func(ms pmetric.MetricSlice) { addGauges(ms, "g", 1, 1, 1) }))
	if !consumererror.IsPermanent(err) {
		t.Fatalf("push error = %v, want a permanent error", err)
	}
}

// The batch limit stops a histogram's expansion midway, not after it.
func TestBatchLimitStopsBucketExpansion(t *testing.T) {
	enc := newMetricsEncoder()
	enc.maxPointBytes, enc.maxBatchBytes = 1<<40, 512<<10
	md := expansionMetrics(1<<10, func(ms pmetric.MetricSlice) { addHistogram(ms, "h", 10_000) })
	if err := enc.encode(md); err == nil {
		t.Fatal("encode accepted a batch over the limit")
	}
	if got := enc.series.labels.Rows(); got > 1_000 {
		t.Errorf("%d series rows built before stopping, want the expansion cut short", got)
	}
}
