// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
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
			if err := enc.encode(tt.md, nil); err != nil {
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

// chunks collects the rows of every chunk an encoder emits.
type chunks struct {
	enc                      *metricsEncoder
	n, series, samples, meta int
	maxSeries                int
}

func (c *chunks) emit() error {
	c.n++
	c.add()
	return nil
}

func (c *chunks) add() {
	series := c.enc.series.labels.Rows()
	c.series += series
	c.maxSeries = max(c.maxSeries, series)
	c.samples += c.enc.samples.value.Rows()
	c.meta += c.enc.meta.attrName.Rows()
}

// A point whose derived series would exceed the per-point limit is dropped and
// the rest of the batch is kept; a batch over the chunk size is inserted in
// several chunks without losing or repeating rows (metrics §6).
func TestMetricsExpansionLimits(t *testing.T) {
	tests := []struct {
		name        string
		md          pmetric.Metrics
		wantDropped int
		wantSeries  int
		wantChunked bool
	}{
		// The review's case: 10,000 bounds would copy the resource into 10,005 series.
		{"histogram over the point limit", expansionMetrics(1<<10, func(ms pmetric.MetricSlice) {
			addHistogram(ms, "huge", 10_000)
			addHistogram(ms, "small", 2)
		}), 1, 3 + 1, false},
		// The name is copied into every series, and JSON escaping can grow it sixfold.
		{"long name over the point limit", expansionMetrics(1<<10, func(ms pmetric.MetricSlice) {
			addHistogram(ms, strings.Repeat("\x01", 4<<10), 10)
		}), 1, 0, false},
		// Every metadata row, one per attribute, copies the description.
		{"long description over the point limit", expansionMetrics(1<<10, func(ms pmetric.MetricSlice) {
			m := ms.AppendEmpty()
			m.SetName("g")
			m.SetDescription(strings.Repeat("d", 64<<10))
			attrs := m.SetEmptyGauge().DataPoints().AppendEmpty().Attributes()
			for i := range 1_000 {
				attrs.PutInt(fmt.Sprint("a", i), 1)
			}
		}), 1, 0, false},
		{"histogram under the point limit", expansionMetrics(1<<10, func(ms pmetric.MetricSlice) {
			addHistogram(ms, "h", 20)
		}), 0, 21 + 1, false},
		// Each point is small, but distinct series multiply the resource.
		{"distinct series over the chunk size", expansionMetrics(1<<10, func(ms pmetric.MetricSlice) {
			addGauges(ms, "g", 1_000, 1_000, 1)
		}), 0, 1_000, true},
		// Repeated points of a few series write one series row each.
		{"repeated series under the chunk size", expansionMetrics(1<<10, func(ms pmetric.MetricSlice) {
			addGauges(ms, "g", 10, 2_000, 1)
		}), 0, 10, false},
		// Metadata copies the resource for every metric name, also for NaN
		// points, which write no series.
		{"metric names over the chunk size", expansionMetrics(1<<10, func(ms pmetric.MetricSlice) {
			for i := range 1_000 {
				addGauges(ms, fmt.Sprint("g", i), 1, 1, math.NaN())
			}
		}), 0, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			core, logs := observer.New(zap.WarnLevel)
			enc := newMetricsEncoder()
			enc.logger = zap.New(core)
			enc.maxPointBytes, enc.maxChunkBytes = 256<<10, 1<<20
			c := &chunks{enc: enc}
			if err := enc.encode(tt.md, c.emit); err != nil {
				t.Fatal(err)
			}
			chunked := c.n > 0
			c.add()
			if chunked != tt.wantChunked {
				t.Errorf("chunked = %v, want %v", chunked, tt.wantChunked)
			}
			if c.series != tt.wantSeries {
				t.Errorf("%d series rows, want %d", c.series, tt.wantSeries)
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

// Chunking writes the same rows as one large chunk, and cuts a histogram's
// expansion midway.
func TestChunksKeepEveryRow(t *testing.T) {
	md := expansionMetrics(1<<10, func(ms pmetric.MetricSlice) {
		addHistogram(ms, "h", 10_000)
		for i := range 100 {
			addGauges(ms, fmt.Sprint("g", i), 5, 20, 1)
		}
	})
	encodeAll := func(chunkBytes int) *chunks {
		enc := newMetricsEncoder()
		enc.maxPointBytes, enc.maxChunkBytes = 1<<40, chunkBytes
		c := &chunks{enc: enc}
		if err := enc.encode(md, c.emit); err != nil {
			t.Fatal(err)
		}
		c.add()
		return c
	}
	whole, split := encodeAll(1<<40), encodeAll(512<<10)
	if whole.n != 0 || split.n < 10 {
		t.Fatalf("%d and %d chunks emitted, want none and many", whole.n, split.n)
	}
	if split.series != whole.series || split.samples != whole.samples {
		t.Errorf("chunks wrote %d series and %d samples, want %d and %d", split.series, split.samples, whole.series, whole.samples)
	}
	if split.meta < whole.meta {
		t.Errorf("chunks wrote %d metadata rows, want at least %d", split.meta, whole.meta)
	}
	if split.maxSeries > 1_000 {
		t.Errorf("a chunk holds %d series rows, want the histogram cut short", split.maxSeries)
	}
}

func TestEmitErrorStopsEncoding(t *testing.T) {
	enc := newMetricsEncoder()
	enc.maxChunkBytes = 64 << 10
	errInsert := errors.New("insert failed")
	calls := 0
	md := expansionMetrics(1<<10, func(ms pmetric.MetricSlice) { addGauges(ms, "g", 1_000, 1_000, 1) })
	err := enc.encode(md, func() error { calls++; return errInsert })
	if !errors.Is(err, errInsert) || calls != 1 {
		t.Fatalf("encode = %v after %d emits, want the insert error after one", err, calls)
	}
}

// The same attribute on the resource or on the point gives two series: env and
// the attribute maps differ (metrics §4).
func TestFingerprintKeepsOrigin(t *testing.T) {
	md := pmetric.NewMetrics()
	for _, onResource := range []bool{true, false} {
		rm := md.ResourceMetrics().AppendEmpty()
		m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName("g")
		p := m.SetEmptyGauge().DataPoints().AppendEmpty()
		if onResource {
			rm.Resource().Attributes().PutStr("deployment.environment", "prod")
		} else {
			p.Attributes().PutStr("deployment.environment", "prod")
		}
	}
	enc := newMetricsEncoder()
	if err := enc.encode(md, nil); err != nil {
		t.Fatal(err)
	}
	if got := enc.series.labels.Rows(); got != 2 {
		t.Fatalf("%d series rows, want 2", got)
	}
	if enc.series.fingerprint.Row(0) == enc.series.fingerprint.Row(1) {
		t.Error("both series have one fingerprint")
	}
}
