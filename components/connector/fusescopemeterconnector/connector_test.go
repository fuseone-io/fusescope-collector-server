// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopemeterconnector

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"github.com/fuseone-io/fusescope-collector-server/components/connector/fusescopemeterconnector/internal/metadatatest"
	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
)

var (
	t0 = time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC)
	t1 = t0.Add(time.Hour)
)

// authData is the auth data of a request, as rumreceiver sets it.
type authData map[string]any

func (a authData) GetAttribute(name string) any { return a[name] }

func (a authData) GetAttributeNames() []string {
	names := make([]string, 0, len(a))
	for k := range a {
		names = append(names, k)
	}
	return names
}

func withKey(id string) context.Context {
	return client.NewContext(context.Background(), client.Info{Auth: authData{ingestpolicy.AuthKeyID: id}})
}

func resource(attrs map[string]string) pcommon.Map {
	m := pcommon.NewMap()
	for k, v := range attrs {
		m.PutStr(k, v)
	}
	return m
}

// traces has, per resource, n spans named name.
func traces(n int, name string, resources ...map[string]string) ptrace.Traces {
	td := ptrace.NewTraces()
	for _, r := range resources {
		rs := td.ResourceSpans().AppendEmpty()
		resource(r).CopyTo(rs.Resource().Attributes())
		ss := rs.ScopeSpans().AppendEmpty()
		for range n {
			s := ss.Spans().AppendEmpty()
			s.SetName(name)
			s.SetTraceID(pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
			s.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})
			s.SetKind(ptrace.SpanKindInternal)
			s.SetStartTimestamp(1793909516542251292)
			s.SetEndTimestamp(1793909516542251292)
		}
	}
	return td
}

func logs(n int, body string, resources ...map[string]string) plog.Logs {
	ld := plog.NewLogs()
	for _, r := range resources {
		rl := ld.ResourceLogs().AppendEmpty()
		resource(r).CopyTo(rl.Resource().Attributes())
		sl := rl.ScopeLogs().AppendEmpty()
		for range n {
			l := sl.LogRecords().AppendEmpty()
			l.SetTimestamp(1793909516542251292)
			l.SetSeverityText("Info")
			l.SetSeverityNumber(9)
			l.Body().SetStr(body)
			l.Attributes().PutStr("app", "server")
		}
	}
	return ld
}

// metrics has a gauge of 2 points, a sum of 3 and a histogram of 1, an
// exponential histogram of 1 and a summary of 1: 8 points in 5 series.
func metrics(r map[string]string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	resource(r).CopyTo(rm.Resource().Attributes())
	ms := rm.ScopeMetrics().AppendEmpty().Metrics()
	g := ms.AppendEmpty().SetEmptyGauge()
	g.DataPoints().AppendEmpty().SetIntValue(1)
	g.DataPoints().AppendEmpty().SetIntValue(2)
	s := ms.AppendEmpty().SetEmptySum()
	for range 3 {
		s.DataPoints().AppendEmpty().SetDoubleValue(1)
	}
	ms.AppendEmpty().SetEmptyHistogram().DataPoints().AppendEmpty()
	ms.AppendEmpty().SetEmptyExponentialHistogram().DataPoints().AppendEmpty()
	ms.AppendEmpty().SetEmptySummary().DataPoints().AppendEmpty()
	return md
}

type fixture struct {
	m    *meter
	sink *consumertest.MetricsSink
	tel  *componenttest.Telemetry
}

func newFixture(t *testing.T, sig signal, edit func(*Config)) *fixture {
	t.Helper()
	cfg := defaultConfig()
	if edit != nil {
		edit(cfg)
	}
	require.NoError(t, cfg.Validate())
	tel := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, tel.Shutdown(context.Background())) })
	sink := &consumertest.MetricsSink{}
	m, err := newMeter(cfg, metadatatest.NewSettings(tel), sink, sig)
	require.NoError(t, err)
	m.now = func() time.Time { return t0 }
	m.windowStart = t0
	return &fixture{m: m, sink: sink, tel: tel}
}

// point is one emitted data point, flattened.
type point struct {
	metric string
	labels map[string]string
	value  int64
}

func (f *fixture) flushAt(t *testing.T, at time.Time) []point {
	t.Helper()
	f.m.now = func() time.Time { return at }
	before := len(f.sink.AllMetrics())
	require.NoError(t, f.m.flush(context.Background()))
	all := f.sink.AllMetrics()
	if len(all) == before {
		return nil
	}
	require.Len(t, all, before+1, "one flush emits one batch")
	return flatten(t, all[before])
}

func flatten(t *testing.T, md pmetric.Metrics) []point {
	t.Helper()
	var out []point
	for _, rm := range md.ResourceMetrics().All() {
		require.Equal(t, 0, rm.Resource().Attributes().Len(), "the resource is empty")
		for _, sm := range rm.ScopeMetrics().All() {
			for _, m := range sm.Metrics().All() {
				for _, p := range m.Sum().DataPoints().All() {
					labels := map[string]string{}
					for k, v := range p.Attributes().All() {
						labels[k] = v.Str()
					}
					out = append(out, point{metric: m.Name(), labels: labels, value: p.IntValue()})
				}
			}
		}
	}
	return out
}

// values maps metric name and service.name label to the value, for sets
// that have a service.
func values(points []point) map[string]int64 {
	out := map[string]int64{}
	for _, p := range points {
		out[p.metric+"|"+p.labels["service.name"]] += p.value
	}
	return out
}

func TestCountsAndSizes(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		sig     signal
		consume func(*meter) error
		want    map[string]int64
	}{
		{
			name: "spans, not traces, sized as records",
			sig:  signalTraces,
			consume: func(m *meter) error {
				return m.ConsumeTraces(ctx, traces(20, strings.Repeat("x", 10), map[string]string{"service.name": "a"}))
			},
			// 64 bytes per span of a 10-character name (meter §4).
			want: map[string]int64{"signoz.meter.span.count|a": 20, "signoz.meter.span.size|a": 20 * 64},
		},
		{
			name: "a longer span name",
			sig:  signalTraces,
			consume: func(m *meter) error {
				return m.ConsumeTraces(ctx, traces(1, strings.Repeat("x", 1010), map[string]string{"service.name": "a"}))
			},
			want: map[string]int64{"signoz.meter.span.count|a": 1, "signoz.meter.span.size|a": 1065},
		},
		{
			name: "log records",
			sig:  signalLogs,
			consume: func(m *meter) error {
				return m.ConsumeLogs(ctx, logs(10, strings.Repeat("x", 10), map[string]string{"service.name": "a"}))
			},
			want: map[string]int64{"signoz.meter.log.count|a": 10, "signoz.meter.log.size|a": 10 * 52},
		},
		{
			name: "points of every metric type, not series; size 0",
			sig:  signalMetrics,
			consume: func(m *meter) error {
				return m.ConsumeMetrics(ctx, metrics(map[string]string{"service.name": "a"}))
			},
			want: map[string]int64{"signoz.meter.metric.datapoint.count|a": 8, "signoz.meter.metric.datapoint.size|a": 0},
		},
		{
			name: "two resources of one service add up",
			sig:  signalLogs,
			consume: func(m *meter) error {
				return m.ConsumeLogs(ctx, logs(2, strings.Repeat("x", 10),
					map[string]string{"service.name": "a"}, map[string]string{"service.name": "a"}))
			},
			want: map[string]int64{"signoz.meter.log.count|a": 4, "signoz.meter.log.size|a": 4 * 52},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, tt.sig, nil)
			require.NoError(t, tt.consume(f.m))
			assert.Equal(t, tt.want, values(f.flushAt(t, t1)))
		})
	}
}

func TestLabels(t *testing.T) {
	tests := []struct {
		name     string
		ctx      context.Context
		resource map[string]string
		want     map[string]string
	}{
		{
			name:     "dimensions from the resource; other attributes are not labels",
			ctx:      context.Background(),
			resource: map[string]string{"service.name": "a", "deployment.environment": "prod", "host.name": "h", "k8s.pod.name": "p"},
			want:     map[string]string{"service.name": "a", "deployment.environment": "prod", "host.name": "h"},
		},
		{
			name:     "absent dimensions are absent, not empty",
			ctx:      context.Background(),
			resource: map[string]string{},
			want:     map[string]string{},
		},
		{
			name:     "a present empty value is kept",
			ctx:      context.Background(),
			resource: map[string]string{"service.name": ""},
			want:     map[string]string{"service.name": ""},
		},
		{
			name:     "deployment.environment.name wins",
			ctx:      context.Background(),
			resource: map[string]string{"deployment.environment.name": "rum", "deployment.environment": "legacy"},
			want:     map[string]string{"deployment.environment": "rum"},
		},
		{
			name:     "an empty deployment.environment.name still wins",
			ctx:      context.Background(),
			resource: map[string]string{"deployment.environment.name": "", "deployment.environment": "legacy"},
			want:     map[string]string{"deployment.environment": ""},
		},
		{
			name:     "unauthenticated: the payload key is copied, unverified",
			ctx:      context.Background(),
			resource: map[string]string{"signoz.workspace.key.id": "claimed"},
			want:     map[string]string{"signoz.workspace.key.id": "claimed"},
		},
		{
			name:     "authenticated: the key comes from the auth data, never the payload",
			ctx:      withKey("k-auth"),
			resource: map[string]string{"signoz.workspace.key.id": "claimed", ingestpolicy.AuthKeyID: "forged"},
			want:     map[string]string{"signoz.workspace.key.id": "k-auth"},
		},
		{
			name:     "authenticated without a key ID: no key, and the payload's is not taken",
			ctx:      client.NewContext(context.Background(), client.Info{Auth: authData{"subject": "x"}}),
			resource: map[string]string{"signoz.workspace.key.id": "claimed"},
			want:     map[string]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, signalLogs, nil)
			require.NoError(t, f.m.ConsumeLogs(tt.ctx, logs(1, "b", tt.resource)))
			points := f.flushAt(t, t1)
			require.Len(t, points, 2)
			for _, p := range points {
				id := p.labels["connector_id"]
				delete(p.labels, "connector_id")
				assert.Equal(t, f.m.id, id)
				assert.Equal(t, tt.want, p.labels, p.metric)
			}
		})
	}
}

func TestFlushShape(t *testing.T) {
	f := newFixture(t, signalTraces, nil)
	require.NoError(t, f.m.ConsumeTraces(context.Background(), traces(1, "s", map[string]string{"service.name": "a"})))
	f.m.now = func() time.Time { return t1 }
	require.NoError(t, f.m.flush(context.Background()))
	md := f.sink.AllMetrics()[0]
	require.Equal(t, 1, md.ResourceMetrics().Len())
	sm := md.ResourceMetrics().At(0).ScopeMetrics()
	require.Equal(t, 1, sm.Len())
	assert.Equal(t, "signozmeterconnector", sm.At(0).Scope().Name())
	assert.Empty(t, sm.At(0).Scope().Version())
	assert.Empty(t, sm.At(0).SchemaUrl())
	want := []struct{ name, unit, description string }{
		{"signoz.meter.span.count", "1", "The number of spans observed."},
		{"signoz.meter.span.size", "By", "The size of spans observed."},
	}
	ms := sm.At(0).Metrics()
	require.Equal(t, len(want), ms.Len())
	for i, w := range want {
		m := ms.At(i)
		assert.Equal(t, w.name, m.Name())
		assert.Equal(t, w.unit, m.Unit())
		assert.Equal(t, w.description, m.Description())
		require.Equal(t, pmetric.MetricTypeSum, m.Type())
		assert.Equal(t, pmetric.AggregationTemporalityDelta, m.Sum().AggregationTemporality())
		assert.True(t, m.Sum().IsMonotonic())
		p := m.Sum().DataPoints().At(0)
		assert.Equal(t, pcommon.NewTimestampFromTime(t0), p.StartTimestamp(), "the window starts at the previous flush")
		assert.Equal(t, pcommon.NewTimestampFromTime(t1), p.Timestamp(), "points carry the flush instant")
	}

	// The next window starts where this one ended, and holds only new records.
	require.NoError(t, f.m.ConsumeTraces(context.Background(), traces(2, "s", map[string]string{"service.name": "a"})))
	t2 := t1.Add(time.Hour)
	assert.Equal(t, map[string]int64{"signoz.meter.span.count|a": 2, "signoz.meter.span.size|a": 2 * 55}, values(f.flushAt(t, t2)))
	p := f.sink.AllMetrics()[1].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0)
	assert.Equal(t, pcommon.NewTimestampFromTime(t1), p.StartTimestamp())

	assert.Nil(t, f.flushAt(t, t2.Add(time.Hour)), "an empty window emits nothing")
}

func TestMetricNames(t *testing.T) {
	for sig, want := range map[signal][]string{
		signalTraces:  {"signoz.meter.span.count", "signoz.meter.span.size"},
		signalLogs:    {"signoz.meter.log.count", "signoz.meter.log.size"},
		signalMetrics: {"signoz.meter.metric.datapoint.count", "signoz.meter.metric.datapoint.size"},
	} {
		f := newFixture(t, sig, nil)
		var err error
		switch sig {
		case signalTraces:
			err = f.m.ConsumeTraces(context.Background(), traces(1, "s", nil))
		case signalLogs:
			err = f.m.ConsumeLogs(context.Background(), logs(1, "b", nil))
		case signalMetrics:
			err = f.m.ConsumeMetrics(context.Background(), metrics(nil))
		}
		require.NoError(t, err)
		var got []string
		for _, p := range f.flushAt(t, t1) {
			got = append(got, p.metric)
		}
		assert.Equal(t, want, got, sig.String())
	}
}

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestConnectorIDIsRandomPerInstance(t *testing.T) {
	a := newFixture(t, signalTraces, nil).m.id
	b := newFixture(t, signalTraces, nil).m.id
	assert.Regexp(t, uuidV4, a)
	assert.Regexp(t, uuidV4, b)
	assert.NotEqual(t, a, b)
}

// Only string values become labels: a typed value is ignored before it is
// converted, so a huge bytes, map or list value costs nothing (meter §3).
func TestTypedDimensionsAreIgnored(t *testing.T) {
	big := strings.Repeat("x", 1<<20)
	tests := []struct {
		name string
		set  func(pcommon.Map, string)
	}{
		{"bytes", func(m pcommon.Map, k string) { m.PutEmptyBytes(k).FromRaw([]byte(big)) }},
		{"map", func(m pcommon.Map, k string) {
			v := m.PutEmptyMap(k)
			for i := range 10_000 {
				v.PutStr(strconv.Itoa(i), big[:100])
			}
		}},
		{"list", func(m pcommon.Map, k string) {
			v := m.PutEmptySlice(k)
			for range 10_000 {
				v.AppendEmpty().SetStr(big[:100])
			}
		}},
		{"int", func(m pcommon.Map, k string) { m.PutInt(k, 7) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, signalLogs, nil)
			ld := logs(1, "b", map[string]string{})
			res := ld.ResourceLogs().At(0).Resource().Attributes()
			for _, k := range []string{"service.name", "deployment.environment.name", "deployment.environment", "host.name", "signoz.workspace.key.id"} {
				tt.set(res, k)
			}
			require.NoError(t, f.m.ConsumeLogs(context.Background(), ld)) // creates the set
			allocs := testing.AllocsPerRun(20, func() { _ = f.m.ConsumeLogs(context.Background(), ld) })
			assert.Zero(t, allocs, "allocations per consume")
			points := f.flushAt(t, t1)
			require.NotEmpty(t, points)
			for _, p := range points {
				delete(p.labels, "connector_id")
				assert.Empty(t, p.labels, "typed values are not labels")
			}
		})
	}
}

func TestTruncation(t *testing.T) {
	f := newFixture(t, signalLogs, func(c *Config) { c.MaxLabelBytes = 8 })
	ctx := context.Background()
	// "ééééX…": 2-byte characters, so 8 bytes cut at a character boundary.
	require.NoError(t, f.m.ConsumeLogs(ctx, logs(1, "b", map[string]string{"service.name": "ééééA-long"})))
	require.NoError(t, f.m.ConsumeLogs(ctx, logs(1, "b", map[string]string{"service.name": "ééééB-other"})))
	// A 3-byte character straddling the limit is dropped whole.
	require.NoError(t, f.m.ConsumeLogs(ctx, logs(1, "b", map[string]string{"service.name": "abcdefg€"})))
	// The authenticated key is not cut.
	require.NoError(t, f.m.ConsumeLogs(withKey("a-key-longer-than-8"), logs(1, "b", map[string]string{"service.name": "s"})))
	got := map[string]int64{}
	keys := map[string]bool{}
	for _, p := range f.flushAt(t, t1) {
		if p.metric == "signoz.meter.log.count" {
			got[p.labels["service.name"]] += p.value
			keys[p.labels["signoz.workspace.key.id"]] = true
		}
	}
	assert.Equal(t, map[string]int64{"éééé": 2, "abcdefg": 1, "s": 1}, got, "values equal in their first 8 bytes add up")
	assert.True(t, keys["a-key-longer-than-8"])
	metadatatest.AssertEqualFusescopeMeterTruncatedValues(t, f.tel, []metricdata.DataPoint[int64]{
		{Attributes: attribute.NewSet(attribute.String("signal", "logs")), Value: 3},
	}, metricdatatest.IgnoreTimestamp())
}

func TestOverflow(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, signalLogs, func(c *Config) {
		c.MaxSeries = 2
		c.MaxAuthenticatedKeys = 1
	})
	res := func(svc string) map[string]string {
		return map[string]string{"service.name": svc, "deployment.environment": "e", "host.name": "h", "signoz.workspace.key.id": "claimed"}
	}
	require.NoError(t, f.m.ConsumeLogs(withKey("k1"), logs(1, "b", res("a"))))
	require.NoError(t, f.m.ConsumeLogs(ctx, logs(1, "b", res("b"))))
	// Over max_series: per-key overflow for k1, then k2 exceeds max_authenticated_keys,
	// and an unauthenticated record has no key to keep.
	require.NoError(t, f.m.ConsumeLogs(withKey("k1"), logs(3, "b", res("c"))))
	require.NoError(t, f.m.ConsumeLogs(withKey("k1"), logs(1, "b", res("d"))))
	require.NoError(t, f.m.ConsumeLogs(withKey("k2"), logs(5, "b", res("e"))))
	require.NoError(t, f.m.ConsumeLogs(ctx, logs(7, "b", res("f"))))

	type set struct{ service, key string }
	got := map[set]int64{}
	for _, p := range f.flushAt(t, t1) {
		if p.metric != "signoz.meter.log.count" {
			continue
		}
		if p.labels["service.name"] == "" {
			for _, k := range []string{"service.name", "deployment.environment", "host.name"} {
				assert.NotContains(t, p.labels, k, "an overflow set drops %s", k)
			}
		}
		got[set{p.labels["service.name"], p.labels["signoz.workspace.key.id"]}] += p.value
	}
	assert.Equal(t, map[set]int64{
		{"a", "k1"}:      1,
		{"b", "claimed"}: 1,
		{"", "k1"}:       4,     // c and d keep their key
		{"", ""}:         5 + 7, // k2 over max_authenticated_keys, and the unverified f
	}, got)
	metadatatest.AssertEqualFusescopeMeterOverflowRecords(t, f.tel, []metricdata.DataPoint[int64]{
		{Attributes: attribute.NewSet(attribute.String("signal", "logs"), attribute.String("reason", "series")), Value: 4},
		{Attributes: attribute.NewSet(attribute.String("signal", "logs"), attribute.String("reason", "keys")), Value: 5},
		{Attributes: attribute.NewSet(attribute.String("signal", "logs"), attribute.String("reason", "no_key")), Value: 7},
	}, metricdatatest.IgnoreTimestamp())

	// After the flush the limits start over.
	require.NoError(t, f.m.ConsumeLogs(ctx, logs(1, "b", res("g"))))
	assert.Equal(t, int64(1), values(f.flushAt(t, t1.Add(time.Hour)))["signoz.meter.log.count|g"])
}

// An overflow set whose labels equal a normal set's is that set, so a flush
// has one point per (metric, labels).
func TestOverflowReusesAnEqualSet(t *testing.T) {
	f := newFixture(t, signalLogs, func(c *Config) { c.MaxSeries = 2 })
	require.NoError(t, f.m.ConsumeLogs(withKey("k1"), logs(1, "b", map[string]string{})))        // labels: the key only
	require.NoError(t, f.m.ConsumeLogs(context.Background(), logs(2, "b", map[string]string{}))) // no labels
	require.NoError(t, f.m.ConsumeLogs(withKey("k1"), logs(3, "b", map[string]string{"service.name": "a"})))
	require.NoError(t, f.m.ConsumeLogs(context.Background(), logs(4, "b", map[string]string{"service.name": "b"})))
	seen := map[string]int64{}
	for _, p := range f.flushAt(t, t1) {
		if p.metric != "signoz.meter.log.count" {
			continue
		}
		id := p.labels["signoz.workspace.key.id"] + "|" + p.labels["service.name"]
		_, dup := seen[id]
		assert.False(t, dup, "two points with labels %v", p.labels)
		seen[id] = p.value
	}
	assert.Equal(t, map[string]int64{"k1|": 1 + 3, "|": 2 + 4}, seen)
}

func TestOverflowByBytes(t *testing.T) {
	f := newFixture(t, signalLogs, func(c *Config) { c.MaxSeriesBytes = setOverhead + 10 })
	require.NoError(t, f.m.ConsumeLogs(withKey("k"), logs(1, "b", map[string]string{"service.name": "short"})))
	require.NoError(t, f.m.ConsumeLogs(withKey("k"), logs(1, "b", map[string]string{"service.name": "much-longer"})))
	got := values(f.flushAt(t, t1))
	assert.Equal(t, int64(1), got["signoz.meter.log.count|short"])
	assert.Equal(t, int64(1), got["signoz.meter.log.count|"])
	metadatatest.AssertEqualFusescopeMeterOverflowRecords(t, f.tel, []metricdata.DataPoint[int64]{
		{Attributes: attribute.NewSet(attribute.String("signal", "logs"), attribute.String("reason", "bytes")), Value: 1},
	}, metricdatatest.IgnoreTimestamp())
}

// The meter counts what reaches it after the processors, whatever a sibling
// exporter does with the same batch: a client that resends a batch the
// ClickHouse exporter refused is counted twice (meter §2).
func TestCountsBatchesASiblingRefused(t *testing.T) {
	f := newFixture(t, signalTraces, nil)
	refusing := consumertest.NewErr(errors.New("clickhouse down"))
	send := func(td ptrace.Traces) error {
		// What the collector's fanout does: every consumer gets the batch and
		// the errors are joined for the receiver.
		return errors.Join(refusing.ConsumeTraces(context.Background(), td), f.m.ConsumeTraces(context.Background(), td))
	}
	td := traces(3, "s", map[string]string{"service.name": "a"})
	require.Error(t, send(td))
	require.Error(t, send(td), "the client resends")
	assert.Equal(t, int64(6), values(f.flushAt(t, t1))["signoz.meter.span.count|a"])
}

// blockingSink holds every ConsumeMetrics until release is closed, ignoring
// the context, like a stuck exporter would.
type blockingSink struct {
	consumertest.MetricsSink
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingSink() *blockingSink {
	return &blockingSink{entered: make(chan struct{}), release: make(chan struct{})}
}

func (b *blockingSink) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return b.MetricsSink.ConsumeMetrics(ctx, md)
}

func TestStuckSinkDoesNotBlockConsume(t *testing.T) {
	f := newFixture(t, signalTraces, nil)
	sink := newBlockingSink()
	f.m.next = sink
	require.NoError(t, f.m.ConsumeTraces(context.Background(), traces(1, "s", map[string]string{"service.name": "a"})))
	flushed := make(chan error, 1)
	go func() { flushed <- f.m.flush(context.Background()) }()
	<-sink.entered

	consumed := make(chan error, 1)
	go func() {
		consumed <- f.m.ConsumeTraces(context.Background(), traces(2, "s", map[string]string{"service.name": "a"}))
	}()
	select {
	case err := <-consumed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("ConsumeTraces waited for a flush stuck in the metrics pipeline")
	}

	close(sink.release)
	require.NoError(t, <-flushed)
	assert.Equal(t, int64(1), values(flatten(t, sink.AllMetrics()[0]))["signoz.meter.span.count|a"])
	f.m.next = f.sink
	assert.Equal(t, int64(2), values(f.flushAt(t, t1))["signoz.meter.span.count|a"], "records consumed during the flush go to the next window")
}

func TestRefusedFlushIsDroppedAndCounted(t *testing.T) {
	f := newFixture(t, signalLogs, nil)
	f.m.next = consumertest.NewErr(errors.New("queue full"))
	require.NoError(t, f.m.ConsumeLogs(context.Background(), logs(1, "b", map[string]string{"service.name": "a"})))
	require.Error(t, f.m.flush(context.Background()))
	metadatatest.AssertEqualFusescopeMeterFlushErrors(t, f.tel, []metricdata.DataPoint[int64]{
		{Attributes: attribute.NewSet(attribute.String("signal", "logs")), Value: 1},
	}, metricdatatest.IgnoreTimestamp())

	f.m.next = f.sink
	assert.Nil(t, f.flushAt(t, t1), "a refused flush is not resent, which could double it")
}

func TestStartFlushesPeriodicallyAndShutdownFlushesTheRest(t *testing.T) {
	f := newFixture(t, signalLogs, func(c *Config) { c.FlushInterval = 20 * time.Millisecond })
	f.m.now = time.Now
	require.NoError(t, f.m.Start(context.Background(), componenttest.NewNopHost()))
	require.NoError(t, f.m.ConsumeLogs(context.Background(), logs(1, "b", map[string]string{"service.name": "a"})))
	require.Eventually(t, func() bool { return len(f.sink.AllMetrics()) == 1 }, 5*time.Second, 5*time.Millisecond)

	require.NoError(t, f.m.ConsumeLogs(context.Background(), logs(2, "b", map[string]string{"service.name": "a"})))
	f.m.cfg.FlushInterval = time.Hour // not used again; the ticker is running
	require.NoError(t, f.m.Shutdown(context.Background()))
	var total int64
	for _, md := range f.sink.AllMetrics() {
		total += values(flatten(t, md))["signoz.meter.log.count|a"]
	}
	assert.Equal(t, int64(3), total, "a graceful shutdown flushes what is counted")
}

func TestShutdownGivesUpOnAStuckSink(t *testing.T) {
	for _, tt := range []struct {
		name     string
		periodic bool // stuck in a periodic flush, or in the final one
	}{
		{"final flush", false},
		{"periodic flush", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, signalLogs, func(c *Config) {
				if tt.periodic {
					c.FlushInterval = 10 * time.Millisecond
				}
			})
			sink := newBlockingSink()
			f.m.next = sink
			require.NoError(t, f.m.Start(context.Background(), componenttest.NewNopHost()))
			require.NoError(t, f.m.ConsumeLogs(context.Background(), logs(1, "b", map[string]string{"service.name": "a"})))
			if tt.periodic {
				<-sink.entered
			}
			shutdown := func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- f.m.Shutdown(ctx) }()
				select {
				case err := <-done:
					return err
				case <-time.After(5 * time.Second):
					t.Fatal("Shutdown did not return at its deadline")
					return nil
				}
			}
			require.ErrorIs(t, shutdown(), context.DeadlineExceeded)
			require.ErrorIs(t, shutdown(), context.DeadlineExceeded, "a second Shutdown neither panics nor hangs")
			close(sink.release) // only once Shutdown has returned
		})
	}
}

func TestShutdownWithoutStart(t *testing.T) {
	f := newFixture(t, signalLogs, nil)
	require.NoError(t, f.m.Shutdown(context.Background()))
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
		err  string
	}{
		{"default", func(*Config) {}, ""},
		{"flush interval", func(c *Config) { c.FlushInterval = 0 }, "flush_interval"},
		{"max series", func(c *Config) { c.MaxSeries = 0 }, "max_series must"},
		{"max series bytes", func(c *Config) { c.MaxSeriesBytes = 0 }, "max_series_bytes"},
		{"max authenticated keys", func(c *Config) { c.MaxAuthenticatedKeys = -1 }, "max_authenticated_keys"},
		{"max label bytes", func(c *Config) { c.MaxLabelBytes = 3 }, "max_label_bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			tt.edit(cfg)
			err := cfg.Validate()
			if tt.err == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.err)
		})
	}
}
