// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopespanmetricsconnector

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/fuseone-io/fusescope-collector-server/components/connector/fusescopespanmetricsconnector/internal/metadata"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"default", func(*Config) {}, false},
		{"reserved dimension", func(c *Config) { c.Dimensions = append(c.Dimensions, Dimension{Name: "operation"}) }, true},
		{"duplicate dimension", func(c *Config) { c.Dimensions = append(c.Dimensions, Dimension{Name: "host.name"}) }, true},
		{"empty dimension", func(c *Config) { c.Dimensions = append(c.Dimensions, Dimension{}) }, true},
		{"generated resource_ prefix", func(c *Config) { c.Dimensions = append(c.Dimensions, Dimension{Name: "resource_host.name"}) }, true},
		{"unordered buckets", func(c *Config) { c.LatencyHistogramBuckets = []time.Duration{time.Second, time.Millisecond} }, true},
		{"no buckets", func(c *Config) { c.LatencyHistogramBuckets = nil }, true},
		{"no flush interval", func(c *Config) { c.MetricsFlushInterval = 0 }, true},
		{"no series", func(c *Config) { c.MaxSeries = 0 }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			tt.mutate(cfg)
			if err := cfg.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() = %v, want error %v", err, tt.wantErr)
			}
		})
	}
}

func TestExternalAddress(t *testing.T) {
	tests := []struct {
		name  string
		attrs map[string]any
		want  string
	}{
		{"http.host first", map[string]any{"http.host": "h", "server.address": "s", "url.full": "https://u/x"}, "h"},
		{"server.address", map[string]any{"server.address": "s", "url.full": "https://u/x"}, "s"},
		{"net.peer.name", map[string]any{"net.peer.name": "p", "http.url": "http://u/x"}, "p"},
		{"port leaves an empty port", map[string]any{"server.address": "s", "server.port": 8080}, "s:"},
		{"peer port too", map[string]any{"net.peer.name": "p", "net.peer.port": 9090}, "p:"},
		{"url.full host and port", map[string]any{"url.full": "https://u:8443/x"}, "u:8443"},
		{"http.url last", map[string]any{"http.url": "http://old/x"}, "old"},
		{"none", map[string]any{"rpc.service": "Stock"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := pcommon.NewMap()
			if err := m.FromRaw(tt.attrs); err != nil {
				t.Fatal(err)
			}
			if got := externalAddress(m); got != tt.want {
				t.Errorf("externalAddress = %q, want %q", got, tt.want)
			}
		})
	}
}

// newTestConnector starts a connector whose flushes go to a sink.
func newTestConnector(t *testing.T, cfg *Config) (*spanMetricsConnector, *consumertest.MetricsSink, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zap.WarnLevel)
	set := connectortest.NewNopSettings(metadata.Type)
	set.Logger = zap.New(core)
	sink := new(consumertest.MetricsSink)
	c := newConnector(cfg, set, sink)
	if err := c.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Shutdown(context.Background()) })
	return c, sink, logs
}

func traceWith(spans int, service string, attrs func(i int, s ptrace.Span)) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", service)
	rs.Resource().Attributes().PutStr("host.name", "res-host")
	ss := rs.ScopeSpans().AppendEmpty().Spans()
	start := pcommon.Timestamp(time.Date(2026, 11, 1, 12, 0, 30, 0, time.UTC).UnixNano())
	for i := range spans {
		s := ss.AppendEmpty()
		s.SetName("op")
		s.SetStartTimestamp(start)
		s.SetEndTimestamp(start + pcommon.Timestamp(time.Millisecond))
		if attrs != nil {
			attrs(i, s)
		}
	}
	return td
}

func points(md pmetric.Metrics, name string) []pcommon.Map {
	var out []pcommon.Map
	for _, rm := range md.ResourceMetrics().All() {
		for _, sm := range rm.ScopeMetrics().All() {
			for _, m := range sm.Metrics().All() {
				if m.Name() != name {
					continue
				}
				switch m.Type() {
				case pmetric.MetricTypeSum:
					for _, p := range m.Sum().DataPoints().All() {
						out = append(out, p.Attributes())
					}
				case pmetric.MetricTypeHistogram:
					for _, p := range m.Histogram().DataPoints().All() {
						out = append(out, p.Attributes())
					}
				}
			}
		}
	}
	return out
}

// The collector's identity is a resource-level dimension, from the collector's
// own service.instance.id.
func TestCollectorIdentity(t *testing.T) {
	set := connectortest.NewNopSettings(metadata.Type)
	set.Resource.Attributes().PutStr("service.instance.id", "instance-1")
	sink := new(consumertest.MetricsSink)
	c := newConnector(defaultConfig(), set, sink)
	if err := c.ConsumeTraces(context.Background(), traceWith(1, "svc", nil)); err != nil {
		t.Fatal(err)
	}
	_ = c.Shutdown(context.Background())
	p := points(sink.AllMetrics()[0], "signoz_calls_total")[0]
	for _, k := range []string{collectorIDKey, "resource_" + collectorIDKey} {
		if v, _ := p.Get(k); v.AsString() != "instance-1" {
			t.Errorf("%s = %q, want the collector's service.instance.id", k, v.AsString())
		}
	}
}

// Series beyond max_series are dropped with a warning, also across families.
func TestMaxSeries(t *testing.T) {
	cfg := defaultConfig()
	cfg.MaxSeries = 3
	c, sink, logs := newTestConnector(t, cfg)
	td := traceWith(10, "svc", func(i int, s ptrace.Span) { s.SetName(fmt.Sprint("op", i)) })
	if err := c.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	c.flush(context.Background())
	if got := sink.DataPointCount(); got != 3 {
		t.Errorf("%d points, want 3", got)
	}
	if logs.FilterMessageSnippet("max_series").Len() != 1 {
		t.Error("no warning about dropped series")
	}
}

// The ticker flushes deltas: a second flush holds only spans seen after the first.
func TestFlushEmitsDeltas(t *testing.T) {
	cfg := defaultConfig()
	cfg.MetricsFlushInterval = 20 * time.Millisecond
	c, sink, _ := newTestConnector(t, cfg)
	for range 2 {
		if err := c.ConsumeTraces(context.Background(), traceWith(2, "svc", nil)); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(2 * time.Second)
		before := len(sink.AllMetrics())
		for len(sink.AllMetrics()) == before {
			if time.Now().After(deadline) {
				t.Fatal("no flush")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	for i, md := range sink.AllMetrics() {
		m := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
		if m.Name() != "signoz_calls_total" || m.Sum().DataPoints().At(0).DoubleValue() != 2 {
			t.Errorf("flush %d: %s = %v, want signoz_calls_total = 2", i, m.Name(), m.Sum().DataPoints().At(0).DoubleValue())
		}
	}
}

// A resource without service.name gives no metrics (apm-metrics §4).
func TestNoServiceNoMetrics(t *testing.T) {
	c, sink, _ := newTestConnector(t, defaultConfig())
	td := traceWith(1, "svc", nil)
	td.ResourceSpans().At(0).Resource().Attributes().Remove("service.name")
	if err := c.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	c.flush(context.Background())
	if sink.DataPointCount() != 0 {
		t.Errorf("%d points for a resource without service.name", sink.DataPointCount())
	}
}

// 100 resources of 100 spans over 20 operations, as an HTTP service.
func BenchmarkConsumeTraces(b *testing.B) {
	td := ptrace.NewTraces()
	start := pcommon.Timestamp(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).UnixNano())
	for r := range 100 {
		rs := td.ResourceSpans().AppendEmpty()
		ra := rs.Resource().Attributes()
		ra.PutStr("service.name", fmt.Sprintf("svc-%d", r%10))
		ra.PutStr("host.name", fmt.Sprintf("node-%d", r))
		ra.PutStr("k8s.namespace.name", "shop")
		spans := rs.ScopeSpans().AppendEmpty().Spans()
		for i := range 100 {
			s := spans.AppendEmpty()
			s.SetName(fmt.Sprintf("GET /items/%d", i%20))
			s.SetKind(ptrace.SpanKindClient)
			s.SetStartTimestamp(start)
			s.SetEndTimestamp(start + pcommon.Timestamp(i)*pcommon.Timestamp(time.Millisecond))
			s.Attributes().PutInt("http.response.status_code", 200)
			s.Attributes().PutStr("server.address", "api.example.com")
		}
	}
	c := newConnector(defaultConfig(), connectortest.NewNopSettings(metadata.Type), consumertest.NewNop())
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = c.ConsumeTraces(context.Background(), td)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*10_000), "ns/span")
}

// Values taken from attributes keep their type; the metadata records it
// (observed: http.status_code is Int on the reference's points).
func TestAttributeValuesKeepTheirType(t *testing.T) {
	c, sink, _ := newTestConnector(t, defaultConfig())
	td := traceWith(1, "svc", func(_ int, s ptrace.Span) {
		s.SetKind(ptrace.SpanKindClient)
		s.Attributes().PutInt("http.response.status_code", 201)
		s.Attributes().PutStr("server.address", "api.example.com")
		s.Attributes().PutInt("k8s.node.name", 7)
	})
	if err := c.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	c.flush(context.Background())
	for _, name := range []string{"signoz_calls_total", "signoz_external_call_latency_count"} {
		p := points(sink.AllMetrics()[0], name)[0]
		for key, want := range map[string]pcommon.ValueType{"http.status_code": pcommon.ValueTypeInt, "k8s.node.name": pcommon.ValueTypeInt, "service.name": pcommon.ValueTypeStr} {
			if v, _ := p.Get(key); v.Type() != want {
				t.Errorf("%s %s is %s, want %s", name, key, v.Type(), want)
			}
		}
	}
}

// A large dimension value over many series is bounded in bytes, not only in
// series (apm-metrics §6).
func TestMaxSeriesBytes(t *testing.T) {
	cfg := defaultConfig()
	cfg.MaxSeriesBytes = 256 << 10
	c, _, logs := newTestConnector(t, cfg)
	td := traceWith(200, "svc", func(i int, s ptrace.Span) { s.SetName(fmt.Sprint("op", i)) })
	td.ResourceSpans().At(0).Resource().Attributes().PutStr("host.name", strings.Repeat("h", 8<<10))
	if err := c.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	held := c.bytes
	c.mu.Unlock()
	if held > cfg.MaxSeriesBytes {
		t.Errorf("%d bytes of series held, want at most %d", held, cfg.MaxSeriesBytes)
	}
	c.flush(context.Background())
	if logs.FilterMessageSnippet("max_series").Len() != 1 {
		t.Error("no warning about dropped series")
	}
}

// Attribute values containing the key's separators still identify their own series.
func TestSeriesKeysAreUnambiguous(t *testing.T) {
	c, sink, _ := newTestConnector(t, defaultConfig())
	td := traceWith(2, "svc", func(i int, s ptrace.Span) {
		if i == 0 {
			s.Attributes().PutStr("service.namespace", "a\xffdeployment.environment\xfeb")
			s.Attributes().PutStr("deployment.environment", "c")
		} else {
			s.Attributes().PutStr("service.namespace", "a")
			s.Attributes().PutStr("deployment.environment", "b\xffdeployment.environment\xfec")
		}
	})
	if err := c.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	c.flush(context.Background())
	if n := len(points(sink.AllMetrics()[0], "signoz_calls_total")); n != 2 {
		t.Errorf("%d series, want 2", n)
	}
}

// blockingSink holds ConsumeMetrics until its context ends.
type blockingSink struct{ consumertest.MetricsSink }

func (b *blockingSink) ConsumeMetrics(ctx context.Context, _ pmetric.Metrics) error {
	<-ctx.Done()
	return ctx.Err()
}

// Shutdown cancels a periodic flush stuck downstream and honours its deadline.
func TestShutdownCancelsFlush(t *testing.T) {
	cfg := defaultConfig()
	cfg.MetricsFlushInterval = 10 * time.Millisecond
	c := newConnector(cfg, connectortest.NewNopSettings(metadata.Type), &blockingSink{})
	if err := c.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	if err := c.ConsumeTraces(context.Background(), traceWith(1, "svc", nil)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // a flush is now blocked downstream
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_ = c.Shutdown(ctx)
	if d := time.Since(start); d > time.Second {
		t.Errorf("shutdown took %v", d)
	}
}

// flakySink refuses its first batches with a retryable or permanent error.
type flakySink struct {
	consumertest.MetricsSink
	refuse []error
}

func (f *flakySink) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if len(f.refuse) > 0 {
		err := f.refuse[0]
		f.refuse = f.refuse[1:]
		return err
	}
	return f.MetricsSink.ConsumeMetrics(ctx, md)
}

// A refused flush is dropped, retryable or not: a resend could write again
// what a partly failed export already stored (apm-metrics §6).
func TestRefusedFlushes(t *testing.T) {
	tests := []struct {
		name       string
		refuse     error
		wantPoints int
	}{
		{"retryable", errors.New("queue is full"), 1},
		{"permanent", consumererror.NewPermanent(errors.New("bad data")), 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := &flakySink{refuse: []error{tt.refuse}}
			c := newConnector(defaultConfig(), connectortest.NewNopSettings(metadata.Type), sink)
			for range 2 {
				if err := c.ConsumeTraces(context.Background(), traceWith(1, "svc", nil)); err != nil {
					t.Fatal(err)
				}
				c.flush(context.Background())
			}
			got := 0
			for _, md := range sink.AllMetrics() {
				got += len(points(md, "signoz_calls_total"))
			}
			if got != tt.wantPoints {
				t.Errorf("%d calls points delivered, want %d", got, tt.wantPoints)
			}
		})
	}
}

// Shutdown reports a final flush that could not be delivered.
func TestShutdownReportsFinalFailure(t *testing.T) {
	sink := &flakySink{refuse: []error{errors.New("down")}}
	c := newConnector(defaultConfig(), connectortest.NewNopSettings(metadata.Type), sink)
	if err := c.ConsumeTraces(context.Background(), traceWith(1, "svc", nil)); err != nil {
		t.Fatal(err)
	}
	if err := c.Shutdown(context.Background()); err == nil {
		t.Error("Shutdown hid the failed final flush")
	}
}

// A database series emits two points (count and sum) and counts as two
// against max_series, which thereby bounds the points of a flush.
func TestTwoPointSeriesCountTwice(t *testing.T) {
	cfg := defaultConfig()
	cfg.MaxSeries = 4
	sink := new(consumertest.MetricsSink)
	c := newConnector(cfg, connectortest.NewNopSettings(metadata.Type), sink)
	td := traceWith(1, "svc", func(_ int, s ptrace.Span) { s.Attributes().PutStr("db.system", "redis") })
	if err := c.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	if c.total != 4 {
		t.Fatalf("one database span counts %d series, want 4 (calls, latency, db count and sum)", c.total)
	}
	_ = c.flush(context.Background())
	if got := sink.DataPointCount(); got != 4 {
		t.Errorf("%d points emitted, want 4", got)
	}
}

// Histogram counters and bounds count against max_series_bytes.
func TestHistogramStorageCounts(t *testing.T) {
	cfg := defaultConfig()
	cfg.Dimensions = nil
	cfg.LatencyHistogramBuckets = nil
	for i := range 10_000 {
		cfg.LatencyHistogramBuckets = append(cfg.LatencyHistogramBuckets, time.Duration(i+1)*time.Microsecond)
	}
	cfg.MaxSeriesBytes = 1 << 20
	c := newConnector(cfg, connectortest.NewNopSettings(metadata.Type), consumertest.NewNop())
	td := traceWith(100, "svc", func(i int, s ptrace.Span) { s.SetName(fmt.Sprint("op", i)) })
	if err := c.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	if n, buckets := len(c.latency.order), len(c.bounds)+1; n*buckets*8 > cfg.MaxSeriesBytes {
		t.Errorf("%d latency series hold %d bytes of counters, over max_series_bytes", n, n*buckets*8)
	}
}

// slowSink delivers after a delay, unless its context ends first.
type slowSink struct {
	consumertest.MetricsSink
	delay  time.Duration
	refuse error
}

func (s *slowSink) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	if s.refuse != nil {
		return s.refuse
	}
	return s.MetricsSink.ConsumeMetrics(ctx, md)
}

// Shutdown lets a flush in progress finish within its deadline, and reports
// that flush's failure.
func TestShutdownDrainsTheActiveFlush(t *testing.T) {
	tests := []struct {
		name    string
		refuse  error
		wantErr bool
	}{
		{"delivered", nil, false},
		{"refused", errors.New("down"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			cfg.MetricsFlushInterval = 10 * time.Millisecond
			sink := &slowSink{delay: 200 * time.Millisecond, refuse: tt.refuse}
			c := newConnector(cfg, connectortest.NewNopSettings(metadata.Type), sink)
			if err := c.Start(context.Background(), componenttest.NewNopHost()); err != nil {
				t.Fatal(err)
			}
			if err := c.ConsumeTraces(context.Background(), traceWith(1, "svc", nil)); err != nil {
				t.Fatal(err)
			}
			time.Sleep(50 * time.Millisecond) // the flush is now in progress
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := c.Shutdown(ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Shutdown = %v, want error %v", err, tt.wantErr)
			}
			if !tt.wantErr && sink.DataPointCount() == 0 {
				t.Error("the flush in progress was cut short")
			}
		})
	}
}

// A dimension holding an array is charged for its elements, not its text.
func TestTypedValuesCountInBytes(t *testing.T) {
	cfg := defaultConfig()
	cfg.MaxSeriesBytes = 1 << 20
	c := newConnector(cfg, connectortest.NewNopSettings(metadata.Type), consumertest.NewNop())
	td := traceWith(500, "svc", func(i int, s ptrace.Span) { s.SetName(fmt.Sprint("op", i)) })
	arr := td.ResourceSpans().At(0).Resource().Attributes().PutEmptySlice("host.name")
	for range 2_000 {
		arr.AppendEmpty().SetInt(1)
	}
	if err := c.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	// Each series holds the array twice (host.name and resource_host.name).
	if held := c.total * 2 * 2_000 * 16; held > 2*cfg.MaxSeriesBytes {
		t.Errorf("%d series hold about %d bytes of array elements, far over %d", c.total, held, cfg.MaxSeriesBytes)
	}
}

// Timestamps are unsigned: the upper range keeps its minute and duration.
func TestUnsignedTimestamps(t *testing.T) {
	c, sink, _ := newTestConnector(t, defaultConfig())
	td := traceWith(1, "svc", func(_ int, s ptrace.Span) {
		start := pcommon.Timestamp(math.MaxInt64 - uint64(time.Millisecond))
		s.SetStartTimestamp(start)
		s.SetEndTimestamp(start + pcommon.Timestamp(2*time.Millisecond)) // crosses MaxInt64
	})
	if err := c.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	_ = c.flush(context.Background())
	m := sink.AllMetrics()[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
	for _, metric := range m.All() {
		if metric.Name() != "signoz_latency" {
			continue
		}
		p := metric.Histogram().DataPoints().At(0)
		wantMinute := uint64(math.MaxInt64-uint64(time.Millisecond)) / uint64(time.Minute) * uint64(time.Minute)
		if uint64(p.Timestamp()) != wantMinute || p.Sum() != 2 {
			t.Errorf("timestamp %d, sum %v; want %d and 2 ms", p.Timestamp(), p.Sum(), wantMinute)
		}
	}
}
