// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopespanmetricsconnector // import "github.com/fuseone-io/fusescope-collector-server/components/connector/fusescopespanmetricsconnector"

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

const (
	// scopeName is the instrumentation scope of every metric (apm-metrics §2).
	scopeName = "signozspanmetricsprocessor"
	// collectorIDKey is the dimension that holds this collector's identity (apm-metrics §3.3).
	collectorIDKey = "signoz.collector.id"
	nanosPerMinute = int64(time.Minute)
	// seriesOverhead covers a series' fixed fields and map entries.
	seriesOverhead = 256
)

// kv is one attribute of a point: value is its string form, which identifies
// the series; src, when set, is the attribute it came from, whose type the
// point keeps (apm-metrics §3).
type kv struct {
	key, value string
	src        pcommon.Value
	typed      bool
}

func attr(key string, v pcommon.Value) kv {
	return kv{key: key, value: v.AsString(), src: v, typed: true}
}

// family is one metric family's series, keyed by attributes and minute.
type family struct {
	series map[string]*series
	order  []string
}

type series struct {
	attrs   pcommon.Map
	minute  int64
	count   uint64
	sum     float64
	buckets []uint64
}

type spanMetricsConnector struct {
	cfg         *Config
	logger      *zap.Logger
	next        consumer.Metrics
	bounds      []float64
	collectorID string
	// resourceKeys[i] is "resource_" + the name of dimension i.
	resourceKeys []string

	mu       sync.Mutex
	calls    family
	latency  family
	db       family
	external family
	total    int
	// bytes estimates what the series hold: their key and their attributes.
	bytes   int
	dropped int
	// flushMu serializes flushes; pending, under it, is a flush refused with a
	// retryable error, sent again first.
	flushMu sync.Mutex
	pending pmetric.Metrics

	// ctx ends periodic flushes on shutdown.
	ctx    context.Context
	cancel context.CancelFunc
	done   sync.WaitGroup

	// scratch, used under mu
	attrs []kv
	extra []kv
	key   []byte
}

func newConnector(cfg *Config, set connector.Settings, next consumer.Metrics) *spanMetricsConnector {
	c := &spanMetricsConnector{cfg: cfg, logger: set.Logger, next: next, pending: pmetric.NewMetrics()}
	for _, b := range cfg.LatencyHistogramBuckets {
		c.bounds = append(c.bounds, float64(b)/float64(time.Millisecond))
	}
	c.collectorID = collectorID(set.Resource)
	for _, d := range cfg.Dimensions {
		c.resourceKeys = append(c.resourceKeys, "resource_"+d.Name)
	}
	c.reset()
	return c
}

// collectorID is the collector's service.instance.id, or a random UUID.
func collectorID(res pcommon.Resource) string {
	if v, ok := res.Attributes().Get("service.instance.id"); ok && v.AsString() != "" {
		return v.AsString()
	}
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6], b[8] = b[6]&0x0f|0x40, b[8]&0x3f|0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (c *spanMetricsConnector) reset() {
	for _, f := range []*family{&c.calls, &c.latency, &c.db, &c.external} {
		f.series, f.order = map[string]*series{}, f.order[:0]
	}
	c.total, c.bytes, c.dropped = 0, 0, 0
}

func (c *spanMetricsConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *spanMetricsConnector) Start(context.Context, component.Host) error {
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.done.Add(1)
	go func() {
		defer c.done.Done()
		t := time.NewTicker(c.cfg.MetricsFlushInterval)
		defer t.Stop()
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-t.C:
				_ = c.flush(c.ctx)
			}
		}
	}()
	return nil
}

// Shutdown ends a periodic flush in progress, then emits what is still
// aggregated within ctx, and reports if that last flush failed.
func (c *spanMetricsConnector) Shutdown(ctx context.Context) error {
	if c.cancel != nil {
		c.cancel()
		c.done.Wait()
		c.cancel = nil
	}
	return c.flush(ctx)
}

func (c *spanMetricsConnector) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rs := range td.ResourceSpans().All() {
		res := rs.Resource().Attributes()
		service, ok := res.Get("service.name")
		if !ok {
			continue // no service, no metrics (apm-metrics §4)
		}
		for _, ss := range rs.ScopeSpans().All() {
			for _, span := range ss.Spans().All() {
				c.addSpan(service.AsString(), res, span)
			}
		}
	}
	return nil
}

func (c *spanMetricsConnector) addSpan(service string, res pcommon.Map, span ptrace.Span) {
	attrs := span.Attributes()
	start, end := int64(span.StartTimestamp()), int64(span.EndTimestamp())
	ms := 0.0
	if end > start {
		ms = float64(end-start) / float64(time.Millisecond)
	}
	minute := start / nanosPerMinute * nanosPerMinute
	status := statusName(span.Status().Code())
	var httpStatus kv
	v, hasHTTPStatus := firstValue(attrs, "http.status_code", "http.response.status_code")
	if hasHTTPStatus {
		httpStatus = attr("http.status_code", v)
	}

	base := c.dimensions(res, attrs)
	base = append(base, kv{key: "service.name", value: service}, kv{key: "status.code", value: status})
	c.attrs = base
	n := len(base)

	// Calls and latency (apm-metrics §3.1); http.status_code is on calls only.
	op := append(base, kv{key: "operation", value: span.Name()}, kv{key: "span.kind", value: kindName(span.Kind())})
	if s := c.add(&c.latency, op, minute); s != nil {
		s.count++
		s.sum += ms
		if s.buckets == nil {
			s.buckets = make([]uint64, len(c.bounds)+1)
		}
		i, _ := slices.BinarySearch(c.bounds, ms)
		s.buckets[i]++
	}
	if hasHTTPStatus {
		op = append(op, httpStatus)
	}
	if s := c.add(&c.calls, op, minute); s != nil {
		s.count++
	}

	// Database calls (apm-metrics §3.2, §4).
	if system, ok := attrs.Get("db.system"); ok {
		db := append(append(c.extra[:0], base[:n]...), attr("db.system", system))
		if name, ok := attrs.Get("db.name"); ok {
			db = append(db, attr("db.name", name))
		}
		c.extra = db
		if s := c.add(&c.db, db, minute); s != nil {
			s.count++
			s.sum += ms
		}
	}

	// External calls (apm-metrics §3.2, §4).
	if span.Kind() == ptrace.SpanKindClient {
		if address := externalAddress(attrs); address != "" {
			ext := append(append(c.extra[:0], base[:n]...), kv{key: "address", value: address})
			if hasHTTPStatus {
				ext = append(ext, httpStatus)
			}
			c.extra = ext
			if s := c.add(&c.external, ext, minute); s != nil {
				s.count++
				s.sum += ms
			}
		}
	}
}

// dimensions resolves the configured dimensions of a span (apm-metrics §3.3).
// The collector's identity counts as a resource attribute.
func (c *spanMetricsConnector) dimensions(res, span pcommon.Map) []kv {
	out := c.attrs[:0]
	for i, d := range c.cfg.Dimensions {
		resValue, onResource := res.Get(d.Name)
		var a kv
		switch v, ok := span.Get(d.Name); {
		case ok:
			a = attr(d.Name, v)
		case onResource:
			a = attr(d.Name, resValue)
		case d.Name == collectorIDKey:
			a, onResource = kv{key: d.Name, value: c.collectorID}, true
		case d.Default != nil:
			a = kv{key: d.Name, value: *d.Default}
		default:
			continue
		}
		out = append(out, a)
		if onResource {
			a.key = c.resourceKeys[i]
			out = append(out, a)
		}
	}
	c.attrs = out
	return out
}

// add returns the series of attrs in minute, creating it within MaxSeries, or
// nil when the limit drops it.
func (c *spanMetricsConnector) add(f *family, attrs []kv, minute int64) *series {
	// Attributes come in a fixed order (configured dimensions, then the
	// connector's own keys), so the key identifies a series without sorting.
	// Lengths prefix keys and values, so no value can mimic a boundary.
	b := binary.AppendVarint(c.key[:0], minute)
	size := 0
	for _, a := range attrs {
		b = binary.AppendUvarint(b, uint64(len(a.key)))
		b = append(b, a.key...)
		b = binary.AppendUvarint(b, uint64(len(a.value)))
		b = append(b, a.value...)
		size += len(a.key) + len(a.value)
	}
	c.key = b
	if s, ok := f.series[string(b)]; ok {
		return s
	}
	// The series keeps its key and a copy of its attributes.
	size += len(b) + seriesOverhead
	if c.total >= c.cfg.MaxSeries || c.bytes+size > c.cfg.MaxSeriesBytes {
		c.dropped++
		return nil
	}
	key := string(b)
	c.bytes += size
	s := &series{attrs: pcommon.NewMap(), minute: minute}
	s.attrs.EnsureCapacity(len(attrs))
	for _, a := range attrs {
		if a.typed {
			a.src.CopyTo(s.attrs.PutEmpty(a.key))
		} else {
			s.attrs.PutStr(a.key, a.value)
		}
	}
	f.series[key] = s
	f.order = append(f.order, key)
	c.total++
	return s
}

// flush emits the deltas aggregated since the last flush, after a batch an
// earlier flush could not deliver.
func (c *spanMetricsConnector) flush(ctx context.Context) error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	c.mu.Lock()
	md, dropped := c.build(), c.dropped
	c.reset()
	c.mu.Unlock()
	if dropped > 0 {
		c.logger.Warn("span metrics over max_series or max_series_bytes were dropped",
			zap.Int("dropped_spans", dropped), zap.Int("max_series", c.cfg.MaxSeries), zap.Int("max_series_bytes", c.cfg.MaxSeriesBytes))
	}
	if c.pending.DataPointCount() > 0 {
		// Deltas of one series in two batches add up downstream; keep the older first.
		md.ResourceMetrics().MoveAndAppendTo(c.pending.ResourceMetrics())
		md, c.pending = c.pending, pmetric.NewMetrics()
	}
	if md.DataPointCount() == 0 {
		return nil
	}
	err := c.next.ConsumeMetrics(ctx, md)
	switch {
	case err == nil:
		return nil
	case consumererror.IsPermanent(err) || md.DataPointCount() > c.cfg.MaxSeries:
		c.logger.Error("span metrics dropped", zap.Error(err), zap.Int("points", md.DataPointCount()))
	default:
		// Refused before acceptance: the spans are acknowledged, so only we can resend.
		c.pending = md
		c.logger.Warn("span metrics refused; sending them again with the next flush", zap.Error(err))
	}
	return err
}

func (c *spanMetricsConnector) build() pmetric.Metrics {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	sm.Scope().SetName(scopeName)
	ms := sm.Metrics()

	sum := func(name, unit string, f *family, value func(*series) float64) {
		if len(f.order) == 0 {
			return
		}
		m := ms.AppendEmpty()
		m.SetName(name)
		m.SetUnit(unit)
		s := m.SetEmptySum()
		s.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		s.SetIsMonotonic(true)
		for _, key := range f.order {
			sr := f.series[key]
			p := s.DataPoints().AppendEmpty()
			point(p.Attributes(), sr)
			p.SetStartTimestamp(pcommon.Timestamp(sr.minute))
			p.SetTimestamp(pcommon.Timestamp(sr.minute))
			p.SetDoubleValue(value(sr))
		}
	}
	count := func(s *series) float64 { return float64(s.count) }
	total := func(s *series) float64 { return s.sum }

	sum("signoz_calls_total", "", &c.calls, count)
	if len(c.latency.order) > 0 {
		m := ms.AppendEmpty()
		m.SetName("signoz_latency")
		m.SetUnit("ms")
		h := m.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		for _, key := range c.latency.order {
			sr := c.latency.series[key]
			p := h.DataPoints().AppendEmpty()
			point(p.Attributes(), sr)
			p.SetStartTimestamp(pcommon.Timestamp(sr.minute))
			p.SetTimestamp(pcommon.Timestamp(sr.minute))
			p.SetCount(sr.count)
			p.SetSum(sr.sum)
			p.ExplicitBounds().FromRaw(c.bounds)
			p.BucketCounts().FromRaw(sr.buckets)
		}
	}
	sum("signoz_db_latency_count", "1", &c.db, count)
	sum("signoz_db_latency_sum", "1", &c.db, total)
	sum("signoz_external_call_latency_count", "1", &c.external, count)
	sum("signoz_external_call_latency_sum", "1", &c.external, total)
	return md
}

func point(m pcommon.Map, s *series) {
	s.attrs.CopyTo(m)
}

func firstValue(m pcommon.Map, keys ...string) (pcommon.Value, bool) {
	for _, k := range keys {
		if v, ok := m.Get(k); ok {
			return v, true
		}
	}
	return pcommon.Value{}, false
}

func firstString(m pcommon.Map, keys ...string) (string, bool) {
	for _, k := range keys {
		if v, ok := m.Get(k); ok {
			return v.AsString(), true
		}
	}
	return "", false
}

func statusName(c ptrace.StatusCode) string {
	switch c {
	case ptrace.StatusCodeOk:
		return "STATUS_CODE_OK"
	case ptrace.StatusCodeError:
		return "STATUS_CODE_ERROR"
	default:
		return "STATUS_CODE_UNSET"
	}
}

func kindName(k ptrace.SpanKind) string {
	switch k {
	case ptrace.SpanKindInternal:
		return "SPAN_KIND_INTERNAL"
	case ptrace.SpanKindServer:
		return "SPAN_KIND_SERVER"
	case ptrace.SpanKindClient:
		return "SPAN_KIND_CLIENT"
	case ptrace.SpanKindProducer:
		return "SPAN_KIND_PRODUCER"
	case ptrace.SpanKindConsumer:
		return "SPAN_KIND_CONSUMER"
	default:
		return "SPAN_KIND_UNSPECIFIED"
	}
}
