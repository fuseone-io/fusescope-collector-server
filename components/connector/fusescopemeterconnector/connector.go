// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopemeterconnector // import "github.com/fuseone-io/fusescope-collector-server/components/connector/fusescopemeterconnector"

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/fuseone-io/fusescope-collector-server/components/connector/fusescopemeterconnector/internal/metadata"
	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
)

const (
	// scopeName is the instrumentation scope of every metric (meter §2).
	scopeName = "signozmeterconnector"
	// setOverhead covers a label set's fixed fields and map entry (meter §7).
	setOverhead = 128

	keyServiceName = "service.name"
	keyEnvironment = "deployment.environment"
	keyEnvName     = "deployment.environment.name"
	keyHostName    = "host.name"
	keyWorkspace   = "signoz.workspace.key.id"
	keyConnectorID = "connector_id"
)

type signal int

const (
	signalTraces signal = iota
	signalLogs
	signalMetrics
)

func (s signal) String() string { return [...]string{"traces", "logs", "metrics"}[s] }

// names are the count and size metrics of each signal (meter §2).
var names = [...][2]metricName{
	signalTraces: {
		{"signoz.meter.span.count", "1", "The number of spans observed."},
		{"signoz.meter.span.size", "By", "The size of spans observed."},
	},
	signalLogs: {
		{"signoz.meter.log.count", "1", "The number of log records observed."},
		{"signoz.meter.log.size", "By", "The size of log records observed."},
	},
	signalMetrics: {
		{"signoz.meter.metric.datapoint.count", "1", "The number of data points observed."},
		{"signoz.meter.metric.datapoint.size", "By", "The size of data points observed."},
	},
}

type metricName struct{ name, unit, description string }

// Overflow reasons (meter §7), in the order of reasonNames.
type reason int

const (
	reasonNone reason = iota
	reasonSeries
	reasonBytes
	reasonKeys
	reasonNoKey
	numReasons
)

var reasonNames = [numReasons]string{"", "series", "bytes", "keys", "no_key"}

// Which labels a set has: absent and empty differ (meter §3).
const (
	hasService uint8 = 1 << iota
	hasEnv
	hasHost
	hasKey
)

// setKey is a label set. An overflow set is the set of its labels alone (the
// key, or nothing), so it is the same set as a normal one with those labels
// and a flush has one point per label set (meter §3, §7).
type setKey struct {
	service, env, host, key string
	has                     uint8
}

func (k setKey) clone() setKey {
	k.service, k.env, k.host, k.key = strings.Clone(k.service), strings.Clone(k.env), strings.Clone(k.host), strings.Clone(k.key)
	return k
}

type counts struct{ count, size int64 }

// window is what one flush emits.
type window struct {
	sets  map[setKey]*counts
	order []setKey
	// normal and bytes count the normal sets against max_series and
	// max_series_bytes; keyOverflows the per-key overflow sets.
	normal, bytes, keyOverflows int
}

func newWindow() *window { return &window{sets: map[setKey]*counts{}} }

type meter struct {
	cfg    *Config
	logger *zap.Logger
	next   consumer.Metrics
	sig    signal
	id     string
	now    func() time.Time

	tb        *metadata.TelemetryBuilder
	sigAttr   metric.AddOption
	overflows [numReasons]metric.AddOption

	mu          sync.Mutex
	cur         *window
	windowStart time.Time

	// flushing serializes flushes without a lock, so a caller can give up on
	// a flush stuck behind another.
	flushing chan struct{}
	quit     chan struct{}
	done     chan struct{}
	cancel   context.CancelFunc
	stopOnce sync.Once
}

func newMeter(cfg *Config, set connector.Settings, next consumer.Metrics, sig signal) (*meter, error) {
	tb, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	m := &meter{
		cfg: cfg, logger: set.Logger, next: next, sig: sig, id: newUUID(), now: time.Now,
		tb: tb, sigAttr: metric.WithAttributeSet(attribute.NewSet(attribute.String("signal", sig.String()))),
		cur: newWindow(), flushing: make(chan struct{}, 1),
	}
	for r := reasonSeries; r < numReasons; r++ {
		m.overflows[r] = metric.WithAttributeSet(attribute.NewSet(
			attribute.String("signal", sig.String()), attribute.String("reason", reasonNames[r])))
	}
	m.windowStart = m.now()
	return m, nil
}

// newUUID is a random version 4 UUID, the connector_id (meter §5).
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6], b[8] = b[6]&0x0f|0x40, b[8]&0x3f|0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (m *meter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (m *meter) Start(context.Context, component.Host) error {
	m.mu.Lock()
	m.windowStart = m.now()
	m.mu.Unlock()
	var ctx context.Context
	ctx, m.cancel = context.WithCancel(context.Background())
	m.quit, m.done = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(m.done)
		t := time.NewTicker(m.cfg.FlushInterval)
		defer t.Stop()
		for {
			select {
			case <-m.quit:
				return
			case <-t.C:
				_ = m.flush(ctx) // logged and counted by flush
			}
		}
	}()
	return nil
}

// Shutdown stops the periodic flushes and emits what is still counted (meter
// §5). It gives up at ctx's deadline, even on a sink that ignores ctx, and
// may be called again.
func (m *meter) Shutdown(ctx context.Context) error {
	if m.quit != nil {
		m.stopOnce.Do(func() { close(m.quit) })
		select {
		case <-m.done:
		case <-ctx.Done():
			m.cancel()
			return ctx.Err()
		}
		m.cancel()
	}
	flushed := make(chan error, 1)
	go func() { flushed <- m.flush(ctx) }()
	select {
	case err := <-flushed:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// flush swaps the current window for an empty one under the lock, then
// builds and sends it without the lock, so a slow metrics pipeline never
// holds up Consume. A refused flush is dropped, as a resend could double it.
func (m *meter) flush(ctx context.Context) error {
	select {
	case m.flushing <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-m.flushing }()

	m.mu.Lock()
	w := m.cur
	m.cur = newWindow()
	start, end := m.windowStart, m.now()
	m.windowStart = end
	m.mu.Unlock()

	if len(w.order) == 0 {
		return nil
	}
	if err := m.next.ConsumeMetrics(ctx, m.build(w, start, end)); err != nil {
		m.tb.FusescopeMeterFlushErrors.Add(context.Background(), 1, m.sigAttr)
		m.logger.Error("the metrics pipeline refused a meter flush; its counts are dropped",
			zap.String("signal", m.sig.String()), zap.Int("sets", len(w.order)), zap.Error(err))
		return err
	}
	return nil
}

func (m *meter) build(w *window, start, end time.Time) pmetric.Metrics {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	sm.Scope().SetName(scopeName)
	for i, n := range names[m.sig] {
		mt := sm.Metrics().AppendEmpty()
		mt.SetName(n.name)
		mt.SetUnit(n.unit)
		mt.SetDescription(n.description)
		sum := mt.SetEmptySum()
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		sum.SetIsMonotonic(true)
		dps := sum.DataPoints()
		dps.EnsureCapacity(len(w.order))
		for _, k := range w.order {
			p := dps.AppendEmpty()
			p.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
			p.SetTimestamp(pcommon.NewTimestampFromTime(end))
			c := w.sets[k]
			if i == 0 {
				p.SetIntValue(c.count)
			} else {
				p.SetIntValue(c.size)
			}
			a := p.Attributes()
			if k.has&hasService != 0 {
				a.PutStr(keyServiceName, k.service)
			}
			if k.has&hasEnv != 0 {
				a.PutStr(keyEnvironment, k.env)
			}
			if k.has&hasHost != 0 {
				a.PutStr(keyHostName, k.host)
			}
			a.PutStr(keyConnectorID, m.id)
			if k.has&hasKey != 0 {
				a.PutStr(keyWorkspace, k.key)
			}
		}
	}
	return md
}

// request is what one Consume call learns once: the authenticated key, and
// the telemetry to record when it ends.
type request struct {
	trusted       string
	authenticated bool
	cuts          int64
	overflows     [numReasons]int64
}

func newRequest(ctx context.Context) request {
	var r request
	if auth := client.FromContext(ctx).Auth; auth != nil {
		r.authenticated = true
		r.trusted, _ = auth.GetAttribute(ingestpolicy.AuthKeyID).(string)
	}
	return r
}

func (m *meter) record(r *request) {
	ctx := context.Background()
	if r.cuts > 0 {
		m.tb.FusescopeMeterTruncatedValues.Add(ctx, r.cuts, m.sigAttr)
	}
	for i, n := range r.overflows {
		if n > 0 {
			m.tb.FusescopeMeterOverflowRecords.Add(ctx, n, m.overflows[i])
		}
	}
}

func (m *meter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	r := newRequest(ctx)
	sizer := ptrace.ProtoMarshaler{}
	for _, rs := range td.ResourceSpans().All() {
		var n, size int64
		for _, ss := range rs.ScopeSpans().All() {
			for _, s := range ss.Spans().All() {
				n++
				size += int64(sizer.SpanSize(s))
			}
		}
		m.add(&r, rs.Resource().Attributes(), n, size)
	}
	m.record(&r)
	return nil
}

func (m *meter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	r := newRequest(ctx)
	sizer := plog.ProtoMarshaler{}
	for _, rl := range ld.ResourceLogs().All() {
		var n, size int64
		for _, sl := range rl.ScopeLogs().All() {
			for _, l := range sl.LogRecords().All() {
				n++
				size += int64(sizer.LogRecordSize(l))
			}
		}
		m.add(&r, rl.Resource().Attributes(), n, size)
	}
	m.record(&r)
	return nil
}

// ConsumeMetrics counts data points, whatever the metric type; their size is
// 0 (meter §2).
func (m *meter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	r := newRequest(ctx)
	for _, rm := range md.ResourceMetrics().All() {
		var n int64
		for _, sm := range rm.ScopeMetrics().All() {
			for _, mt := range sm.Metrics().All() {
				n += int64(dataPoints(mt))
			}
		}
		m.add(&r, rm.Resource().Attributes(), n, 0)
	}
	m.record(&r)
	return nil
}

func dataPoints(m pmetric.Metric) int {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		return m.Gauge().DataPoints().Len()
	case pmetric.MetricTypeSum:
		return m.Sum().DataPoints().Len()
	case pmetric.MetricTypeHistogram:
		return m.Histogram().DataPoints().Len()
	case pmetric.MetricTypeExponentialHistogram:
		return m.ExponentialHistogram().DataPoints().Len()
	case pmetric.MetricTypeSummary:
		return m.Summary().DataPoints().Len()
	}
	return 0
}

// add counts n records of size bytes of one resource.
func (m *meter) add(r *request, res pcommon.Map, n, size int64) {
	if n == 0 {
		return
	}
	k := m.labels(r, res)
	m.mu.Lock()
	why := m.cur.add(m.cfg, k, r.trusted, n, size)
	m.mu.Unlock()
	if why != reasonNone {
		r.overflows[why] += n
	}
}

// labels resolves a resource's label set (meter §3).
func (m *meter) labels(r *request, res pcommon.Map) setKey {
	var k setKey
	if v, ok := m.value(r, res, keyServiceName); ok {
		k.service, k.has = v, k.has|hasService
	}
	if v, ok := m.value(r, res, keyEnvName); ok {
		k.env, k.has = v, k.has|hasEnv
	} else if v, ok := m.value(r, res, keyEnvironment); ok {
		k.env, k.has = v, k.has|hasEnv
	}
	if v, ok := m.value(r, res, keyHostName); ok {
		k.host, k.has = v, k.has|hasHost
	}
	switch {
	case r.trusted != "":
		k.key, k.has = r.trusted, k.has|hasKey
	case !r.authenticated:
		if v, ok := m.value(r, res, keyWorkspace); ok {
			k.key, k.has = v, k.has|hasKey
		}
	}
	return k
}

// value is res[key], cut to max_label_bytes at a UTF-8 boundary. Only a
// string counts: another type is ignored before any conversion, which for
// bytes, maps and lists would build the whole string first (meter §3).
func (m *meter) value(r *request, res pcommon.Map, key string) (string, bool) {
	v, ok := res.Get(key)
	if !ok || v.Type() != pcommon.ValueTypeStr {
		return "", false
	}
	s := v.Str()
	if limit := m.cfg.MaxLabelBytes; len(s) > limit {
		for limit > 0 && !utf8.RuneStart(s[limit]) {
			limit--
		}
		s = s[:limit]
		r.cuts++
	}
	return s, true
}

// add counts into k's set, or into an overflow set when the limits refuse a
// new one, and reports why it overflowed (meter §7).
func (w *window) add(cfg *Config, k setKey, trusted string, n, size int64) reason {
	if c, ok := w.sets[k]; ok {
		c.count += n
		c.size += size
		return reasonNone
	}
	cost := setOverhead + len(k.service) + len(k.env) + len(k.host) + len(k.key)
	var why reason
	switch {
	case w.normal >= cfg.MaxSeries:
		why = reasonSeries
	case w.bytes+cost > cfg.MaxSeriesBytes:
		why = reasonBytes
	default:
		w.insert(k, n, size)
		w.normal++
		w.bytes += cost
		return reasonNone
	}
	if trusted == "" {
		w.addLast(n, size)
		return reasonNoKey
	}
	ok := setKey{key: trusted, has: hasKey}
	if c, found := w.sets[ok]; found {
		c.count += n
		c.size += size
		return why
	}
	if w.keyOverflows < cfg.MaxAuthenticatedKeys {
		w.insert(ok, n, size)
		w.keyOverflows++
		return why
	}
	w.addLast(n, size)
	return reasonKeys
}

func (w *window) addLast(n, size int64) {
	last := setKey{}
	if c, ok := w.sets[last]; ok {
		c.count += n
		c.size += size
		return
	}
	w.insert(last, n, size)
}

func (w *window) insert(k setKey, n, size int64) {
	k = k.clone()
	w.sets[k] = &counts{count: n, size: size}
	w.order = append(w.order, k)
}
