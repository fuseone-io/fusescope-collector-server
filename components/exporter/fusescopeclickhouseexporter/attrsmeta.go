// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"
	"unsafe"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter/internal/metadata"
)

const (
	amTable = "distributed_attributes_metadata"
	// amWindowMillis is the width of a metadata window (attributes-metadata §3).
	amWindowMillis = int64(6 * time.Hour / time.Millisecond)
	// Costs of a queued row (attributes-metadata §5): fixed, and per map entry.
	amRowOverhead       = 256
	amAttributeOverhead = 64
	// Costs in the deduplication state: a key, and a resource of a window's
	// quota index. TestMetadataStateFootprint checks them against the heap.
	amKeyCost      = 64
	amResourceCost = 64
)

// Reasons a metadata row is dropped (attributes-metadata §5), in the order of
// dropNames.
type dropReason int

const (
	dropWindow dropReason = iota
	dropResource
	dropRowBytes
	dropQueue
	dropStateBytes
	dropTooOld
	dropInsert
	numDropReasons
)

var dropNames = [numDropReasons]string{"window", "resource", "row_bytes", "queue", "state_bytes", "too_old", "insert"}

// amKV is one map entry of a row. Before a row is admitted its value may
// point into the converter's buffer; an admitted row owns copies.
type amKV struct{ key, value string }

type amKey struct{ resource, attrs uint64 }

// amRow is one attributes_metadata row waiting for insert.
type amRow struct {
	window   int64
	key      amKey
	resource []amKV
	attrs    []amKV
	cost     int
}

// amWindow is the deduplication state of one window.
type amWindow struct {
	keys        map[amKey]struct{}
	perResource map[uint64]int
}

// attrsMetadata records the attribute sets of one signal into
// signoz_metadata.attributes_metadata (attributes-metadata). It is fed only
// with data whose insert succeeded, never fails or waits on the data path,
// and inserts its rows from its own goroutine and connection.
type attrsMetadata struct {
	cfg    *AttributesMetadataConfig
	signal string
	logger *zap.Logger
	client *clickhouseClient
	body   string
	insert *insertTelemetry
	drops  [numDropReasons][]metric.AddOption
	tb     *metadata.TelemetryBuilder
	now    func() time.Time
	// timeout bounds each insert, as the exporter's timeout does its own.
	timeout time.Duration

	// scratch converts one record at a time; push runs on several queue
	// consumers, so each takes its own.
	scratch sync.Pool

	mu         sync.Mutex
	windows    map[int64]*amWindow
	stateBytes int
	queue      []amRow
	queueBytes int

	quit chan struct{}
	done chan struct{}
	stop sync.Once
}

func newAttrsMetadata(cfg *Config, signal string, set component.TelemetrySettings) (*attrsMetadata, error) {
	mc := &cfg.AttributesMetadata
	m := &attrsMetadata{
		cfg: mc, signal: signal, logger: set.Logger, now: time.Now, timeout: cfg.Timeout,
		client:  &clickhouseClient{cfg: cfg, database: mc.Database, logger: set.Logger},
		windows: map[int64]*amWindow{},
	}
	m.scratch.New = func() any { return &amScratch{} }
	m.body = fmt.Sprintf("INSERT INTO %s.%s %s VALUES", mc.Database, amTable, newAMColumns().input().Columns())
	var err error
	if m.insert, err = newInsertTelemetry(set, []string{amTable}); err != nil {
		return nil, err
	}
	if m.tb, err = metadata.NewTelemetryBuilder(set); err != nil {
		return nil, err
	}
	for r := range numDropReasons {
		m.drops[r] = []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(attribute.String("signal", signal), attribute.String("reason", dropNames[r])))}
	}
	return m, nil
}

// start never fails the exporter: without a connection, rows are dropped at
// each insert attempt, which dials again.
func (m *attrsMetadata) start(ctx context.Context) {
	if err := m.client.start(ctx); err != nil {
		m.logger.Warn("attribute metadata has no ClickHouse connection yet", zap.String("signal", m.signal), zap.Error(err))
	}
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
				m.flush(context.Background())
			}
		}
	}()
}

// shutdown stops the periodic inserts and inserts what is queued, within ctx.
func (m *attrsMetadata) shutdown(ctx context.Context) error {
	if m.quit != nil {
		m.stop.Do(func() { close(m.quit) })
		select {
		case <-m.done:
		case <-ctx.Done():
			return ctx.Err()
		}
		m.flush(ctx)
	}
	return m.client.shutdown(ctx)
}

func (m *attrsMetadata) dropped(r dropReason, n int) {
	m.tb.ExporterFusescopeClickhouseAttributesMetadataDropped.Add(context.Background(), int64(n), m.drops[r]...)
}

// amScratch is the per-record working memory: no allocation once warm.
type amScratch struct {
	conv   boundedString
	refs   []amRef
	attrs  []amKV
	res    []amKV
	resBuf []byte
}

// amRef is an entry whose value is either a string of the record or a
// range of conv.buf, resolved once conversion is done.
type amRef struct {
	key      string
	value    string
	off, end int
}

// resourceInfo of the current resource, converted once for its records.
type amResource struct {
	attrs []amKV
	fp    uint64
	cost  int
	ok    bool
}

// convert turns m's entries, plus extra if set, into sorted string pairs
// within budget bytes of keys and values; it fails without converting the
// rest once the budget is spent.
func (s *amScratch) convert(dst []amKV, m pcommon.Map, extraKey, extraValue string, budget int) ([]amKV, int, bool) {
	s.refs = s.refs[:0]
	s.conv.buf = s.conv.buf[:0]
	used := 0
	for k, v := range m.All() {
		if extraKey != "" && k == extraKey {
			continue
		}
		used += len(k)
		if used > budget {
			return dst, 0, false
		}
		if v.Type() == pcommon.ValueTypeStr {
			used += len(v.Str())
			if used > budget {
				return dst, 0, false
			}
			s.refs = append(s.refs, amRef{key: k, value: v.Str(), off: -1})
			continue
		}
		off := len(s.conv.buf)
		if !s.conv.append(v, budget-used) {
			return dst, 0, false
		}
		used += len(s.conv.buf) - off
		s.refs = append(s.refs, amRef{key: k, off: off, end: len(s.conv.buf)})
	}
	if extraKey != "" {
		used += len(extraKey) + len(extraValue)
		if used > budget {
			return dst, 0, false
		}
		s.refs = append(s.refs, amRef{key: extraKey, value: extraValue, off: -1})
	}
	dst = dst[:0]
	for _, r := range s.refs {
		v := r.value
		if r.off >= 0 && r.end > r.off {
			v = unsafe.String(&s.conv.buf[r.off], r.end-r.off)
		}
		dst = append(dst, amKV{r.key, v})
	}
	slices.SortFunc(dst, func(a, b amKV) int { return cmp.Compare(a.key, b.key) })
	return dst, used, true
}

func amFingerprint(kvs []amKV) uint64 {
	h := uint64(fnvOffset64)
	for _, kv := range kvs {
		h = fnvString(fnvString(h, kv.key), kv.value)
	}
	return h
}

// resource converts a resource once for all its records (§2).
func (m *attrsMetadata) resource(s *amScratch, res pcommon.Map) amResource {
	if res.Len() > m.cfg.MaxAttributesPerRow {
		return amResource{}
	}
	attrs, used, ok := s.convert(s.res, res, "", "", m.cfg.MaxRowBytes-amRowOverhead)
	if !ok {
		return amResource{}
	}
	// The resource's values outlive the next convert: keep them in resBuf.
	s.resBuf = s.resBuf[:0]
	for _, kv := range attrs {
		s.resBuf = append(s.resBuf, kv.value...)
	}
	off := 0
	for i := range attrs {
		n := len(attrs[i].value)
		if n > 0 {
			attrs[i].value = unsafe.String(&s.resBuf[off], n)
		}
		off += n
	}
	s.res = attrs
	return amResource{attrs: attrs, fp: amFingerprint(attrs), cost: used + amAttributeOverhead*len(attrs), ok: true}
}

// record offers one record's set, stamped at ms (§3, §4, §5).
func (m *attrsMetadata) record(s *amScratch, res amResource, attrs pcommon.Map, extraKey, extraValue string, ms int64) {
	n := attrs.Len()
	if extraKey != "" {
		n++
		if _, dup := attrs.Get(extraKey); dup {
			n--
		}
	}
	if !res.ok || len(res.attrs)+n > m.cfg.MaxAttributesPerRow {
		m.dropped(dropRowBytes, 1)
		return
	}
	budget := m.cfg.MaxRowBytes - amRowOverhead - res.cost - amAttributeOverhead*n
	kvs, _, ok := s.convert(s.attrs, attrs, extraKey, extraValue, budget)
	s.attrs = kvs
	if !ok {
		m.dropped(dropRowBytes, 1)
		return
	}
	key := amKey{resource: res.fp, attrs: amFingerprint(kvs)}
	cost := amRowOverhead + res.cost + amAttributeOverhead*len(kvs)
	for _, kv := range kvs {
		cost += len(kv.key) + len(kv.value)
	}
	if why, ok := m.admit(key, ms, cost, res.attrs, kvs); !ok {
		m.dropped(why, 1)
	}
}

// admit checks the row against the state and every limit, in the order of
// attributes-metadata §5, and queues it; nothing is admitted unless it is
// queued. A duplicate is not a drop.
func (m *attrsMetadata) admit(key amKey, ms int64, cost int, res, attrs []amKV) (dropReason, bool) {
	now := m.now().UnixMilli()
	current := now - now%amWindowMillis
	oldest := current - int64(m.cfg.MaxWindows-1)*amWindowMillis
	w := ms - ms%amWindowMillis
	if ms < 0 {
		w -= amWindowMillis
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.expire(oldest)
	switch {
	case w > current:
		w = current
	case w < oldest:
		return dropTooOld, false
	}
	ws := m.windows[w]
	if ws != nil {
		if _, seen := ws.keys[key]; seen {
			return 0, true
		}
	}
	if cost > m.cfg.MaxRowBytes {
		return dropRowBytes, false
	}
	count, perResource := 0, 0
	if ws != nil {
		count, perResource = len(ws.keys), ws.perResource[key.resource]
	}
	if count >= m.cfg.MaxSetsPerWindow {
		return dropWindow, false
	}
	if perResource >= m.cfg.MaxSetsPerResource {
		return dropResource, false
	}
	state := amKeyCost
	if perResource == 0 {
		state += amResourceCost
	}
	if m.stateBytes+state > m.cfg.MaxStateBytes {
		return dropStateBytes, false
	}
	if len(m.queue) >= m.cfg.MaxQueueRows || m.queueBytes+cost > m.cfg.MaxQueueBytes {
		return dropQueue, false
	}
	if ws == nil {
		ws = &amWindow{keys: map[amKey]struct{}{}, perResource: map[uint64]int{}}
		m.windows[w] = ws
	}
	ws.keys[key] = struct{}{}
	ws.perResource[key.resource]++
	m.stateBytes += state
	m.queue = append(m.queue, amRow{window: w, key: key, resource: ownKVs(res), attrs: ownKVs(attrs), cost: cost})
	m.queueBytes += cost
	return 0, true
}

// expire frees the windows older than oldest; called with mu held.
func (m *attrsMetadata) expire(oldest int64) {
	for w, ws := range m.windows {
		if w < oldest {
			m.stateBytes -= amKeyCost*len(ws.keys) + amResourceCost*len(ws.perResource)
			delete(m.windows, w)
		}
	}
}

// release takes back the keys of rows whose insert failed, so a later
// occurrence writes them again (attributes-metadata §5).
func (m *attrsMetadata) release(rows []amRow) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range rows {
		ws := m.windows[r.window]
		if ws == nil {
			continue // expired meanwhile
		}
		if _, ok := ws.keys[r.key]; !ok {
			continue
		}
		delete(ws.keys, r.key)
		m.stateBytes -= amKeyCost
		if ws.perResource[r.key.resource]--; ws.perResource[r.key.resource] == 0 {
			delete(ws.perResource, r.key.resource)
			m.stateBytes -= amResourceCost
		}
	}
}

// ownKVs copies kvs and their strings, which may point into the batch or the
// scratch, into one allocation for the strings.
func ownKVs(kvs []amKV) []amKV {
	if len(kvs) == 0 {
		return nil
	}
	n := 0
	for _, kv := range kvs {
		n += len(kv.key) + len(kv.value)
	}
	buf := make([]byte, 0, n)
	for _, kv := range kvs {
		buf = append(buf, kv.key...)
		buf = append(buf, kv.value...)
	}
	all := unsafe.String(unsafe.SliceData(buf), len(buf))
	out := make([]amKV, len(kvs))
	off := 0
	for i, kv := range kvs {
		out[i].key = all[off : off+len(kv.key)]
		off += len(kv.key)
		out[i].value = all[off : off+len(kv.value)]
		off += len(kv.value)
	}
	return out
}

// flush inserts the queued rows in one block. A failed insert drops them,
// counted, and releases their keys.
func (m *attrsMetadata) flush(ctx context.Context) {
	m.mu.Lock()
	rows := m.queue
	m.queue, m.queueBytes = nil, 0
	m.mu.Unlock()
	if len(rows) == 0 {
		return
	}
	c := newAMColumns()
	for _, r := range rows {
		c.append(m.signal, r)
	}
	if m.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.timeout)
		defer cancel()
	}
	start := time.Now()
	var err error
	if m.client.do == nil && m.client.pool.Load() == nil {
		err = m.client.start(ctx) // the first dial failed: try again
	} else {
		err = m.client.refresh(ctx)
	}
	if err == nil {
		err = m.client.insert(ctx, ch.Query{Body: m.body, Input: c.input()}, "")
	}
	if err != nil {
		err = classify(fmt.Errorf("insert into %s.%s: %w", m.cfg.Database, amTable, err))
	}
	m.insert.record(ctx, 0, time.Since(start), len(rows), err)
	if err != nil {
		m.release(rows)
		m.dropped(dropInsert, len(rows))
		m.logger.Warn("dropped attribute metadata rows whose insert failed",
			zap.String("signal", m.signal), zap.Int("rows", len(rows)), zap.Error(err))
	}
}

// observeTraces records the sets of spans: the span attributes plus the span
// name, in the window of the span start (§2, §3).
func (m *attrsMetadata) observeTraces(td ptrace.Traces) {
	s := m.scratch.Get().(*amScratch)
	defer m.scratch.Put(s)
	for _, rs := range td.ResourceSpans().All() {
		res := m.resource(s, rs.Resource().Attributes())
		for _, ss := range rs.ScopeSpans().All() {
			for _, span := range ss.Spans().All() {
				m.record(s, res, span.Attributes(), "name", span.Name(), int64(span.StartTimestamp())/1e6)
			}
		}
	}
	s.release()
}

// observeLogs records the sets of log records, in the window of the record
// time, else its observed time, else the arrival (§3).
func (m *attrsMetadata) observeLogs(ld plog.Logs) {
	s := m.scratch.Get().(*amScratch)
	defer m.scratch.Put(s)
	now := m.now().UnixMilli()
	for _, rl := range ld.ResourceLogs().All() {
		res := m.resource(s, rl.Resource().Attributes())
		for _, sl := range rl.ScopeLogs().All() {
			for _, l := range sl.LogRecords().All() {
				ms := now
				switch {
				case l.Timestamp() != 0:
					ms = int64(l.Timestamp()) / 1e6
				case l.ObservedTimestamp() != 0:
					ms = int64(l.ObservedTimestamp()) / 1e6
				}
				m.record(s, res, l.Attributes(), "", "", ms)
			}
		}
	}
	s.release()
}

// observeMetrics records the sets of data points, in the window of the
// arrival (§3).
func (m *attrsMetadata) observeMetrics(md pmetric.Metrics) {
	s := m.scratch.Get().(*amScratch)
	defer m.scratch.Put(s)
	now := m.now().UnixMilli()
	for _, rm := range md.ResourceMetrics().All() {
		res := m.resource(s, rm.Resource().Attributes())
		for _, sm := range rm.ScopeMetrics().All() {
			for _, mt := range sm.Metrics().All() {
				forEachPointAttrs(mt, func(attrs pcommon.Map) { m.record(s, res, attrs, "", "", now) })
			}
		}
	}
	s.release()
}

func forEachPointAttrs(m pmetric.Metric, f func(pcommon.Map)) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		for _, p := range m.Gauge().DataPoints().All() {
			f(p.Attributes())
		}
	case pmetric.MetricTypeSum:
		for _, p := range m.Sum().DataPoints().All() {
			f(p.Attributes())
		}
	case pmetric.MetricTypeHistogram:
		for _, p := range m.Histogram().DataPoints().All() {
			f(p.Attributes())
		}
	case pmetric.MetricTypeExponentialHistogram:
		for _, p := range m.ExponentialHistogram().DataPoints().All() {
			f(p.Attributes())
		}
	case pmetric.MetricTypeSummary:
		for _, p := range m.Summary().DataPoints().All() {
			f(p.Attributes())
		}
	}
}

// release drops the scratch's references to the batch just observed.
func (s *amScratch) release() {
	clear(s.refs[:cap(s.refs)])
	clear(s.attrs[:cap(s.attrs)])
	clear(s.res[:cap(s.res)])
	for i := range s.conv.keys {
		clear(s.conv.keys[i][:cap(s.conv.keys[i])])
	}
}

// amColumns is one block of attributes_metadata.
type amColumns struct {
	unixMilli  proto.ColUInt64
	dataSource proto.ColStr
	resourceFP proto.ColUInt64
	attrsFP    proto.ColUInt64
	resource   *strMap
	attributes *strMap
}

func newAMColumns() *amColumns {
	return &amColumns{resource: newStrMap(), attributes: newStrMap()}
}

func (c *amColumns) input() proto.Input {
	return proto.Input{
		{Name: "unix_milli", Data: &c.unixMilli},
		{Name: "data_source", Data: &c.dataSource},
		{Name: "resource_fingerprint", Data: &c.resourceFP},
		{Name: "attrs_fingerprint", Data: &c.attrsFP},
		{Name: "resource_attributes", Data: c.resource},
		{Name: "attributes", Data: c.attributes},
	}
}

func (c *amColumns) append(signal string, r amRow) {
	c.unixMilli.Append(uint64(r.window))
	c.dataSource.Append(signal)
	c.resourceFP.Append(r.key.resource)
	c.attrsFP.Append(r.key.attrs)
	for _, kv := range r.resource {
		c.resource.Keys.Append(kv.key)
		c.resource.Values.Append(kv.value)
	}
	endRow(c.resource)
	for _, kv := range r.attrs {
		c.attributes.Keys.Append(kv.key)
		c.attributes.Values.Append(kv.value)
	}
	endRow(c.attributes)
}
