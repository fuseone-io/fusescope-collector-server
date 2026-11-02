// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"cmp"
	"math"
	"slices"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// A point becomes several series, each with a copy of the resource, scope and
// point attributes, so a small request can expand by orders of magnitude
// (metrics §6). Sizes are estimates of the encoded column bytes; a batch is
// inserted in chunks of about defaultChunkBytes.
const (
	defaultMaxPointBytes = 16 << 20
	defaultChunkBytes    = 256 << 20
	// sampleBytes is one samples_v4 row: five 8-byte columns and three dictionary indexes.
	sampleBytes = 64
	// seriesOverhead covers the generated labels and the fixed columns of one series.
	seriesOverhead = 256
	// jsonGrowth is the worst case of JSON escaping: one byte becomes \u00XX or \ufffd.
	jsonGrowth = 6
)

// scopeGenerated is the number of __scope.*__ entries leading e.scope.
const scopeGenerated = 3

// typedKV is a stringified attribute with its pdata type name and origin.
type typedKV struct {
	key, value, datatype, origin string
}

// seriesSpec describes one derived series (metrics §2).
type seriesSpec struct {
	name, metricType, temporality, unit, description string
	// metaUnit is the unit recorded in metadata, which differs for .count series.
	metaUnit  string
	monotonic bool
}

// textBytes is the size of the strings a series or metadata row copies from spec.
func (s seriesSpec) textBytes() int {
	return len(s.name) + len(s.description) + len(s.unit) + len(s.metaUnit)
}

type seriesRow struct {
	fingerprint uint64
	hour        int64
}

type metaKey struct {
	temporality, metricName, attrName, attrType, datatype, value string
}

type metaRow struct {
	spec        seriesSpec
	key         metaKey
	first, last uint64
}

// metaGroup is a series of the current scope whose resource and scope metadata
// rows are already indexed.
type metaGroup struct {
	name, temporality string
}

// metricsEncoder turns pmetric.Metrics into one column block per table. It is
// reused across batches and is not safe for concurrent use.
type metricsEncoder struct {
	samples *sampleColumns
	series  *seriesColumns
	meta    *metadataColumns

	seenSeries map[seriesRow]struct{}
	metaIndex  map[metaKey]int
	metaRows   []metaRow
	// scopeMeta caches the resource and scope metadata rows of each series of
	// the current scope, so a point does not hash those values again.
	scopeMeta map[metaGroup][]int

	resource []typedKV
	scope    []typedKV
	point    []typedKV
	labels   []typedKV
	buf      []byte
	env      string
	now      func() time.Time
	logger   *zap.Logger

	// Fingerprint contributions of the current resource, scope and point (metrics §4).
	resourceHash, scopeHash, pointHash uint64

	maxPointBytes, maxChunkBytes int
	resourceBytes, scopeBytes    int
	// expanded estimates the bytes of the current chunk; dropped counts points over maxPointBytes.
	expanded, dropped int
	// emit inserts the current chunk; err is its first failure.
	emit func() error
	err  error
}

// setAttempt fixes the ingest time of the next encode.
func (e *metricsEncoder) setAttempt(now time.Time, _ func([]byte)) {
	e.now = func() time.Time { return now }
}

func newMetricsEncoder() *metricsEncoder {
	return &metricsEncoder{
		samples:    newSampleColumns(),
		series:     newSeriesColumns(),
		meta:       newMetadataColumns(),
		seenSeries: map[seriesRow]struct{}{},
		metaIndex:  map[metaKey]int{},
		scopeMeta:  map[metaGroup][]int{},
		now:        time.Now,
		logger:     zap.NewNop(),

		maxPointBytes: defaultMaxPointBytes,
		maxChunkBytes: defaultChunkBytes,
	}
}

// resetChunk empties the column blocks and the metadata of the current chunk.
// Series rows already written are remembered, so a later chunk does not repeat them.
func (e *metricsEncoder) resetChunk() {
	e.samples.input().Reset()
	e.series.input().Reset()
	e.meta.input().Reset()
	clear(e.metaIndex)
	clear(e.scopeMeta)
	e.metaRows = e.metaRows[:0]
	e.expanded = 0
}

// encode builds the rows of md. When they exceed maxChunkBytes it calls emit to
// insert them and continues with an empty chunk; the last chunk is left for the
// caller. It returns the first error of emit.
func (e *metricsEncoder) encode(md pmetric.Metrics, emit func() error) error {
	e.resetChunk()
	clear(e.seenSeries)
	e.dropped, e.emit, e.err = 0, emit, nil
	insertedAt := e.now().UnixMilli()
	for _, rm := range md.ResourceMetrics().All() {
		e.resource = sortedByKey(typedStrings(e.resource[:0], rm.Resource().Attributes(), "resource"))
		e.resourceBytes = kvBytes(e.resource)
		e.resourceHash = fnvKVs(fnvString(fnvOffset64, "resource"), e.resource)
		e.env = ""
		if v, ok := rm.Resource().Attributes().Get("deployment.environment"); ok {
			e.env = valueString(v)
		}
		for _, sm := range rm.ScopeMetrics().All() {
			scope := sm.Scope()
			e.scope = append(e.scope[:0],
				typedKV{key: "__scope.name__", value: scope.Name(), datatype: "Str", origin: "scope"},
				typedKV{key: "__scope.schema_url__", value: sm.SchemaUrl(), datatype: "Str", origin: "scope"},
				typedKV{key: "__scope.version__", value: scope.Version(), datatype: "Str", origin: "scope"})
			e.scope = typedStrings(e.scope, scope.Attributes(), "scope")
			sortedByKey(e.scope[scopeGenerated:])
			e.scopeBytes = kvBytes(e.scope)
			e.scopeHash = fnvKVs(fnvString(fnvOffset64, "scope"), e.scope)
			clear(e.scopeMeta)
			for _, m := range sm.Metrics().All() {
				e.appendMetric(m, insertedAt)
				if e.err != nil {
					return e.err
				}
			}
		}
	}
	e.flushMetadata()
	if e.dropped > 0 {
		e.logger.Warn("dropped metric points whose derived series exceed the size limit",
			zap.Int("dropped_points", e.dropped), zap.Int("limit_bytes", e.maxPointBytes))
	}
	return nil
}

// checkpoint inserts the current chunk once it exceeds maxChunkBytes, and
// reports whether encoding may go on.
func (e *metricsEncoder) checkpoint() bool {
	if e.err != nil {
		return false
	}
	if e.expanded <= e.maxChunkBytes || e.emit == nil {
		return true
	}
	e.flushMetadata()
	if e.err = e.emit(); e.err != nil {
		return false
	}
	e.resetChunk()
	return true
}

// fits reports whether a point of spec with the attributes in e.point may
// expand into series series, and counts it as dropped otherwise. Each series
// holds the attributes once in JSON labels, with the name, and once in maps,
// and has a metadata row per attribute; series and metadata rows copy the
// name, description and unit.
func (e *metricsEncoder) fits(spec seriesSpec, series int) bool {
	attrs := e.resourceBytes + e.scopeBytes + kvBytes(e.point)
	metaRows := len(e.resource) + len(e.scope) + len(e.point) + 2
	text := spec.textBytes()
	perSeries := jsonGrowth*(attrs+len(spec.name)) + 2*attrs + text + seriesOverhead + metaRows*(text+seriesOverhead)
	if series*perSeries <= e.maxPointBytes {
		return true
	}
	e.dropped++
	return false
}

func (e *metricsEncoder) appendMetric(m pmetric.Metric, insertedAt int64) {
	base := seriesSpec{name: m.Name(), unit: m.Unit(), metaUnit: m.Unit(), description: m.Description()}
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		spec := base
		spec.metricType, spec.temporality = "Gauge", "Unspecified"
		for _, p := range m.Gauge().DataPoints().All() {
			if !e.checkpoint() {
				return
			}
			e.appendNumber(spec, p, insertedAt)
		}
	case pmetric.MetricTypeSum:
		spec := base
		spec.metricType, spec.temporality, spec.monotonic = "Sum", temporalityName(m.Sum().AggregationTemporality()), m.Sum().IsMonotonic()
		for _, p := range m.Sum().DataPoints().All() {
			if !e.checkpoint() {
				return
			}
			e.appendNumber(spec, p, insertedAt)
		}
	case pmetric.MetricTypeHistogram:
		temporality := temporalityName(m.Histogram().AggregationTemporality())
		for _, p := range m.Histogram().DataPoints().All() {
			if !e.checkpoint() {
				return
			}
			e.appendHistogram(base, temporality, p, insertedAt)
		}
	case pmetric.MetricTypeSummary:
		for _, p := range m.Summary().DataPoints().All() {
			if !e.checkpoint() {
				return
			}
			e.appendSummary(base, p, insertedAt)
		}
	}
}

func (e *metricsEncoder) appendNumber(spec seriesSpec, p pmetric.NumberDataPoint, insertedAt int64) {
	var v float64
	switch p.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		v = float64(p.IntValue())
	case pmetric.NumberDataPointValueTypeDouble:
		v = p.DoubleValue()
	}
	e.setPoint(p.Attributes())
	if !e.fits(spec, 1) {
		return
	}
	e.appendPoint(spec, typedKV{}, uint64(p.Timestamp()), v, uint32(p.Flags()), insertedAt)
}

func (e *metricsEncoder) appendHistogram(base seriesSpec, temporality string, p pmetric.HistogramDataPoint, insertedAt int64) {
	e.setPoint(p.Attributes())
	// Buckets, +Inf, count, sum, min and max.
	if !e.fits(base, p.ExplicitBounds().Len()+5) {
		return
	}
	ts, flags := uint64(p.Timestamp()), uint32(p.Flags())

	bucket := base
	bucket.name, bucket.metricType, bucket.temporality, bucket.monotonic = base.name+".bucket", "Histogram", temporality, true
	bounds, counts := p.ExplicitBounds(), p.BucketCounts()
	var cumulative uint64
	for i := range bounds.Len() {
		if !e.checkpoint() {
			return
		}
		if i < counts.Len() {
			cumulative += counts.At(i)
		}
		e.appendPoint(bucket, typedKV{key: "le", value: formatBound(bounds.At(i)), datatype: "Str", origin: "point"}, ts, float64(cumulative), flags, insertedAt)
	}
	e.appendPoint(bucket, typedKV{key: "le", value: "+Inf", datatype: "Str", origin: "point"}, ts, float64(p.Count()), flags, insertedAt)

	sum := base
	sum.metricType, sum.temporality, sum.monotonic = "Sum", temporality, true
	e.appendPoint(countSpec(sum, base.name), typedKV{}, ts, float64(p.Count()), flags, insertedAt)
	// Absent sum, min and max still get metadata rows, but no series (metrics §2).
	sum.name = base.name + ".sum"
	e.appendPoint(sum, typedKV{}, ts, valueOrNaN(p.HasSum(), p.Sum()), flags, insertedAt)
	gauge := base
	gauge.metricType, gauge.temporality = "Gauge", "Unspecified"
	gauge.name = base.name + ".min"
	e.appendPoint(gauge, typedKV{}, ts, valueOrNaN(p.HasMin(), p.Min()), flags, insertedAt)
	gauge.name = base.name + ".max"
	e.appendPoint(gauge, typedKV{}, ts, valueOrNaN(p.HasMax(), p.Max()), flags, insertedAt)
}

func (e *metricsEncoder) appendSummary(base seriesSpec, p pmetric.SummaryDataPoint, insertedAt int64) {
	e.setPoint(p.Attributes())
	if !e.fits(base, p.QuantileValues().Len()+2) {
		return
	}
	ts, flags := uint64(p.Timestamp()), uint32(p.Flags())
	sum := base
	sum.metricType, sum.temporality, sum.monotonic = "Sum", "Cumulative", true
	e.appendPoint(countSpec(sum, base.name), typedKV{}, ts, float64(p.Count()), flags, insertedAt)
	sum.name = base.name + ".sum"
	e.appendPoint(sum, typedKV{}, ts, p.Sum(), flags, insertedAt)
	quantile := base
	quantile.name, quantile.metricType, quantile.temporality = base.name+".quantile", "Summary", "Unspecified"
	for _, q := range p.QuantileValues().All() {
		if !e.checkpoint() {
			return
		}
		e.appendPoint(quantile, typedKV{key: "quantile", value: formatBound(q.Quantile()), datatype: "Str", origin: "point"}, ts, q.Value(), flags, insertedAt)
	}
}

func (e *metricsEncoder) setPoint(attrs pcommon.Map) {
	e.point = sortedByKey(typedStrings(e.point[:0], attrs, "point"))
	e.pointHash = fnvKVs(fnvString(fnvOffset64, "point"), e.point)
}

// appendPoint writes one sample of one derived series, its series row and its
// metadata. extra is the le/quantile label, if any.
func (e *metricsEncoder) appendPoint(spec seriesSpec, extra typedKV, ts uint64, value float64, flags uint32, insertedAt int64) {
	// Metadata follows the attribute maps. A series that is not written (NaN, or
	// an absent sum, min or max) gets only its resource and scope rows (metrics §5).
	e.addScopeMetadata(spec, ts)
	if math.IsNaN(value) {
		return
	}
	temporalityKV := typedKV{key: "__temporality__", value: spec.temporality, datatype: "Str", origin: "point"}
	for _, kv := range e.point {
		if extra.key == "" || kv.key != extra.key {
			e.addMetadata(spec, kv, ts)
		}
	}
	if extra.key != "" {
		e.addMetadata(spec, extra, ts)
	}
	e.addMetadata(spec, temporalityKV, ts)

	// The fingerprint covers each level apart, so the origin of an attribute and
	// values that labels shadow both tell series apart (metrics §4).
	h := fnvUint64(fnvUint64(fnvUint64(fnvOffset64, e.resourceHash), e.scopeHash), e.pointHash)
	h = fnvString(fnvString(h, spec.name), spec.temporality)
	if extra.key != "" {
		h = fnvString(fnvString(h, extra.key), extra.value)
	}
	fingerprint := h

	ms := int64(ts / 1e6)
	s := e.samples
	s.env.Append(e.env)
	s.temporality.Append(spec.temporality)
	s.metricName.Append(spec.name)
	s.fingerprint.Append(fingerprint)
	s.unixMilli.Append(ms)
	s.value.Append(value)
	s.flags.Append(flags)
	s.insertedAt.Append(insertedAt)
	e.expanded += sampleBytes

	row := seriesRow{fingerprint: fingerprint, hour: ms / tagBucketMillis * tagBucketMillis}
	if _, ok := e.seenSeries[row]; ok {
		return
	}
	e.seenSeries[row] = struct{}{}
	e.appendSeries(spec, fingerprint, row.hour, extra, temporalityKV, insertedAt)
}

func (e *metricsEncoder) appendSeries(spec seriesSpec, fingerprint uint64, hour int64, extra, temporality typedKV, insertedAt int64) {
	c := e.series
	c.env.Append(e.env)
	c.temporality.Append(spec.temporality)
	c.metricName.Append(spec.name)
	c.description.Append(spec.description)
	c.unit.Append(spec.unit)
	c.metricType.Append(spec.metricType)
	c.isMonotonic.Append(spec.monotonic)
	c.fingerprint.Append(fingerprint)
	c.unixMilli.Append(hour)

	// Of equal keys labels keep the first: generated, then resource, scope and
	// point (metrics §3).
	e.labels = append(e.labels[:0], typedKV{key: "__name__", value: spec.name}, temporality)
	if extra.key != "" {
		e.labels = append(e.labels, extra)
	}
	e.labels = append(e.labels, e.scope[:scopeGenerated]...)
	e.labels = append(e.labels, e.resource...)
	e.labels = append(e.labels, e.scope[scopeGenerated:]...)
	e.labels = append(e.labels, e.point...)
	slices.SortStableFunc(e.labels, func(a, b typedKV) int { return cmp.Compare(a.key, b.key) })
	e.labels = slices.CompactFunc(e.labels, func(a, b typedKV) bool { return a.key == b.key })
	b := append(e.buf[:0], '{')
	for i, kv := range e.labels {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendJSONLabel(b, kv.key)
		b = append(b, ':')
		b = appendJSONLabel(b, kv.value)
	}
	e.buf = append(b, '}')
	c.labels.AppendBytes(e.buf)

	for _, kv := range e.point {
		if extra.key != "" && kv.key == extra.key {
			continue
		}
		c.attrs.Keys.Append(kv.key)
		c.attrs.Values.Append(kv.value)
	}
	if extra.key != "" {
		c.attrs.Keys.Append(extra.key)
		c.attrs.Values.Append(extra.value)
	}
	c.attrs.Keys.Append(temporality.key)
	c.attrs.Values.Append(temporality.value)
	endRow(c.attrs)
	for _, kv := range e.scope {
		c.scopeAttrs.Keys.Append(kv.key)
		c.scopeAttrs.Values.Append(kv.value)
	}
	endRow(c.scopeAttrs)
	for _, kv := range e.resource {
		c.resourceAttrs.Keys.Append(kv.key)
		c.resourceAttrs.Values.Append(kv.value)
	}
	endRow(c.resourceAttrs)
	c.normalized.Append(false)
	c.insertedAt.Append(insertedAt)
	e.expanded += len(e.buf) + e.resourceBytes + e.scopeBytes + kvBytes(e.point) + len(extra.key) + len(extra.value) + spec.textBytes() + seriesOverhead
}

// addScopeMetadata adds the resource and scope metadata rows of spec, looking
// them up once per series and scope.
func (e *metricsEncoder) addScopeMetadata(spec seriesSpec, ts uint64) {
	g := metaGroup{name: spec.name, temporality: spec.temporality}
	if rows, ok := e.scopeMeta[g]; ok {
		ms := ts / 1e6
		for _, i := range rows {
			r := &e.metaRows[i]
			r.first, r.last = min(r.first, ms), max(r.last, ms)
		}
		return
	}
	rows := make([]int, 0, len(e.resource)+len(e.scope))
	for _, kv := range e.resource {
		rows = append(rows, e.addMetadata(spec, kv, ts))
	}
	for _, kv := range e.scope {
		rows = append(rows, e.addMetadata(spec, kv, ts))
	}
	e.scopeMeta[g] = rows
}

// addMetadata adds or widens the metadata row of kv and returns its index.
func (e *metricsEncoder) addMetadata(spec seriesSpec, kv typedKV, ts uint64) int {
	ms := ts / 1e6
	k := metaKey{temporality: spec.temporality, metricName: spec.name, attrName: kv.key, attrType: kv.origin, datatype: kv.datatype, value: kv.value}
	if i, ok := e.metaIndex[k]; ok {
		r := &e.metaRows[i]
		r.first, r.last = min(r.first, ms), max(r.last, ms)
		return i
	}
	i := len(e.metaRows)
	e.metaIndex[k] = i
	e.metaRows = append(e.metaRows, metaRow{spec: spec, key: k, first: ms, last: ms})
	e.expanded += len(k.attrName) + len(k.value) + spec.textBytes() + seriesOverhead
	return i
}

func (e *metricsEncoder) flushMetadata() {
	c := e.meta
	for _, r := range e.metaRows {
		c.temporality.Append(r.key.temporality)
		c.metricName.Append(r.key.metricName)
		c.description.Append(r.spec.description)
		c.unit.Append(r.spec.metaUnit)
		c.metricType.Append(r.spec.metricType)
		c.isMonotonic.Append(r.spec.monotonic)
		c.attrName.Append(r.key.attrName)
		c.attrType.Append(r.key.attrType)
		c.attrDatatype.Append(r.key.datatype)
		c.attrValue.Append(r.key.value)
		c.firstReported.Append(r.first)
		c.lastReported.Append(r.last)
	}
}

// typedStrings appends m's attributes, stringified, with their pdata type names.
// It drops the keys that the map of origin generates itself: __temporality__ in
// attrs and the __scope.*__ entries in scope_attrs (metrics §3).
func typedStrings(dst []typedKV, m pcommon.Map, origin string) []typedKV {
	for k, v := range m.All() {
		switch {
		case origin == "point" && k == "__temporality__",
			origin == "scope" && (k == "__scope.name__" || k == "__scope.schema_url__" || k == "__scope.version__"):
			continue
		}
		dst = append(dst, typedKV{key: k, value: valueString(v), datatype: v.Type().String(), origin: origin})
	}
	return dst
}

func kvBytes(kvs []typedKV) int {
	n := 0
	for _, kv := range kvs {
		n += len(kv.key) + len(kv.value)
	}
	return n
}

func sortedByKey(kvs []typedKV) []typedKV {
	slices.SortFunc(kvs, func(a, b typedKV) int { return cmp.Compare(a.key, b.key) })
	return kvs
}

// fnvString adds s and a 0xFF separator to the FNV-1a 64 hash h.
func fnvString(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime64
	}
	h ^= 0xFF
	return h * fnvPrime64
}

func fnvKVs(h uint64, kvs []typedKV) uint64 {
	for _, kv := range kvs {
		h = fnvString(fnvString(h, kv.key), kv.value)
	}
	return h
}

func fnvUint64(h, v uint64) uint64 {
	for range 8 {
		h ^= v & 0xFF
		h *= fnvPrime64
		v >>= 8
	}
	return h
}

// countSpec is the .count series: unit 1 in time_series_v4, the metric's unit
// (or 1 when empty) in metadata (metrics §5).
func countSpec(sum seriesSpec, name string) seriesSpec {
	count := sum
	count.name, count.unit = name+".count", "1"
	if count.metaUnit == "" {
		count.metaUnit = "1"
	}
	return count
}

func valueOrNaN(ok bool, v float64) float64 {
	if !ok {
		return math.NaN()
	}
	return v
}

func temporalityName(t pmetric.AggregationTemporality) string {
	switch t {
	case pmetric.AggregationTemporalityCumulative:
		return "Cumulative"
	case pmetric.AggregationTemporalityDelta:
		return "Delta"
	default:
		return "Unspecified"
	}
}

func formatBound(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}
