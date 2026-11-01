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
)

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

// metricsEncoder turns pmetric.Metrics into one column block per table. It is
// reused across batches and is not safe for concurrent use.
type metricsEncoder struct {
	samples *sampleColumns
	series  *seriesColumns
	meta    *metadataColumns

	seenSeries map[seriesRow]struct{}
	metaIndex  map[metaKey]int
	metaRows   []metaRow

	resource []typedKV
	scope    []typedKV
	point    []typedKV
	labels   []typedKV
	buf      []byte
	env      string
	now      func() time.Time
}

func newMetricsEncoder() *metricsEncoder {
	return &metricsEncoder{
		samples:    newSampleColumns(),
		series:     newSeriesColumns(),
		meta:       newMetadataColumns(),
		seenSeries: map[seriesRow]struct{}{},
		metaIndex:  map[metaKey]int{},
		now:        time.Now,
	}
}

func (e *metricsEncoder) reset() {
	e.samples.input().Reset()
	e.series.input().Reset()
	e.meta.input().Reset()
	clear(e.seenSeries)
	clear(e.metaIndex)
	e.metaRows = e.metaRows[:0]
}

func (e *metricsEncoder) encode(md pmetric.Metrics) {
	e.reset()
	insertedAt := e.now().UnixMilli()
	for _, rm := range md.ResourceMetrics().All() {
		e.resource = typedStrings(e.resource[:0], rm.Resource().Attributes(), "resource")
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
			for _, m := range sm.Metrics().All() {
				e.appendMetric(m, insertedAt)
			}
		}
	}
	e.flushMetadata()
}

func (e *metricsEncoder) appendMetric(m pmetric.Metric, insertedAt int64) {
	base := seriesSpec{name: m.Name(), unit: m.Unit(), metaUnit: m.Unit(), description: m.Description()}
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		spec := base
		spec.metricType, spec.temporality = "Gauge", "Unspecified"
		for _, p := range m.Gauge().DataPoints().All() {
			e.appendNumber(spec, p, insertedAt)
		}
	case pmetric.MetricTypeSum:
		spec := base
		spec.metricType, spec.temporality, spec.monotonic = "Sum", temporalityName(m.Sum().AggregationTemporality()), m.Sum().IsMonotonic()
		for _, p := range m.Sum().DataPoints().All() {
			e.appendNumber(spec, p, insertedAt)
		}
	case pmetric.MetricTypeHistogram:
		temporality := temporalityName(m.Histogram().AggregationTemporality())
		for _, p := range m.Histogram().DataPoints().All() {
			e.appendHistogram(base, temporality, p, insertedAt)
		}
	case pmetric.MetricTypeSummary:
		for _, p := range m.Summary().DataPoints().All() {
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
	e.point = typedStrings(e.point[:0], p.Attributes(), "point")
	e.appendPoint(spec, typedKV{}, uint64(p.Timestamp()), v, uint32(p.Flags()), insertedAt)
}

func (e *metricsEncoder) appendHistogram(base seriesSpec, temporality string, p pmetric.HistogramDataPoint, insertedAt int64) {
	e.point = typedStrings(e.point[:0], p.Attributes(), "point")
	ts, flags := uint64(p.Timestamp()), uint32(p.Flags())

	bucket := base
	bucket.name, bucket.metricType, bucket.temporality, bucket.monotonic = base.name+".bucket", "Histogram", temporality, true
	bounds, counts := p.ExplicitBounds(), p.BucketCounts()
	var cumulative uint64
	for i := range bounds.Len() {
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
	e.point = typedStrings(e.point[:0], p.Attributes(), "point")
	ts, flags := uint64(p.Timestamp()), uint32(p.Flags())
	sum := base
	sum.metricType, sum.temporality, sum.monotonic = "Sum", "Cumulative", true
	e.appendPoint(countSpec(sum, base.name), typedKV{}, ts, float64(p.Count()), flags, insertedAt)
	sum.name = base.name + ".sum"
	e.appendPoint(sum, typedKV{}, ts, p.Sum(), flags, insertedAt)
	quantile := base
	quantile.name, quantile.metricType, quantile.temporality = base.name+".quantile", "Summary", "Unspecified"
	for _, q := range p.QuantileValues().All() {
		e.appendPoint(quantile, typedKV{key: "quantile", value: formatBound(q.Quantile()), datatype: "Str", origin: "point"}, ts, q.Value(), flags, insertedAt)
	}
}

// appendPoint writes one sample of one derived series, its series row and its
// metadata. extra is the le/quantile label, if any.
func (e *metricsEncoder) appendPoint(spec seriesSpec, extra typedKV, ts uint64, value float64, flags uint32, insertedAt int64) {
	temporalityKV := typedKV{key: "__temporality__", value: spec.temporality, datatype: "Str", origin: "point"}
	e.labels = append(e.labels[:0], e.resource...)
	e.labels = append(e.labels, e.scope...)
	e.labels = append(e.labels, e.point...)
	if extra.key != "" {
		e.labels = append(e.labels, extra)
	}
	for _, kv := range e.labels {
		e.addMetadata(spec, kv, ts)
	}
	// A series that is not written (NaN, or an absent sum, min or max) still
	// gets attribute metadata, but no __temporality__ row (metrics §5).
	if math.IsNaN(value) {
		return
	}
	e.addMetadata(spec, temporalityKV, ts)
	e.labels = append(e.labels, temporalityKV, typedKV{key: "__name__", value: spec.name})
	slices.SortStableFunc(e.labels, func(a, b typedKV) int { return cmp.Compare(a.key, b.key) })
	e.labels = slices.CompactFunc(e.labels, func(a, b typedKV) bool { return a.key == b.key })
	fingerprint := typedHash(e.labels)

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

	b := append(e.buf[:0], '{')
	for i, kv := range e.labels {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendJSONString(b, kv.key)
		b = append(b, ':')
		b = appendJSONString(b, kv.value)
	}
	e.buf = append(b, '}')
	c.labels.AppendBytes(e.buf)

	for _, kv := range e.point {
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
}

func (e *metricsEncoder) addMetadata(spec seriesSpec, kv typedKV, ts uint64) {
	ms := ts / 1e6
	k := metaKey{temporality: spec.temporality, metricName: spec.name, attrName: kv.key, attrType: kv.origin, datatype: kv.datatype, value: kv.value}
	if i, ok := e.metaIndex[k]; ok {
		r := &e.metaRows[i]
		r.first, r.last = min(r.first, ms), max(r.last, ms)
		return
	}
	e.metaIndex[k] = len(e.metaRows)
	e.metaRows = append(e.metaRows, metaRow{spec: spec, key: k, first: ms, last: ms})
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
func typedStrings(dst []typedKV, m pcommon.Map, origin string) []typedKV {
	for k, v := range m.All() {
		dst = append(dst, typedKV{key: k, value: valueString(v), datatype: v.Type().String(), origin: origin})
	}
	return dst
}

func typedHash(sorted []typedKV) uint64 {
	h := uint64(fnvOffset64)
	add := func(s string) {
		for i := 0; i < len(s); i++ {
			h ^= uint64(s[i])
			h *= fnvPrime64
		}
		h ^= 0xFF
		h *= fnvPrime64
	}
	for _, kv := range sorted {
		add(kv.key)
		add(kv.value)
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
