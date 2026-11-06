// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"cmp"
	"slices"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// meterColumns is one block of signoz_meter.samples (meter §6).
type meterColumns struct {
	temporality *proto.ColLowCardinality[string]
	metricName  *proto.ColLowCardinality[string]
	description *proto.ColLowCardinality[string]
	unit        *proto.ColLowCardinality[string]
	metricType  *proto.ColLowCardinality[string]
	isMonotonic proto.ColBool
	labels      proto.ColStr
	fingerprint proto.ColUInt64
	unixMilli   proto.ColInt64
	value       proto.ColFloat64
}

func newMeterColumns() *meterColumns {
	return &meterColumns{temporality: newLCStr(), metricName: newLCStr(), description: newLCStr(), unit: newLCStr(), metricType: newLCStr()}
}

func (c *meterColumns) input() proto.Input {
	return proto.Input{
		{Name: "temporality", Data: c.temporality},
		{Name: "metric_name", Data: c.metricName},
		{Name: "description", Data: c.description},
		{Name: "unit", Data: c.unit},
		{Name: "type", Data: c.metricType},
		{Name: "is_monotonic", Data: &c.isMonotonic},
		{Name: "labels", Data: &c.labels},
		{Name: "fingerprint", Data: &c.fingerprint},
		{Name: "unix_milli", Data: &c.unixMilli},
		{Name: "value", Data: &c.value},
	}
}

var meterTables = []table[*meterEncoder]{
	{"distributed_samples", func(e *meterEncoder) proto.Input { return e.rows.input() }},
}

// meterEncoder turns the usage metrics of fusescope_meter into samples rows,
// one per number point, with the labels and fingerprint of a metrics series
// (clickhouse-metrics §3, §4). It is reused across batches and is not safe
// for concurrent use.
type meterEncoder struct {
	rows     *meterColumns
	resource []typedKV
	scope    []typedKV
	point    []typedKV
	labels   []typedKV
	buf      []byte
	logger   *zap.Logger

	resourceHash, scopeHash uint64
}

func newMeterEncoder() *meterEncoder {
	return &meterEncoder{rows: newMeterColumns(), logger: zap.NewNop()}
}

// setAttempt is a no-op: a meter row depends on nothing but its point.
func (*meterEncoder) setAttempt(time.Time, func([]byte)) {}

// encode builds one block of md's rows; a meter flush is small, so emit is
// never called.
func (e *meterEncoder) encode(md pmetric.Metrics, _ func() error) error {
	e.rows.input().Reset()
	skipped := 0
	for _, rm := range md.ResourceMetrics().All() {
		e.resource = sortedByKey(typedStrings(e.resource[:0], rm.Resource().Attributes(), "resource"))
		e.resourceHash = fnvKVs(fnvString(fnvOffset64, "resource"), e.resource)
		for _, sm := range rm.ScopeMetrics().All() {
			scope := sm.Scope()
			e.scope = append(e.scope[:0],
				typedKV{key: "__scope.name__", value: scope.Name()},
				typedKV{key: "__scope.schema_url__", value: sm.SchemaUrl()},
				typedKV{key: "__scope.version__", value: scope.Version()})
			e.scope = typedStrings(e.scope, scope.Attributes(), "scope")
			sortedByKey(e.scope[scopeGenerated:])
			e.scopeHash = fnvKVs(fnvString(fnvOffset64, "scope"), e.scope)
			for _, m := range sm.Metrics().All() {
				switch m.Type() {
				case pmetric.MetricTypeSum:
					s := m.Sum()
					e.appendPoints(m, temporalityName(s.AggregationTemporality()), s.IsMonotonic(), s.DataPoints())
				case pmetric.MetricTypeGauge:
					e.appendPoints(m, "Unspecified", false, m.Gauge().DataPoints())
				default:
					skipped++
				}
			}
		}
	}
	if skipped > 0 {
		e.logger.Warn("dropped meter metrics that are not sums or gauges", zap.Int("metrics", skipped))
	}
	return nil
}

func (e *meterEncoder) appendPoints(m pmetric.Metric, temporality string, monotonic bool, points pmetric.NumberDataPointSlice) {
	c := e.rows
	for _, p := range points.All() {
		e.point = sortedByKey(typedStrings(e.point[:0], p.Attributes(), "point"))
		pointHash := fnvKVs(fnvString(fnvOffset64, "point"), e.point)
		h := fnvUint64(fnvUint64(fnvUint64(fnvOffset64, e.resourceHash), e.scopeHash), pointHash)
		h = fnvString(fnvString(h, m.Name()), temporality)

		// Of equal keys labels keep the first: generated, then resource, scope
		// and point (clickhouse-metrics §3).
		e.labels = append(e.labels[:0], typedKV{key: "__name__", value: m.Name()}, typedKV{key: "__temporality__", value: temporality})
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

		value := p.DoubleValue()
		if p.ValueType() == pmetric.NumberDataPointValueTypeInt {
			value = float64(p.IntValue())
		}
		c.temporality.Append(temporality)
		c.metricName.Append(m.Name())
		c.description.Append(m.Description())
		c.unit.Append(m.Unit())
		c.metricType.Append(m.Type().String())
		c.isMonotonic.Append(monotonic)
		c.labels.AppendBytes(e.buf)
		c.fingerprint.Append(h)
		c.unixMilli.Append(int64(p.Timestamp()) / 1e6)
		c.value.Append(value)
	}
}
