// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"github.com/ClickHouse/ch-go/proto"
)

// sampleColumns is one block of samples_v4 (metrics §1).
type sampleColumns struct {
	env         *proto.ColLowCardinality[string]
	temporality *proto.ColLowCardinality[string]
	metricName  *proto.ColLowCardinality[string]
	fingerprint proto.ColUInt64
	unixMilli   proto.ColInt64
	value       proto.ColFloat64
	flags       proto.ColUInt32
	insertedAt  proto.ColInt64
}

func newSampleColumns() *sampleColumns {
	return &sampleColumns{env: newLCStr(), temporality: newLCStr(), metricName: newLCStr()}
}

func (c *sampleColumns) input() proto.Input {
	return proto.Input{
		{Name: "env", Data: c.env},
		{Name: "temporality", Data: c.temporality},
		{Name: "metric_name", Data: c.metricName},
		{Name: "fingerprint", Data: &c.fingerprint},
		{Name: "unix_milli", Data: &c.unixMilli},
		{Name: "value", Data: &c.value},
		{Name: "flags", Data: &c.flags},
		{Name: "inserted_at_unix_milli", Data: &c.insertedAt},
	}
}

// seriesColumns is one block of time_series_v4 (metrics §1).
type seriesColumns struct {
	env           *proto.ColLowCardinality[string]
	temporality   *proto.ColLowCardinality[string]
	metricName    *proto.ColLowCardinality[string]
	description   *proto.ColLowCardinality[string]
	unit          *proto.ColLowCardinality[string]
	metricType    *proto.ColLowCardinality[string]
	isMonotonic   proto.ColBool
	fingerprint   proto.ColUInt64
	unixMilli     proto.ColInt64
	labels        proto.ColStr
	attrs         *strMap
	scopeAttrs    *strMap
	resourceAttrs *strMap
	normalized    proto.ColBool
	insertedAt    proto.ColInt64
}

func newSeriesColumns() *seriesColumns {
	return &seriesColumns{
		env: newLCStr(), temporality: newLCStr(), metricName: newLCStr(), description: newLCStr(),
		unit: newLCStr(), metricType: newLCStr(),
		attrs: newStrMap(), scopeAttrs: newStrMap(), resourceAttrs: newStrMap(),
	}
}

func (c *seriesColumns) input() proto.Input {
	return proto.Input{
		{Name: "env", Data: c.env},
		{Name: "temporality", Data: c.temporality},
		{Name: "metric_name", Data: c.metricName},
		{Name: "description", Data: c.description},
		{Name: "unit", Data: c.unit},
		{Name: "type", Data: c.metricType},
		{Name: "is_monotonic", Data: &c.isMonotonic},
		{Name: "fingerprint", Data: &c.fingerprint},
		{Name: "unix_milli", Data: &c.unixMilli},
		{Name: "labels", Data: &c.labels},
		{Name: "attrs", Data: c.attrs},
		{Name: "scope_attrs", Data: c.scopeAttrs},
		{Name: "resource_attrs", Data: c.resourceAttrs},
		{Name: "__normalized", Data: &c.normalized},
		{Name: "inserted_at_unix_milli", Data: &c.insertedAt},
	}
}

// metadataColumns is one block of metadata (metrics §5).
type metadataColumns struct {
	temporality   *proto.ColLowCardinality[string]
	metricName    *proto.ColLowCardinality[string]
	description   proto.ColStr
	unit          *proto.ColLowCardinality[string]
	metricType    *proto.ColLowCardinality[string]
	isMonotonic   proto.ColBool
	attrName      *proto.ColLowCardinality[string]
	attrType      *proto.ColLowCardinality[string]
	attrDatatype  *proto.ColLowCardinality[string]
	attrValue     proto.ColStr
	firstReported proto.ColUInt64
	lastReported  proto.ColUInt64
}

func newMetadataColumns() *metadataColumns {
	return &metadataColumns{
		temporality: newLCStr(), metricName: newLCStr(), unit: newLCStr(), metricType: newLCStr(),
		attrName: newLCStr(), attrType: newLCStr(), attrDatatype: newLCStr(),
	}
}

func (c *metadataColumns) input() proto.Input {
	return proto.Input{
		{Name: "temporality", Data: c.temporality},
		{Name: "metric_name", Data: c.metricName},
		{Name: "description", Data: &c.description},
		{Name: "unit", Data: c.unit},
		{Name: "type", Data: c.metricType},
		{Name: "is_monotonic", Data: &c.isMonotonic},
		{Name: "attr_name", Data: c.attrName},
		{Name: "attr_type", Data: c.attrType},
		{Name: "attr_datatype", Data: c.attrDatatype},
		{Name: "attr_string_value", Data: &c.attrValue},
		{Name: "first_reported_unix_milli", Data: &c.firstReported},
		{Name: "last_reported_unix_milli", Data: &c.lastReported},
	}
}
