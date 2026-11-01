// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"github.com/ClickHouse/ch-go/proto"
)

// logColumns is one block of logs_v2 (logs §3).
type logColumns struct {
	tsBucketStart       proto.ColUInt64
	resourceFingerprint proto.ColStr
	timestamp           proto.ColUInt64
	observedTimestamp   proto.ColUInt64
	id                  proto.ColStr
	traceID             proto.ColStr
	spanID              proto.ColStr
	traceFlags          proto.ColUInt32
	severityText        *proto.ColLowCardinality[string]
	severityNumber      proto.ColUInt8
	body                proto.ColStr
	bodyV2              proto.ColJSONStr
	bodyPromoted        proto.ColJSONStr
	attributesString    *strMap
	attributesNumber    *proto.ColMap[string, float64]
	attributesBool      *proto.ColMap[string, bool]
	resourcesString     *strMap
	resource            proto.ColJSONStr
	scopeName           proto.ColStr
	scopeVersion        proto.ColStr
	scopeString         *strMap
	insertedAt          *proto.ColDateTime64
}

func newLogColumns() *logColumns {
	return &logColumns{
		severityText:     newLCStr(),
		attributesString: newStrMap(),
		attributesNumber: newNumMap(),
		attributesBool:   newBoolMap(),
		resourcesString:  newStrMap(),
		scopeString:      newStrMap(),
		insertedAt:       new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli),
	}
}

func (c *logColumns) input() proto.Input {
	return proto.Input{
		{Name: "ts_bucket_start", Data: &c.tsBucketStart},
		{Name: "resource_fingerprint", Data: &c.resourceFingerprint},
		{Name: "timestamp", Data: &c.timestamp},
		{Name: "observed_timestamp", Data: &c.observedTimestamp},
		{Name: "id", Data: &c.id},
		{Name: "trace_id", Data: &c.traceID},
		{Name: "span_id", Data: &c.spanID},
		{Name: "trace_flags", Data: &c.traceFlags},
		{Name: "severity_text", Data: c.severityText},
		{Name: "severity_number", Data: &c.severityNumber},
		{Name: "body", Data: &c.body},
		{Name: "body_v2", Data: &c.bodyV2},
		{Name: "body_promoted", Data: &c.bodyPromoted},
		{Name: "attributes_string", Data: c.attributesString},
		{Name: "attributes_number", Data: c.attributesNumber},
		{Name: "attributes_bool", Data: c.attributesBool},
		{Name: "resources_string", Data: c.resourcesString},
		{Name: "resource", Data: &c.resource},
		{Name: "scope_name", Data: &c.scopeName},
		{Name: "scope_version", Data: &c.scopeVersion},
		{Name: "scope_string", Data: c.scopeString},
		{Name: "inserted_at", Data: c.insertedAt},
	}
}

// nameTypeColumns is one block of logs_attribute_keys or logs_resource_keys.
type nameTypeColumns struct {
	name     proto.ColStr
	datatype proto.ColStr
	seen     map[keyRow]struct{}
}

func newNameTypeColumns() *nameTypeColumns {
	return &nameTypeColumns{seen: map[keyRow]struct{}{}}
}

func (c *nameTypeColumns) input() proto.Input {
	return proto.Input{
		{Name: "name", Data: &c.name},
		{Name: "datatype", Data: &c.datatype},
	}
}

func (c *nameTypeColumns) reset() {
	c.input().Reset()
	clear(c.seen)
}

func (c *nameTypeColumns) add(name string, dt dataType) {
	k := keyRow{key: name, dataType: dt}
	if _, ok := c.seen[k]; ok {
		return
	}
	c.seen[k] = struct{}{}
	c.name.Append(name)
	c.datatype.Append(dt.String())
}
