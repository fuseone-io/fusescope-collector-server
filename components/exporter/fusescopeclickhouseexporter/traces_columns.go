// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"github.com/ClickHouse/ch-go/proto"
)

type strMap = proto.ColMap[string, string]

func newLCStr() *proto.ColLowCardinality[string] { return new(proto.ColStr).LowCardinality() }

func newStrMap() *strMap {
	return proto.NewMap[string, string](newLCStr(), new(proto.ColStr))
}

func newNumMap() *proto.ColMap[string, float64] {
	return proto.NewMap[string, float64](newLCStr(), new(proto.ColFloat64))
}

func newBoolMap() *proto.ColMap[string, bool] {
	return proto.NewMap[string, bool](newLCStr(), new(proto.ColBool))
}

func endRow[K comparable, V any](m *proto.ColMap[K, V]) {
	m.Offsets.Append(uint64(m.Keys.Rows()))
}

// indexColumns is one block of signoz_index_v3 (§3).
type indexColumns struct {
	tsBucketStart       proto.ColUInt64
	resourceFingerprint proto.ColStr
	timestamp           *proto.ColDateTime64
	traceID             proto.ColFixedStr32
	spanID              proto.ColStr
	traceState          proto.ColStr
	parentSpanID        proto.ColStr
	flags               proto.ColUInt32
	name                *proto.ColLowCardinality[string]
	kind                proto.ColInt8
	kindString          proto.ColStr
	durationNano        proto.ColUInt64
	statusCode          proto.ColInt16
	statusMessage       proto.ColStr
	statusCodeString    proto.ColStr
	attributesString    *strMap
	attributesNumber    *proto.ColMap[string, float64]
	attributesBool      *proto.ColMap[string, bool]
	attributes          proto.ColJSONStr
	attributesPromoted  proto.ColJSONStr
	resourcesString     *strMap
	resource            proto.ColJSONStr
	scope               proto.ColJSONStr
	events              *proto.ColArr[string]
	links               proto.ColStr
	responseStatusCode  *proto.ColLowCardinality[string]
	externalHTTPURL     *proto.ColLowCardinality[string]
	httpURL             *proto.ColLowCardinality[string]
	externalHTTPMethod  *proto.ColLowCardinality[string]
	httpMethod          *proto.ColLowCardinality[string]
	httpHost            *proto.ColLowCardinality[string]
	dbName              *proto.ColLowCardinality[string]
	dbOperation         *proto.ColLowCardinality[string]
	hasError            proto.ColBool
	isRemote            *proto.ColLowCardinality[string]
	insertedAt          *proto.ColDateTime64
}

func newIndexColumns() *indexColumns {
	return &indexColumns{
		timestamp:          new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		name:               newLCStr(),
		attributesString:   newStrMap(),
		attributesNumber:   newNumMap(),
		attributesBool:     newBoolMap(),
		resourcesString:    newStrMap(),
		events:             new(proto.ColStr).Array(),
		responseStatusCode: newLCStr(),
		externalHTTPURL:    newLCStr(),
		httpURL:            newLCStr(),
		externalHTTPMethod: newLCStr(),
		httpMethod:         newLCStr(),
		httpHost:           newLCStr(),
		dbName:             newLCStr(),
		dbOperation:        newLCStr(),
		isRemote:           newLCStr(),
		insertedAt:         new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli),
	}
}

func (c *indexColumns) input() proto.Input {
	return proto.Input{
		{Name: "ts_bucket_start", Data: &c.tsBucketStart},
		{Name: "resource_fingerprint", Data: &c.resourceFingerprint},
		{Name: "timestamp", Data: c.timestamp},
		{Name: "trace_id", Data: &c.traceID},
		{Name: "span_id", Data: &c.spanID},
		{Name: "trace_state", Data: &c.traceState},
		{Name: "parent_span_id", Data: &c.parentSpanID},
		{Name: "flags", Data: &c.flags},
		{Name: "name", Data: c.name},
		{Name: "kind", Data: &c.kind},
		{Name: "kind_string", Data: &c.kindString},
		{Name: "duration_nano", Data: &c.durationNano},
		{Name: "status_code", Data: &c.statusCode},
		{Name: "status_message", Data: &c.statusMessage},
		{Name: "status_code_string", Data: &c.statusCodeString},
		{Name: "attributes_string", Data: c.attributesString},
		{Name: "attributes_number", Data: c.attributesNumber},
		{Name: "attributes_bool", Data: c.attributesBool},
		{Name: "attributes", Data: &c.attributes},
		{Name: "attributes_promoted", Data: &c.attributesPromoted},
		{Name: "resources_string", Data: c.resourcesString},
		{Name: "resource", Data: &c.resource},
		{Name: "scope", Data: &c.scope},
		{Name: "events", Data: c.events},
		{Name: "links", Data: &c.links},
		{Name: "response_status_code", Data: c.responseStatusCode},
		{Name: "external_http_url", Data: c.externalHTTPURL},
		{Name: "http_url", Data: c.httpURL},
		{Name: "external_http_method", Data: c.externalHTTPMethod},
		{Name: "http_method", Data: c.httpMethod},
		{Name: "http_host", Data: c.httpHost},
		{Name: "db_name", Data: c.dbName},
		{Name: "db_operation", Data: c.dbOperation},
		{Name: "has_error", Data: &c.hasError},
		{Name: "is_remote", Data: c.isRemote},
		{Name: "inserted_at", Data: c.insertedAt},
	}
}

// errorColumns is one block of signoz_error_index_v2 (§4).
type errorColumns struct {
	timestamp           *proto.ColDateTime64
	errorID             proto.ColFixedStr32
	groupID             proto.ColFixedStr32
	traceID             proto.ColFixedStr32
	spanID              proto.ColStr
	serviceName         *proto.ColLowCardinality[string]
	exceptionType       *proto.ColLowCardinality[string]
	exceptionMessage    proto.ColStr
	exceptionStacktrace proto.ColStr
	exceptionEscaped    proto.ColBool
	resourceTagsMap     *strMap
}

func newErrorColumns() *errorColumns {
	return &errorColumns{
		timestamp:       new(proto.ColDateTime64).WithPrecision(proto.PrecisionNano),
		serviceName:     newLCStr(),
		exceptionType:   newLCStr(),
		resourceTagsMap: newStrMap(),
	}
}

func (c *errorColumns) input() proto.Input {
	return proto.Input{
		{Name: "timestamp", Data: c.timestamp},
		{Name: "errorID", Data: &c.errorID},
		{Name: "groupID", Data: &c.groupID},
		{Name: "traceID", Data: &c.traceID},
		{Name: "spanID", Data: &c.spanID},
		{Name: "serviceName", Data: c.serviceName},
		{Name: "exceptionType", Data: c.exceptionType},
		{Name: "exceptionMessage", Data: &c.exceptionMessage},
		{Name: "exceptionStacktrace", Data: &c.exceptionStacktrace},
		{Name: "exceptionEscaped", Data: &c.exceptionEscaped},
		{Name: "resourceTagsMap", Data: c.resourceTagsMap},
	}
}

// resourceColumns is one block of traces_v3_resource (§5).
type resourceColumns struct {
	labels      proto.ColStr
	fingerprint proto.ColStr
	seenAt      proto.ColInt64
}

func (c *resourceColumns) input() proto.Input {
	return proto.Input{
		{Name: "labels", Data: &c.labels},
		{Name: "fingerprint", Data: &c.fingerprint},
		{Name: "seen_at_ts_bucket_start", Data: &c.seenAt},
	}
}

// tagColumns is one block of tag_attributes_v2 (§6).
type tagColumns struct {
	unixMilli   proto.ColInt64
	tagKey      proto.ColStr
	tagType     *proto.ColLowCardinality[string]
	dataType    *proto.ColLowCardinality[string]
	stringValue proto.ColStr
	numberValue *proto.ColNullable[float64]
}

func newTagColumns() *tagColumns {
	return &tagColumns{
		tagType:     newLCStr(),
		dataType:    newLCStr(),
		numberValue: new(proto.ColFloat64).Nullable(),
	}
}

func (c *tagColumns) input() proto.Input {
	return proto.Input{
		{Name: "unix_milli", Data: &c.unixMilli},
		{Name: "tag_key", Data: &c.tagKey},
		{Name: "tag_type", Data: c.tagType},
		{Name: "tag_data_type", Data: c.dataType},
		{Name: "string_value", Data: &c.stringValue},
		{Name: "number_value", Data: c.numberValue},
	}
}

// keyColumns is one block of span_attributes_keys (§7).
type keyColumns struct {
	tagKey    *proto.ColLowCardinality[string]
	tagType   proto.ColEnum
	dataType  proto.ColEnum
	isColumn  proto.ColBool
	timestamp proto.ColDateTime
}

func newKeyColumns() *keyColumns {
	return &keyColumns{tagKey: newLCStr()}
}

func (c *keyColumns) input() proto.Input {
	return proto.Input{
		{Name: "tagKey", Data: c.tagKey},
		{Name: "tagType", Data: &c.tagType},
		{Name: "dataType", Data: &c.dataType},
		{Name: "isColumn", Data: &c.isColumn},
		{Name: "timestamp", Data: &c.timestamp},
	}
}
