// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// logsEncoder turns plog.Logs into one column block per table. It is reused
// across batches and is not safe for concurrent use.
type logsEncoder struct {
	rows      *logColumns
	resources *resourceSet
	tags      *tagSet
	attrKeys  *nameTypeColumns
	resKeys   *nameTypeColumns

	buf    []byte
	scope  []keyValue
	now    func() time.Time
	random func([]byte)
}

func newLogsEncoder() *logsEncoder {
	return &logsEncoder{
		rows:      newLogColumns(),
		resources: newResourceSet(),
		tags:      newTagSet(),
		attrKeys:  newNameTypeColumns(),
		resKeys:   newNameTypeColumns(),
		now:       time.Now,
		random:    func(b []byte) { _, _ = rand.Read(b) },
	}
}

func (e *logsEncoder) reset() {
	e.rows.input().Reset()
	e.resources.reset()
	e.tags.reset()
	e.attrKeys.reset()
	e.resKeys.reset()
}

func (e *logsEncoder) encode(ld plog.Logs) {
	e.reset()
	now := e.now()
	hour := hourBucket(uint64(now.UnixNano()))
	for _, rl := range ld.ResourceLogs().All() {
		var res resourceInfo
		res, e.buf = newResourceInfo(rl.Resource(), e.buf)
		for _, kv := range res.attrs {
			e.resKeys.add(kv.key, dataString)
			e.tags.addString(hour, kv.key, tagResource, kv.value)
		}
		for _, sl := range rl.ScopeLogs().All() {
			scope := sl.Scope()
			e.scope = sortedStrings(e.scope[:0], scope.Attributes())
			for _, kv := range e.scope {
				e.tags.addString(hour, kv.key, tagScope, kv.value)
			}
			for _, lr := range sl.LogRecords().All() {
				e.appendRecord(&res, scope, lr, now, hour)
			}
		}
	}
}

func (e *logsEncoder) appendRecord(res *resourceInfo, scope pcommon.InstrumentationScope, lr plog.LogRecord, now time.Time, hour int64) {
	ts, observed := uint64(lr.Timestamp()), uint64(lr.ObservedTimestamp())
	if observed == 0 {
		observed = uint64(now.UnixNano())
	}
	if ts == 0 {
		ts = observed
	}
	bucket := tsBucket(ts)
	e.resources.add(res, bucket)

	c := e.rows
	c.tsBucketStart.Append(uint64(bucket))
	c.resourceFingerprint.Append(res.fingerprint)
	c.timestamp.Append(ts)
	c.observedTimestamp.Append(observed)
	id := e.newLogID(ts)
	c.id.AppendBytes(id[:])
	e.buf = appendTraceHex(e.buf[:0], lr.TraceID())
	c.traceID.AppendBytes(e.buf)
	e.buf = appendSpanHex(e.buf[:0], lr.SpanID())
	c.spanID.AppendBytes(e.buf)
	c.traceFlags.Append(uint32(lr.Flags()))
	c.severityText.Append(lr.SeverityText())
	c.severityNumber.Append(uint8(lr.SeverityNumber()))
	c.body.Append(logBody(lr.Body()))
	c.bodyV2.Append("{}")
	c.bodyPromoted.Append("{}")

	for k, v := range lr.Attributes().All() {
		switch dt := leafType(v); dt {
		case dataFloat64:
			c.attributesNumber.Keys.Append(k)
			c.attributesNumber.Values.Append(asFloat(v))
		case dataBool:
			c.attributesBool.Keys.Append(k)
			c.attributesBool.Values.Append(v.Bool())
		default:
			c.attributesString.Keys.Append(k)
			c.attributesString.Values.Append(valueString(v))
		}
		e.attrKeys.add(k, leafType(v))
		e.tags.addValue(hour, k, tagAttribute, v)
	}
	endRow(c.attributesString)
	endRow(c.attributesNumber)
	endRow(c.attributesBool)

	for _, kv := range res.attrs {
		c.resourcesString.Keys.Append(kv.key)
		c.resourcesString.Values.Append(kv.value)
	}
	endRow(c.resourcesString)
	c.resource.Append(res.labels)
	c.scopeName.Append(scope.Name())
	c.scopeVersion.Append(scope.Version())
	for _, kv := range e.scope {
		c.scopeString.Keys.Append(kv.key)
		c.scopeString.Values.Append(kv.value)
	}
	endRow(c.scopeString)
	c.insertedAt.AppendRaw(proto.DateTime64(now.UnixMilli()))

	e.tags.add(tagRow{hour: hour, key: "severity_text", tagType: tagLogField, dataType: dataString, str: lr.SeverityText()})
	e.tags.addNumber(hour, "severity_number", tagLogField, float64(lr.SeverityNumber()))
}

// logBody stores bytes raw, unlike attributes, which store them as base64 (logs §3).
func logBody(v pcommon.Value) string {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return v.Str()
	case pcommon.ValueTypeBytes:
		return string(v.Bytes().AsRaw())
	default:
		return valueString(v)
	}
}

func appendTraceHex(dst []byte, id pcommon.TraceID) []byte {
	if id.IsEmpty() {
		return dst
	}
	return hex.AppendEncode(dst, id[:])
}

const (
	logIDEpochNanos = 1_400_000_000_000_000_000
	base62Alphabet  = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

// newLogID returns the 27-character, time-ordered id of logs §3.
func (e *logsEncoder) newLogID(ts uint64) [27]byte {
	var raw [20]byte
	if ts > logIDEpochNanos {
		binary.BigEndian.PutUint64(raw[:8], ts-logIDEpochNanos)
	}
	e.random(raw[8:])
	return base62(raw)
}

func base62(src [20]byte) (dst [27]byte) {
	var parts [5]uint32
	for i := range parts {
		parts[i] = binary.BigEndian.Uint32(src[i*4:])
	}
	for i := len(dst) - 1; i >= 0; i-- {
		var rem uint64
		for j := range parts {
			acc := rem<<32 | uint64(parts[j])
			parts[j] = uint32(acc / 62)
			rem = acc % 62
		}
		dst[i] = base62Alphabet[rem]
	}
	return dst
}
