// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"encoding/hex"
	"net/url"
	"strconv"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var kindNames = [...]string{"Unspecified", "Internal", "Server", "Client", "Producer", "Consumer"}

var statusNames = [...]string{"Unset", "Ok", "Error"}

type rowTimes struct {
	bucket uint64
	hour   int64
	now    time.Time
}

// spanDerived holds the denormalised columns of §3. For each column the
// candidate key that comes last in attribute order wins.
type spanDerived struct {
	method, url, host, statusCode, dbName, dbOperation string
	externalMethod, externalURL                        string
	// oldURL is http.url, which the host fallback prefers (§3).
	oldURL string
}

func deriveSpan(span ptrace.Span) spanDerived {
	var d spanDerived
	for k, v := range span.Attributes().All() {
		switch k {
		case "http.request.method", "http.method":
			d.method = valueString(v)
		case "url.full", "http.url":
			d.url = valueString(v)
			if k == "http.url" {
				d.oldURL = d.url
			}
		case "server.address", "http.host", "net.peer.name":
			d.host = valueString(v)
		case "http.response.status_code", "http.status_code":
			d.statusCode = valueString(v)
		case "db.namespace", "db.name":
			d.dbName = valueString(v)
		case "db.operation.name", "db.operation":
			d.dbOperation = valueString(v)
		}
	}
	// Only client spans are external calls: they get the external columns,
	// with or without a method, and a host from the URL when no host key has
	// one, http.url first.
	if span.Kind() != ptrace.SpanKindClient {
		return d
	}
	d.externalMethod = d.method
	if d.url != "" {
		d.externalURL = hostname(d.url)
	}
	if d.host == "" {
		if d.oldURL != "" {
			d.host = hostname(d.oldURL)
		} else {
			d.host = hostname(d.url)
		}
	}
	return d
}

func hostname(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

func kindName(k ptrace.SpanKind) string {
	if int(k) >= 0 && int(k) < len(kindNames) {
		return kindNames[k]
	}
	return kindNames[0]
}

func statusName(c ptrace.StatusCode) string {
	if int(c) >= 0 && int(c) < len(statusNames) {
		return statusNames[c]
	}
	return statusNames[0]
}

func isRemote(flags uint32) string {
	const hasIsRemote, remote = 0x100, 0x200
	switch {
	case flags&hasIsRemote == 0:
		return "unknown"
	case flags&remote != 0:
		return "yes"
	default:
		return "no"
	}
}

func traceHex(id pcommon.TraceID) (h [32]byte) {
	hex.Encode(h[:], id[:])
	return h
}

func appendSpanHex(dst []byte, id pcommon.SpanID) []byte {
	if id.IsEmpty() {
		return dst
	}
	return hex.AppendEncode(dst, id[:])
}

func (e *tracesEncoder) appendIndexRow(res *resourceInfo, scopeJSON string, span ptrace.Span, d *spanDerived, t rowTimes) {
	c := e.index
	traceID := traceHex(span.TraceID())
	c.tsBucketStart.Append(t.bucket)
	c.resourceFingerprint.Append(res.fingerprint)
	c.timestamp.AppendRaw(proto.DateTime64(span.StartTimestamp()))
	c.traceID.Append(traceID)
	e.buf = appendSpanHex(e.buf[:0], span.SpanID())
	c.spanID.AppendBytes(e.buf)
	c.traceState.Append(span.TraceState().AsRaw())
	e.buf = appendSpanHex(e.buf[:0], span.ParentSpanID())
	c.parentSpanID.AppendBytes(e.buf)
	c.flags.Append(span.Flags())
	c.name.Append(span.Name())
	c.kind.Append(int8(span.Kind()))
	c.kindString.Append(kindName(span.Kind()))
	c.durationNano.Append(uint64(span.EndTimestamp()) - uint64(span.StartTimestamp()))
	c.statusCode.Append(int16(span.Status().Code()))
	c.statusMessage.Append(span.Status().Message())
	c.statusCodeString.Append(statusName(span.Status().Code()))

	e.appendSpanAttributes(span.Attributes(), t)
	e.buf = appendAttributesJSON(e.buf[:0], span.Attributes())
	c.attributes.AppendBytes(e.buf)
	c.attributesPromoted.Append("{}")
	for _, kv := range res.attrs {
		c.resourcesString.Keys.Append(kv.key)
		c.resourcesString.Values.Append(kv.value)
	}
	endRow(c.resourcesString)
	c.resource.Append(res.labels)
	c.scope.Append(scopeJSON)

	for _, ev := range span.Events().All() {
		e.buf = e.appendEventJSON(e.buf[:0], ev)
		e.eventsData.AppendBytes(e.buf)
	}
	c.events.Offsets.Append(uint64(e.eventsData.Rows()))
	e.buf = appendLinksJSON(e.buf[:0], traceID, span)
	c.links.AppendBytes(e.buf)

	c.responseStatusCode.Append(d.statusCode)
	c.externalHTTPURL.Append(d.externalURL)
	c.httpURL.Append(d.url)
	c.externalHTTPMethod.Append(d.externalMethod)
	c.httpMethod.Append(d.method)
	c.httpHost.Append(d.host)
	c.dbName.Append(d.dbName)
	c.dbOperation.Append(d.dbOperation)
	c.hasError.Append(span.Status().Code() == ptrace.StatusCodeError)
	c.isRemote.Append(isRemote(span.Flags()))
	c.insertedAt.AppendRaw(proto.DateTime64(t.now.UnixMilli()))
}

func (e *tracesEncoder) appendSpanAttributes(m pcommon.Map, t rowTimes) {
	c := e.index
	walkLeaves("", m, func(k string, v pcommon.Value) {
		// The reference drops NaN and infinite span attributes everywhere (traces §2.2).
		if isNumberNaNOrInf(v) {
			return
		}
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
		e.addKey(k, tagAttribute, leafType(v))
		e.tags.addValue(t.hour, k, tagAttribute, v)
	})
	endRow(c.attributesString)
	endRow(c.attributesNumber)
	endRow(c.attributesBool)
}

func (e *tracesEncoder) appendSpanFieldTags(hour int64, span ptrace.Span, d *spanDerived) {
	e.tags.addString(hour, "name", tagSpanField, span.Name())
	e.tags.addNumber(hour, "kind", tagSpanField, float64(span.Kind()))
	e.tags.addString(hour, "kind_string", tagSpanField, kindName(span.Kind()))
	e.tags.addNumber(hour, "status_code", tagSpanField, float64(span.Status().Code()))
	e.tags.addString(hour, "status_code_string", tagSpanField, statusName(span.Status().Code()))
	e.tags.addString(hour, "http_url", tagSpanField, d.url)
	e.tags.addString(hour, "http_method", tagSpanField, d.method)
	e.tags.addString(hour, "http_host", tagSpanField, d.host)
	e.tags.addString(hour, "external_http_url", tagSpanField, d.externalURL)
	e.tags.addString(hour, "external_http_method", tagSpanField, d.externalMethod)
	e.tags.addString(hour, "db_name", tagSpanField, d.dbName)
	e.tags.addString(hour, "db_operation", tagSpanField, d.dbOperation)
	e.tags.addString(hour, "response_status_code", tagSpanField, d.statusCode)
	e.tags.addString(hour, "is_remote", tagSpanField, isRemote(span.Flags()))
	e.tags.add(tagRow{hour: hour, key: "has_error", tagType: tagSpanField, dataType: dataBool})
}

func (e *tracesEncoder) appendEventJSON(dst []byte, ev ptrace.SpanEvent) []byte {
	dst = append(dst, `{"name":`...)
	dst = appendJSONString(dst, ev.Name())
	dst = append(dst, `,"timeUnixNano":`...)
	dst = strconv.AppendUint(dst, uint64(ev.Timestamp()), 10)
	if ev.Attributes().Len() > 0 {
		e.kvs = sortedStrings(e.kvs[:0], ev.Attributes())
		dst = append(dst, `,"attributeMap":{`...)
		for i, kv := range e.kvs {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendJSONString(dst, kv.key)
			dst = append(dst, ':')
			dst = appendJSONString(dst, kv.value)
		}
		dst = append(dst, '}')
	}
	if ev.Name() == "exception" {
		dst = append(dst, `,"isError":true`...)
	}
	return append(dst, '}')
}

func appendLinksJSON(dst []byte, traceID [32]byte, span ptrace.Span) []byte {
	dst = append(dst, `[{"traceId":"`...)
	dst = append(dst, traceID[:]...)
	dst = append(dst, '"')
	if !span.ParentSpanID().IsEmpty() {
		dst = append(dst, `,"spanId":"`...)
		dst = appendSpanHex(dst, span.ParentSpanID())
		dst = append(dst, '"')
	}
	dst = append(dst, `,"refType":"CHILD_OF"}`...)
	for _, l := range span.Links().All() {
		dst = append(dst, `,{"traceId":"`...)
		lt := traceHex(l.TraceID())
		dst = append(dst, lt[:]...)
		dst = append(dst, '"')
		if !l.SpanID().IsEmpty() {
			dst = append(dst, `,"spanId":"`...)
			dst = appendSpanHex(dst, l.SpanID())
			dst = append(dst, '"')
		}
		dst = append(dst, `,"refType":"FOLLOWS_FROM"}`...)
	}
	return append(dst, ']')
}
