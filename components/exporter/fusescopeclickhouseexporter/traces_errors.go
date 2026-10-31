// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"crypto/md5" //nolint:gosec // groupID is md5 by contract (§4), not a security boundary
	"encoding/hex"
	"strconv"

	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// appendErrors writes one signoz_error_index_v2 row per exception event (§4).
func (e *tracesEncoder) appendErrors(res *resourceInfo, span ptrace.Span) {
	for _, ev := range span.Events().All() {
		if ev.Name() != "exception" {
			continue
		}
		attrs := ev.Attributes()
		typ := stringAttr(attrs, "exception.type")
		msg := stringAttr(attrs, "exception.message")

		c := e.errors
		c.timestamp.AppendRaw(proto.DateTime64(ev.Timestamp()))
		c.errorID.Append(e.newErrorID())
		c.groupID.Append(groupID(e.buf[:0], res.serviceName, typ, msg))
		c.traceID.Append(traceHex(span.TraceID()))
		e.buf = appendSpanHex(e.buf[:0], span.SpanID())
		c.spanID.AppendBytes(e.buf)
		c.serviceName.Append(res.serviceName)
		c.exceptionType.Append(typ)
		c.exceptionMessage.Append(msg)
		c.exceptionStacktrace.Append(stringAttr(attrs, "exception.stacktrace"))
		c.exceptionEscaped.Append(boolAttr(attrs, "exception.escaped"))
		for _, kv := range res.attrs {
			c.resourceTagsMap.Keys.Append(kv.key)
			c.resourceTagsMap.Values.Append(kv.value)
		}
		endRow(c.resourceTagsMap)
	}
}

func groupID(buf []byte, service, typ, msg string) (h [32]byte) {
	buf = append(buf, service...)
	buf = append(buf, typ...)
	buf = append(buf, msg...)
	sum := md5.Sum(buf) //nolint:gosec // see import
	hex.Encode(h[:], sum[:])
	return h
}

// newErrorID returns a random UUIDv4 as 32 hex digits.
func (e *tracesEncoder) newErrorID() (h [32]byte) {
	var u [16]byte
	e.random(u[:])
	u[6] = u[6]&0x0f | 0x40
	u[8] = u[8]&0x3f | 0x80
	hex.Encode(h[:], u[:])
	return h
}

func stringAttr(m pcommon.Map, key string) string {
	if v, ok := m.Get(key); ok {
		return v.AsString()
	}
	return ""
}

func boolAttr(m pcommon.Map, key string) bool {
	v, ok := m.Get(key)
	if !ok {
		return false
	}
	if v.Type() == pcommon.ValueTypeBool {
		return v.Bool()
	}
	b, _ := strconv.ParseBool(v.AsString())
	return b
}
