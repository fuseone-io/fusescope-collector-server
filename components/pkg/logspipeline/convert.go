// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package logspipeline // import "github.com/fuseone-io/fusescope-collector-server/components/pkg/logspipeline"

import (
	"fmt"
	"reflect"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/adapter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func toEntry(r plog.LogRecord, now time.Time) *entry.Entry {
	e := &entry.Entry{
		SeverityText: r.SeverityText(),
		Attributes:   r.Attributes().AsRaw(),
		Body:         r.Body().AsRaw(),
	}
	if n := r.SeverityNumber(); n >= plog.SeverityNumberTrace && n <= plog.SeverityNumberFatal4 {
		e.Severity = entry.Severity(n)
	}
	if ts := r.Timestamp(); ts != 0 {
		e.Timestamp = ts.AsTime()
	}
	if ts := r.ObservedTimestamp(); ts != 0 {
		e.ObservedTimestamp = ts.AsTime()
	} else {
		e.ObservedTimestamp = now
	}
	if id := r.TraceID(); !id.IsEmpty() {
		e.TraceID = id[:]
	}
	if id := r.SpanID(); !id.IsEmpty() {
		e.SpanID = id[:]
	}
	if flags := r.Flags(); flags != 0 {
		e.TraceFlags = []byte{byte(flags)}
	}
	return e
}

// logsBuilder groups entries by resource and then by scope name, in the
// order each group first appears (spec §5.2), as they come out of the
// pipeline, so no more than one record's entries are held at a time.
type logsBuilder struct {
	ld     plog.Logs
	count  int
	groups map[uint64]*resourceGroup
	// lastResource and lastKey skip rehashing a resource map shared by
	// consecutive entries.
	lastResource map[string]any
	lastKey      uint64
}

type resourceGroup struct {
	rl     plog.ResourceLogs
	scopes map[string]plog.ScopeLogs
}

func newLogsBuilder() *logsBuilder {
	return &logsBuilder{ld: plog.NewLogs(), groups: map[uint64]*resourceGroup{}}
}

func (b *logsBuilder) add(e *entry.Entry) {
	var key uint64
	if b.lastResource != nil && sameMap(b.lastResource, e.Resource) {
		key = b.lastKey
	} else {
		key = adapter.HashResource(e.Resource)
		b.lastResource, b.lastKey = e.Resource, key
	}
	g, ok := b.groups[key]
	if !ok {
		g = &resourceGroup{rl: b.ld.ResourceLogs().AppendEmpty(), scopes: map[string]plog.ScopeLogs{}}
		putMap(e.Resource, g.rl.Resource().Attributes())
		b.groups[key] = g
	}
	sl, ok := g.scopes[e.ScopeName]
	if !ok {
		sl = g.rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName(e.ScopeName)
		g.scopes[e.ScopeName] = sl
	}
	fromEntry(e, sl.LogRecords().AppendEmpty())
	b.count++
}

// sameMap reports whether a and b are the same map, not equal ones.
func sameMap(a, b map[string]any) bool {
	return b != nil && reflect.ValueOf(a).UnsafePointer() == reflect.ValueOf(b).UnsafePointer()
}

func fromEntry(e *entry.Entry, r plog.LogRecord) {
	if !e.Timestamp.IsZero() {
		r.SetTimestamp(pcommon.NewTimestampFromTime(e.Timestamp))
	}
	r.SetObservedTimestamp(pcommon.NewTimestampFromTime(e.ObservedTimestamp))
	if e.Severity >= entry.Trace && e.Severity <= entry.Fatal4 {
		r.SetSeverityNumber(plog.SeverityNumber(e.Severity))
		if e.SeverityText == "" {
			// entry.Severity's names are OTLP's short names (INFO, INFO2, ...).
			r.SetSeverityText(e.Severity.String())
		}
	}
	if e.SeverityText != "" {
		r.SetSeverityText(e.SeverityText)
	}
	putMap(e.Attributes, r.Attributes())
	if e.Body != nil {
		putValue(e.Body, r.Body())
	}
	if e.TraceID != nil {
		var id pcommon.TraceID
		copy(id[:], rightAligned(e.TraceID, len(id)))
		r.SetTraceID(id)
	}
	if e.SpanID != nil {
		var id pcommon.SpanID
		copy(id[:], rightAligned(e.SpanID, len(id)))
		r.SetSpanID(id)
	}
	if len(e.TraceFlags) > 0 {
		r.SetFlags(plog.LogRecordFlags(e.TraceFlags[0]))
	}
}

// rightAligned places an ID shorter than size at the end, as a hex ID
// missing its leading zeros (spec §5.2).
func rightAligned(id []byte, size int) []byte {
	if len(id) >= size {
		return id
	}
	aligned := make([]byte, size)
	copy(aligned[size-len(id):], id)
	return aligned
}

func putMap(m map[string]any, dest pcommon.Map) {
	dest.EnsureCapacity(len(m))
	for k, v := range m {
		putValue(v, dest.PutEmpty(k))
	}
}

// putValue writes value into dest; nil becomes the string "<nil>" and
// unknown types their fmt %v form (spec §5.2).
func putValue(value any, dest pcommon.Value) {
	switch v := value.(type) {
	case nil:
		dest.SetStr("<nil>")
	case string:
		dest.SetStr(v)
	case bool:
		dest.SetBool(v)
	case int:
		dest.SetInt(int64(v))
	case int8:
		dest.SetInt(int64(v))
	case int16:
		dest.SetInt(int64(v))
	case int32:
		dest.SetInt(int64(v))
	case int64:
		dest.SetInt(v)
	case uint:
		dest.SetInt(int64(v))
	case uint8:
		dest.SetInt(int64(v))
	case uint16:
		dest.SetInt(int64(v))
	case uint32:
		dest.SetInt(int64(v))
	case uint64:
		dest.SetInt(int64(v))
	case float32:
		dest.SetDouble(float64(v))
	case float64:
		dest.SetDouble(v)
	case []byte:
		dest.SetEmptyBytes().FromRaw(v)
	case map[string]any:
		putMap(v, dest.SetEmptyMap())
	case []any:
		s := dest.SetEmptySlice()
		s.EnsureCapacity(len(v))
		for _, item := range v {
			putValue(item, s.AppendEmpty())
		}
	case []string:
		s := dest.SetEmptySlice()
		s.EnsureCapacity(len(v))
		for _, item := range v {
			s.AppendEmpty().SetStr(item)
		}
	default:
		dest.SetStr(fmt.Sprintf("%v", v))
	}
}
