// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"crypto/rand"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const (
	tsBucketSeconds = 1800
	tagBucketMillis = 3_600_000
	maxTagValueLen  = 256
)

type tagType uint8

const (
	tagSpan tagType = iota + 1
	tagResource
	tagScope
	tagSpanField
)

var tagTypeNames = [...]string{tagSpan: "tag", tagResource: "resource", tagScope: "scope", tagSpanField: "spanfield"}

type tagRow struct {
	hour     int64
	key      string
	tagType  tagType
	dataType dataType
	str      string
	num      float64
}

type keyRow struct {
	key      string
	tagType  tagType
	dataType dataType
}

type resourceRow struct {
	fingerprint string
	bucket      int64
}

// tracesEncoder turns ptrace.Traces into one column block per table. It is
// reused across batches and is not safe for concurrent use.
type tracesEncoder struct {
	index      *indexColumns
	eventsData *proto.ColStr
	errors     *errorColumns
	resources  resourceColumns
	tags       *tagColumns
	keys       *keyColumns

	seenResources map[resourceRow]struct{}
	seenTags      map[tagRow]struct{}
	seenKeys      map[keyRow]struct{}

	buf    []byte
	kvs    []keyValue
	now    func() time.Time
	random func([]byte)
}

func newTracesEncoder() *tracesEncoder {
	e := &tracesEncoder{
		index:         newIndexColumns(),
		eventsData:    new(proto.ColStr),
		errors:        newErrorColumns(),
		tags:          newTagColumns(),
		keys:          newKeyColumns(),
		seenResources: map[resourceRow]struct{}{},
		seenTags:      map[tagRow]struct{}{},
		seenKeys:      map[keyRow]struct{}{},
		now:           time.Now,
		random:        func(b []byte) { _, _ = rand.Read(b) },
	}
	e.index.events = proto.NewArray[string](e.eventsData)
	return e
}

func (e *tracesEncoder) reset() {
	e.index.input().Reset()
	e.errors.input().Reset()
	e.resources.input().Reset()
	e.tags.input().Reset()
	e.keys.input().Reset()
	clear(e.seenResources)
	clear(e.seenTags)
	clear(e.seenKeys)
}

func (e *tracesEncoder) encode(td ptrace.Traces) {
	e.reset()
	now := e.now()
	for _, rs := range td.ResourceSpans().All() {
		var res resourceInfo
		res, e.buf = newResourceInfo(rs.Resource(), e.buf)
		for _, kv := range res.attrs {
			e.addKey(kv.key, tagResource, dataString, now)
		}
		for _, ss := range rs.ScopeSpans().All() {
			scope := ss.Scope()
			scopeJSON := e.scopeJSON(scope)
			e.addKey("scope.name", tagScope, dataString, now)
			e.addKey("scope.version", tagScope, dataString, now)
			walkLeaves("", scope.Attributes(), func(k string, v pcommon.Value) {
				e.addKey(k, tagScope, leafType(v), now)
			})
			for _, span := range ss.Spans().All() {
				e.appendSpan(&res, scope, scopeJSON, span, now)
			}
		}
	}
}

func (e *tracesEncoder) scopeJSON(scope pcommon.InstrumentationScope) string {
	b := append(e.buf[:0], `{"name":`...)
	b = appendJSONString(b, scope.Name())
	b = append(b, `,"version":`...)
	b = appendJSONString(b, scope.Version())
	b = append(b, `,"attributes":`...)
	b = appendAttributesJSON(b, scope.Attributes())
	b = append(b, '}')
	e.buf = b
	return string(b)
}

func (e *tracesEncoder) appendSpan(res *resourceInfo, scope pcommon.InstrumentationScope, scopeJSON string, span ptrace.Span, now time.Time) {
	start := uint64(span.StartTimestamp())
	bucket := int64(start/1e9) / tsBucketSeconds * tsBucketSeconds
	hour := int64(start/1e6) / tagBucketMillis * tagBucketMillis
	d := deriveSpan(span)

	if _, ok := e.seenResources[resourceRow{res.fingerprint, bucket}]; !ok {
		e.seenResources[resourceRow{res.fingerprint, bucket}] = struct{}{}
		e.resources.labels.Append(res.labels)
		e.resources.fingerprint.Append(res.fingerprint)
		e.resources.seenAt.Append(bucket)
	}
	for _, kv := range res.attrs {
		e.addStringTag(hour, kv.key, tagResource, kv.value)
	}
	e.addStringTag(hour, "scope.name", tagScope, scope.Name())
	e.addStringTag(hour, "scope.version", tagScope, scope.Version())
	walkLeaves("", scope.Attributes(), func(k string, v pcommon.Value) {
		e.addValueTag(hour, k, tagScope, v)
	})

	e.appendIndexRow(res, scopeJSON, span, &d, rowTimes{bucket: uint64(bucket), hour: hour, now: now})
	e.appendSpanFieldTags(hour, span, &d)
	e.appendErrors(res, span)
}

func (e *tracesEncoder) addKey(key string, t tagType, dt dataType, now time.Time) {
	k := keyRow{key: key, tagType: t, dataType: dt}
	if _, ok := e.seenKeys[k]; ok {
		return
	}
	e.seenKeys[k] = struct{}{}
	e.keys.tagKey.Append(key)
	e.keys.tagType.Append(tagTypeNames[t])
	e.keys.dataType.Append(dt.String())
	e.keys.isColumn.Append(false)
	e.keys.timestamp.Append(now)
}

func (e *tracesEncoder) addValueTag(hour int64, key string, t tagType, v pcommon.Value) {
	switch dt := leafType(v); dt {
	case dataFloat64:
		e.addTag(tagRow{hour: hour, key: key, tagType: t, dataType: dt, num: asFloat(v)})
	case dataBool:
		e.addTag(tagRow{hour: hour, key: key, tagType: t, dataType: dt})
	default:
		s := v.AsString()
		if t != tagSpan && s == "" {
			return
		}
		e.addTag(tagRow{hour: hour, key: key, tagType: t, dataType: dt, str: s})
	}
}

func (e *tracesEncoder) addStringTag(hour int64, key string, t tagType, s string) {
	if s == "" {
		return
	}
	e.addTag(tagRow{hour: hour, key: key, tagType: t, dataType: dataString, str: s})
}

func (e *tracesEncoder) addNumberTag(hour int64, key string, n float64) {
	e.addTag(tagRow{hour: hour, key: key, tagType: tagSpanField, dataType: dataFloat64, num: n})
}

func (e *tracesEncoder) addTag(r tagRow) {
	if len(r.str) > maxTagValueLen {
		return
	}
	if _, ok := e.seenTags[r]; ok {
		return
	}
	e.seenTags[r] = struct{}{}
	e.tags.unixMilli.Append(r.hour)
	e.tags.tagKey.Append(r.key)
	e.tags.tagType.Append(tagTypeNames[r.tagType])
	e.tags.dataType.Append(r.dataType.String())
	e.tags.stringValue.Append(r.str)
	if r.dataType == dataFloat64 {
		e.tags.numberValue.Append(proto.NewNullable(r.num))
	} else {
		e.tags.numberValue.Append(proto.Null[float64]())
	}
}
