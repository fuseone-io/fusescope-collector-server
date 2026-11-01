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

type keyRow struct {
	key      string
	tagType  tagType
	dataType dataType
}

// tracesEncoder turns ptrace.Traces into one column block per table. It is
// reused across batches and is not safe for concurrent use.
type tracesEncoder struct {
	chunker

	index      *indexColumns
	eventsData *proto.ColStr
	errors     *errorColumns
	resources  *resourceSet
	tags       *tagSet
	keys       *keyColumns

	seenKeys map[keyRow]struct{}

	buf    []byte
	kvs    []keyValue
	now    func() time.Time
	random func([]byte)
}

// setAttempt fixes the ingest time and the random IDs of the next encode.
func (e *tracesEncoder) setAttempt(now time.Time, random func([]byte)) {
	e.now = func() time.Time { return now }
	e.random = random
}

func newTracesEncoder() *tracesEncoder {
	e := &tracesEncoder{
		chunker:    newChunker(),
		index:      newIndexColumns(),
		eventsData: new(proto.ColStr),
		errors:     newErrorColumns(),
		resources:  newResourceSet(),
		tags:       newTagSet(),
		keys:       newKeyColumns(),
		seenKeys:   map[keyRow]struct{}{},
		now:        time.Now,
		random:     func(b []byte) { _, _ = rand.Read(b) },
	}
	e.index.events = proto.NewArray[string](e.eventsData)
	return e
}

func (e *tracesEncoder) reset() {
	e.index.input().Reset()
	e.errors.input().Reset()
	e.resources.reset()
	e.tags.reset()
	e.keys.input().Reset()
	clear(e.seenKeys)
}

// encode builds the rows of td, inserting a chunk through emit whenever the
// copies of resources and scopes exceed maxChunkBytes; the last chunk is left
// for the caller. It returns the first error of emit.
func (e *tracesEncoder) encode(td ptrace.Traces, emit func() error) error {
	e.reset()
	e.begin(emit)
	now := e.now()
	for _, rs := range td.ResourceSpans().All() {
		var res resourceInfo
		res, e.buf = newResourceInfo(rs.Resource(), e.buf)
		for _, kv := range res.attrs {
			e.addKey(kv.key, tagResource, dataString)
		}
		for _, ss := range rs.ScopeSpans().All() {
			scope := ss.Scope()
			scopeJSON := e.scopeJSON(scope)
			e.addKey("scope.name", tagScope, dataString)
			e.addKey("scope.version", tagScope, dataString)
			walkLeaves("", scope.Attributes(), func(k string, v pcommon.Value) {
				e.addKey(k, tagScope, leafType(v))
			})
			for _, span := range ss.Spans().All() {
				e.appendSpan(&res, scope, scopeJSON, span, now)
				// resources_string and resource each copy the resource, scope_string the scope.
				if !e.add(2*len(res.labels)+len(scopeJSON), e.reset) {
					return e.err
				}
			}
		}
	}
	return nil
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
	bucket := tsBucket(start)
	hour := hourBucket(start)
	d := deriveSpan(span)

	e.resources.add(res, bucket)
	for _, kv := range res.attrs {
		e.tags.addString(hour, kv.key, tagResource, kv.value)
	}
	e.tags.addString(hour, "scope.name", tagScope, scope.Name())
	e.tags.addString(hour, "scope.version", tagScope, scope.Version())
	walkLeaves("", scope.Attributes(), func(k string, v pcommon.Value) {
		e.tags.addValue(hour, k, tagScope, v)
	})

	e.appendIndexRow(res, scopeJSON, span, &d, rowTimes{bucket: uint64(bucket), hour: hour, now: now})
	e.appendSpanFieldTags(hour, span, &d)
	e.appendErrors(res, span)
}

func (e *tracesEncoder) addKey(key string, t tagType, dt dataType) {
	k := keyRow{key: key, tagType: t, dataType: dt}
	if _, ok := e.seenKeys[k]; ok {
		return
	}
	e.seenKeys[k] = struct{}{}
	e.keys.tagKey.Append(key)
	e.keys.tagType.Append(tagTypeNames[t])
	e.keys.dataType.Append(dt.String())
	e.keys.isColumn.Append(false)
}
