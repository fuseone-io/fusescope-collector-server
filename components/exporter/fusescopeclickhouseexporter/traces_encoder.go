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
// for the caller. It returns the first error of emit. Chunks hold whole
// traces: the service-call view joins a parent and its child within one insert
// (traces §1). Only a trace larger than a chunk is split.
func (e *tracesEncoder) encode(td ptrace.Traces, emit func() error) error {
	e.reset()
	e.begin(emit)
	chunkOf, chunks := e.planChunks(td)
	for chunk := range chunks {
		if chunk > 0 {
			if e.err = e.emit(); e.err != nil {
				return e.err
			}
			e.reset()
			e.expanded = 0
		}
		if !e.encodeChunk(td, chunkOf, chunk) {
			return e.err
		}
	}
	return nil
}

// copied is what one span copies: resources_string and resource each hold the
// resource, scope_string the scope.
func copied(resourceLabels, scopeJSON string) int {
	return 2*len(resourceLabels) + len(scopeJSON)
}

// planChunks assigns each trace to a chunk, in order of first appearance, when
// td does not fit one chunk; chunkOf is nil when it does.
func (e *tracesEncoder) planChunks(td ptrace.Traces) (chunkOf map[pcommon.TraceID]int, chunks int) {
	if e.emit == nil {
		return nil, 1
	}
	total := 0
	e.spanCopies(td, func(_ pcommon.TraceID, n int) { total += n })
	if total <= e.maxChunkBytes {
		return nil, 1
	}
	size := map[pcommon.TraceID]int{}
	var order []pcommon.TraceID
	e.spanCopies(td, func(id pcommon.TraceID, n int) {
		if _, ok := size[id]; !ok {
			order = append(order, id)
		}
		size[id] += n
	})
	chunkOf = make(map[pcommon.TraceID]int, len(order))
	used := 0
	for _, id := range order {
		if used > 0 && used+size[id] > e.maxChunkBytes {
			chunks++
			used = 0
		}
		used += size[id]
		chunkOf[id] = chunks
	}
	return chunkOf, chunks + 1
}

// spanCopies calls f with each span's trace and the bytes its row copies, as
// counted by the chunker.
func (e *tracesEncoder) spanCopies(td ptrace.Traces, f func(pcommon.TraceID, int)) {
	for _, rs := range td.ResourceSpans().All() {
		var res resourceInfo
		res, e.buf = newResourceInfo(rs.Resource(), e.buf)
		for _, ss := range rs.ScopeSpans().All() {
			n := copied(res.labels, e.scopeJSON(ss.Scope())) + rowOverhead
			for _, span := range ss.Spans().All() {
				f(span.TraceID(), n)
			}
		}
	}
}

// encodeChunk appends the spans of chunk, or all spans when chunkOf is nil.
// It reports whether encoding may go on.
func (e *tracesEncoder) encodeChunk(td ptrace.Traces, chunkOf map[pcommon.TraceID]int, chunk int) bool {
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
				if chunkOf != nil && chunkOf[span.TraceID()] != chunk {
					continue
				}
				e.appendSpan(&res, scope, scopeJSON, span, now)
				// A trace larger than a chunk is split here.
				if !e.add(copied(res.labels, scopeJSON), e.reset) {
					return false
				}
			}
		}
	}
	return true
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
