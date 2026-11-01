// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"crypto/rand"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
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
	logger *zap.Logger

	// Each resource and its scopes of the batch being encoded (derive).
	resInfos   []resourceInfo
	scopeJSONs [][]string
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
		logger:     zap.NewNop(),
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

// spanRef locates a span of the batch being encoded.
type spanRef struct{ resource, scope, span int }

// encode builds the rows of td, inserting a chunk through emit whenever the
// copies of resources and scopes exceed maxChunkBytes; the last chunk is left
// for the caller. It returns the first error of emit. Chunks hold whole
// traces, since the service-call and sub-root views join a parent and its
// child within one insert (traces §1); a trace larger than a chunk is split,
// with a warning.
func (e *tracesEncoder) encode(td ptrace.Traces, emit func() error) error {
	e.reset()
	e.begin(emit)
	e.derive(td)
	members := e.planChunks(td)
	if members == nil {
		e.encodeSpans(td, nil)
		return e.err
	}
	for chunk, refs := range members {
		if chunk > 0 {
			if e.err = e.emit(); e.err != nil {
				return e.err
			}
			e.reset()
			e.expanded = 0
		}
		if !e.encodeSpans(td, refs) {
			return e.err
		}
	}
	return nil
}

// derive computes each resource and scope once per batch, for every chunk.
func (e *tracesEncoder) derive(td ptrace.Traces) {
	e.resInfos, e.scopeJSONs = e.resInfos[:0], e.scopeJSONs[:0]
	for _, rs := range td.ResourceSpans().All() {
		var res resourceInfo
		res, e.buf = newResourceInfo(rs.Resource(), e.buf)
		e.resInfos = append(e.resInfos, res)
		var scopes []string
		for _, ss := range rs.ScopeSpans().All() {
			scopes = append(scopes, e.scopeJSON(ss.Scope()))
		}
		e.scopeJSONs = append(e.scopeJSONs, scopes)
	}
}

// copied is what one span's row copies: resources_string and resource each
// hold the resource, scope_string the scope.
func (e *tracesEncoder) copied(resource, scope int) int {
	return 2*len(e.resInfos[resource].labels) + len(e.scopeJSONs[resource][scope])
}

// planChunks returns the spans of each chunk, whole traces in order of first
// appearance, or nil when td fits one chunk.
func (e *tracesEncoder) planChunks(td ptrace.Traces) [][]spanRef {
	if e.emit == nil {
		return nil
	}
	total := 0
	for r, rs := range td.ResourceSpans().All() {
		for sc, ss := range rs.ScopeSpans().All() {
			total += ss.Spans().Len() * (e.copied(r, sc) + rowOverhead)
		}
	}
	if total <= e.maxChunkBytes {
		return nil
	}
	type trace struct{ size, chunk int }
	traces := map[pcommon.TraceID]*trace{}
	var order []*trace
	for r, rs := range td.ResourceSpans().All() {
		for sc, ss := range rs.ScopeSpans().All() {
			n := e.copied(r, sc) + rowOverhead
			for _, span := range ss.Spans().All() {
				t := traces[span.TraceID()]
				if t == nil {
					t = &trace{}
					traces[span.TraceID()] = t
					order = append(order, t)
				}
				t.size += n
			}
		}
	}
	chunk, used, oversized := 0, 0, 0
	for _, t := range order {
		if used > 0 && used+t.size > e.maxChunkBytes {
			chunk++
			used = 0
		}
		used += t.size
		t.chunk = chunk
		if t.size > e.maxChunkBytes {
			oversized++
		}
	}
	if oversized > 0 {
		e.logger.Warn("traces larger than an insert chunk are split; their service-call edges and sub-root operations may be missing",
			zap.Int("traces", oversized), zap.Int("chunk_bytes", e.maxChunkBytes))
	}
	members := make([][]spanRef, chunk+1)
	for r, rs := range td.ResourceSpans().All() {
		for sc, ss := range rs.ScopeSpans().All() {
			for i, span := range ss.Spans().All() {
				c := traces[span.TraceID()].chunk
				members[c] = append(members[c], spanRef{r, sc, i})
			}
		}
	}
	return members
}

// encodeSpans appends the spans refs points to, or all spans when refs is nil.
// It reports whether encoding may go on.
func (e *tracesEncoder) encodeSpans(td ptrace.Traces, refs []spanRef) bool {
	now := e.now()
	all := td.ResourceSpans()
	lastResource, lastScope := -1, -1
	visit := func(r, sc int, span ptrace.Span) bool {
		res := &e.resInfos[r]
		rs := all.At(r)
		ss := rs.ScopeSpans().At(sc)
		if r != lastResource {
			for _, kv := range res.attrs {
				e.addKey(kv.key, tagResource, dataString)
			}
			lastResource, lastScope = r, -1
		}
		if sc != lastScope {
			e.addKey("scope.name", tagScope, dataString)
			e.addKey("scope.version", tagScope, dataString)
			walkLeaves("", ss.Scope().Attributes(), func(k string, v pcommon.Value) {
				e.addKey(k, tagScope, leafType(v))
			})
			lastScope = sc
		}
		scopeJSON := e.scopeJSONs[r][sc]
		e.appendSpan(res, ss.Scope(), scopeJSON, span, now)
		// A trace larger than a chunk is split here.
		return e.add(e.copied(r, sc), e.reset)
	}
	if refs != nil {
		for _, ref := range refs {
			span := all.At(ref.resource).ScopeSpans().At(ref.scope).Spans().At(ref.span)
			if !visit(ref.resource, ref.scope, span) {
				return false
			}
		}
		return true
	}
	for r, rs := range all.All() {
		for sc, ss := range rs.ScopeSpans().All() {
			for _, span := range ss.Spans().All() {
				if !visit(r, sc, span) {
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
