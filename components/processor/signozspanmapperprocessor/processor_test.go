// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozspanmapperprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/fuseone-io/fusescope-collector-server/components/processor/signozspanmapperprocessor/internal/metadata"
)

// kv is an ordered attribute list; order is part of the contract (spec O1, O2).
type kv [][2]any

func putAll(m pcommon.Map, attrs kv) {
	for _, a := range attrs {
		if err := m.PutEmpty(a[0].(string)).FromRaw(a[1]); err != nil {
			panic(err)
		}
	}
}

func ordered(m pcommon.Map) kv {
	out := kv{}
	for k, v := range m.All() {
		out = append(out, [2]any{k, v.AsRaw()})
	}
	return out
}

// resourceSpans is one resource with its spans (one scope each unless scopes > 1).
type resourceSpans struct {
	resource kv
	scopes   [][]kv
}

func build(in []resourceSpans) ptrace.Traces {
	td := ptrace.NewTraces()
	for _, r := range in {
		rs := td.ResourceSpans().AppendEmpty()
		putAll(rs.Resource().Attributes(), r.resource)
		for _, scope := range r.scopes {
			ss := rs.ScopeSpans().AppendEmpty()
			for _, attrs := range scope {
				putAll(ss.Spans().AppendEmpty().Attributes(), attrs)
			}
		}
	}
	return td
}

func dump(td ptrace.Traces) []resourceSpans {
	var out []resourceSpans
	for _, rs := range td.ResourceSpans().All() {
		r := resourceSpans{resource: ordered(rs.Resource().Attributes())}
		for _, ss := range rs.ScopeSpans().All() {
			var scope []kv
			for _, s := range ss.Spans().All() {
				scope = append(scope, ordered(s.Attributes()))
			}
			r.scopes = append(r.scopes, scope)
		}
		out = append(out, r)
	}
	return out
}

func run(t *testing.T, block string, in []resourceSpans) []resourceSpans {
	t.Helper()
	cfg, err := load(t, block)
	require.NoError(t, err)
	sink := new(consumertest.TracesSink)
	p, err := NewFactory().CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, p.Start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()
	require.NoError(t, p.ConsumeTraces(context.Background(), build(in)))
	require.Len(t, sink.AllTraces(), 1)
	return dump(sink.AllTraces()[0])
}

func one(resource kv, spans ...kv) []resourceSpans {
	return []resourceSpans{{resource: resource, scopes: [][]kv{spans}}}
}

func TestProcessor(t *testing.T) {
	tests := []struct {
		name  string
		block string
		in    []resourceSpans
		want  []resourceSpans
	}{
		{
			name:  "copy appends the target (R3, O1)",
			block: "groups: [{exists_any: {attributes: [src]}, attributes: [{target: t, sources: [{key: src.a}]}]}]",
			in:    one(kv{}, kv{{"src.a", "A"}, {"x", 1}}),
			want:  one(kv{}, kv{{"src.a", "A"}, {"x", int64(1)}, {"t", "A"}}),
		},
		{
			name:  "existing target is kept and nothing moves (R2)",
			block: "groups: [{exists_any: {attributes: [src]}, attributes: [{target: t, sources: [{key: src.a, action: move}]}]}]",
			in:    one(kv{}, kv{{"src.a", "A"}, {"t", "old"}}),
			want:  one(kv{}, kv{{"src.a", "A"}, {"t", "old"}}),
		},
		{
			name:  "first present source wins, only it moves (R3, P6)",
			block: "groups: [{exists_any: {attributes: [src]}, attributes: [{target: t, sources: [{key: missing}, {key: src.b, action: move}, {key: src.a, action: move}]}]}]",
			in:    one(kv{}, kv{{"src.a", "A"}, {"src.b", "B"}, {"x", "x"}}),
			want:  one(kv{}, kv{{"src.a", "A"}, {"t", "B"}, {"x", "x"}}),
		},
		{
			name:  "move within a map leaves the target in the source's slot (O2)",
			block: "groups: [{exists_any: {attributes: [k]}, attributes: [{target: t, sources: [{key: k2, action: move}]}]}]",
			in:    one(kv{}, kv{{"k1", 1}, {"k2", 2}, {"k3", 3}, {"k4", 4}}),
			want:  one(kv{}, kv{{"k1", int64(1)}, {"t", int64(2)}, {"k3", int64(3)}, {"k4", int64(4)}}),
		},
		{
			name:  "move out of a map puts its last attribute in the source's slot (O2)",
			block: "groups: [{exists_any: {attributes: [src]}, attributes: [{target: t, context: resource, sources: [{key: src.a, action: move}]}]}]",
			in:    one(kv{{"r", "R"}}, kv{{"src.a", "A"}, {"src.b", "B"}, {"o", "o"}}),
			want:  one(kv{{"r", "R"}, {"t", "A"}}, kv{{"o", "o"}, {"src.b", "B"}}),
		},
		{
			name:  "resource. prefix reads the resource, empty key included (P1, P5)",
			block: "groups: [{exists_any: {attributes: [k]}, attributes: [{target: t, sources: [{key: resource.}]}, {target: u, sources: [{key: resource.r}]}, {target: v, sources: [{key: resource.k}]}]}]",
			in:    one(kv{{"", "empty"}, {"r", "R"}}, kv{{"k", "span"}, {"resource.k", "literal"}}),
			want:  one(kv{{"", "empty"}, {"r", "R"}}, kv{{"k", "span"}, {"resource.k", "literal"}, {"t", "empty"}, {"u", "R"}}),
		},
		{
			name:  "values keep their type, empty values count (P3, P4)",
			block: "groups: [{exists_any: {attributes: [m]}, attributes: [{target: a, sources: [{key: m.map}]}, {target: b, sources: [{key: m.empty}]}, {target: c, sources: [{key: m.none}]}]}]",
			in:    one(kv{}, kv{{"m.map", map[string]any{"k": []any{"x", int64(1)}}}, {"m.empty", ""}, {"m.none", nil}}),
			want: one(kv{}, kv{{"m.map", map[string]any{"k": []any{"x", int64(1)}}}, {"m.empty", ""}, {"m.none", nil},
				{"a", map[string]any{"k": []any{"x", int64(1)}}}, {"b", ""}, {"c", nil}}),
		},
		{
			name:  "condition matches keys, case-sensitive, not values (G1, G2)",
			block: "groups: [{exists_any: {attributes: [SRC, val]}, attributes: [{target: t, sources: [{key: k}]}]}]",
			in:    one(kv{}, kv{{"src", "x"}, {"k", "val"}}),
			want:  one(kv{}, kv{{"src", "x"}, {"k", "val"}}),
		},
		{
			name: "attribute condition sees earlier groups (G3)",
			block: `groups:
  - {id: g1, exists_any: {attributes: [src]}, attributes: [{target: mid.x, sources: [{key: src.a, action: move}]}]}
  - {id: g2, exists_any: {attributes: [mid.]}, attributes: [{target: final, sources: [{key: mid.x}]}]}
  - {id: g3, exists_any: {attributes: [src.a]}, attributes: [{target: after, sources: [{key: mid.x}]}]}`,
			in:   one(kv{}, kv{{"src.a", "A"}}),
			want: one(kv{}, kv{{"mid.x", "A"}, {"final", "A"}}),
		},
		{
			name: "resource condition is fixed per resource spans (G4)",
			block: `groups:
  - {id: g1, exists_any: {attributes: [src]}, attributes: [{target: flag.x, context: resource, sources: [{key: src.a}]}, {target: t, sources: [{key: resource.res.a, action: move}]}]}
  - {id: g2, exists_any: {resource: [flag., res.a]}, attributes: [{target: seen, sources: [{key: o}]}]}`,
			in: []resourceSpans{
				{resource: kv{{"res.a", "R1"}}, scopes: [][]kv{{{{"src.a", "a"}, {"o", "0"}}, {{"o", "1"}}}, {{{"o", "2"}}}}},
				{resource: kv{}, scopes: [][]kv{{{{"src.a", "b"}, {"o", "3"}}, {{"o", "4"}}}}},
			},
			want: []resourceSpans{
				{resource: kv{{"flag.x", "a"}}, scopes: [][]kv{{{{"src.a", "a"}, {"o", "0"}, {"t", "R1"}, {"seen", "0"}}, {{"o", "1"}, {"seen", "1"}}}, {{{"o", "2"}, {"seen", "2"}}}}},
				{resource: kv{{"flag.x", "b"}}, scopes: [][]kv{{{{"src.a", "b"}, {"o", "3"}}, {{"o", "4"}}}}},
			},
		},
		{
			name:  "first span writing a resource target wins (P7)",
			block: "groups: [{exists_any: {attributes: [src]}, attributes: [{target: t, context: resource, sources: [{key: src.a}]}]}]",
			in:    one(kv{}, kv{{"src.a", "first"}}, kv{{"src.a", "second"}}),
			want:  one(kv{{"t", "first"}}, kv{{"src.a", "first"}}, kv{{"src.a", "second"}}),
		},
		{
			name:  "rules chain and repeat targets in order",
			block: "groups: [{exists_any: {attributes: [src]}, attributes: [{target: t1, sources: [{key: src.a}]}, {target: t2, sources: [{key: t1}]}, {target: t1, sources: [{key: src.b}]}]}]",
			in:    one(kv{}, kv{{"src.a", "A"}, {"src.b", "B"}}),
			want:  one(kv{}, kv{{"src.a", "A"}, {"src.b", "B"}, {"t1", "A"}, {"t2", "A"}}),
		},
		{
			name:  "no groups passes data through (E2)",
			block: "groups: []",
			in:    one(kv{{"r", 1}}, kv{{"src.a", "A"}}),
			want:  one(kv{{"r", int64(1)}}, kv{{"src.a", "A"}}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, run(t, tt.block, tt.in))
		})
	}
}

func TestEventsUntouched(t *testing.T) {
	cfg, err := load(t, "groups: [{exists_any: {attributes: [src]}, attributes: [{target: t, sources: [{key: src.a, action: move}]}]}]")
	require.NoError(t, err)
	sink := new(consumertest.TracesSink)
	p, err := NewFactory().CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	td := ptrace.NewTraces()
	span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.Attributes().PutStr("src.a", "span")
	span.Events().AppendEmpty().Attributes().PutStr("src.a", "event")
	require.NoError(t, p.ConsumeTraces(context.Background(), td))
	got := sink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	assert.Equal(t, map[string]any{"t": "span"}, got.Attributes().AsRaw())
	assert.Equal(t, map[string]any{"src.a": "event"}, got.Events().At(0).Attributes().AsRaw())
}

func TestMutatesData(t *testing.T) {
	cfg, err := load(t, "")
	require.NoError(t, err)
	p, err := NewFactory().CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
	require.NoError(t, err)
	assert.True(t, p.Capabilities().MutatesData)
}
