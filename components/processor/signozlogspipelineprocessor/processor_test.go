// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozlogspipelineprocessor

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

func newTestProcessor(t *testing.T, operators ...map[string]any) *logsProcessor {
	t.Helper()
	p, err := newProcessor(&Config{Operators: operators}, componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	p.now = func() time.Time { return time.Unix(100, 0) }
	return p
}

func oneRecord(body string, attrs map[string]any) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "svc")
	r := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	r.Body().SetStr(body)
	_ = r.Attributes().FromRaw(attrs)
	return ld
}

func firstRecord(ld plog.Logs) plog.LogRecord {
	return ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
}

func TestProcessRecords(t *testing.T) {
	tests := []struct {
		name      string
		operators []map[string]any
		body      string
		attrs     map[string]any
		check     func(t *testing.T, r plog.LogRecord)
	}{
		{
			name:      "if false skips the operator",
			operators: []map[string]any{{"type": "add", "field": "attributes.a", "value": "1", "if": "attributes.x == 1"}},
			check:     func(t *testing.T, r plog.LogRecord) { assert.Equal(t, 0, r.Attributes().Len()) },
		},
		{
			name:      "body field reads a JSON string body",
			operators: []map[string]any{{"type": "add", "field": "attributes.lvl", "value": "EXPR(body.level)", "if": "body?.level != nil"}},
			body:      `{"level":"warn"}`,
			check: func(t *testing.T, r plog.LogRecord) {
				assert.Equal(t, map[string]any{"lvl": "warn"}, r.Attributes().AsRaw(), "the parsed body cache does not leave the processor")
			},
		},
		{
			name:      "nil values become <nil>",
			operators: []map[string]any{{"type": "add", "field": "attributes.n", "value": "EXPR(attributes.missing)"}},
			check: func(t *testing.T, r plog.LogRecord) {
				assert.Equal(t, map[string]any{"n": "<nil>"}, r.Attributes().AsRaw())
			},
		},
		{
			name:      "severity text defaults to the number's name",
			operators: []map[string]any{{"type": "severity_parser", "parse_from": "attributes.l", "mapping": map[string]any{"info2": []any{"x"}}}},
			attrs:     map[string]any{"l": "x"},
			check: func(t *testing.T, r plog.LogRecord) {
				assert.Equal(t, plog.SeverityNumberInfo2, r.SeverityNumber())
				assert.Equal(t, "x", r.SeverityText())
			},
		},
		{
			name:      "an int64 severity is an error, sent on",
			operators: []map[string]any{{"type": "severity_parser", "parse_from": "attributes.l", "mapping": map[string]any{"error": []any{"500"}}}},
			attrs:     map[string]any{"l": 500},
			check: func(t *testing.T, r plog.LogRecord) {
				assert.Equal(t, plog.SeverityNumberUnspecified, r.SeverityNumber())
				assert.Empty(t, r.SeverityText())
			},
		},
		{
			name:      "short trace ids are right-aligned",
			operators: []map[string]any{{"type": "trace_parser", "trace_id": map[string]any{"parse_from": "attributes.t"}}},
			attrs:     map[string]any{"t": "abcd"},
			check: func(t *testing.T, r plog.LogRecord) {
				assert.Equal(t, "0000000000000000000000000000abcd", r.TraceID().String())
			},
		},
		{
			name:      "time without a zone is UTC",
			operators: []map[string]any{{"type": "time_parser", "parse_from": "attributes.t", "layout_type": "strptime", "layout": "%Y-%m-%d %H:%M:%S"}},
			attrs:     map[string]any{"t": "2024-01-02 03:04:05"},
			check: func(t *testing.T, r plog.LogRecord) {
				assert.Equal(t, time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), r.Timestamp().AsTime())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.attrs == nil {
				tt.attrs = map[string]any{}
			}
			out, err := newTestProcessor(t, tt.operators...).processLogs(context.Background(), oneRecord(tt.body, tt.attrs))
			require.NoError(t, err)
			require.Equal(t, 1, out.LogRecordCount())
			tt.check(t, firstRecord(out))
		})
	}
}

// Values written into a record are copies: a record never contains itself
// and records never share maps (regressions from review of #29).
func TestWrittenValuesAreCopies(t *testing.T) {
	m := map[string]any{"k": "v"}
	tests := []struct {
		name      string
		operators []map[string]any
		body      any
		attrs     map[string]any
		want      map[string]any
		wantBody  any
	}{
		{
			name:      "add EXPR(attributes) into attributes",
			operators: []map[string]any{{"type": "add", "field": "attributes.snapshot", "value": "EXPR(attributes)"}},
			attrs:     map[string]any{"a": "1"},
			want:      map[string]any{"a": "1", "snapshot": map[string]any{"a": "1"}},
		},
		{
			name:      "copy a map into its own member",
			operators: []map[string]any{{"type": "copy", "from": "attributes.m", "to": "attributes.m.self"}},
			attrs:     map[string]any{"m": map[string]any{"k": "v"}},
			want:      map[string]any{"m": map[string]any{"k": "v", "self": map[string]any{"k": "v"}}},
		},
		{
			name:      "json_parser from a map body into its own member",
			operators: []map[string]any{{"type": "json_parser", "parse_from": "body", "parse_to": "body.parsed"}},
			body:      map[string]any{"k": "v"},
			want:      map[string]any{},
			wantBody:  map[string]any{"k": "v", "parsed": map[string]any{"k": "v"}},
		},
		{
			name: "a literal map is not shared with the configuration",
			operators: []map[string]any{
				{"id": "set", "type": "add", "field": "attributes.m", "value": m},
				{"type": "add", "field": "attributes.m.k", "value": "changed"},
			},
			want: map[string]any{"m": map[string]any{"k": "changed"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestProcessor(t, tt.operators...)
			var wg sync.WaitGroup
			for range 4 {
				wg.Go(func() {
					for range 20 {
						ld := oneRecord("", tt.attrs)
						if tt.body != nil {
							require.NoError(t, firstRecord(ld).Body().FromRaw(tt.body))
						}
						out, err := p.processLogs(context.Background(), ld)
						if assert.NoError(t, err) {
							assert.Equal(t, tt.want, firstRecord(out).Attributes().AsRaw())
							if tt.wantBody != nil {
								assert.Equal(t, tt.wantBody, firstRecord(out).Body().AsRaw())
							}
						}
					}
				})
			}
			wg.Wait()
			assert.Equal(t, map[string]any{"k": "v"}, m)
		})
	}
}

// JSON null is not an object (regression from review of #29).
func TestJSONNullIsAnError(t *testing.T) {
	for _, tt := range []struct {
		onError string
		kept    bool
	}{{"send", true}, {"drop", false}} {
		t.Run(tt.onError, func(t *testing.T) {
			p := newTestProcessor(t, map[string]any{"type": "json_parser", "parse_from": "body", "parse_to": "body", "on_error": tt.onError})
			out, err := p.processLogs(context.Background(), oneRecord("null", map[string]any{}))
			if !tt.kept {
				assert.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "null", firstRecord(out).Body().Str())
		})
	}
}

// A JSON object followed by anything is not an object for normalize
// (regression from review of #29).
func TestNormalizeTrailingContent(t *testing.T) {
	p := newTestProcessor(t, map[string]any{"type": "normalize"})
	for _, body := range []string{`{"msg":"hello"}}`, `{"msg":"hello"} x`, `{"a":1}{"b":2}`} {
		out, err := p.processLogs(context.Background(), oneRecord(body, map[string]any{}))
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"message": body}, firstRecord(out).Body().AsRaw(), body)
	}
	out, err := p.processLogs(context.Background(), oneRecord(`{"msg":"hello"}`, map[string]any{}))
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"message": "hello"}, firstRecord(out).Body().AsRaw())
}

func TestWritesResource(t *testing.T) {
	for _, tt := range []struct {
		op   map[string]any
		want bool
	}{
		{map[string]any{"type": "add", "field": "attributes.a", "value": "1"}, false},
		{map[string]any{"type": "copy", "from": "resource.a", "to": "attributes.a"}, false},
		{map[string]any{"type": "add", "field": "resource.a", "value": "1"}, true},
		{map[string]any{"type": "move", "from": "resource.a", "to": "attributes.a"}, true},
		{map[string]any{"type": "copy", "from": "attributes.a", "to": "resource.a"}, true},
		{map[string]any{"type": "remove", "field": "resource.a"}, true},
		{map[string]any{"type": "regex_parser", "regex": "(?P<a>.)", "parse_to": "resource"}, true},
	} {
		p := newTestProcessor(t, tt.op)
		assert.Equal(t, tt.want, p.pipeline.writesResource, "%v", tt.op)
	}
}

// Records of a resource share one copy of it unless the pipeline writes the
// resource, so a large resource is not copied per record (regression from
// review of #29).
func TestSharedResourceMemory(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	for i := range 2000 {
		rl.Resource().Attributes().PutStr(string(rune('a'+i%26))+time.Duration(i).String(), "value")
	}
	records := rl.ScopeLogs().AppendEmpty().LogRecords()
	for range 2000 {
		records.AppendEmpty().Body().SetStr("x")
	}
	p := newTestProcessor(t, map[string]any{"type": "add", "field": "attributes.a", "value": "EXPR(resource.a0s)"})
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	out, err := p.processLogs(context.Background(), ld)
	runtime.ReadMemStats(&after)
	require.NoError(t, err)
	assert.Equal(t, 2000, out.LogRecordCount())
	assert.Equal(t, 1, out.ResourceLogs().Len())
	// A copy per record would allocate 4 million map entries, several hundred MB.
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<20))
}

func TestOnError(t *testing.T) {
	// attributes.n + 1 fails at run time for a string n.
	for _, tt := range []struct {
		onError string
		kept    bool
	}{{"", true}, {"send", true}, {"send_quiet", true}, {"drop", false}, {"drop_quiet", false}} {
		t.Run("if/"+tt.onError, func(t *testing.T) {
			op := map[string]any{"type": "add", "field": "attributes.r", "value": "1", "if": "attributes.n + 1 > 2"}
			if tt.onError != "" {
				op["on_error"] = tt.onError
			}
			out, err := newTestProcessor(t, op).processLogs(context.Background(), oneRecord("b", map[string]any{"n": "x"}))
			if !tt.kept {
				assert.ErrorIs(t, err, processorhelper.ErrSkipProcessingData)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, map[string]any{"n": "x"}, firstRecord(out).Attributes().AsRaw())
		})
	}
}

func TestRouterWithoutDefaultDrops(t *testing.T) {
	p := newTestProcessor(t,
		map[string]any{"id": "r", "type": "router", "routes": []any{map[string]any{"expr": `attributes.k == "a"`, "output": "add"}}},
		map[string]any{"id": "add", "type": "add", "field": "attributes.routed", "value": true},
	)
	ld := oneRecord("one", map[string]any{"k": "a"})
	r := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().AppendEmpty()
	r.Attributes().PutStr("k", "b")
	out, err := p.processLogs(context.Background(), ld)
	require.NoError(t, err)
	require.Equal(t, 1, out.LogRecordCount())
	assert.Equal(t, map[string]any{"k": "a", "routed": true}, firstRecord(out).Attributes().AsRaw())
}

// Records are regrouped by resource and scope name; other resource, scope
// and record metadata is not kept (spec §5.2).
func TestRegroup(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.SetSchemaUrl("https://schema")
	rl.Resource().Attributes().PutStr("service.name", "a")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("lib")
	sl.Scope().SetVersion("1.0")
	for _, tier := range []string{"gold", "", "gold"} {
		r := sl.LogRecords().AppendEmpty()
		r.SetEventName("event")
		r.SetDroppedAttributesCount(2)
		r.Attributes().PutStr("tier", tier)
	}
	p := newTestProcessor(t, map[string]any{"type": "move", "from": "attributes.tier", "to": "resource.tier", "if": `attributes.tier == "gold"`})
	out, err := p.processLogs(context.Background(), ld)
	require.NoError(t, err)

	require.Equal(t, 2, out.ResourceLogs().Len())
	gold := out.ResourceLogs().At(0)
	assert.Equal(t, map[string]any{"service.name": "a", "tier": "gold"}, gold.Resource().Attributes().AsRaw())
	assert.Empty(t, gold.SchemaUrl())
	require.Equal(t, 1, gold.ScopeLogs().Len())
	assert.Equal(t, "lib", gold.ScopeLogs().At(0).Scope().Name())
	assert.Empty(t, gold.ScopeLogs().At(0).Scope().Version())
	assert.Equal(t, 2, gold.ScopeLogs().At(0).LogRecords().Len())
	r := gold.ScopeLogs().At(0).LogRecords().At(0)
	assert.Empty(t, r.EventName())
	assert.Zero(t, r.DroppedAttributesCount())
	assert.Equal(t, pcommon.NewTimestampFromTime(time.Unix(100, 0)), r.ObservedTimestamp(), "a missing observed time is the processor's clock")
	assert.Equal(t, 1, out.ResourceLogs().At(1).ScopeLogs().At(0).LogRecords().Len())
}

func TestConcurrentCalls(t *testing.T) {
	p := newTestProcessor(t,
		map[string]any{"type": "regex_parser", "parse_from": "body", "regex": `^(?P<n>\d+)$`},
		map[string]any{"type": "add", "field": "attributes.copy", "value": "EXPR(attributes.n)"},
	)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			for range 50 {
				body := string(rune('0' + i%10))
				out, err := p.processLogs(context.Background(), oneRecord(body, map[string]any{}))
				if assert.NoError(t, err) {
					assert.Equal(t, map[string]any{"n": body, "copy": body}, firstRecord(out).Attributes().AsRaw())
				}
			}
		})
	}
	wg.Wait()
}

type panickingOperator struct{ operator.Operator }

func (panickingOperator) Process(context.Context, *entry.Entry) error { panic("boom") }

// A panicking operator is an operator error under on_error (CLAUDE.md: no
// panic on data paths).
func TestOperatorPanicIsAnError(t *testing.T) {
	for _, tt := range []struct {
		onError string
		kept    bool
	}{{"send", true}, {"drop", false}} {
		t.Run(tt.onError, func(t *testing.T) {
			n := &stanzaNode{id: "p", op: panickingOperator{}, onError: tt.onError, next: exit, logger: componenttest.NewNopTelemetrySettings().Logger}
			p := &pipeline{nodes: []node{n}}
			var out []*entry.Entry
			p.process(newRun(context.Background()), entry.New(), func(e *entry.Entry) { out = append(out, e) })
			assert.Equal(t, tt.kept, len(out) == 1)
		})
	}
}

func TestExpressionPanicIsAnError(t *testing.T) {
	prog, err := compile(`attributes.f()`)
	require.NoError(t, err)
	e := entry.New()
	e.Attributes = map[string]any{"f": func() any { panic("boom") }}
	_, err = prog.run(e)
	assert.Error(t, err)
	assert.False(t, errors.Is(err, errPanicked))
}
