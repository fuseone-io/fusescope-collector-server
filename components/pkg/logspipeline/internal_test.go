// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package logspipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"
)

// These tests reach the pipeline's internals. They moved here with the code
// from the processor (ADR 0012); logsProcessor keeps their calls unchanged.

type logsProcessor struct {
	pipeline *pipeline
}

func newTestProcessor(t *testing.T, operators ...map[string]any) *logsProcessor {
	t.Helper()
	p, err := New(operators, componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	return &logsProcessor{pipeline: p.pipeline}
}

var errNoRecord = errors.New("no record came out")

func (p *logsProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	out, ok := (&Pipeline{pipeline: p.pipeline}).Process(ctx, ld, time.Unix(100, 0))
	if !ok {
		return ld, errNoRecord
	}
	return out, nil
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

// Attributes a record has, or an operator writes, are never taken for the
// scratch attributes of body reads.
func TestScratchNameCollision(t *testing.T) {
	const old = "__signozlogspipeline_bodyfield_0__" // the name before it was random
	tests := []struct {
		name     string
		operator map[string]any
		body     string
		want     map[string]any
	}{
		{"body field present", map[string]any{"type": "copy", "from": "body.msg", "to": "attributes.out", "on_error": "send"},
			`{"msg":"hello"}`, map[string]any{old: "mine", "out": "hello"}},
		{"body field missing", map[string]any{"type": "copy", "from": "body.msg", "to": "attributes.out", "on_error": "send"},
			`{"other":1}`, map[string]any{old: "mine"}},
		{"operator writes the name", map[string]any{"type": "json_parser", "parse_from": "body.payload", "parse_to": "attributes"},
			`{"payload":"{\"` + old + `\":\"parsed\"}"}`, map[string]any{old: "parsed"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestProcessor(t, tt.operator)
			out, err := p.processLogs(context.Background(), oneRecord(tt.body, map[string]any{old: "mine"}))
			require.NoError(t, err)
			assert.Equal(t, tt.want, firstRecord(out).Attributes().AsRaw())
			for k := range firstRecord(out).Attributes().AsRaw() {
				assert.False(t, strings.HasPrefix(k, scratchPrefix), "scratch attribute %q left on the record", k)
			}
		})
	}
	assert.Regexp(t, `^__signozlogspipeline_bodyfield_[0-9a-f]{32}_$`, scratchPrefix)
}

// A pattern that fails to compile is compiled once, when the operator is
// built: every record gets the same error, and the library is not called again.
func TestGrokCompileErrorKept(t *testing.T) {
	for _, pattern := range []string{"%{NOSUCHPATTERN:x}", "(?<x>[a-"} {
		t.Run(pattern, func(t *testing.T) {
			p := newTestProcessor(t, map[string]any{"id": "g", "type": "grok_parser", "parse_from": "body", "parse_to": "attributes", "pattern": pattern})
			op := findGrok(t, p)
			require.Error(t, op.compileErr)
			op.grok = nil // any further use of the library would panic
			for range 3 {
				out, err := p.processLogs(context.Background(), oneRecord("GET /a 200", map[string]any{}))
				require.NoError(t, err)
				assert.Equal(t, 1, out.LogRecordCount())
			}
			_, err := op.parse("GET /a 200")
			assert.ErrorIs(t, err, op.compileErr)
		})
	}
}

func findGrok(t *testing.T, p *logsProcessor) *grokOperator {
	t.Helper()
	for _, n := range p.pipeline.nodes {
		if s, ok := n.(*stanzaNode); ok {
			if op, ok := s.op.(*grokOperator); ok {
				return op
			}
		}
	}
	t.Fatal("no grok_parser operator")
	return nil
}

// A pushed grok pattern is bounded before it is expanded: repeating a large
// macro would otherwise expand into hundreds of megabytes in quadratic time
// during validation (regression from review of #31).
func TestGrokPatternLimits(t *testing.T) {
	grok := func(pattern string) []map[string]any {
		return []map[string]any{{"type": "grok_parser", "parse_from": "body", "pattern": pattern}}
	}
	tests := []struct {
		name    string
		pattern string
		wantErr string
	}{
		{"repeated macro", strings.Repeat("%{IPV6}", 500_000), "longer than"},
		{"too many references", strings.Repeat("%{IPV6}", maxGrokPatternRefs+1), "references"},
		{"too long", strings.Repeat("a", maxGrokPatternBytes+1), "longer than"},
		{"at the reference limit", strings.Repeat("%{COMBINEDAPACHELOG} ", maxGrokPatternRefs), ""},
		{"at the length limit", strings.Repeat("a", maxGrokPatternBytes), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Now()
			err := Validate(grok(tt.pattern))
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.wantErr)
			}
			assert.Less(t, time.Since(start), 5*time.Second, "validation is bounded")
		})
	}
}
