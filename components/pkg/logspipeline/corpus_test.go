// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package logspipeline_test

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/logspipeline"
	"github.com/fuseone-io/fusescope-collector-server/components/pkg/logspipeline/corpus"
)

var updateMessages = flag.Bool("update-messages", false, "rewrite corpus/library/messages.json from the library")

// TestCorpusMessages runs every observed case on the library, one record at
// a time, and compares the messages logged while processing each record with
// corpus/library/messages.json (ADR 0012, step 4). Build-time logs, such as a
// grok pattern that does not compile, are dropped before the records run; a
// case the library refuses to build is not evaluated.
func TestCorpusMessages(t *testing.T) {
	got := corpus.Messages{Cases: map[string][]corpus.Message{}}
	observed, err := corpus.Observed()
	require.NoError(t, err)
	for _, c := range observed.Cases {
		msgs, ok := caseMessages(t, c, observed.Inputs[c.LogSet])
		if ok {
			got.Cases[c.Name] = msgs
		}
	}

	path := filepath.Join("corpus", "library", "messages.json")
	if *updateMessages {
		b, err := json.MarshalIndent(got, "", " ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, append(b, '\n'), 0o644))
	}
	want, err := corpus.LibraryMessages()
	require.NoError(t, err)
	assert.Equal(t, want, got, "regenerate with go test -run TestCorpusMessages -update-messages")
}

// caseMessages builds the case's pipelines and runs each record of its log
// set through them alone, in order, collecting what they log at warn or above.
func caseMessages(t *testing.T, c corpus.Case, docs []json.RawMessage) ([]corpus.Message, bool) {
	t.Helper()
	core, recorded := observer.New(zapcore.WarnLevel)
	set := component.TelemetrySettings{Logger: zap.New(core)}
	var pipelines []*logspipeline.Pipeline
	for _, name := range c.ProcessorNames {
		operators, ok := operatorsOf(t, c.Processors[name])
		if !ok {
			return nil, false
		}
		p, err := logspipeline.New(operators, set)
		if err != nil {
			return nil, false
		}
		pipelines = append(pipelines, p)
	}
	recorded.TakeAll()

	msgs := []corpus.Message{}
	for i, ld := range records(t, docs) {
		for _, p := range pipelines {
			out, ok := p.Process(context.Background(), ld, time.Unix(1, 0))
			if !ok {
				break
			}
			ld = out
		}
		for _, entry := range recorded.TakeAll() {
			fields := entry.ContextMap()
			id, _ := fields["operator_id"].(string)
			text, _ := fields["error"].(string)
			if text == "" {
				text = entry.Message
			}
			msgs = append(msgs, corpus.Message{Input: i, OperatorID: id, Error: text})
		}
	}
	return msgs, true
}

// operatorsOf decodes a processor configuration as the collector does for
// the configs the backend pushes: `$$` becomes `$` and numbers take YAML's
// types. A configuration with keys besides operators is not evaluated.
func operatorsOf(t *testing.T, raw json.RawMessage) ([]map[string]any, bool) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var cfg map[string]any
	require.NoError(t, d.Decode(&cfg))
	list, ok := cfg["operators"].([]any)
	if !ok || len(cfg) != 1 {
		return nil, false
	}
	operators := make([]map[string]any, 0, len(list))
	for _, op := range list {
		m, ok := unescape(op).(map[string]any)
		if !ok {
			return nil, false
		}
		operators = append(operators, m)
	}
	return operators, true
}

func unescape(v any) any {
	switch v := v.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return int(i)
		}
		f, _ := v.Float64()
		return f
	case string:
		return strings.ReplaceAll(v, "$$", "$")
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			out[k] = unescape(item)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = unescape(item)
		}
		return out
	}
	return v
}

// records splits a log set into one plog.Logs per record, in the order of
// Message.Input, each with its resource and scope.
func records(t *testing.T, docs []json.RawMessage) []plog.Logs {
	t.Helper()
	var out []plog.Logs
	for _, raw := range docs {
		ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs(raw)
		require.NoError(t, err)
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			rl := ld.ResourceLogs().At(i)
			for j := 0; j < rl.ScopeLogs().Len(); j++ {
				sl := rl.ScopeLogs().At(j)
				for k := 0; k < sl.LogRecords().Len(); k++ {
					one := plog.NewLogs()
					orl := one.ResourceLogs().AppendEmpty()
					rl.Resource().CopyTo(orl.Resource())
					orl.SetSchemaUrl(rl.SchemaUrl())
					osl := orl.ScopeLogs().AppendEmpty()
					sl.Scope().CopyTo(osl.Scope())
					osl.SetSchemaUrl(sl.SchemaUrl())
					sl.LogRecords().At(k).CopyTo(osl.LogRecords().AppendEmpty())
					out = append(out, one)
				}
			}
		}
	}
	return out
}

// The corpus's own guarantees: every evaluated case is an observed case, and
// a case whose operators all use a quiet on_error mode logs nothing.
func TestCorpusMessagesShape(t *testing.T) {
	m, err := corpus.LibraryMessages()
	require.NoError(t, err)
	observed, err := corpus.Observed()
	require.NoError(t, err)
	names := map[string]corpus.Case{}
	for _, c := range observed.Cases {
		names[c.Name] = c
	}
	for name, msgs := range m.Cases {
		c, ok := names[name]
		require.True(t, ok, "%s is not an observed case", name)
		assert.True(t, slices.IsSortedFunc(msgs, func(a, b corpus.Message) int { return a.Input - b.Input }), "%s: messages ordered by input", name)
		for _, msg := range msgs {
			assert.NotEmpty(t, msg.OperatorID, "%s: every message names its operator", name)
			// The build warning reads "no pattern found for %{...}"; a record's
			// error names the operator's field first.
			assert.False(t, strings.HasPrefix(msg.Error, "no pattern found for %{"), "%s: a build-time grok log is not a record message", name)
		}
		if quietOnly(t, c) {
			assert.Empty(t, msgs, "%s: quiet on_error modes log nothing", name)
		}
	}
}

func quietOnly(t *testing.T, c corpus.Case) bool {
	t.Helper()
	any := false
	for _, name := range c.ProcessorNames {
		operators, ok := operatorsOf(t, c.Processors[name])
		if !ok {
			return false
		}
		for _, op := range operators {
			mode, _ := op["on_error"].(string)
			switch mode {
			case "send_quiet", "drop_quiet":
				any = true
			case "":
			default:
				return false
			}
		}
	}
	return any
}
