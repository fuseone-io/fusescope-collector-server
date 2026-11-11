// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozlogspipelineprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

func TestGrokParser(t *testing.T) {
	grok := func(pattern string, extra map[string]any) map[string]any {
		op := map[string]any{"type": "grok_parser", "parse_from": "body", "parse_to": "attributes", "pattern": pattern}
		for k, v := range extra {
			op[k] = v
		}
		return op
	}
	tests := []struct {
		name     string
		op       map[string]any
		body     string
		attrs    map[string]any
		want     map[string]any
		wantBody any
		dropped  bool
	}{
		{
			name: "named captures only",
			op:   grok("%{WORD} %{URIPATH:path} %{NUMBER}", nil),
			body: "GET /a/b 200",
			want: map[string]any{"path": "/a/b"},
		},
		{
			name: "int and float conversions",
			op:   grok("%{WORD:method} %{NUMBER:status:int} %{NUMBER:duration:float} %{NUMBER:raw}", nil),
			body: "GET 200 0.5 7",
			want: map[string]any{"method": "GET", "status": int64(200), "duration": 0.5, "raw": "7"},
		},
		{
			name: "int conversion of a non-integer is 0",
			op:   grok("%{NUMBER:n:int}", nil),
			body: "0.042",
			want: map[string]any{"n": int64(0)},
		},
		{
			name: "dotted names are kept",
			op:   grok("%{WORD:http.method} %{NUMBER:http.status_code:int}", nil),
			body: "GET 404",
			want: map[string]any{"http.method": "GET", "http.status_code": int64(404)},
		},
		{
			name: "inline named group",
			op:   grok("(?<verb>[A-Z]+) (?<rest>.*)", nil),
			body: "POST /x y",
			want: map[string]any{"verb": "POST", "rest": "/x y"},
		},
		{
			name: "unmatched optional group is empty",
			op:   grok("%{WORD:method}(?: %{WORD:optional})?", nil),
			body: "GET",
			want: map[string]any{"method": "GET", "optional": ""},
		},
		{
			name: "unanchored match",
			op:   grok("%{WORD:w}", nil),
			body: "  hello world",
			want: map[string]any{"w": "hello"},
		},
		{
			name:  "no match is not an error and writes an empty map",
			op:    grok("^%{IP:client}$", map[string]any{"on_error": "drop"}),
			body:  "not an ip",
			attrs: map[string]any{"keep": "1"},
			want:  map[string]any{"keep": "1"},
		},
		{
			name:     "no match into the body replaces a string body",
			op:       grok("^%{IP:client}$", map[string]any{"parse_to": "body"}),
			body:     "not an ip",
			want:     map[string]any{},
			wantBody: map[string]any{},
		},
		{
			name: "parse from a body field of a JSON string body",
			op:   grok("%{WORD:first}", map[string]any{"parse_from": "body.msg"}),
			body: `{"msg":"hello there"}`,
			want: map[string]any{"first": "hello"},
		},
		{
			name:  "a value that is not a string is an error, sent on",
			op:    grok("%{WORD:w}", map[string]any{"parse_from": "attributes.n"}),
			attrs: map[string]any{"n": 5},
			want:  map[string]any{"n": int64(5)},
		},
		{
			name:    "a value that is not a string is an error, dropped",
			op:      grok("%{WORD:w}", map[string]any{"parse_from": "attributes.n", "on_error": "drop"}),
			attrs:   map[string]any{"n": 5},
			dropped: true,
		},
		// A pattern that does not compile is accepted; it fails each record
		// and on_error decides the record's fate (spec §3.2).
		{
			name:  "an unknown pattern name is an error, sent on",
			op:    grok("%{NOSUCHPATTERN:x}", nil),
			body:  "GET /a 200",
			attrs: map[string]any{"keep": "1"},
			want:  map[string]any{"keep": "1"},
		},
		{
			name:  "an unknown pattern name is an error, sent on quietly",
			op:    grok("%{WORD:w} %{NOSUCHPATTERN:x}", map[string]any{"on_error": "send_quiet"}),
			body:  "GET /a 200",
			attrs: map[string]any{"keep": "1"},
			want:  map[string]any{"keep": "1"},
		},
		{
			name:    "an unknown pattern name is an error, dropped",
			op:      grok("%{NOSUCHPATTERN:x}", map[string]any{"on_error": "drop"}),
			body:    "GET /a 200",
			dropped: true,
		},
		{
			name:    "an unknown pattern name is an error, dropped quietly",
			op:      grok("%{NOSUCHPATTERN:x}", map[string]any{"on_error": "drop_quiet"}),
			body:    "GET /a 200",
			dropped: true,
		},
		{
			name:  "an invalid regex is an error, sent on",
			op:    grok("(?<x>[a-", nil),
			body:  "abc",
			attrs: map[string]any{"keep": "1"},
			want:  map[string]any{"keep": "1"},
		},
		{
			name:    "an invalid regex is an error, dropped",
			op:      grok("(?<x>[a-", map[string]any{"on_error": "drop"}),
			body:    "abc",
			dropped: true,
		},
		{
			name:  "an invalid conversion type is an error, sent on",
			op:    grok("%{WORD:w:bool}", nil),
			body:  "abc",
			attrs: map[string]any{"keep": "1"},
			want:  map[string]any{"keep": "1"},
		},
		{
			name:    "a missing field is an error, dropped",
			op:      grok("%{WORD:w}", map[string]any{"parse_from": "attributes.absent", "on_error": "drop_quiet"}),
			dropped: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.attrs == nil {
				tt.attrs = map[string]any{}
			}
			out, err := newTestProcessor(t, tt.op).processLogs(context.Background(), oneRecord(tt.body, tt.attrs))
			if tt.dropped {
				assert.ErrorIs(t, err, processorhelper.ErrSkipProcessingData, "every record is dropped")
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, out.LogRecordCount())
			r := firstRecord(out)
			if tt.wantBody != nil {
				assert.Equal(t, tt.wantBody, r.Body().AsRaw())
				return
			}
			assert.Equal(t, tt.want, r.Attributes().AsRaw())
		})
	}
}

// One grok operator serves concurrent calls; run with -race.
func TestGrokConcurrent(t *testing.T) {
	p := newTestProcessor(t, map[string]any{"type": "grok_parser", "parse_from": "body", "parse_to": "attributes", "pattern": "%{COMBINEDAPACHELOG}"})
	body := `10.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "http://www.example.com/start.html" "Mozilla/4.08"`
	done := make(chan plog.Logs)
	for range 8 {
		go func() {
			out, _ := p.processLogs(context.Background(), oneRecord(body, map[string]any{}))
			done <- out
		}()
	}
	for range 8 {
		out := <-done
		require.Equal(t, 1, out.LogRecordCount())
		attrs := firstRecord(out).Attributes().AsRaw()
		assert.Equal(t, "10.0.0.1", attrs["clientip"])
		assert.Equal(t, `"Mozilla/4.08"`, attrs["agent"])
	}
}

// env is not in the expression environment, so no alias reaches it (spec §4.1).
func TestEnvIsNotInTheEnvironment(t *testing.T) {
	t.Setenv("FUSESCOPE_TEST_ENV", "leaked")
	p := newTestProcessor(t, map[string]any{"type": "add", "field": "attributes.a", "value": `EXPR([type($env["os_env_func"]), type($env["env"])])`})
	out, err := p.processLogs(context.Background(), oneRecord("x", map[string]any{}))
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"a": []any{"nil", "nil"}}, firstRecord(out).Attributes().AsRaw())
}
