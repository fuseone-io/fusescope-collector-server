// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozlogspipelineprocessor

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"
)

// benchOperators is a pipeline as the backend generates it: a filter, a
// JSON parser with a severity mapping, and a move guarded by a nil check.
var benchOperators = []map[string]any{
	{"id": "router_signoz", "type": "router", "default": "noop",
		"routes": []any{map[string]any{"expr": `attributes["service"] == "checkout"`, "output": "json"}}},
	{"id": "json", "type": "json_parser", "parse_from": "body", "parse_to": "attributes", "on_error": "send_quiet", "output": "sev"},
	{"id": "sev", "type": "severity_parser", "parse_from": `attributes["level"]`, "on_error": "send_quiet", "output": "move",
		"if": `attributes["level"] != nil && type(attributes["level"]) == "string"`},
	{"id": "move", "type": "move", "from": `attributes["msg"]`, "to": "body", "if": `attributes["msg"] != nil`, "output": "noop"},
	{"id": "noop", "type": "noop"},
}

func benchLogs(n int, service string) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", service)
	records := rl.ScopeLogs().AppendEmpty().LogRecords()
	for i := range n {
		r := records.AppendEmpty()
		r.Body().SetStr(fmt.Sprintf(`{"level":"info","msg":"order %d placed","user":"u%d"}`, i, i%7))
		r.Attributes().PutStr("service", service)
		r.Attributes().PutInt("n", int64(i))
	}
	return ld
}

func BenchmarkProcessLogs(b *testing.B) {
	for _, tt := range []struct {
		name    string
		service string
	}{{"matching", "checkout"}, {"not matching", "payment"}} {
		b.Run(tt.name, func(b *testing.B) {
			p, err := newProcessor(&Config{Operators: benchOperators}, componenttest.NewNopTelemetrySettings())
			if err != nil {
				b.Fatal(err)
			}
			in := benchLogs(100, tt.service)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				ld := plog.NewLogs()
				in.CopyTo(ld)
				if _, err := p.processLogs(context.Background(), ld); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGrok parses 100 access log lines per call, as a pipeline the
// backend generates for a grok processor.
func BenchmarkGrok(b *testing.B) {
	for _, tt := range []struct {
		name    string
		pattern string
		body    string
	}{
		{"simple", "%{WORD:method} %{URIPATH:path} %{NUMBER:status:int} %{NUMBER:bytes:int} %{NUMBER:duration:float}", "GET /api/orders 200 1532 0.042"},
		{"combined apache log", "%{COMBINEDAPACHELOG}", `10.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "http://www.example.com/start.html" "Mozilla/4.08"`},
		{"no match", "^%{IP:client} %{WORD:verb}$", "GET /api/orders 200 1532 0.042"},
	} {
		b.Run(tt.name, func(b *testing.B) {
			operators := []map[string]any{
				{"id": "router_signoz", "type": "router", "default": "noop",
					"routes": []any{map[string]any{"expr": "body != nil", "output": "grok"}}},
				{"id": "grok", "type": "grok_parser", "parse_from": "body", "parse_to": "attributes", "pattern": tt.pattern,
					"if": "body != nil", "on_error": "send", "output": "noop"},
				{"id": "noop", "type": "noop"},
			}
			p, err := newProcessor(&Config{Operators: operators}, componenttest.NewNopTelemetrySettings())
			if err != nil {
				b.Fatal(err)
			}
			in := plog.NewLogs()
			records := in.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
			for range 100 {
				records.AppendEmpty().Body().SetStr(tt.body)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				ld := plog.NewLogs()
				in.CopyTo(ld)
				if _, err := p.processLogs(context.Background(), ld); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
