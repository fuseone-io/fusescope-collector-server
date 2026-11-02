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
