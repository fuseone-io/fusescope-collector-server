// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozspanmapperprocessor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// BenchmarkProcessTraces maps the backend's example LLM, agent and tool spans
// with the shipped definitions (spec E3), 100 spans per resource.
func BenchmarkProcessTraces(b *testing.B) {
	raw, err := os.ReadFile(filepath.Join(observed, "cases.json"))
	if err != nil {
		b.Fatal(err)
	}
	var cases observedCases
	if err := json.Unmarshal(raw, &cases); err != nil {
		b.Fatal(err)
	}
	var cfg *Config
	in := ptrace.NewTraces()
	rs := in.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "chat")
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	for _, c := range cases.Accepted {
		if c.Name != "system_llm" && c.Name != "system_agent" && c.Name != "system_tool" {
			continue
		}
		if cfg, err = decode(c.Config); err != nil {
			b.Fatal(err)
		}
		td, err := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(c.Input)
		if err != nil {
			b.Fatal(err)
		}
		for _, r := range td.ResourceSpans().All() {
			for _, ss := range r.ScopeSpans().All() {
				for _, s := range ss.Spans().All() {
					s.CopyTo(spans.AppendEmpty())
				}
			}
		}
	}
	for n := spans.Len(); spans.Len() < 100; {
		spans.At(spans.Len() % n).CopyTo(spans.AppendEmpty())
	}
	m := newMapper(cfg)
	batches := make([]ptrace.Traces, b.N)
	for i := range batches {
		batches[i] = ptrace.NewTraces()
		in.CopyTo(batches[i])
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := m.processTraces(context.Background(), batches[i]); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*spans.Len()), "ns/span")
}

// BenchmarkNoMatch is the common case: spans no group matches.
func BenchmarkNoMatch(b *testing.B) {
	cfg := &Config{Groups: []Group{{ExistsAny: ExistsAny{Attributes: []string{"gen_ai", "llm"}, Resource: []string{"llm"}},
		Attributes: []Rule{{Target: "t", Sources: []Source{{Key: "llm.model"}}}}}}}
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	for range 100 {
		s := spans.AppendEmpty()
		s.Attributes().PutStr("http.method", "GET")
		s.Attributes().PutStr("http.route", "/cart")
		s.Attributes().PutInt("http.status_code", 200)
	}
	m := newMapper(cfg)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := m.processTraces(context.Background(), td); err != nil {
			b.Fatal(err)
		}
	}
}
