// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozllmpricingprocessor

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// A batch of 1000 spans, a third of them LLM calls, against 200 rules of the
// shapes the backend syncs (literal, prefix and glob patterns).
func BenchmarkProcessTraces(b *testing.B) {
	var rules []Rule
	for i := range 200 {
		rules = append(rules, Rule{
			Pattern: []string{fmt.Sprintf("model-%d", i), fmt.Sprintf("provider/model-%d-*", i), fmt.Sprintf("model-%d-v[0-9]", i)},
			Cache:   &Cache{Mode: "subtract", Read: 0.5, Write: 1}, In: 3, Out: 15,
		})
	}
	p := newPricer(testConfig(rules...))

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "llm-app")
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	for i := range 1000 {
		attrs := spans.AppendEmpty().Attributes()
		attrs.PutStr("http.method", "POST")
		if i%3 != 0 {
			continue
		}
		models := []string{fmt.Sprintf("model-%d", i%200), fmt.Sprintf("provider/model-%d-2024", i%200), fmt.Sprintf("model-%d-v7", i%200), "unknown"}
		attrs.PutStr(model, models[i%4])
		attrs.PutInt(in, 1200)
		attrs.PutInt(out, 300)
		attrs.PutInt(cacheRead, 200)
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := p.processTraces(context.Background(), td); err != nil {
			b.Fatal(err)
		}
	}
}
