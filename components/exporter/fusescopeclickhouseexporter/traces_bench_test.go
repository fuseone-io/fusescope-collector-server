// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// syntheticTraces resembles an HTTP service: 10 resources, 15 attributes per
// span, one event in ten spans.
func syntheticTraces(spans int) ptrace.Traces {
	td := ptrace.NewTraces()
	for r := range 10 {
		rs := td.ResourceSpans().AppendEmpty()
		ra := rs.Resource().Attributes()
		ra.PutStr("service.name", fmt.Sprintf("svc-%d", r))
		ra.PutStr("deployment.environment", "prod")
		ra.PutStr("host.name", fmt.Sprintf("node-%d", r))
		ra.PutStr("k8s.pod.name", fmt.Sprintf("svc-%d-7d9f", r))
		ra.PutStr("telemetry.sdk.language", "go")
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("bench")
		for i := range spans / 10 {
			s := ss.Spans().AppendEmpty()
			s.SetTraceID(pcommon.TraceID{byte(r), byte(i), byte(i >> 8), 1})
			s.SetSpanID(pcommon.SpanID{byte(r), byte(i), byte(i >> 8), 2})
			s.SetParentSpanID(pcommon.SpanID{byte(r), 3})
			s.SetName(fmt.Sprintf("GET /items/%d", i%50))
			s.SetKind(ptrace.SpanKindServer)
			s.SetStartTimestamp(pcommon.Timestamp(1_793_483_525_000_000_000 + i*1_000_000))
			s.SetEndTimestamp(s.StartTimestamp() + 5_000_000)
			a := s.Attributes()
			a.PutStr("http.request.method", "GET")
			a.PutStr("http.route", "/items/{id}")
			a.PutStr("url.full", fmt.Sprintf("https://shop.example.com/items/%d", i))
			a.PutInt("http.response.status_code", 200)
			a.PutStr("server.address", "shop.example.com")
			a.PutStr("user_agent.original", "bench/1.0")
			a.PutStr("network.protocol.version", "1.1")
			a.PutInt("server.port", 443)
			a.PutStr("client.address", fmt.Sprintf("10.0.%d.%d", r, i%250))
			a.PutBool("cache.hit", i%2 == 0)
			a.PutDouble("payload.ratio", 0.5)
			a.PutStr("tenant.id", fmt.Sprintf("t-%d", i%20))
			a.PutStr("feature.flag", "on")
			a.PutStr("db.system.name", "postgresql")
			a.PutStr("db.namespace", "shop")
			if i%10 == 0 {
				ev := s.Events().AppendEmpty()
				ev.SetName("exception")
				ev.Attributes().PutStr("exception.type", "TimeoutError")
				ev.Attributes().PutStr("exception.message", "upstream timed out")
				s.Status().SetCode(ptrace.StatusCodeError)
			}
		}
	}
	return td
}

func BenchmarkEncodeTraces(b *testing.B) {
	td := syntheticTraces(10_000)
	enc := newTracesEncoder()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = enc.encode(td, nil)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*10_000), "ns/span")
}

func BenchmarkEncodeLogs(b *testing.B) {
	ld := plog.NewLogs()
	for r := range 10 {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", r))
		rl.Resource().Attributes().PutStr("host.name", fmt.Sprintf("node-%d", r))
		records := rl.ScopeLogs().AppendEmpty().LogRecords()
		for i := range 1_000 {
			lr := records.AppendEmpty()
			lr.SetTimestamp(pcommon.Timestamp(1_793_483_525_000_000_000 + i*1_000_000))
			lr.SetSeverityText("INFO")
			lr.SetSeverityNumber(plog.SeverityNumberInfo)
			lr.Body().SetStr(fmt.Sprintf("request %d served in %d ms", i, i%300))
			lr.Attributes().PutStr("http.route", "/items/{id}")
			lr.Attributes().PutInt("http.response.status_code", 200)
			lr.Attributes().PutStr("tenant.id", fmt.Sprintf("t-%d", i%20))
		}
	}
	enc := newLogsEncoder()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = enc.encode(ld, nil)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*10_000), "ns/record")
}

// TestEncodeMaxBatch encodes a batch at the default sending_queue max_size and
// checks that reusing the encoder for a smaller batch leaves no rows behind.
func TestEncodeMaxBatch(t *testing.T) {
	enc := newTracesEncoder()
	_ = enc.encode(syntheticTraces(100_000), nil)
	if got := enc.index.input()[0].Data.Rows(); got != 100_000 {
		t.Fatalf("index rows = %d, want 100000", got)
	}
	_ = enc.encode(syntheticTraces(10), nil)
	for _, col := range enc.index.input() {
		if got := col.Data.Rows(); got != 10 {
			t.Fatalf("column %s has %d rows after reuse, want 10", col.Name, got)
		}
	}
}
