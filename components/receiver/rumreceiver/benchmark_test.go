// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package rumreceiver

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// benchTraces is a browser-like batch: one resource, 50 spans with URL,
// session and query attributes, half of them needing scrubbing.
func benchTraces() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr(attributeServiceName, "shop-web")
	rs.Resource().Attributes().PutStr(attributeDistroName, "@fusescope/browser")
	rs.Resource().Attributes().PutStr(attributeDistroVersion, "0.2.0")
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	for i := range 50 {
		s := spans.AppendEmpty()
		s.SetTraceID([16]byte{1, byte(i)})
		s.SetSpanID([8]byte{2, byte(i)})
		s.SetName("GET /api/items")
		url := "https://shop.example.com/api/items"
		if i%2 == 0 {
			url += "?page=2&email=a@b.c"
		}
		s.Attributes().PutStr("url.full", url)
		s.Attributes().PutStr("http.request.method", "GET")
		s.Attributes().PutStr(attributeSessionID, fmt.Sprintf("session-%d", i%3))
	}
	return td
}

func benchmarkAccepted(b *testing.B, contentType string, body []byte) {
	f := newFixture(b, func(cfg *Config) {
		cfg.RateLimit.Key = Limit{Rate: 1e12, Burst: 1e12}
		cfg.RateLimit.Client = Limit{Rate: 1e12, Burst: 1e12}
	})
	f.handler.traces = consumertest.NewNop()
	headers := http.Header{}
	headers.Set("Content-Type", contentType)
	headers.Set(KeyHeader, webKey)
	headers.Set("Origin", "https://shop.example.com")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, tracesPath, bytes.NewReader(body))
	req.Header = headers
	f.handler.ServeHTTP(rec, req)
	require.Equal(b, http.StatusOK, rec.Code, rec.Body.String())

	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	for b.Loop() {
		req := httptest.NewRequest(http.MethodPost, tracesPath, bytes.NewReader(body))
		req.Header = headers
		f.handler.ServeHTTP(httptest.NewRecorder(), req)
	}
}

func BenchmarkAcceptedJSON(b *testing.B) {
	body, err := (&ptrace.JSONMarshaler{}).MarshalTraces(benchTraces())
	require.NoError(b, err)
	benchmarkAccepted(b, "application/json", body)
}

func BenchmarkAcceptedProtobuf(b *testing.B) {
	body, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(benchTraces())
	require.NoError(b, err)
	benchmarkAccepted(b, "application/x-protobuf", body)
}

// BenchmarkProcessTraces is the receiver's own work on a decoded batch,
// without decoding: it should not allocate per record.
func BenchmarkProcessTraces(b *testing.B) {
	app, _ := testApplications(b)
	base := benchTraces()
	wk := &work{}
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		td := ptrace.NewTraces()
		base.CopyTo(td)
		wk.batch.Reset()
		b.StartTimer()
		processTraces(app, webKeyID, td, &wk.batch)
	}
}
