// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// syntheticMetrics resembles an HTTP service: 10 resources, per resource a
// request counter and a 10-bucket latency histogram, 5 attributes per point.
func syntheticMetrics(points int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	for r := range 10 {
		rm := md.ResourceMetrics().AppendEmpty()
		ra := rm.Resource().Attributes()
		ra.PutStr("service.name", fmt.Sprintf("svc-%d", r))
		ra.PutStr("deployment.environment", "prod")
		ra.PutStr("host.name", fmt.Sprintf("node-%d", r))
		ra.PutStr("k8s.pod.name", fmt.Sprintf("svc-%d-7d9f", r))
		ra.PutStr("telemetry.sdk.language", "go")
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("bench")
		sum := sm.Metrics().AppendEmpty()
		sum.SetName("http.server.requests")
		sum.SetEmptySum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		hist := sm.Metrics().AppendEmpty()
		hist.SetName("http.server.duration")
		hist.SetUnit("ms")
		hist.SetEmptyHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		for i := range points / 20 {
			ts := pcommon.Timestamp(1_793_483_525_000_000_000 + i*1_000_000)
			attrs := func(m pcommon.Map) {
				m.PutStr("http.request.method", "GET")
				m.PutStr("http.route", fmt.Sprintf("/items/%d", i%50))
				m.PutInt("http.response.status_code", 200)
				m.PutStr("server.address", "shop.example.com")
				m.PutStr("tenant.id", fmt.Sprintf("t-%d", i%20))
			}
			p := sum.Sum().DataPoints().AppendEmpty()
			p.SetTimestamp(ts)
			p.SetIntValue(int64(i))
			attrs(p.Attributes())
			h := hist.Histogram().DataPoints().AppendEmpty()
			h.SetTimestamp(ts)
			h.ExplicitBounds().FromRaw([]float64{5, 10, 25, 50, 75, 100, 250, 500, 1000})
			h.BucketCounts().FromRaw([]uint64{1, 2, 3, 4, 5, 4, 3, 2, 1, 1})
			h.SetCount(26)
			h.SetSum(1234)
			attrs(h.Attributes())
		}
	}
	return md
}

func BenchmarkEncodeMetrics(b *testing.B) {
	md := syntheticMetrics(10_000)
	enc := newMetricsEncoder()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := enc.encode(md, nil); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*10_000), "ns/point")
}

// A shared 64 KiB resource over 10,000 points of 10 series: the cost per point
// must not grow with the resource.
func BenchmarkEncodeMetricsSharedResource(b *testing.B) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("k8s.pod.annotations", strings.Repeat("x", 64<<10))
	g := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("queue.depth")
	gauge := g.SetEmptyGauge()
	for i := range 10_000 {
		p := gauge.DataPoints().AppendEmpty()
		p.SetTimestamp(pcommon.Timestamp(1_793_483_525_000_000_000 + i))
		p.SetIntValue(int64(i))
		p.Attributes().PutInt("queue", int64(i%10))
	}
	enc := newMetricsEncoder()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := enc.encode(md, nil); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*10_000), "ns/point")
}
