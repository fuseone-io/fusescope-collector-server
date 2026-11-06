// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopemeterconnector

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"

	"github.com/fuseone-io/fusescope-collector-server/components/connector/fusescopemeterconnector/internal/metadatatest"
)

// The consume path runs in the request's goroutine: counting a batch whose
// sets already exist must not allocate.
func BenchmarkConsume(b *testing.B) {
	var resources []map[string]string
	for i := range 10 {
		resources = append(resources, map[string]string{
			"service.name": "svc-" + strconv.Itoa(i), "deployment.environment.name": "prod", "host.name": "host-1",
		})
	}
	td := traces(100, "GET /items/{id}", resources...)
	ld := logs(100, "served the request in 12 ms", resources...)
	md := metrics(resources[0])
	tel := componenttest.NewTelemetry()
	defer func() { require.NoError(b, tel.Shutdown(context.Background())) }()
	ctx := withKey("k1")
	for _, bc := range []struct {
		name    string
		sig     signal
		consume func(*meter) error
		records int
	}{
		{"traces", signalTraces, func(m *meter) error { return m.ConsumeTraces(ctx, td) }, td.SpanCount()},
		{"logs", signalLogs, func(m *meter) error { return m.ConsumeLogs(ctx, ld) }, ld.LogRecordCount()},
		{"metrics", signalMetrics, func(m *meter) error { return m.ConsumeMetrics(ctx, md) }, md.DataPointCount()},
	} {
		b.Run(bc.name, func(b *testing.B) {
			m, err := newMeter(defaultConfig(), metadatatest.NewSettings(tel), consumertest.NewNop(), bc.sig)
			require.NoError(b, err)
			require.NoError(b, bc.consume(m)) // create the sets
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := bc.consume(m); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*bc.records), "ns/record")
		})
	}
}

// Overflowing and cutting record telemetry without allocating, so a sender
// that fills the limits does not make the data path allocate.
func TestOverflowAndCutsDoNotAllocate(t *testing.T) {
	f := newFixture(t, signalLogs, func(c *Config) {
		c.MaxSeries = 1
		c.MaxLabelBytes = 8
	})
	require.NoError(t, f.m.ConsumeLogs(context.Background(), logs(1, "b", map[string]string{"service.name": "first"})))
	over := logs(1, "b", map[string]string{"service.name": "a-name-longer-than-8"})
	require.NoError(t, f.m.ConsumeLogs(withKey("k"), over)) // creates the overflow set
	for _, ctx := range []context.Context{withKey("k"), context.Background()} {
		allocs := testing.AllocsPerRun(50, func() { _ = f.m.ConsumeLogs(ctx, over) })
		require.Zero(t, allocs, "allocations per overflowing, cut record")
	}
}

func BenchmarkConsumeOverflow(b *testing.B) {
	tel := componenttest.NewTelemetry()
	defer func() { require.NoError(b, tel.Shutdown(context.Background())) }()
	cfg := defaultConfig()
	cfg.MaxSeries, cfg.MaxLabelBytes = 1, 8
	m, err := newMeter(cfg, metadatatest.NewSettings(tel), consumertest.NewNop(), signalLogs)
	require.NoError(b, err)
	require.NoError(b, m.ConsumeLogs(context.Background(), logs(1, "b", map[string]string{"service.name": "first"})))
	ld := logs(100, "b", map[string]string{"service.name": "a-name-longer-than-8"})
	ctx := withKey("k")
	require.NoError(b, m.ConsumeLogs(ctx, ld))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = m.ConsumeLogs(ctx, ld)
	}
}
