// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter/internal/metadata"
)

// NewFactory returns the fusescope_clickhouse exporter factory.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		func() component.Config { return createDefaultConfig() },
		exporter.WithTraces(createTraces, metadata.TracesStability),
		exporter.WithLogs(createLogs, metadata.LogsStability),
		exporter.WithMetrics(createMetrics, metadata.MetricsStability),
	)
}

func createTraces(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	c := cfg.(*Config)
	x := newTracesExporter(c, set.Logger)
	return exporterhelper.NewTraces(ctx, set, cfg, x.push, helperOptions(c, x.start, x.shutdown)...)
}

func createLogs(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	c := cfg.(*Config)
	x := newLogsExporter(c, set.Logger)
	return exporterhelper.NewLogs(ctx, set, cfg, x.push, helperOptions(c, x.start, x.shutdown)...)
}

func createMetrics(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	c := cfg.(*Config)
	x := newMetricsExporter(c, set.Logger)
	return exporterhelper.NewMetrics(ctx, set, cfg, x.push, helperOptions(c, x.start, x.shutdown)...)
}

func helperOptions(c *Config, start component.StartFunc, shutdown component.ShutdownFunc) []exporterhelper.Option {
	return []exporterhelper.Option{
		exporterhelper.WithStart(start),
		exporterhelper.WithShutdown(shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithTimeout(c.TimeoutConfig),
		exporterhelper.WithRetry(c.RetryConfig),
		exporterhelper.WithQueue(c.QueueConfig),
	}
}
