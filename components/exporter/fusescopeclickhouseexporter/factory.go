// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

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
	return newTraces(ctx, set, c, newTracesExporter(c, set.Logger))
}

func createLogs(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	c := cfg.(*Config)
	return newLogs(ctx, set, c, newLogsExporter(c, set.Logger))
}

func createMetrics(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	c := cfg.(*Config)
	return newMetrics(ctx, set, c, newMetricsExporter(c, set.Logger))
}

func newTraces(ctx context.Context, set exporter.Settings, c *Config, x *signalExporter[*tracesEncoder, ptrace.Traces]) (exporter.Traces, error) {
	exp, err := exporterhelper.NewTraces(ctx, set, c, x.push, helperOptions(c, x.start, x.shutdown)...)
	if err != nil {
		return nil, err
	}
	return tracesExporter{exp, x.stopRetries}, nil
}

func newLogs(ctx context.Context, set exporter.Settings, c *Config, x *signalExporter[*logsEncoder, plog.Logs]) (exporter.Logs, error) {
	exp, err := exporterhelper.NewLogs(ctx, set, c, x.push, helperOptions(c, x.start, x.shutdown)...)
	if err != nil {
		return nil, err
	}
	return logsExporter{exp, x.stopRetries}, nil
}

func newMetrics(ctx context.Context, set exporter.Settings, c *Config, x *signalExporter[*metricsEncoder, pmetric.Metrics]) (exporter.Metrics, error) {
	exp, err := exporterhelper.NewMetrics(ctx, set, c, x.push, helperOptions(c, x.start, x.shutdown)...)
	if err != nil {
		return nil, err
	}
	return metricsExporter{exp, x.stopRetries}, nil
}

// The wrappers end retries in flight before the helper's Shutdown, which waits
// for its queue consumers (see signalExporter.stopRetries).
type tracesExporter struct {
	exporter.Traces
	stopRetries func()
}

func (e tracesExporter) Shutdown(ctx context.Context) error {
	e.stopRetries()
	return e.Traces.Shutdown(ctx)
}

type logsExporter struct {
	exporter.Logs
	stopRetries func()
}

func (e logsExporter) Shutdown(ctx context.Context) error {
	e.stopRetries()
	return e.Logs.Shutdown(ctx)
}

type metricsExporter struct {
	exporter.Metrics
	stopRetries func()
}

func (e metricsExporter) Shutdown(ctx context.Context) error {
	e.stopRetries()
	return e.Metrics.Shutdown(ctx)
}

func helperOptions(c *Config, start component.StartFunc, shutdown component.ShutdownFunc) []exporterhelper.Option {
	return []exporterhelper.Option{
		exporterhelper.WithStart(start),
		exporterhelper.WithShutdown(shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		// The exporter applies timeout and retry_on_failure itself, so that all
		// attempts of a request share its deduplication identity (requests.go).
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{}),
		exporterhelper.WithRetry(configretry.BackOffConfig{}),
		exporterhelper.WithQueue(c.QueueConfig),
	}
}
