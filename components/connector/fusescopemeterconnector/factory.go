// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopemeterconnector // import "github.com/fuseone-io/fusescope-collector-server/components/connector/fusescopemeterconnector"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"

	"github.com/fuseone-io/fusescope-collector-server/components/connector/fusescopemeterconnector/internal/metadata"
)

// NewFactory returns the fusescope_meter connector factory. The collector
// creates one instance per signal, each with its own connector_id (meter §5).
func NewFactory() connector.Factory {
	return connector.NewFactory(
		metadata.Type,
		func() component.Config { return defaultConfig() },
		connector.WithTracesToMetrics(createTracesToMetrics, metadata.TracesToMetricsStability),
		connector.WithLogsToMetrics(createLogsToMetrics, metadata.LogsToMetricsStability),
		connector.WithMetricsToMetrics(createMetricsToMetrics, metadata.MetricsToMetricsStability),
	)
}

func createTracesToMetrics(_ context.Context, set connector.Settings, cfg component.Config, next consumer.Metrics) (connector.Traces, error) {
	return newMeter(cfg.(*Config), set, next, signalTraces)
}

func createLogsToMetrics(_ context.Context, set connector.Settings, cfg component.Config, next consumer.Metrics) (connector.Logs, error) {
	return newMeter(cfg.(*Config), set, next, signalLogs)
}

func createMetricsToMetrics(_ context.Context, set connector.Settings, cfg component.Config, next consumer.Metrics) (connector.Metrics, error) {
	return newMeter(cfg.(*Config), set, next, signalMetrics)
}
