// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopespanmetricsconnector // import "github.com/fuseone-io/fusescope-collector-server/components/connector/fusescopespanmetricsconnector"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"

	"github.com/fuseone-io/fusescope-collector-server/components/connector/fusescopespanmetricsconnector/internal/metadata"
)

// NewFactory returns the fusescope_spanmetrics connector factory.
func NewFactory() connector.Factory {
	return connector.NewFactory(
		metadata.Type,
		func() component.Config { return defaultConfig() },
		connector.WithTracesToMetrics(createTracesToMetrics, metadata.TracesToMetricsStability),
	)
}

func createTracesToMetrics(_ context.Context, set connector.Settings, cfg component.Config, next consumer.Metrics) (connector.Traces, error) {
	return newConnector(cfg.(*Config), set, next), nil
}
