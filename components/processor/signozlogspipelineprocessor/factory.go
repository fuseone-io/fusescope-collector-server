// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozlogspipelineprocessor // import "github.com/fuseone-io/fusescope-collector-server/components/processor/signozlogspipelineprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/fuseone-io/fusescope-collector-server/components/processor/signozlogspipelineprocessor/internal/metadata"
)

// NewFactory returns the signozlogspipeline processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		func() component.Config { return &Config{} },
		processor.WithLogs(createLogs, metadata.LogsStability),
	)
}

func createLogs(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Logs) (processor.Logs, error) {
	p, err := newProcessor(cfg.(*Config), set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewLogs(ctx, set, cfg, next, p.processLogs,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}))
}
