// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozspanmapperprocessor // import "github.com/fuseone-io/fusescope-collector-server/components/processor/signozspanmapperprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/fuseone-io/fusescope-collector-server/components/processor/signozspanmapperprocessor/internal/metadata"
)

// NewFactory returns the signozspanmapper processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		func() component.Config { return &Config{} },
		processor.WithTraces(createTraces, metadata.TracesStability),
	)
}

func createTraces(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
	m := newMapper(cfg.(*Config))
	return processorhelper.NewTraces(ctx, set, cfg, next, m.processTraces,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}))
}
