// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozllmpricingprocessor // import "github.com/fuseone-io/fusescope-collector-server/components/processor/signozllmpricingprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/fuseone-io/fusescope-collector-server/components/processor/signozllmpricingprocessor/internal/metadata"
)

// NewFactory returns the signozllmpricing processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		// Empty, so a null block is refused as the reference refuses it (spec §2.1).
		func() component.Config { return &Config{} },
		processor.WithTraces(createTraces, metadata.TracesStability),
	)
}

func createTraces(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
	p := newPricer(cfg.(*Config))
	return processorhelper.NewTraces(ctx, set, cfg, next, p.processTraces,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}))
}
