// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozlogspipelineprocessor // import "github.com/fuseone-io/fusescope-collector-server/components/processor/signozlogspipelineprocessor"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/logspipeline"
)

type logsProcessor struct {
	pipeline *logspipeline.Pipeline
	now      func() time.Time
}

func newProcessor(cfg *Config, set component.TelemetrySettings) (*logsProcessor, error) {
	p, err := logspipeline.New(cfg.Operators, set)
	if err != nil {
		return nil, err
	}
	return &logsProcessor{pipeline: p, now: time.Now}, nil
}

// processLogs runs the pipeline (logspipeline.Pipeline.Process). The call
// itself never fails; when no record comes out, the batch is skipped.
func (p *logsProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	out, ok := p.pipeline.Process(ctx, ld, p.now())
	if !ok {
		return ld, processorhelper.ErrSkipProcessingData
	}
	return out, nil
}
