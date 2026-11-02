// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozlogspipelineprocessor // import "github.com/fuseone-io/fusescope-collector-server/components/processor/signozlogspipelineprocessor"

import (
	"context"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

type logsProcessor struct {
	pipeline *pipeline
	now      func() time.Time
}

func newProcessor(cfg *Config, set component.TelemetrySettings) (*logsProcessor, error) {
	p, err := newPipeline(cfg.Operators, set)
	if err != nil {
		return nil, err
	}
	return &logsProcessor{pipeline: p, now: time.Now}, nil
}

// processLogs runs every record through the operators and rebuilds the
// logs from the records that come out (spec §5). Operator failures are
// handled per record by on_error, so the call itself never fails.
func (p *logsProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	entries := toEntries(ld, p.now(), make([]*entry.Entry, 0, ld.LogRecordCount()))
	out := p.pipeline.process(ctx, entries)
	if len(out) == 0 {
		return ld, processorhelper.ErrSkipProcessingData
	}
	return fromEntries(out), nil
}
