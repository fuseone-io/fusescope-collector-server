// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozlogspipelineprocessor // import "github.com/fuseone-io/fusescope-collector-server/components/processor/signozlogspipelineprocessor"

import (
	"context"
	"time"

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
// logs from the records that come out (spec §5), one record at a time so a
// large shared resource is not copied for the whole batch at once.
// Operator failures are handled per record by on_error, so the call itself
// never fails.
func (p *logsProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	now := p.now()
	out := newLogsBuilder()
	r := newRun(ctx)
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		var shared map[string]any
		if !p.pipeline.writesResource {
			shared = rl.Resource().Attributes().AsRaw()
		}
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				e := toEntry(sl.LogRecords().At(k), now)
				e.ScopeName = sl.Scope().Name()
				e.Resource = shared
				if e.Resource == nil {
					e.Resource = rl.Resource().Attributes().AsRaw()
				}
				p.pipeline.process(r, e, out.add)
			}
		}
	}
	if out.count == 0 {
		return ld, processorhelper.ErrSkipProcessingData
	}
	return out.ld, nil
}
