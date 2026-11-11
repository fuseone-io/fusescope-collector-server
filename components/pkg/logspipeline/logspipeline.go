// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package logspipeline runs the log pipelines FuseScope pushes to the
// collector, as docs/spec/signozlogspipeline.md specifies. The
// signozlogspipeline processor and the backend's pipeline preview both use
// it (ADR 0012), so the preview runs the collector's behaviour.
package logspipeline // import "github.com/fuseone-io/fusescope-collector-server/components/pkg/logspipeline"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

// Pipeline is one built log pipeline: stanza operators, run in order unless
// an operator names its output (spec §1). It is safe for concurrent use.
type Pipeline struct {
	pipeline *pipeline
}

// New builds the operators. An error means the pipeline cannot run.
func New(operators []map[string]any, set component.TelemetrySettings) (*Pipeline, error) {
	p, err := newPipeline(operators, set)
	if err != nil {
		return nil, err
	}
	return &Pipeline{pipeline: p}, nil
}

// Validate builds the operators and discards them, so a configuration that
// cannot run is refused before it is used.
func Validate(operators []map[string]any) error {
	_, err := newPipeline(operators, component.TelemetrySettings{Logger: zap.NewNop()})
	return err
}

// Process runs every record through the operators and rebuilds the logs
// from the records that come out (spec §5), one record at a time so a large
// shared resource is not copied for the whole batch at once. now stands for
// the collector's clock where a record has no observed timestamp. Operator
// failures are handled per record by on_error, so Process never fails; ok is
// false when no record came out.
func (p *Pipeline) Process(ctx context.Context, ld plog.Logs, now time.Time) (out plog.Logs, ok bool) {
	b := newLogsBuilder()
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
				p.pipeline.process(r, e, b.add)
			}
		}
	}
	return b.ld, b.count > 0
}
