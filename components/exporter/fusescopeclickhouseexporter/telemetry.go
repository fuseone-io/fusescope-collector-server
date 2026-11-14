// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter/internal/metadata"
)

// insertOutcome is the class of one insert attempt, as the exporter returns it.
type insertOutcome int

const (
	outcomeOK insertOutcome = iota
	outcomeRetryable
	outcomePermanent
	numOutcomes
)

var outcomeNames = [numOutcomes]string{"ok", "retryable", "permanent"}

func (o insertOutcome) String() string { return outcomeNames[o] }

// outcomeOf classifies an error that classify has already marked.
func outcomeOf(err error) insertOutcome {
	switch {
	case err == nil:
		return outcomeOK
	case consumererror.IsPermanent(err):
		return outcomePermanent
	default:
		return outcomeRetryable
	}
}

// insertTelemetry records the insert attempts of one signal's tables. The
// attribute options are built once per table and outcome, so recording an
// attempt allocates nothing.
type insertTelemetry struct {
	tb     *metadata.TelemetryBuilder
	tables []tableTelemetry
}

type tableTelemetry struct {
	rows     []metric.AddOption
	duration [numOutcomes][]metric.RecordOption
	errors   [numOutcomes][]metric.AddOption
}

// newInsertTelemetry prepares the attributes of tables, indexed as given,
// each naming the exporter instance, so two instances in one process keep
// their own series.
func newInsertTelemetry(set component.TelemetrySettings, exporterID string, tables []string) (*insertTelemetry, error) {
	tb, err := metadata.NewTelemetryBuilder(set)
	if err != nil {
		return nil, err
	}
	t := &insertTelemetry{tb: tb, tables: make([]tableTelemetry, len(tables))}
	exporter := attribute.String("exporter", exporterID)
	for i, name := range tables {
		table := attribute.String("table", name)
		tt := &t.tables[i]
		tt.rows = []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(exporter, table))}
		for o := range numOutcomes {
			attrs := metric.WithAttributeSet(attribute.NewSet(exporter, table, attribute.String("outcome", outcomeNames[o])))
			tt.duration[o] = []metric.RecordOption{attrs}
			tt.errors[o] = []metric.AddOption{attrs}
		}
	}
	return t, nil
}

// record records one attempt into table i that took d to send rows and
// returned err, already classified.
func (t *insertTelemetry) record(ctx context.Context, i int, d time.Duration, rows int, err error) {
	tt := &t.tables[i]
	o := outcomeOf(err)
	t.tb.ExporterFusescopeClickhouseInsertDuration.Record(ctx, d.Seconds(), tt.duration[o]...)
	if o == outcomeOK {
		t.tb.ExporterFusescopeClickhouseInsertedRows.Add(ctx, int64(rows), tt.rows...)
		return
	}
	t.tb.ExporterFusescopeClickhouseInsertErrors.Add(ctx, 1, tt.errors[o]...)
}
