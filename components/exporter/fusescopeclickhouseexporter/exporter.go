// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// permanentCodes are data errors that no retry can fix. Schema and
// availability errors stay retryable so a migration in progress loses nothing.
var permanentCodes = []proto.Error{
	proto.ErrCannotParseText,
	proto.ErrCannotParseInputAssertionFailed,
	proto.ErrTypeMismatch,
	proto.ErrCannotConvertType,
	proto.ErrIncorrectData,
	proto.ErrIllegalColumn,
	proto.ErrIncorrectNumberOfColumns,
}

type table[E any] struct {
	name  string
	input func(E) proto.Input
}

// Insert order: idempotent ReplacingMergeTree tables first, so a retry after a
// partial failure duplicates as little as possible.
var tracesTables = []table[*tracesEncoder]{
	{"distributed_traces_v3_resource", func(e *tracesEncoder) proto.Input { return e.resources.cols.input() }},
	{"distributed_tag_attributes_v2", func(e *tracesEncoder) proto.Input { return e.tags.cols.input() }},
	{"distributed_span_attributes_keys", func(e *tracesEncoder) proto.Input { return e.keys.input() }},
	{"distributed_signoz_error_index_v2", func(e *tracesEncoder) proto.Input { return e.errors.input() }},
	{"distributed_signoz_index_v3", func(e *tracesEncoder) proto.Input { return e.index.input() }},
}

var logsTables = []table[*logsEncoder]{
	{"distributed_logs_v2_resource", func(e *logsEncoder) proto.Input { return e.resources.cols.input() }},
	{"distributed_tag_attributes_v2", func(e *logsEncoder) proto.Input { return e.tags.cols.input() }},
	{"distributed_logs_attribute_keys", func(e *logsEncoder) proto.Input { return e.attrKeys.input() }},
	{"distributed_logs_resource_keys", func(e *logsEncoder) proto.Input { return e.resKeys.input() }},
	{"distributed_logs_v2", func(e *logsEncoder) proto.Input { return e.rows.input() }},
}

var metricsTables = []table[*metricsEncoder]{
	{"distributed_metadata", func(e *metricsEncoder) proto.Input { return e.meta.input() }},
	{"distributed_time_series_v4", func(e *metricsEncoder) proto.Input { return e.series.input() }},
	{"distributed_samples_v4", func(e *metricsEncoder) proto.Input { return e.samples.input() }},
}

// signalExporter encodes a batch of one signal and inserts one block per table.
type signalExporter[E attempter, D any] struct {
	*clickhouseClient
	tables   []table[E]
	bodies   []string
	encoders sync.Pool
	encode   func(E, D, func() error) error
	// telemetry records each insert attempt, indexed like tables.
	telemetry *insertTelemetry
	// stopping is closed when the exporter shuts down, to end waits between retries.
	stopping chan struct{}
	stopOnce sync.Once
	// meta, when enabled, gets every request whose export succeeded whole,
	// through observe (attributes-metadata §5).
	meta    *attrsMetadata
	observe func(D)
}

// attempter is an encoder that takes the ingest time and the ID generator of
// one export request, so every retry encodes the same rows.
type attempter interface {
	setAttempt(now time.Time, random func([]byte))
}

func newSignalExporter[E attempter, D any](cfg *Config, database string, set exporter.Settings, tables []table[E],
	newEncoder func() E, encode func(E, D, func() error) error,
) (*signalExporter[E, D], error) {
	x := &signalExporter[E, D]{
		clickhouseClient: &clickhouseClient{cfg: cfg, database: database, logger: set.Logger},
		tables:           tables,
		encode:           encode,
		stopping:         make(chan struct{}),
	}
	x.encoders.New = func() any { return newEncoder() }
	enc := newEncoder()
	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = t.name
		x.bodies = append(x.bodies, fmt.Sprintf("INSERT INTO %s.%s %s VALUES", database, t.name, t.input(enc).Columns()))
	}
	var err error
	if x.telemetry, err = newInsertTelemetry(set.TelemetrySettings, set.ID.String(), names); err != nil {
		return nil, err
	}
	return x, nil
}

func newTracesExporter(cfg *Config, set exporter.Settings) (*signalExporter[*tracesEncoder, ptrace.Traces], error) {
	newEncoder := func() *tracesEncoder {
		e := newTracesEncoder()
		e.logger = set.Logger
		return e
	}
	x, err := newSignalExporter(cfg, cfg.Traces.Database, set, tracesTables, newEncoder, (*tracesEncoder).encode)
	if err != nil || !cfg.AttributesMetadata.Enabled {
		return x, err
	}
	if x.meta, err = newAttrsMetadata(cfg, "traces", set.TelemetrySettings, set.ID.String()); err != nil {
		return nil, err
	}
	x.observe = x.meta.observeTraces
	return x, nil
}

func newLogsExporter(cfg *Config, set exporter.Settings) (*signalExporter[*logsEncoder, plog.Logs], error) {
	x, err := newSignalExporter(cfg, cfg.Logs.Database, set, logsTables, newLogsEncoder, (*logsEncoder).encode)
	if err != nil || !cfg.AttributesMetadata.Enabled {
		return x, err
	}
	if x.meta, err = newAttrsMetadata(cfg, "logs", set.TelemetrySettings, set.ID.String()); err != nil {
		return nil, err
	}
	x.observe = x.meta.observeLogs
	return x, nil
}

func newMetricsExporter(cfg *Config, set exporter.Settings) (*signalExporter[*metricsEncoder, pmetric.Metrics], error) {
	newEncoder := func() *metricsEncoder {
		e := newMetricsEncoder()
		e.logger = set.Logger
		return e
	}
	x, err := newSignalExporter(cfg, cfg.Metrics.Database, set, metricsTables, newEncoder, (*metricsEncoder).encode)
	if err != nil || !cfg.AttributesMetadata.Enabled {
		return x, err
	}
	if x.meta, err = newAttrsMetadata(cfg, "metrics", set.TelemetrySettings, set.ID.String()); err != nil {
		return nil, err
	}
	x.observe = x.meta.observeMetrics
	return x, nil
}

func newMeterExporter(cfg *Config, set exporter.Settings) (*signalExporter[*meterEncoder, pmetric.Metrics], error) {
	newEncoder := func() *meterEncoder {
		e := newMeterEncoder()
		e.logger = set.Logger
		return e
	}
	return newSignalExporter(cfg, cfg.Meter.Database, set, meterTables, newEncoder, (*meterEncoder).encode)
}

// randomShards are the tables whose Distributed engine shards by rand(): a
// retry routes their rows to other shards, where a reused token would drop rows
// that shard never had. They are Replacing or Aggregating merge trees, so the
// repeats a retry writes merge away.
var randomShards = map[string]bool{
	"distributed_tag_attributes_v2":    true,
	"distributed_span_attributes_keys": true,
	"distributed_metadata":             true,
}

// tableToken is the deduplication token of one table's block of a request
// attempt, or "" for none.
func tableToken(request, table string, chunk int) string {
	if randomShards[table] {
		return ""
	}
	token := request + "-" + table
	if chunk > 0 {
		token += "-" + strconv.Itoa(chunk)
	}
	return token
}

func (x *signalExporter[E, D]) start(ctx context.Context, _ component.Host) error {
	if err := x.clickhouseClient.start(ctx); err != nil {
		return err
	}
	if x.meta != nil {
		x.meta.start(ctx)
	}
	return nil
}

// shutdown inserts the queued metadata, then closes the connection.
func (x *signalExporter[E, D]) shutdown(ctx context.Context) error {
	var err error
	if x.meta != nil {
		err = x.meta.shutdown(ctx)
	}
	return errors.Join(err, x.clickhouseClient.shutdown(ctx))
}

// stopRetries ends the retries of requests in flight. It must run before the
// exporter helper shuts down, as the helper first waits for its queue consumers.
func (x *signalExporter[E, D]) stopRetries() {
	x.stopOnce.Do(func() { close(x.stopping) })
}

// push exports one request, retrying it itself (see retry): every attempt
// shares the request's identity, bounded by timeout.
func (x *signalExporter[E, D]) push(ctx context.Context, data D) error {
	id := newRequestID(time.Now())
	err := retry(ctx, x.cfg.RetryConfig, x.logger, x.stopping, func(ctx context.Context) error {
		if x.cfg.Timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, x.cfg.Timeout)
			defer cancel()
		}
		if err := x.refresh(ctx); err != nil {
			return err
		}
		return x.export(ctx, data, id)
	})
	// Only once every table of every attempt is confirmed: never per attempt
	// or per table.
	if err == nil && x.observe != nil {
		x.observe(data)
	}
	return err
}

// export encodes and inserts one attempt of request id. Its token identifies
// the request, not the content: equal telemetry sent twice is written twice,
// while a retry is deduplicated (DESIGN §4).
func (x *signalExporter[E, D]) export(ctx context.Context, data D, id requestID) error {
	enc := x.encoders.Get().(E)
	defer x.encoders.Put(enc)
	enc.setAttempt(id.started, id.random())
	token := id.token()
	// The encoder inserts the chunks that fill up and leaves the last to us.
	// Chunks are cut deterministically, so a retry gives each the same token.
	chunk := 0
	insert := func() error {
		for i, t := range x.tables {
			input := t.input(enc)
			rows := input[0].Data.Rows()
			if rows == 0 {
				continue
			}
			// One observation per attempt, recorded with the error as returned.
			start := time.Now()
			err := x.insert(ctx, ch.Query{Body: x.bodies[i], Input: input}, tableToken(token, t.name, chunk))
			if err != nil {
				err = classify(fmt.Errorf("insert into %s.%s: %w", x.database, t.name, err))
			}
			x.telemetry.record(ctx, i, time.Since(start), rows, err)
			if err != nil {
				return err
			}
		}
		chunk++
		return nil
	}
	if err := x.encode(enc, data, insert); err != nil {
		return err
	}
	return insert()
}

func classify(err error) error {
	if ch.IsErr(err, permanentCodes...) {
		return consumererror.NewPermanent(err)
	}
	return err
}
