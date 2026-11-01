// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
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
}

// attempter is an encoder that takes the ingest time and the ID generator of
// one export request, so every retry encodes the same rows.
type attempter interface {
	setAttempt(now time.Time, random func([]byte))
}

func newSignalExporter[E attempter, D any](cfg *Config, database string, logger *zap.Logger, tables []table[E],
	newEncoder func() E, encode func(E, D, func() error) error,
) *signalExporter[E, D] {
	x := &signalExporter[E, D]{
		clickhouseClient: &clickhouseClient{cfg: cfg, database: database, logger: logger},
		tables:           tables,
		encode:           encode,
	}
	x.encoders.New = func() any { return newEncoder() }
	enc := newEncoder()
	for _, t := range tables {
		x.bodies = append(x.bodies, fmt.Sprintf("INSERT INTO %s.%s %s VALUES", database, t.name, t.input(enc).Columns()))
	}
	return x
}

func newTracesExporter(cfg *Config, logger *zap.Logger) *signalExporter[*tracesEncoder, ptrace.Traces] {
	return newSignalExporter(cfg, cfg.Traces.Database, logger, tracesTables, newTracesEncoder, infallible((*tracesEncoder).encode))
}

func newLogsExporter(cfg *Config, logger *zap.Logger) *signalExporter[*logsEncoder, plog.Logs] {
	return newSignalExporter(cfg, cfg.Logs.Database, logger, logsTables, newLogsEncoder, infallible((*logsEncoder).encode))
}

func newMetricsExporter(cfg *Config, logger *zap.Logger) *signalExporter[*metricsEncoder, pmetric.Metrics] {
	newEncoder := func() *metricsEncoder {
		e := newMetricsEncoder()
		e.logger = logger
		return e
	}
	return newSignalExporter(cfg, cfg.Metrics.Database, logger, metricsTables, newEncoder, (*metricsEncoder).encode)
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

// infallible adapts an encoder that builds one chunk and cannot fail.
func infallible[E any, D any](encode func(E, D)) func(E, D, func() error) error {
	return func(e E, d D, _ func() error) error {
		encode(e, d)
		return nil
	}
}

func (x *signalExporter[E, D]) start(ctx context.Context, _ component.Host) error {
	return x.clickhouseClient.start(ctx)
}

// push exports one request, retrying it itself (see retry): every attempt
// shares the request's identity, bounded by timeout.
func (x *signalExporter[E, D]) push(ctx context.Context, data D) error {
	id := newRequestID(time.Now())
	return retry(ctx, x.cfg.RetryConfig, x.logger, func(ctx context.Context) error {
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
			if input[0].Data.Rows() == 0 {
				continue
			}
			if err := x.insert(ctx, ch.Query{Body: x.bodies[i], Input: input}, tableToken(token, t.name, chunk)); err != nil {
				return classify(fmt.Errorf("insert into %s.%s: %w", x.database, t.name, err))
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
