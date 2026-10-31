// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
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

type table struct {
	name  string
	input func(*tracesEncoder) proto.Input
}

// tracesTables is the insert order: idempotent ReplacingMergeTree tables first,
// so a retry after a partial failure duplicates as little as possible.
var tracesTables = []table{
	{"distributed_traces_v3_resource", func(e *tracesEncoder) proto.Input { return e.resources.input() }},
	{"distributed_tag_attributes_v2", func(e *tracesEncoder) proto.Input { return e.tags.input() }},
	{"distributed_span_attributes_keys", func(e *tracesEncoder) proto.Input { return e.keys.input() }},
	{"distributed_signoz_error_index_v2", func(e *tracesEncoder) proto.Input { return e.errors.input() }},
	{"distributed_signoz_index_v3", func(e *tracesEncoder) proto.Input { return e.index.input() }},
}

type tracesExporter struct {
	cfg      *Config
	logger   *zap.Logger
	pool     *chpool.Pool
	encoders sync.Pool
	bodies   []string
}

func newTracesExporter(cfg *Config, logger *zap.Logger) *tracesExporter {
	x := &tracesExporter{cfg: cfg, logger: logger}
	x.encoders.New = func() any { return newTracesEncoder() }
	enc := newTracesEncoder()
	for _, t := range tracesTables {
		x.bodies = append(x.bodies, fmt.Sprintf("INSERT INTO %s.%s %s VALUES", cfg.Traces.Database, t.name, t.input(enc).Columns()))
	}
	return x
}

func (x *tracesExporter) start(ctx context.Context, _ component.Host) error {
	opts, err := x.clientOptions(ctx)
	if err != nil {
		return err
	}
	pool, err := chpool.New(ctx, chpool.Options{ClientOptions: opts, MaxConns: x.cfg.MaxConnections})
	if err != nil {
		return fmt.Errorf("create ClickHouse pool: %w", err)
	}
	x.pool = pool
	return nil
}

func (x *tracesExporter) shutdown(context.Context) error {
	if x.pool != nil {
		x.pool.Close()
	}
	return nil
}

func (x *tracesExporter) clientOptions(ctx context.Context) (ch.Options, error) {
	cfg := x.cfg
	opts := ch.Options{
		Logger:      x.logger,
		Address:     cfg.Endpoint,
		Database:    cfg.Traces.Database,
		User:        cfg.Username,
		Compression: ch.CompressionLZ4,
		ClientName:  "fusescope-collector",
		DialTimeout: cfg.DialTimeout,
		ReadTimeout: cfg.Timeout,
	}
	if cfg.PasswordFile != "" {
		b, err := os.ReadFile(cfg.PasswordFile)
		if err != nil {
			return opts, fmt.Errorf("read password_file: %w", err)
		}
		opts.Password = strings.TrimRight(string(b), "\r\n")
	}
	if cfg.TLS.Insecure {
		x.logger.Warn("TLS to ClickHouse is disabled; credentials and telemetry travel in clear text", zap.String("endpoint", cfg.Endpoint))
		return opts, nil
	}
	tlsCfg, err := cfg.TLS.LoadTLSConfig(ctx)
	if err != nil {
		return opts, fmt.Errorf("load TLS config: %w", err)
	}
	opts.TLS = tlsCfg
	return opts, nil
}

func (x *tracesExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	enc := x.encoders.Get().(*tracesEncoder)
	defer x.encoders.Put(enc)
	enc.encode(td)
	for i, t := range tracesTables {
		input := t.input(enc)
		if input[0].Data.Rows() == 0 {
			continue
		}
		if err := x.pool.Do(ctx, ch.Query{Body: x.bodies[i], Input: input}); err != nil {
			return classify(fmt.Errorf("insert into %s.%s: %w", x.cfg.Traces.Database, t.name, err))
		}
	}
	return nil
}

func classify(err error) error {
	if ch.IsErr(err, permanentCodes...) {
		return consumererror.NewPermanent(err)
	}
	return err
}
