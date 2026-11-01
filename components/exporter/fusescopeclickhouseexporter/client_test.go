// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter/internal/metadata"
)

func TestTLSMinimumIsAlways13(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.Endpoint = "clickhouse:9440"
	cfg.TLS.MinVersion = ""
	opts, err := newTracesExporter(cfg, zap.NewNop()).clientOptions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if opts.TLS == nil || opts.TLS.MinVersion != tls.VersionTLS13 {
		t.Fatalf("TLS = %+v, want a minimum of TLS 1.3 when min_version is empty", opts.TLS)
	}
}

func TestPasswordFileChangeRecreatesThePool(t *testing.T) {
	path := filepath.Join(t.TempDir(), "password")
	write := func(s string, mod time.Time) {
		t.Helper()
		if err := os.WriteFile(path, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, mod, mod); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now().Add(-time.Hour)
	write("old\n", start)

	cfg := createDefaultConfig()
	cfg.Endpoint = "testing.invalid:9440"
	cfg.PasswordFile = path
	x := newTracesExporter(cfg, zap.NewNop())
	ctx := context.Background()
	if err := x.start(ctx, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = x.shutdown(ctx) })

	first := x.pool.Load()
	if err := x.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if x.pool.Load() != first {
		t.Fatal("pool recreated although the password file did not change")
	}

	write("new\n", start.Add(time.Minute))
	if err := x.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if x.pool.Load() == first {
		t.Fatal("pool kept after the password file changed")
	}
	opts, err := x.clientOptions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Password != "new" {
		t.Fatalf("password = %q, want the rotated one", opts.Password)
	}
}

// The stamp describes the file the password was read from, so a replacement
// after the read is seen by the next refresh.
func TestPasswordStampIsTheReadFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "password")
	mod := time.Now().Add(-time.Hour).Truncate(time.Second)
	if err := os.WriteFile(path, []byte("old-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mod, mod); err != nil {
		t.Fatal(err)
	}
	password, stamp, err := readPassword(path)
	if err != nil {
		t.Fatal(err)
	}
	if password != "old-secret" || stamp.size != int64(len("old-secret\n")) || !stamp.modTime.Equal(mod) {
		t.Fatalf("read %q with stamp %+v, want the stamp of that content", password, stamp)
	}
}

// A retry rebuilds the same log IDs and ingest times, so each record goes to
// the same shard (logs_v2 shards by id) and deduplicates there.
func TestRetryKeepsLogIDs(t *testing.T) {
	ld := plog.NewLogs()
	records := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for range 3 {
		records.AppendEmpty().Body().SetStr("no time")
	}
	encode := func(id requestID) []string {
		enc := newLogsEncoder()
		enc.setAttempt(id.started, id.random())
		enc.encode(ld)
		var out []string
		for i := range enc.rows.id.Rows() {
			out = append(out, fmt.Sprint(enc.rows.id.Row(i), enc.rows.timestamp.Row(i)))
		}
		return out
	}
	id := newRequestID(time.Now())
	if first, retry := encode(id), encode(id); !slices.Equal(first, retry) {
		t.Errorf("retry wrote %v, want %v", retry, first)
	}
	if other := encode(newRequestID(time.Now())); slices.Equal(other, encode(id)) {
		t.Error("another request wrote the same IDs")
	}
}

// Tables sharded by rand() route a retry's rows to other shards, where a reused
// token would drop them; they get no token (their repeats merge away).
func TestTableTokens(t *testing.T) {
	tests := []struct {
		table string
		chunk int
		want  string
	}{
		{"distributed_signoz_index_v3", 0, "id-distributed_signoz_index_v3"},
		{"distributed_samples_v4", 2, "id-distributed_samples_v4-2"},
		{"distributed_tag_attributes_v2", 0, ""},
		{"distributed_span_attributes_keys", 0, ""},
		{"distributed_metadata", 1, ""},
	}
	for _, tt := range tests {
		if got := tableToken("id", tt.table, tt.chunk); got != tt.want {
			t.Errorf("tableToken(%s, %d) = %q, want %q", tt.table, tt.chunk, got, tt.want)
		}
	}
}

// helperLogs runs the logs exporter behind the real exporter helper, without a
// queue, with do in place of ClickHouse.
func helperLogs(t *testing.T, cfg *Config, do func(context.Context, ch.Query) error) exporter.Logs {
	t.Helper()
	cfg.Endpoint = "testing.invalid:9440"
	cfg.QueueConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
	x := newLogsExporter(cfg, zap.NewNop())
	x.do = do
	ctx := context.Background()
	exp, err := exporterhelper.NewLogs(ctx, exportertest.NewNopSettings(metadata.Type), cfg, x.push, helperOptions(cfg, x.start, x.shutdown)...)
	if err != nil {
		t.Fatal(err)
	}
	if err := exp.Start(ctx, componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = exp.Shutdown(ctx) })
	return exp
}

func fastRetries() *Config {
	cfg := createDefaultConfig()
	cfg.Timeout = 50 * time.Millisecond
	cfg.RetryConfig.InitialInterval = 5 * time.Millisecond
	cfg.RetryConfig.MaxElapsedTime = 5 * time.Second
	return cfg
}

func oneLog() plog.Logs {
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("once")
	return ld
}

// logsTokens records the token of every logs_v2 insert and answers with reply.
func logsTokens(mu *sync.Mutex, tokens *[]string, reply func(ctx context.Context, attempt int) error) func(context.Context, ch.Query) error {
	return func(ctx context.Context, q ch.Query) error {
		if !strings.Contains(q.Body, "distributed_logs_v2 ") {
			return nil
		}
		mu.Lock()
		for _, s := range q.Settings {
			if s.Key == "insert_deduplication_token" {
				*tokens = append(*tokens, s.Value)
			}
		}
		n := len(*tokens)
		mu.Unlock()
		return reply(ctx, n)
	}
}

func TestRetries(t *testing.T) {
	permanent := consumererror.NewPermanent(errors.New("bad data"))
	tests := []struct {
		name       string
		cfg        func() *Config
		reply      func(ctx context.Context, attempt int) error
		wantErr    bool
		wantTokens func([]string) bool
	}{
		// Committed, but the reply outlasts the attempt timeout: the retry
		// carries the same token, so ClickHouse drops the repeat.
		{"timeout after commit", fastRetries, func(ctx context.Context, n int) error {
			if n == 1 {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}, false, func(tk []string) bool { return len(tk) == 2 && tk[0] == tk[1] }},
		{"gives up after max_elapsed_time", func() *Config {
			cfg := fastRetries()
			cfg.RetryConfig.MaxElapsedTime = 100 * time.Millisecond
			return cfg
		}, func(context.Context, int) error { return errors.New("connection refused") },
			true, func(tk []string) bool { return len(tk) > 1 }},
		{"permanent error is not retried", fastRetries, func(context.Context, int) error { return permanent },
			true, func(tk []string) bool { return len(tk) == 1 }},
		{"retries off", func() *Config {
			cfg := fastRetries()
			cfg.RetryConfig.Enabled = false
			return cfg
		}, func(context.Context, int) error { return errors.New("connection refused") },
			true, func(tk []string) bool { return len(tk) == 1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mu sync.Mutex
			var tokens []string
			exp := helperLogs(t, tt.cfg(), logsTokens(&mu, &tokens, tt.reply))
			err := exp.ConsumeLogs(context.Background(), oneLog())
			if (err != nil) != tt.wantErr {
				t.Fatalf("ConsumeLogs = %v, want error %v", err, tt.wantErr)
			}
			if !tt.wantTokens(tokens) {
				t.Errorf("tokens %q", tokens)
			}
		})
	}
}

// Two consumes of one immutable pdata value are two requests: neither may be
// deduplicated as a retry of the other.
func TestConcurrentConsumesGetDistinctTokens(t *testing.T) {
	var mu sync.Mutex
	var tokens []string
	both := make(chan struct{})
	var arrived sync.WaitGroup
	arrived.Add(2)
	exp := helperLogs(t, fastRetries(), logsTokens(&mu, &tokens, func(context.Context, int) error {
		arrived.Done()
		<-both
		return nil
	}))
	ld := oneLog()
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			if err := exp.ConsumeLogs(context.Background(), ld); err != nil {
				t.Error(err)
			}
		})
	}
	arrived.Wait()
	close(both)
	wg.Wait()
	if len(tokens) != 2 || tokens[0] == tokens[1] {
		t.Fatalf("tokens %q, want two distinct", tokens)
	}
}
