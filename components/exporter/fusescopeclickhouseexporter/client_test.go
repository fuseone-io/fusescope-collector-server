// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
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

// A retry of one request keeps its identity; separate requests with equal
// content get distinct ones, so neither is deduplicated against the other.
func TestRequestIdentity(t *testing.T) {
	now := time.Unix(1_000, 0)
	r := newRequestIDs[plog.Logs](time.Minute)
	r.now = func() time.Time { return now }
	a, b := plog.NewLogs(), plog.NewLogs()
	first := r.get(a)
	if r.get(a) != first {
		t.Error("a retry got a new identity")
	}
	if r.get(b).nonce == first.nonce {
		t.Error("two requests with equal content share an identity")
	}
	r.done(a)
	if r.get(a).nonce == first.nonce {
		t.Error("a finished request kept its identity")
	}
	now = now.Add(2 * time.Minute)
	r.expire()
	r.get(plog.NewLogs())
	if len(r.ids) != 1 {
		t.Errorf("%d identities kept, want only the one after expiry", len(r.ids))
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
	r := newRequestIDs[plog.Logs](0)
	id := r.get(ld)
	time.Sleep(time.Millisecond)
	if first, retry := encode(id), encode(id); !slices.Equal(first, retry) {
		t.Errorf("retry wrote %v, want %v", retry, first)
	}
	if other := encode(r.get(plog.NewLogs())); slices.Equal(other, encode(id)) {
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

// An identity is kept for the next retry, and released when the caller gave up.
func TestPushReleasesIdentities(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.RetryConfig.MaxElapsedTime = 0
	x := newLogsExporter(cfg, zap.NewNop())
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	if err := x.push(context.Background(), ld); err == nil {
		t.Fatal("push without a pool succeeded")
	}
	if len(x.requests.ids) != 1 {
		t.Fatalf("%d identities after a retryable failure, want 1 kept for the retry", len(x.requests.ids))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = x.push(ctx, ld)
	if len(x.requests.ids) != 0 {
		t.Errorf("%d identities after a canceled push, want none", len(x.requests.ids))
	}
	if x.requests.ttl <= 0 {
		t.Errorf("ttl = %v with unlimited retries, want a bound", x.requests.ttl)
	}
}

func TestRequestIdentitiesAreBounded(t *testing.T) {
	r := newRequestIDs[plog.Logs](time.Hour)
	r.max = 3
	for range 10 {
		r.get(plog.NewLogs())
	}
	if len(r.ids) > 3 {
		t.Errorf("%d identities kept, want at most 3", len(r.ids))
	}
}

// An insert that commits but whose reply outlasts the attempt timeout is
// retried by the exporter helper with the same token, so ClickHouse drops the
// repeat.
func TestTimedOutAttemptKeepsItsToken(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.Endpoint = "testing.invalid:9440"
	cfg.QueueConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
	cfg.Timeout = 50 * time.Millisecond
	cfg.RetryConfig.InitialInterval = 5 * time.Millisecond
	cfg.RetryConfig.MaxElapsedTime = 5 * time.Second
	x := newLogsExporter(cfg, zap.NewNop())
	var tokens []string
	x.do = func(ctx context.Context, q ch.Query) error {
		if !strings.Contains(q.Body, "distributed_logs_v2 ") {
			return nil
		}
		for _, s := range q.Settings {
			if s.Key == "insert_deduplication_token" {
				tokens = append(tokens, s.Value)
			}
		}
		if len(tokens) == 1 {
			<-ctx.Done() // committed, but the reply is lost
			return ctx.Err()
		}
		return nil
	}
	ctx := context.Background()
	exp, err := exporterhelper.NewLogs(ctx, exportertest.NewNopSettings(metadata.Type), cfg, x.push, helperOptions(cfg, x.start, x.shutdown)...)
	if err != nil {
		t.Fatal(err)
	}
	if err := exp.Start(ctx, componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = exp.Shutdown(ctx) })
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("once")
	if err := exp.ConsumeLogs(ctx, ld); err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 || tokens[0] != tokens[1] {
		t.Fatalf("tokens %q, want the retry to reuse the first", tokens)
	}
}

// Identities of requests the helper gave up on expire without further pushes.
func TestAbandonedIdentitiesExpire(t *testing.T) {
	r := newRequestIDs[plog.Logs](20 * time.Millisecond)
	r.get(plog.NewLogs())
	r.start(5 * time.Millisecond)
	defer r.stop()
	deadline := time.Now().Add(2 * time.Second)
	for {
		r.mu.Lock()
		n := len(r.ids)
		r.mu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d identities still held", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
