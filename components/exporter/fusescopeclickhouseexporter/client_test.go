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
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
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
