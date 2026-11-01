// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"context"
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
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

func TestBatchTokenIdentifiesTheBatchContent(t *testing.T) {
	marshal := func(td ptrace.Traces) []byte {
		b, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	a, b := syntheticTraces(100), syntheticTraces(100)
	if batchToken(marshal(a)) != batchToken(marshal(b)) {
		t.Error("equal batches get different tokens, so a retry would not be deduplicated")
	}
	b.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).SetName("changed")
	if batchToken(marshal(a)) == batchToken(marshal(b)) {
		t.Error("different batches share a token, so one would be dropped as a duplicate")
	}
}
