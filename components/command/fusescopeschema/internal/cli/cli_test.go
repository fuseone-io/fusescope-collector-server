// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/chstore"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/coord"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/migrate"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

func TestLoadPrecedence(t *testing.T) {
	file := filepath.Join(t.TempDir(), "c.json")
	if err := os.WriteFile(file, []byte(`{"endpoint": "file:9000", "username": "fileuser", "cluster": "filecluster",
		"databases": {"logs": "file_logs"}, "tls": {"ca_file": "/ca.pem"}, "lock_timeout": "2m"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, rest, err := load([]string{"--config", file, "--username", "flaguser", "extra"}, env(map[string]string{
		"FUSESCOPE_SCHEMA_USERNAME":        "envuser",
		"FUSESCOPE_SCHEMA_CLUSTER":         "envcluster",
		"FUSESCOPE_SCHEMA_DATABASE_TRACES": "env_traces",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Conn.Endpoint != "file:9000" || cfg.Conn.Username != "flaguser" || cfg.Conn.Cluster != "envcluster" || cfg.Conn.TLS.CAFile != "/ca.pem" || cfg.LockTimeout != 2*time.Minute {
		t.Fatalf("config %+v", cfg)
	}
	if cfg.Databases["logs"] != "file_logs" || cfg.Databases["traces"] != "env_traces" || cfg.Databases["meter"] != schema.DefaultDatabases["meter"] {
		t.Fatalf("databases %v", cfg.Databases)
	}
	if len(rest) != 1 || rest[0] != "extra" {
		t.Fatalf("rest %v", rest)
	}
}

func TestLoadRefusals(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	tests := []struct {
		name string
		args []string
		env  map[string]string
	}{
		{"an inline password in the environment", []string{"--endpoint", "h:9000"}, map[string]string{"FUSESCOPE_SCHEMA_PASSWORD": "s3cret"}},
		{"an inline password in the file", []string{"--config", write("p.json", `{"endpoint": "h:9000", "password": "s3cret"}`)}, nil},
		{"an unknown key in the file", []string{"--config", write("u.json", `{"endpoint": "h:9000", "endpiont": "x"}`)}, nil},
		{"an unknown flag", []string{"--password", "s3cret"}, nil},
		{"a database for an unknown role", nil, map[string]string{"FUSESCOPE_SCHEMA_ENDPOINT": "h:9000"}},
		{"two roles on one database", []string{"--endpoint", "h:9000", "--database-logs", "x", "--database-traces", "x"}, nil},
	}
	tests[4].args = []string{"--config", write("r.json", `{"endpoint": "h:9000", "databases": {"logz": "x"}}`)}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := load(tt.args, env(tt.env))
			if !errors.Is(err, ErrUsage) {
				t.Fatalf("load = %v, want ErrUsage", err)
			}
			if strings.Contains(err.Error(), "s3cret") {
				t.Fatalf("the error shows the password: %v", err)
			}
		})
	}
}

// Exit codes come from typed errors and Result.Started, never from text.
func TestExitCodes(t *testing.T) {
	drift := &migrate.DriftError{Diffs: []schema.Diff{{Replica: "r", Object: "o", Kind: schema.Missing}}}
	transport := errors.New("connection reset by peer")
	refusals := []error{
		drift, migrate.ErrInterrupted, migrate.ErrUnknownMigration, migrate.ErrChecksum, migrate.ErrRepairRefused,
		coord.ErrInterrupted, coord.ErrUnknownLayout, coord.ErrDivergent, coord.ErrConfig, chstore.ErrPrecheck, chstore.ErrNoKeeperMapPrefix,
		chstore.ErrInvalidConfig, ErrUsage,
	}
	for _, err := range refusals {
		wrapped := fmt.Errorf("context: %w", err)
		if got := migrateCode(migrate.Result{}, wrapped); got != ExitRefused {
			t.Errorf("migrate %v = %d, want 1", err, got)
		}
		if got := migrateCode(migrate.Result{Started: true}, wrapped); got != ExitStarted {
			t.Errorf("migrate %v after started = %d, want 4", err, got)
		}
		if got := repairCode(wrapped); got != ExitRefused {
			t.Errorf("repair %v = %d, want 1", err, got)
		}
	}
	for _, err := range []error{transport, coord.ErrLeaseTimeout, coord.ErrLeaseLost, coord.ErrUnavailable, context.Canceled, context.DeadlineExceeded} {
		if got := migrateCode(migrate.Result{}, err); got != ExitRetry {
			t.Errorf("migrate %v = %d, want 2", err, got)
		}
		if got := migrateCode(migrate.Result{Started: true}, err); got != ExitStarted {
			t.Errorf("migrate %v after started = %d, want 4", err, got)
		}
		if got := repairCode(err); got != ExitRetry {
			t.Errorf("repair %v = %d, want 2", err, got)
		}
	}
	if migrateCode(migrate.Result{}, nil) != ExitOK || repairCode(nil) != ExitOK {
		t.Error("success is not 0")
	}
	// A message that merely reads like a refusal is still a transport error.
	if got := migrateCode(migrate.Result{}, errors.New(migrate.ErrChecksum.Error())); got != ExitRetry {
		t.Errorf("an untyped error with a refusal's text = %d, want 2", got)
	}
	for verdict, want := range map[migrate.Verdict]int{migrate.UpToDate: ExitOK, migrate.Pending: ExitPending, migrate.InProgress: ExitPending} {
		if got := checkCode(migrate.State{Verdict: verdict}, nil); got != want {
			t.Errorf("check %s = %d, want %d", verdict, got, want)
		}
	}
	if checkCode(migrate.State{}, drift) != ExitRefused || checkCode(migrate.State{}, transport) != ExitRetry {
		t.Error("check's refusal or transport code")
	}
}

func TestWaitCheck(t *testing.T) {
	seq := func(codes ...int) func(context.Context) int {
		i := 0
		return func(context.Context) int {
			c := codes[min(i, len(codes)-1)]
			i++
			return c
		}
	}
	tests := []struct {
		name  string
		wait  time.Duration
		once  func(context.Context) int
		want  int
		ticks int
	}{
		{"no wait: pending answers at once", 0, seq(ExitPending), ExitPending, 0},
		{"pending, then up to date", time.Minute, seq(ExitPending, ExitPending, ExitOK), ExitOK, 2},
		{"refused at once ends the wait", time.Minute, seq(ExitRefused), ExitRefused, 0},
		{"pending past the wait", 30 * time.Second, seq(ExitPending), ExitPending, 3},
		{"connection errors, then up to date", time.Minute, seq(ExitRetry, ExitRetry, ExitOK), ExitOK, 2},
		{"connection errors past the wait", 30 * time.Second, seq(ExitRetry), ExitRetry, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var now time.Time
			ticks := 0
			clock := func() time.Time { return now }
			sleep := func(ctx context.Context, d time.Duration) error {
				ticks++
				now = now.Add(d)
				return ctx.Err()
			}
			got := waitCheck(context.Background(), tt.wait, 10*time.Second, tt.once, clock, sleep)
			if got != tt.want || ticks != tt.ticks {
				t.Fatalf("waitCheck = %d after %d polls, want %d after %d", got, ticks, tt.want, tt.ticks)
			}
		})
	}
	t.Run("pending, then an attempt cut by the deadline: still pending", func(t *testing.T) {
		calls := 0
		got := waitCheck(context.Background(), 300*time.Millisecond, 10*time.Millisecond, func(ctx context.Context) int {
			calls++
			if calls == 1 {
				return ExitPending
			}
			<-ctx.Done()
			return ExitRetry
		}, time.Now, sleepCtx)
		if got != ExitPending {
			t.Fatalf("waitCheck = %d, want 3, the last answer", got)
		}
	})
	t.Run("an attempt that never answers ends at the deadline", func(t *testing.T) {
		start := time.Now()
		got := waitCheck(context.Background(), 200*time.Millisecond, time.Second, func(ctx context.Context) int {
			<-ctx.Done() // a connection accepted that never replies
			return ExitRetry
		}, time.Now, sleepCtx)
		if got != ExitRetry || time.Since(start) > 5*time.Second {
			t.Fatalf("waitCheck = %d after %s, want 2 at the deadline", got, time.Since(start))
		}
	})
	t.Run("a signal ends the wait with the last answer", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var now time.Time
		got := waitCheck(ctx, time.Hour, time.Second, seq(ExitPending), func() time.Time { return now }, func(ctx context.Context, d time.Duration) error {
			cancel()
			return ctx.Err()
		})
		if got != ExitPending {
			t.Fatalf("waitCheck = %d, want 3", got)
		}
	})
}

// A failed release is reported but never replaces the run's own outcome.
func TestWithLeaseKeepsTheMainError(t *testing.T) {
	main := fmt.Errorf("statement 3: %w", migrate.ErrChecksum)
	releaseErr := errors.New("release: connection reset")
	var stderr bytes.Buffer
	err := withLease(context.Background(), &stderr, func(context.Context) error { return nil }, func(context.Context) {},
		func(context.Context) error { return main }, func(context.Context) error { return releaseErr })
	if err != main {
		t.Fatalf("withLease = %v, want the body's error", err)
	}
	if !strings.Contains(stderr.String(), "release: connection reset") {
		t.Fatalf("the release failure is not reported: %q", stderr.String())
	}
	stderr.Reset()
	if err := withLease(context.Background(), &stderr, func(context.Context) error { return nil }, func(context.Context) {},
		func(context.Context) error { return nil }, func(context.Context) error { return releaseErr }); err != nil {
		t.Fatalf("a successful run with a failed release = %v, want nil", err)
	}
	// The release runs even after the run's context ends.
	ctx, cancel := context.WithCancel(context.Background())
	released := false
	_ = withLease(ctx, &stderr, func(context.Context) error { return nil }, func(context.Context) {},
		func(context.Context) error { cancel(); return context.Canceled },
		func(rctx context.Context) error { released = rctx.Err() == nil; return nil })
	if !released {
		t.Fatal("the lease was not released with a live context after a signal")
	}
	// No release when the lease was not taken.
	acquireErr := errors.New("timeout")
	called := false
	if err := withLease(context.Background(), &stderr, func(context.Context) error { return acquireErr }, func(context.Context) {},
		func(context.Context) error { called = true; return nil }, func(context.Context) error { called = true; return nil }); err != acquireErr || called {
		t.Fatalf("withLease = %v, body or release called %v", err, called)
	}
}

// --version prints the version the binary was built with, as the collector's
// does, and needs no configuration.
func TestVersion(t *testing.T) {
	for _, args := range [][]string{{"--version"}, {"version"}} {
		var out, errOut bytes.Buffer
		code := Run(context.Background(), Env{Args: args, Getenv: env(nil), Stdout: &out, Stderr: &errOut, Version: "1.2.3+abc"})
		if code != ExitOK || out.String() != "fusescope-schema version 1.2.3+abc\n" || errOut.Len() > 0 {
			t.Fatalf("Run(%v) = %d, %q, %q", args, code, out.String(), errOut.String())
		}
	}
	var out bytes.Buffer
	Run(context.Background(), Env{Args: []string{"--version"}, Getenv: env(nil), Stdout: &out, Stderr: &out})
	if out.String() != "fusescope-schema version dev\n" {
		t.Fatalf("without a version: %q", out.String())
	}
}

func TestUsage(t *testing.T) {
	for _, args := range [][]string{nil, {"frobnicate"}, {"repair", "--endpoint", "h:9000"}, {"repair", "--endpoint", "h:9000", "--id", "1", "--mark", "maybe"}, {"check", "--wait", "soon"}} {
		var out, errOut bytes.Buffer
		code := Run(context.Background(), Env{Args: args, Getenv: env(nil), Stdout: &out, Stderr: &errOut})
		if code != ExitRefused || !strings.Contains(errOut.String(), "usage") {
			t.Errorf("Run(%v) = %d, stderr %q", args, code, errOut.String())
		}
	}
}

// Neither a connection failure nor a refusal prints the password, and the
// configuration is printed without it.
func TestOutputHoldsNoSecret(t *testing.T) {
	const secret = "pw-9f3b6c1e-never-printed"
	file := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(file, []byte(secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, cmd := range []string{"migrate", "check", "plan", "status"} {
		var out, errOut bytes.Buffer
		code := Run(context.Background(), Env{
			Args:   []string{cmd, "--endpoint", "127.0.0.1:1", "--username", "u", "--password-file", file, "--tls-insecure", "--dial-timeout", "200ms"},
			Getenv: env(nil), Stdout: &out, Stderr: &errOut,
		})
		if code != ExitRetry {
			t.Errorf("%s against nothing = %d, want 2 (stderr %q)", cmd, code, errOut.String())
		}
		if strings.Contains(out.String()+errOut.String(), secret) {
			t.Fatalf("%s printed the password", cmd)
		}
	}
}
