// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/chstore"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/coord"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/migrations"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

// These tests need the cluster of test/schema/cluster.sh; see chstore's.
func clusterAddrs(t *testing.T) []string {
	t.Helper()
	v := os.Getenv("FUSESCOPE_SCHEMA_TEST_ADDRS")
	if v == "" {
		if os.Getenv("FUSESCOPE_SCHEMA_TEST_REQUIRED") == "1" {
			t.Fatal("FUSESCOPE_SCHEMA_TEST_REQUIRED=1 but FUSESCOPE_SCHEMA_TEST_ADDRS is not set")
		}
		t.Skip("FUSESCOPE_SCHEMA_TEST_ADDRS not set: run test/schema/cluster.sh up")
	}
	return strings.Split(v, ",")
}

func conn(t *testing.T, addr string) *chstore.Conn {
	t.Helper()
	c, err := chstore.Dial(context.Background(), chstore.Config{Endpoint: addr, Username: "default", Cluster: "cluster", TLS: chstore.TLSConfig{Insecure: true}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// clean drops the six databases and fusescope_schema.
func clean(t *testing.T, addr string) {
	t.Helper()
	c := conn(t, addr)
	drop := func() {
		for _, db := range append(values(schema.DefaultDatabases), chstore.Database) {
			_ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" ON CLUSTER cluster SYNC SETTINGS distributed_ddl_output_mode = 'none'")
		}
	}
	drop()
	t.Cleanup(drop)
}

func values(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

type result struct {
	code           int
	stdout, stderr string
	queries        []string
}

// run runs a command against addr with the connection flags, recording its
// queries.
func run(ctx context.Context, t *testing.T, addr string, hook func(uint32, int) error, args ...string) result {
	t.Helper()
	var out, errOut bytes.Buffer
	var mu sync.Mutex
	var queries []string
	full := append(append([]string{}, args...), "--endpoint", addr, "--tls-insecure")
	code := Run(ctx, Env{
		Args: full, Getenv: func(string) string { return "" }, Stdout: &out, Stderr: &errOut, Hostname: "test",
		Trace:          func(q string) { mu.Lock(); queries = append(queries, q); mu.Unlock() },
		AfterStatement: hook, CheckEvery: 200 * time.Millisecond,
	})
	return result{code: code, stdout: out.String(), stderr: errOut.String(), queries: queries}
}

var writeRe = regexp.MustCompile(`(?is)^\s*(CREATE|ALTER|DROP|INSERT|DELETE|RENAME|TRUNCATE|EXCHANGE|ATTACH|DETACH|OPTIMIZE|SYSTEM|GRANT|REVOKE)\b`)

func requireNoWrites(t *testing.T, r result) {
	t.Helper()
	for _, q := range r.queries {
		if writeRe.MatchString(q) {
			t.Fatalf("a read-only command wrote: %.160s", q)
		}
	}
}

func requireCode(t *testing.T, r result, want int, what string) {
	t.Helper()
	if r.code != want {
		t.Fatalf("%s = %d, want %d\nstdout: %s\nstderr: %s", what, r.code, want, r.stdout, r.stderr)
	}
}

// check, plan and status write nothing, before the bootstrap too; migrate
// twice; then drift.
func TestIntegrationCommandsLifecycle(t *testing.T) {
	addrs := clusterAddrs(t)
	clean(t, addrs[0])
	ctx := context.Background()

	r := run(ctx, t, addrs[1], nil, "check")
	requireCode(t, r, ExitPending, "check on an empty cluster")
	requireNoWrites(t, r)
	r = run(ctx, t, addrs[1], nil, "plan")
	requireCode(t, r, ExitOK, "plan")
	requireNoWrites(t, r)
	if !strings.Contains(r.stdout, "advisory") || !strings.Contains(r.stdout, "starting point: empty") || !strings.Contains(r.stdout, "0001 base") {
		t.Fatalf("plan printed %q", r.stdout)
	}
	r = run(ctx, t, addrs[2], nil, "status")
	requireCode(t, r, ExitOK, "status")
	requireNoWrites(t, r)
	if !strings.Contains(r.stdout, "(empty)") || !strings.Contains(r.stdout, "lease: none") {
		t.Fatalf("status printed %q", r.stdout)
	}

	r = run(ctx, t, addrs[0], nil, "migrate")
	requireCode(t, r, ExitOK, "migrate")
	if !strings.Contains(r.stdout, "starting point: empty") || !strings.Contains(r.stdout, "applied 0001 base") {
		t.Fatalf("migrate printed %q", r.stdout)
	}
	r = run(ctx, t, addrs[3], nil, "migrate")
	requireCode(t, r, ExitOK, "a second migrate")
	if strings.Contains(r.stdout, "applied") || !strings.Contains(r.stdout, "up to date") {
		t.Fatalf("the second migrate printed %q", r.stdout)
	}
	for _, cmd := range []string{"check", "plan", "status"} {
		r = run(ctx, t, addrs[1], nil, cmd)
		requireCode(t, r, ExitOK, cmd+" after migrate")
		requireNoWrites(t, r)
	}
	if !strings.Contains(r.stdout, "0001 base: done") || !strings.Contains(r.stdout, "lease: free") {
		t.Fatalf("status printed %q", r.stdout)
	}

	if err := conn(t, addrs[0]).Exec(ctx, "ALTER TABLE signoz_meter.samples ON CLUSTER cluster ADD COLUMN extra String SETTINGS distributed_ddl_output_mode = 'none', distributed_ddl_task_timeout = 180"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second) // the ALTER reaches every host
	for cmd, want := range map[string]int{"migrate": ExitRefused, "check": ExitRefused, "plan": ExitRefused, "status": ExitOK} {
		r = run(ctx, t, addrs[2], nil, cmd)
		requireCode(t, r, want, cmd+" with drift")
		if !strings.Contains(r.stdout+r.stderr, "meter.samples column extra") {
			t.Fatalf("%s does not name the drift: %q %q", cmd, r.stdout, r.stderr)
		}
	}
}

// A run cancelled midway exits 4; check and migrate then refuse; the wrong
// repairs are refused; finishing by hand and repair --mark done recover.
func TestIntegrationInterruptedThenRepaired(t *testing.T) {
	addrs := clusterAddrs(t)
	clean(t, addrs[0])
	ctx, cancel := context.WithCancel(context.Background())
	r := run(ctx, t, addrs[0], func(_ uint32, i int) error {
		if i == 40 {
			cancel() // the signal
		}
		return nil
	}, "migrate")
	requireCode(t, r, ExitStarted, "a cancelled migrate")
	if !strings.Contains(r.stderr, "stays started") {
		t.Fatalf("stderr %q", r.stderr)
	}
	bg := context.Background()
	requireCode(t, run(bg, t, addrs[1], nil, "check"), ExitRefused, "check after the interruption")
	requireCode(t, run(bg, t, addrs[1], nil, "migrate"), ExitRefused, "migrate after the interruption")
	st := run(bg, t, addrs[2], nil, "status")
	requireCode(t, st, ExitOK, "status")
	if !strings.Contains(st.stdout, "0001 base: started") || !strings.Contains(st.stdout, "lease: free") {
		t.Fatalf("status printed %q", st.stdout)
	}
	requireCode(t, run(bg, t, addrs[3], nil, "repair", "--id", "1", "--mark", "absent"), ExitRefused, "repair --mark absent on 0001")
	requireCode(t, run(bg, t, addrs[3], nil, "repair", "--id", "1", "--mark", "done"), ExitRefused, "repair --mark done with half the objects")

	// The operator finishes the remaining statements by hand.
	all, err := migrations.All()
	if err != nil {
		t.Fatal(err)
	}
	c := conn(t, addrs[0])
	for _, s := range all[0].Statements[40:] {
		if err := c.ExecDiscard(bg, migrations.Render(s, schema.DefaultDatabases, "cluster")); err != nil {
			t.Fatal(err)
		}
	}
	r = run(bg, t, addrs[3], nil, "repair", "--id", "1", "--mark", "done")
	requireCode(t, r, ExitOK, "repair --mark done")
	for _, q := range r.queries {
		if writeRe.MatchString(q) && !strings.Contains(q, "`fusescope_schema`.") {
			t.Fatalf("repair wrote outside fusescope_schema: %.160s", q)
		}
	}
	requireCode(t, run(bg, t, addrs[1], nil, "check"), ExitOK, "check after the repair")
	requireCode(t, run(bg, t, addrs[1], nil, "migrate"), ExitOK, "migrate after the repair")
}

// check --wait returns 0 once a concurrent migrate is done.
func TestIntegrationCheckWaitsForMigrate(t *testing.T) {
	addrs := clusterAddrs(t)
	clean(t, addrs[0])
	waited := make(chan result, 1)
	go func() { waited <- run(context.Background(), t, addrs[3], nil, "check", "--wait", "5m") }()
	time.Sleep(time.Second)
	requireCode(t, run(context.Background(), t, addrs[0], nil, "migrate"), ExitOK, "migrate")
	select {
	case r := <-waited:
		requireCode(t, r, ExitOK, "check --wait")
		requireNoWrites(t, r)
	case <-time.After(2 * time.Minute):
		t.Fatal("check --wait did not return after migrate")
	}
}

// Connection, authentication and lease failures exit 2, with no secret in
// the output.
func TestIntegrationConnectionAndLeaseErrors(t *testing.T) {
	addrs := clusterAddrs(t)
	clean(t, addrs[0])
	bg := context.Background()
	for _, args := range [][]string{{"migrate"}, {"check"}, {"check", "--wait", "1s"}, {"repair", "--id", "1", "--mark", "done"}} {
		requireCode(t, run(bg, t, "127.0.0.1:1", nil, append(args, "--dial-timeout", "300ms")...), ExitRetry, strings.Join(args, " ")+" against nothing")
	}

	const secret = "wrong-pw-5b1d0a7c"
	file := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(file, []byte(secret), 0o600); err != nil {
		t.Fatal(err)
	}
	r := run(bg, t, addrs[0], nil, "migrate", "--password-file", file)
	requireCode(t, r, ExitRetry, "migrate with a wrong password")
	if strings.Contains(r.stdout+r.stderr, secret) {
		t.Fatal("the password was printed")
	}

	// Another migrator holds the lease for longer than --lock-timeout.
	requireCode(t, run(bg, t, addrs[0], nil, "plan"), ExitOK, "plan")
	s := chstore.NewStore(conn(t, addrs[1]), "cluster")
	if err := coord.Bootstrap(bg, s); err != nil {
		t.Fatal(err)
	}
	holder, err := coord.New(s, coord.Config{Owner: "holder"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Acquire(bg); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Release(bg) }()
	r = run(bg, t, addrs[2], nil, "migrate", "--lock-timeout", "3s")
	requireCode(t, r, ExitRetry, "migrate while the lease is held")
	if !strings.Contains(r.stderr, coord.ErrLeaseTimeout.Error()) {
		t.Fatalf("stderr %q", r.stderr)
	}
}

// The binary, sent SIGTERM while a migration is started, exits 4 and
// releases the lease.
func TestIntegrationSIGTERM(t *testing.T) {
	addrs := clusterAddrs(t)
	clean(t, addrs[0])
	bin := filepath.Join(t.TempDir(), "fusescope-schema")
	build := exec.Command("go", "build", "-o", bin, "../../cmd/fusescope-schema")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	cmd := exec.Command(bin, "migrate", "--endpoint", addrs[0], "--tls-insecure")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	s := chstore.NewStore(conn(t, addrs[1]), "cluster")
	deadline := time.Now().Add(time.Minute)
	for {
		h, err := s.History(context.Background())
		if err == nil && len(h) == 1 && !h[0].Done {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			t.Fatalf("0001 never started: %v %+v", err, h)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err := cmd.Wait()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != ExitStarted {
		t.Fatalf("migrate after SIGTERM: %v\n%s", err, stderr.String())
	}
	l, ok, err := s.ReadLease(context.Background())
	if err != nil || !ok || l.Owner != "" {
		t.Fatalf("lease after SIGTERM: %+v, %v, %v; want released", l, ok, err)
	}
	if h, _ := s.History(context.Background()); len(h) != 1 || h[0].Done {
		t.Fatalf("history %+v, want 0001 started", h)
	}
}

// check, plan and status read fusescope_schema on every replica: an unknown
// table on one replica is refused, partial coordination is pending; neither
// writes.
func TestIntegrationReadOnlyCommandsInspectTheCoordination(t *testing.T) {
	addrs := clusterAddrs(t)
	clean(t, addrs[0])
	bg := context.Background()
	requireCode(t, run(bg, t, addrs[0], nil, "migrate"), ExitOK, "migrate")

	other := conn(t, addrs[2])
	if err := other.Exec(bg, "CREATE TABLE fusescope_schema.probe (k String) ENGINE = Memory"); err != nil {
		t.Fatal(err)
	}
	for cmd, want := range map[string]int{"check": ExitRefused, "plan": ExitRefused, "status": ExitOK} {
		r := run(bg, t, addrs[0], nil, cmd)
		requireCode(t, r, want, cmd+" with an unknown table on one replica")
		requireNoWrites(t, r)
		if !strings.Contains(r.stdout+r.stderr, "unknown table fusescope_schema.probe") {
			t.Fatalf("%s does not name the table: %q %q", cmd, r.stdout, r.stderr)
		}
	}
	if err := other.Exec(bg, "DROP TABLE fusescope_schema.probe SYNC"); err != nil {
		t.Fatal(err)
	}

	if err := conn(t, addrs[3]).Exec(bg, "DROP TABLE fusescope_schema.meta SYNC"); err != nil {
		t.Fatal(err)
	}
	r := run(bg, t, addrs[0], nil, "check")
	requireCode(t, r, ExitPending, "check with meta missing on one replica")
	requireNoWrites(t, r)
	requireCode(t, run(bg, t, addrs[0], nil, "migrate"), ExitOK, "migrate completes the coordination")
	requireCode(t, run(bg, t, addrs[0], nil, "check"), ExitOK, "check after")
}

// A replica that is down is retryable: 2, also once --wait passes.
func TestIntegrationAReplicaDownIsRetryable(t *testing.T) {
	addrs := clusterAddrs(t)
	compose := os.Getenv("FUSESCOPE_SCHEMA_TEST_COMPOSE")
	if compose == "" {
		if os.Getenv("FUSESCOPE_SCHEMA_TEST_REQUIRED") == "1" {
			t.Fatal("FUSESCOPE_SCHEMA_TEST_COMPOSE is not set")
		}
		t.Skip("FUSESCOPE_SCHEMA_TEST_COMPOSE not set")
	}
	clean(t, addrs[0])
	bg := context.Background()
	requireCode(t, run(bg, t, addrs[0], nil, "migrate"), ExitOK, "migrate")
	dc := func(args ...string) {
		if out, err := exec.Command("docker", append([]string{"compose", "-f", compose}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("docker compose %v: %v\n%s", args, err, out)
		}
	}
	dc("stop", "ch-2-2")
	t.Cleanup(func() { dc("start", "ch-2-2") })
	for _, args := range [][]string{{"check"}, {"check", "--wait", "3s"}, {"migrate"}} {
		requireCode(t, run(bg, t, addrs[0], nil, args...), ExitRetry, strings.Join(args, " ")+" with ch-2-2 down")
	}
	dc("start", "ch-2-2")
	requireCode(t, run(bg, t, addrs[0], nil, "check", "--wait", "2m"), ExitOK, "check --wait once ch-2-2 is back")
}
