// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
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
	return runEnv(ctx, t, addr, func(e *Env) { e.AfterStatement = hook }, args...)
}

// runEnv is run with the environment adjusted by set.
func runEnv(ctx context.Context, t *testing.T, addr string, set func(*Env), args ...string) result {
	t.Helper()
	var out, errOut bytes.Buffer
	var mu sync.Mutex
	var queries []string
	full := append(append([]string{}, args...), "--endpoint", addr, "--tls-insecure")
	env := Env{
		Args: full, Getenv: func(string) string { return "" }, Stdout: &out, Stderr: &errOut, Hostname: "test",
		Trace:      func(q string) { mu.Lock(); queries = append(queries, q); mu.Unlock() },
		CheckEvery: 200 * time.Millisecond,
	}
	set(&env)
	code := Run(ctx, env)
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
	// 0002 is still pending, and the migrate after the repair runs it.
	requireCode(t, run(bg, t, addrs[1], nil, "check"), ExitPending, "check after the repair")
	requireCode(t, run(bg, t, addrs[1], nil, "migrate"), ExitOK, "migrate after the repair")
	requireCode(t, run(bg, t, addrs[1], nil, "check"), ExitOK, "check after the migrate")
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
		if err := dockerCompose(compose, args...); err != nil {
			t.Fatal(err)
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

// composeOf is the harness's compose file and its coordination members.
func composeOf(t *testing.T) (string, []string) {
	t.Helper()
	compose, members, _ := harnessOf(t)
	return compose, members
}

// harnessOf is the harness's compose file, its coordination members, and
// whether they run keeper or zookeeper.
func harnessOf(t *testing.T) (string, []string, string) {
	t.Helper()
	compose := os.Getenv("FUSESCOPE_SCHEMA_TEST_COMPOSE")
	if compose == "" {
		if os.Getenv("FUSESCOPE_SCHEMA_TEST_REQUIRED") == "1" {
			t.Fatal("FUSESCOPE_SCHEMA_TEST_COMPOSE is not set")
		}
		t.Skip("FUSESCOPE_SCHEMA_TEST_COMPOSE not set")
	}
	b, err := os.ReadFile(filepath.Join(filepath.Dir(compose), "members"))
	if err != nil {
		t.Fatal(err)
	}
	kind, err := os.ReadFile(filepath.Join(filepath.Dir(compose), "coordination"))
	if err != nil {
		t.Fatal(err)
	}
	return compose, strings.Fields(string(b)), strings.TrimSpace(string(kind))
}

// quorumProbe proves the surviving members still form one ensemble with a
// quorum: a throwaway key written through the first is read through the
// second, then removed, its absence read back through the first. Each step
// is retried within one 30 s window, as a leader is elected and a member
// catches up; the window also bounds every command and every wait.
func quorumProbe(compose, kind string, survivors []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	key := fmt.Sprintf("/fusescope-schema-probe-%d", time.Now().UnixNano())
	value := "probe-" + key[len("/fusescope-schema-probe-"):]
	cli := func(member string, args ...string) (string, error) {
		var full []string
		if kind == "keeper" {
			for i, a := range args[1:] {
				args[i+1] = "'" + a + "'"
			}
			full = []string{"exec", "-T", member, "clickhouse-keeper-client", "-h", "localhost", "-p", "9181", "-q", strings.Join(args, " ")}
		} else {
			full = append([]string{"exec", "-T", member, "zkCli.sh", "-server", "localhost:2181"}, args...)
		}
		out, err := boundedCmd(ctx, "docker", append([]string{"compose", "-f", compose}, full...)...).CombinedOutput()
		return string(out), err
	}
	step := func(what string, try func() error) error {
		var last error
		for {
			if last = try(); last == nil {
				return nil
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("the surviving members %v, %s, within 30 s: %w", survivors, what, last)
			case <-time.After(time.Second):
			}
		}
	}
	first, second := survivors[0], survivors[1]
	if err := step("write a key through "+first, func() error {
		out, err := cli(first, "create", key, value)
		if err != nil || kind == "zookeeper" && !strings.Contains(out, "Created") {
			return fmt.Errorf("create %s: %v %s", key, err, out)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := step("read it through "+second, func() error {
		out, err := cli(second, "get", key)
		if err != nil || !slices.Contains(strings.Fields(out), value) {
			return fmt.Errorf("%s reads %q (%v), want %s", key, out, err, value)
		}
		return nil
	}); err != nil {
		return err
	}
	exists, remove := "stat", "delete"
	if kind == "keeper" {
		exists, remove = "exists", "rm"
	}
	return step("remove it and read its absence through "+first, func() error {
		_, _ = cli(second, remove, key) // a retry after a removal that took finds nothing to remove
		out, err := cli(first, exists, key)
		absent := kind == "keeper" && err == nil && strings.TrimSpace(out) == "0" ||
			kind == "zookeeper" && strings.Contains(out, "Node does not exist")
		if !absent {
			return fmt.Errorf("%s still reads as present: %q (%v)", key, out, err)
		}
		return nil
	})
}

// boundedCmd runs under ctx; WaitDelay ends the wait even when a child of
// docker still holds the output pipes.
func boundedCmd(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.WaitDelay = 5 * time.Second
	return cmd
}

func dockerCompose(compose string, args ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := boundedCmd(ctx, "docker", append([]string{"compose", "-f", compose}, args...)...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose %v: %w\n%s", args, err, out)
	}
	return nil
}

// The coordination service, one node or one member of an ensemble, stopped
// during a migration and started again (§9). Safety holds whatever the
// availability: 0 only with an ensemble, and then everything is done; 4
// leaves 0001 started with no statement sent after the lease was lost, the
// rerun refused and recovery only through §7. Never 1, 2 or a hang.
func TestIntegrationCoordinationLostDuringMigrate(t *testing.T) {
	addrs := clusterAddrs(t)
	compose, members, kind := harnessOf(t)
	clean(t, addrs[0])
	bg := context.Background()
	// The member the migrator's server holds its session on: losing it fails
	// that server's requests in flight, not only a quiet follower's.
	rows, err := conn(t, addrs[0]).Rows(bg, "SELECT host FROM system.zookeeper_connection", 1)
	if err != nil || len(rows) != 1 || !slices.Contains(members, rows[0][0]) {
		t.Fatalf("system.zookeeper_connection: %v %v, members %v", rows, err, members)
	}
	victim, single := rows[0][0], len(members) == 1
	t.Cleanup(func() { _ = dockerCompose(compose, "start", victim) })
	var restarted sync.WaitGroup
	var othersUp, quorum error
	hook := func(_ uint32, i int) error {
		if i != 20 {
			return nil
		}
		if err := dockerCompose(compose, "stop", victim); err != nil {
			return err
		}
		othersUp = othersRunning(compose, members, victim)
		if !single {
			// Before the victim returns: the two others alone keep a quorum
			// and share one state, whatever the migrator then does.
			quorum = quorumProbe(compose, kind, slices.DeleteFunc(slices.Clone(members), func(m string) bool { return m == victim }))
		}
		restarted.Add(1)
		go func() {
			defer restarted.Done()
			time.Sleep(8 * time.Second)
			if err := dockerCompose(compose, "start", victim); err != nil {
				t.Error(err)
			}
		}()
		return nil
	}
	short := coord.Config{LeaseFor: 20 * time.Second, RenewEvery: 2 * time.Second, LostAfter: 5 * time.Second, RereadFor: 5 * time.Second, ClockSkew: 5 * time.Second}
	ctx, cancel := context.WithTimeout(bg, 5*time.Minute)
	defer cancel()
	r := runEnv(ctx, t, addrs[0], func(e *Env) { e.AfterStatement, e.Coord = hook, short }, "migrate")
	restarted.Wait()
	if ctx.Err() != nil {
		t.Fatalf("migrate hung past 5 minutes: %s", r.stderr)
	}
	if othersUp != nil {
		t.Fatal(othersUp)
	}
	if quorum != nil {
		t.Fatal(quorum)
	}
	t.Logf("migrate with %s stopped (%d members): exit %d\n%s", victim, len(members), r.code, r.stderr)
	if c := run(bg, t, addrs[1], nil, "check", "--wait", "3m"); c.code == ExitRetry {
		t.Fatalf("check after the restart = 2: %s", c.stderr)
	}
	history := func() []string {
		h, err := chstore.NewStore(conn(t, addrs[2]), "cluster").History(bg)
		if err != nil {
			t.Fatal(err)
		}
		out := []string{}
		for _, row := range h {
			out = append(out, fmt.Sprintf("%+v", row))
		}
		return out
	}
	switch {
	case r.code == ExitOK && !single:
		if h := history(); len(h) != 1 || !strings.Contains(h[0], "Done:true") {
			t.Fatalf("history after exit 0: %v, want 0001 done", h)
		}
		requireCode(t, run(bg, t, addrs[1], nil, "check"), ExitOK, "check after a migrate that rode out the loss")
		if again := run(bg, t, addrs[3], nil, "migrate"); again.code != ExitOK || strings.Contains(again.stdout, "applied") {
			t.Fatalf("a second migrate = %d: %s", again.code, again.stdout)
		}
	case r.code == ExitStarted:
		requireNoStatementAfterTheLoss(t, r)
		before := history()
		if len(before) != 1 || !strings.Contains(before[0], "Done:false") {
			t.Fatalf("history after exit 4: %v, want 0001 started", before)
		}
		requireCode(t, run(bg, t, addrs[1], nil, "migrate", "--lock-timeout", "2m"), ExitRefused, "the rerun after exit 4")
		requireCode(t, run(bg, t, addrs[1], nil, "check"), ExitRefused, "check after exit 4")
		if after := history(); strings.Join(after, "|") != strings.Join(before, "|") {
			t.Fatalf("the history changed: %v, then %v", before, after)
		}
		// Recovery through §7 only: finish by hand, repair, migrate.
		all, err := migrations.All()
		if err != nil {
			t.Fatal(err)
		}
		c := conn(t, addrs[0])
		for _, stmt := range all[0].Statements {
			if err := c.ExecDiscard(bg, migrations.Render(stmt, schema.DefaultDatabases, "cluster")); err != nil {
				t.Fatal(err)
			}
		}
		requireCode(t, run(bg, t, addrs[2], nil, "repair", "--id", "1", "--mark", "done", "--lock-timeout", "2m"), ExitOK, "repair --mark done")
		requireCode(t, run(bg, t, addrs[1], nil, "migrate"), ExitOK, "migrate after the repair")
		requireCode(t, run(bg, t, addrs[1], nil, "check"), ExitOK, "check after the repair")
	default:
		t.Fatalf("migrate with the coordination lost (%d members) = %d, want 4, or 0 with an ensemble", len(members), r.code)
	}
}

// othersRunning requires every member but the one stopped to be running.
func othersRunning(compose string, members []string, stopped string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := boundedCmd(ctx, "docker", "compose", "-f", compose, "ps", "--status", "running", "--services").Output()
	if err != nil {
		return err
	}
	running := strings.Fields(string(out))
	for _, m := range members {
		if m == stopped && slices.Contains(running, m) {
			return fmt.Errorf("%s is still running", m)
		}
		if m != stopped && !slices.Contains(running, m) {
			return fmt.Errorf("%s stopped too, only %s should have", m, stopped)
		}
	}
	return nil
}

var stmtErrRe = regexp.MustCompile(`0001 base: (before )?statement (\d+)`)

// requireNoStatementAfterTheLoss: the run's error names the statement it
// stopped at, and the run sent exactly the statements before it (the lease
// lost before it), or up to it (it failed itself).
func requireNoStatementAfterTheLoss(t *testing.T, r result) {
	t.Helper()
	m := stmtErrRe.FindStringSubmatch(r.stderr)
	if m == nil {
		t.Fatalf("the error names no statement: %s", r.stderr)
	}
	n, _ := strconv.Atoi(m[2])
	want := n
	if m[1] != "" {
		want = n - 1
	}
	sent := 0
	for _, q := range r.queries {
		if strings.HasPrefix(q, "CREATE ") && strings.Contains(q, " ON CLUSTER ") && !strings.Contains(q, "fusescope_schema") {
			sent++
		}
	}
	if sent != want {
		t.Fatalf("%d statements sent, the run stopped at statement %d (%q): want %d", sent, n, m[0], want)
	}
}

// status, with 0001 done and the table as chart 0.162.0 left it, reports the
// next migration's admission and exits 0: the three rows 0002 would insert
// when two are there (check 3), the extra row when one contradicts (check 1).
func TestIntegrationStatusReportsTheNextMigrationsRows(t *testing.T) {
	addrs := clusterAddrs(t)
	clean(t, addrs[0])
	bg := context.Background()
	requireCode(t, run(bg, t, addrs[0], nil, "migrate"), ExitOK, "migrate")
	c := conn(t, addrs[0])
	for _, q := range []string{
		"DELETE FROM fusescope_schema.migrations WHERE id = 2 SETTINGS keeper_map_strict_mode = 1",
		"TRUNCATE TABLE signoz_metadata.column_evolution_metadata ON CLUSTER cluster SYNC SETTINGS distributed_ddl_output_mode = 'none'",
	} {
		if err := c.Exec(bg, q); err != nil {
			t.Fatal(err)
		}
	}
	insert := func(values string) {
		t.Helper()
		if err := c.Exec(bg, "INSERT INTO signoz_metadata.distributed_column_evolution_metadata (signal, column_name, column_type, field_context, field_name, version, release_time) SETTINGS distributed_foreground_insert = 1, insert_deduplicate = 0 VALUES "+values); err != nil {
			t.Fatal(err)
		}
	}
	insert("('logs', 'resources_string', 'Map(LowCardinality(String), Float64)', 'resource', '__all__', 0, 0), ('traces', 'resources_string', 'Map(LowCardinality(String), Float64)', 'resource', '__all__', 0, 0)")
	st := run(bg, t, addrs[1], nil, "status")
	requireCode(t, st, ExitOK, "status with two rows")
	if !strings.Contains(st.stdout, "0002 column_evolution_baseline inserts 3 rows:") || strings.Count(st.stdout, ", __all__, 1)") != 3 {
		t.Fatalf("status printed %q", st.stdout)
	}
	requireCode(t, run(bg, t, addrs[1], nil, "check"), ExitPending, "check with two rows")

	insert("('traces', 'attributes', 'Map(String, String)', 'attribute', '__all__', 1, 5)")
	st = run(bg, t, addrs[2], nil, "status")
	requireCode(t, st, ExitOK, "status with a contradiction")
	if !strings.Contains(st.stdout, "refused:") || !strings.Contains(st.stdout, "(traces, attributes, Map(String, String), attribute, __all__, 1) is extra") {
		t.Fatalf("status printed %q", st.stdout)
	}
	requireCode(t, run(bg, t, addrs[2], nil, "check"), ExitRefused, "check with a contradiction")
	requireCode(t, run(bg, t, addrs[2], nil, "migrate"), ExitRefused, "migrate with a contradiction")
}
