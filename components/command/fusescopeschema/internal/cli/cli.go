// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package cli is fusescope-schema's commands (docs/spec/fusescope-schema.md
// §1): migrate, check, plan, status and repair. Exit codes come from typed
// errors and migrate.Result.Started only.
package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/chstore"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/coord"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/migrate"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/migrations"
)

// Exit codes (§1).
const (
	ExitOK      = 0
	ExitRefused = 1 // refused (§7), or a usage error: nothing was done
	ExitRetry   = 2 // connection, ClickHouse or lease error, nothing started: safe to retry
	ExitPending = 3 // check: pending or in progress
	ExitStarted = 4 // migrate: the error came after a migration was started
)

const usage = `usage: fusescope-schema <command> [flags]

commands:
  migrate                          take the lease and migrate
  check [--wait D]                 0 up to date, 3 pending or in progress, 1 refused
  plan                             the starting point and the pending migrations, advisory
  status                           the history, the lease and any difference
  repair --id N --mark done|absent record an interrupted migration's outcome

connection flags (also FUSESCOPE_SCHEMA_<FLAG>, or --config FILE with JSON keys):
  --endpoint host:port  --username U  --password-file F  --cluster C
  --tls-insecure  --tls-ca-file F  --tls-insecure-skip-verify  --dial-timeout D
  --database-<role> NAME (analytics, logs, metadata, meter, metrics, traces)
  --lock-timeout D`

// Env is what Run needs from the process; the hooks are for tests.
type Env struct {
	Args     []string
	Getenv   func(string) string
	Stdout   io.Writer
	Stderr   io.Writer
	Hostname string
	Version  string

	Trace          func(query string)
	AfterStatement func(migration uint32, statement int) error
	CheckEvery     time.Duration
	Coord          coord.Config // timings; zero fields take the defaults
}

// releaseTimeout bounds releasing the lease after the run, signal or not.
const releaseTimeout = 15 * time.Second

// Run runs one command and returns its exit code.
func Run(ctx context.Context, env Env) int {
	if len(env.Args) == 0 {
		fmt.Fprintln(env.Stderr, usage)
		return ExitRefused
	}
	cmd, args := env.Args[0], env.Args[1:]
	c := &command{env: env, name: cmd}
	var register func(*flag.FlagSet)
	switch cmd {
	case "migrate", "plan", "status":
	case "check":
		register = func(fs *flag.FlagSet) {
			fs.DurationVar(&c.wait, "wait", 0, "poll until up to date or refused, for at most D")
		}
	case "repair":
		register = func(fs *flag.FlagSet) {
			fs.Func("id", "the migration's id", func(v string) error {
				id, err := strconv.ParseUint(v, 10, 32)
				c.id = uint32(id)
				return err
			})
			fs.StringVar((*string)(&c.mark), "mark", "", "done or absent")
		}
	default:
		return c.usage(fmt.Errorf("%w: unknown command %q", ErrUsage, cmd))
	}
	regs := []func(*flag.FlagSet){}
	if register != nil {
		regs = append(regs, register)
	}
	cfg, rest, err := load(args, env.Getenv, regs...)
	if err != nil {
		return c.usage(err)
	}
	if len(rest) > 0 {
		return c.usage(fmt.Errorf("%w: unexpected arguments %v", ErrUsage, rest))
	}
	if cmd == "repair" && (c.id == 0 || c.mark != migrate.MarkDone && c.mark != migrate.MarkAbsent) {
		return c.usage(fmt.Errorf("%w: repair needs --id N and --mark done or absent", ErrUsage))
	}
	if err := cfg.Conn.Validate(); err != nil {
		return c.usage(err)
	}
	c.cfg = cfg
	switch cmd {
	case "migrate":
		return c.migrate(ctx)
	case "check":
		return c.check(ctx)
	case "plan":
		return c.plan(ctx)
	case "status":
		return c.status(ctx)
	default:
		return c.repair(ctx)
	}
}

type command struct {
	env  Env
	name string
	cfg  Config
	wait time.Duration
	id   uint32
	mark migrate.Mark
}

func (c *command) usage(err error) int {
	fmt.Fprintf(c.env.Stderr, "fusescope-schema %s: %v\n\n%s\n", c.name, err, usage)
	return ExitRefused
}

func (c *command) out(format string, a ...any) { fmt.Fprintf(c.env.Stdout, format+"\n", a...) }

func (c *command) fail(err error) {
	fmt.Fprintf(c.env.Stderr, "fusescope-schema %s: %v\n", c.name, err)
}

// session dials and builds the store and the runner.
func (c *command) session(ctx context.Context) (*chstore.Store, *migrate.Runner, func(), error) {
	conn, err := chstore.Dial(ctx, c.cfg.Conn)
	if err != nil {
		return nil, nil, nil, err
	}
	if c.env.Trace != nil {
		conn.Trace(c.env.Trace)
	}
	all, err := migrations.All()
	if err != nil {
		conn.Close()
		return nil, nil, nil, err
	}
	store := chstore.NewStore(conn, c.cfg.Conn.Cluster)
	version := c.env.Version
	if version == "" {
		version = "dev"
	}
	r := &migrate.Runner{Store: store, Migrations: all, Databases: c.cfg.Databases, Cluster: c.cfg.Conn.Cluster,
		Version: version, Host: c.env.Hostname, AfterStatement: c.env.AfterStatement}
	return store, r, conn.Close, nil
}

func (c *command) coordinator(store *chstore.Store, allowInterrupted bool) (*coord.Coordinator, error) {
	cfg := c.env.Coord
	cfg.Owner = owner(c.env.Hostname)
	cfg.AcquireTimeout = c.cfg.LockTimeout
	cfg.AllowInterrupted = allowInterrupted
	return coord.New(store, cfg)
}

// owner names this process in the lease: host, pid and a random suffix.
func owner(host string) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	if host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s/%d/%s", host, os.Getpid(), hex.EncodeToString(b[:]))
}

// withLease takes the lease, renews it while body runs, then releases it with
// a context of its own, so a signal still releases. A failed release is
// reported and never replaces body's outcome: the lease then expires.
func withLease(ctx context.Context, stderr io.Writer, acquire func(context.Context) error, renew func(context.Context),
	body func(context.Context) error, release func(context.Context) error) error {
	if err := acquire(ctx); err != nil {
		return err
	}
	rctx, stop := context.WithCancel(ctx)
	renewing := make(chan struct{})
	go func() { renew(rctx); close(renewing) }()
	err := body(ctx)
	stop()
	<-renewing
	relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	defer cancel()
	if rerr := release(relCtx); rerr != nil {
		fmt.Fprintf(stderr, "fusescope-schema: warning: the lease was not released and expires on its own: %v\n", rerr)
	}
	return err
}

func refusal(err error) bool {
	var drift *migrate.DriftError
	if errors.As(err, &drift) {
		return true
	}
	for _, target := range []error{
		migrate.ErrInterrupted, migrate.ErrUnknownMigration, migrate.ErrChecksum, migrate.ErrRepairRefused,
		coord.ErrInterrupted, coord.ErrUnknownLayout, coord.ErrDivergent, coord.ErrConfig,
		chstore.ErrPrecheck, chstore.ErrNoKeeperMapPrefix, chstore.ErrInvalidConfig, ErrUsage,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// migrateCode: a started migration decides first, whatever the error (4);
// then a refusal (1); any other error is retryable (2).
func migrateCode(res migrate.Result, err error) int {
	switch {
	case err == nil:
		return ExitOK
	case res.Started:
		return ExitStarted
	case refusal(err):
		return ExitRefused
	}
	return ExitRetry
}

func repairCode(err error) int {
	switch {
	case err == nil:
		return ExitOK
	case refusal(err):
		return ExitRefused
	}
	return ExitRetry
}

func checkCode(st migrate.State, err error) int {
	switch {
	case err != nil && refusal(err):
		return ExitRefused
	case err != nil:
		return ExitRetry
	case st.Verdict == migrate.UpToDate:
		return ExitOK
	}
	return ExitPending
}

func (c *command) migrate(ctx context.Context) int {
	store, r, closeConn, err := c.session(ctx)
	if err != nil {
		c.fail(err)
		return repairCode(err)
	}
	defer closeConn()
	if err := store.Precheck(ctx); err != nil {
		c.fail(err)
		return repairCode(err)
	}
	if err := coord.Bootstrap(ctx, store); err != nil {
		c.fail(err)
		return repairCode(err)
	}
	co, err := c.coordinator(store, false)
	if err != nil {
		c.fail(err)
		return repairCode(err)
	}
	r.Lease = co
	var res migrate.Result
	err = withLease(ctx, c.env.Stderr,
		func(ctx context.Context) error { _, err := co.Acquire(ctx); return err }, co.RunRenewals,
		func(ctx context.Context) (err error) { res, err = r.Migrate(ctx); return err }, co.Release)
	if res.Point != "" {
		c.out("starting point: %s", res.Point)
	}
	for _, id := range res.Applied {
		c.out("applied %s", c.migrationName(r, id))
	}
	code := migrateCode(res, err)
	switch code {
	case ExitOK:
		c.out("up to date")
	case ExitStarted:
		c.fail(fmt.Errorf("%w\nthe migration stays started: run status, then repair (runbook, §10)", err))
	default:
		c.fail(err)
	}
	return code
}

func (c *command) migrationName(r *migrate.Runner, id uint32) string {
	for _, m := range r.Migrations {
		if m.ID == id {
			return fmt.Sprintf("%04d %s", m.ID, m.Name)
		}
	}
	return fmt.Sprintf("%04d", id)
}

// inspect runs the read-only precheck and reads the state; it writes nothing.
func (c *command) inspect(ctx context.Context, store *chstore.Store, r *migrate.Runner) (migrate.State, *coord.Lease, error) {
	if err := store.Precheck(ctx); err != nil {
		return migrate.State{}, nil, err
	}
	// fusescope_schema on every replica first: the history and the lease are
	// then read on the connected server only.
	complete, err := coord.InspectLayout(ctx, store)
	if err != nil {
		return migrate.State{}, nil, err
	}
	var lease *coord.Lease
	now := int64(0)
	if complete {
		if lease, now, err = store.LeaseIfPresent(ctx); err != nil {
			return migrate.State{}, nil, err
		}
	}
	st, err := r.Inspect(ctx, lease, now)
	if err == nil && !complete && st.Verdict == migrate.UpToDate {
		st.Verdict = migrate.Pending // migrate completes the coordination
	}
	return st, lease, err
}

func (c *command) check(ctx context.Context) int {
	store, r, closeConn, err := c.session(ctx)
	if err != nil {
		c.fail(err)
		return checkCode(migrate.State{}, err)
	}
	defer closeConn()
	var last string
	once := func(ctx context.Context) int {
		st, _, err := c.inspect(ctx, store, r)
		code := checkCode(st, err)
		switch {
		case err != nil:
			last = err.Error()
		case code == ExitOK:
			last = "up to date"
		case st.Verdict == migrate.InProgress:
			last = "in progress: " + c.migrationName(r, st.History[len(st.History)-1].ID)
		default:
			last = c.pending(r, st)
		}
		return code
	}
	every := c.env.CheckEvery
	if every == 0 {
		every = 5 * time.Second
	}
	code := waitCheck(ctx, c.wait, every, once, time.Now, sleepCtx)
	if code == ExitOK || code == ExitPending {
		c.out("%s", last)
	} else {
		c.fail(errors.New(last))
	}
	return code
}

func (c *command) pending(r *migrate.Runner, st migrate.State) string {
	var parts []string
	if st.Point == migrate.PointFork {
		parts = append(parts, "record "+c.migrationName(r, 1)+" as adopted")
	}
	for _, id := range st.Pending {
		parts = append(parts, c.migrationName(r, id))
	}
	return "pending: " + strings.Join(parts, ", ")
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// waitCheck polls once until it answers up to date or refused, or wait
// passes; then the code is the last answer given before the deadline. An
// attempt the deadline cuts short answers nothing, unless it is the first:
// a check that stayed pending ends 3, one that never got an answer ends 2. A
// signal ends the wait the same way. Connection errors are polled through,
// as ClickHouse may come up later.
func waitCheck(ctx context.Context, wait, every time.Duration, once func(context.Context) int,
	now func() time.Time, sleep func(context.Context, time.Duration) error) int {
	deadline := now().Add(wait)
	last := -1
	for {
		actx, cancel := ctx, func() {}
		if wait > 0 {
			// Each attempt ends at the deadline, so a server that accepts and
			// never replies cannot hold the wait past it.
			actx, cancel = context.WithTimeout(ctx, deadline.Sub(now()))
		}
		code := once(actx)
		cut := wait > 0 && actx.Err() != nil && ctx.Err() == nil
		cancel()
		if cut && last >= 0 {
			return last
		}
		if code == ExitOK || code == ExitRefused || wait == 0 || ctx.Err() != nil {
			return code
		}
		last = code
		left := deadline.Sub(now())
		if left <= 0 {
			return last
		}
		if err := sleep(ctx, min(every, left)); err != nil || !now().Before(deadline) {
			return last
		}
	}
}

func (c *command) plan(ctx context.Context) int {
	store, r, closeConn, err := c.session(ctx)
	if err != nil {
		c.fail(err)
		return checkCode(migrate.State{}, err)
	}
	defer closeConn()
	st, _, err := c.inspect(ctx, store, r)
	if err != nil {
		c.fail(err)
		return checkCode(st, err)
	}
	c.out("advisory: migrate decides again under the lease")
	switch st.Verdict {
	case migrate.InProgress:
		c.out("in progress: %s", c.migrationName(r, st.History[len(st.History)-1].ID))
	case migrate.UpToDate:
		c.out("starting point: %s\nup to date", st.Point)
	default:
		c.out("starting point: %s\n%s", st.Point, c.pending(r, st))
	}
	return ExitOK
}

func (c *command) status(ctx context.Context) int {
	store, r, closeConn, err := c.session(ctx)
	if err != nil {
		c.fail(err)
		return checkCode(migrate.State{}, err)
	}
	defer closeConn()
	st, lease, err := c.inspect(ctx, store, r)
	if err != nil && !refusal(err) {
		c.fail(err)
		return ExitRetry
	}
	c.out("history:")
	if len(st.History) == 0 {
		c.out("  (empty)")
	}
	for _, h := range st.History {
		state := "started"
		if h.Done {
			state = "done"
		}
		if h.Adopted {
			state += ", adopted"
		}
		c.out("  %04d %s: %s (lease token %d, checksum %s)", h.ID, h.Name, state, h.Token, h.Checksum)
	}
	switch {
	case lease == nil:
		c.out("lease: none (no bootstrap yet)")
	case lease.Owner == "":
		c.out("lease: free (token %d)", lease.Token)
	default:
		c.out("lease: held by %s (token %d, expires %s)", lease.Owner, lease.Token, time.UnixMilli(lease.ExpiresMs).UTC().Format(time.RFC3339))
	}
	switch {
	case err != nil:
		c.out("refused: %v", err)
	case st.Verdict == migrate.InProgress:
		c.out("in progress")
	case st.Verdict == migrate.UpToDate:
		c.out("schema: as expected")
	default:
		c.out("schema: starting point %s; %s", st.Point, c.pending(r, st))
	}
	return ExitOK
}

func (c *command) repair(ctx context.Context) int {
	store, r, closeConn, err := c.session(ctx)
	if err != nil {
		c.fail(err)
		return repairCode(err)
	}
	defer closeConn()
	if err := store.Precheck(ctx); err != nil {
		c.fail(err)
		return repairCode(err)
	}
	history, err := store.History(ctx)
	if err == nil && len(history) == 0 {
		err = fmt.Errorf("%w: the history is empty", migrate.ErrRepairRefused)
	}
	if err != nil {
		c.fail(err)
		return repairCode(err)
	}
	co, err := c.coordinator(store, true)
	if err != nil {
		c.fail(err)
		return repairCode(err)
	}
	r.Lease = co
	err = withLease(ctx, c.env.Stderr,
		func(ctx context.Context) error { _, err := co.Acquire(ctx); return err }, co.RunRenewals,
		func(ctx context.Context) error { return r.Repair(ctx, c.id, c.mark) }, co.Release)
	if err != nil {
		c.fail(err)
		return repairCode(err)
	}
	c.out("%s: marked %s", c.migrationName(r, c.id), c.mark)
	return ExitOK
}
