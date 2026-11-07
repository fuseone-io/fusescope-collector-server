// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package migrate decides the starting point and runs the pending migrations
// under the lease (docs/spec/fusescope-schema.md §4, §5, §7). It reads the
// telemetry schema only from the system tables, and the history only from
// fusescope_schema.migrations: never any table the fork wrote.
package migrate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/coord"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/migrations"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

// Row is one row of fusescope_schema.migrations.
type Row struct {
	ID       uint32
	Name     string
	Checksum string
	Done     bool
	Adopted  bool
	Token    uint64
}

// Store is what the runner reads and writes.
type Store interface {
	schema.Canon
	ReplicaSnapshots(ctx context.Context, databases map[string]string) (map[string]schema.Snapshot, error)
	History(ctx context.Context) ([]Row, error)
	// InsertStarted and InsertAdopted insert in strict mode and confirm the
	// row by reading it back; an existing id is an error. InsertStarted
	// wraps ErrNotWritten only when it proved that no row of this run
	// exists; any other error may hide a written row.
	InsertStarted(ctx context.Context, r Row, version, host string) error
	InsertAdopted(ctx context.Context, r Row, version, host string) error
	// MarkDone marks a started row of this lease generation done, confirmed
	// by reading it back.
	MarkDone(ctx context.Context, id uint32, token uint64) error
	// ExecDDL runs a statement ON CLUSTER and waits for every host.
	ExecDDL(ctx context.Context, statement string) error
	// ExecData runs a data statement once, through the connected server,
	// into a Distributed table, returning once each shard wrote (§5.2).
	ExecData(ctx context.Context, statement string) error
	// DataRows reads a data condition's table in its scope on every replica,
	// by replication group, each column outside the key read with its merge
	// function. A replica that does not answer is an error.
	DataRows(ctx context.Context, c schema.DataCondition, merged, databases map[string]string) (schema.DataRead, error)
	// RepairDone marks a started row done under the current lease token,
	// and DeleteRow deletes a row; both confirm by reading back (§10.1).
	RepairDone(ctx context.Context, id uint32, oldToken, newToken uint64) error
	DeleteRow(ctx context.Context, id uint32, token uint64) error
}

// Lease is the held lease.
type Lease interface {
	Valid(ctx context.Context) error
	Held() coord.Lease
}

// Point is the starting point (§4).
type Point string

const (
	PointEmpty   Point = "empty"
	PointFork    Point = "fork 0.145.0"
	PointManaged Point = "managed"
)

var (
	// ErrInterrupted: a migration is still started (§7.3).
	ErrInterrupted = errors.New("a migration was interrupted")
	// ErrUnknownMigration: the history has an id this binary does not know (§7.1).
	ErrUnknownMigration = errors.New("the history holds a migration this binary does not know")
	// ErrChecksum: a recorded checksum differs from this binary's (§7.2).
	ErrChecksum = errors.New("a recorded migration differs from this binary's")
	// ErrNotWritten: the store proved that the row this run meant to write
	// does not exist.
	ErrNotWritten = errors.New("the history row was not written")
)

// DataError lists how the live rows contradict a pending migration's data
// conditions (§7.8). Nothing was written.
type DataError struct {
	Against string
	Diffs   []schema.Diff
}

func (e *DataError) Error() string {
	lines := make([]string, 0, len(e.Diffs))
	for _, d := range e.Diffs {
		lines = append(lines, "  "+d.String())
	}
	return fmt.Sprintf("the rows contradict %s's data conditions in %d places (see the runbook, §10):\n%s", e.Against, len(e.Diffs), strings.Join(lines, "\n"))
}

// DriftError lists how the live schema differs from what it must be (§7.4,
// §7.5).
type DriftError struct {
	Against string
	Diffs   []schema.Diff
}

func (e *DriftError) Error() string {
	lines := make([]string, 0, len(e.Diffs))
	for _, d := range e.Diffs {
		lines = append(lines, "  "+d.String())
	}
	return fmt.Sprintf("the schema differs from %s in %d places:\n%s", e.Against, len(e.Diffs), strings.Join(lines, "\n"))
}

// Result is what a run did. Started reports that a migration was started, so
// a failure leaves it started (exit 4, §1).
type Result struct {
	Point   Point
	Applied []uint32
	Started bool
}

// Runner runs the migrations on one cluster.
type Runner struct {
	Store      Store
	Lease      Lease
	Migrations []migrations.Migration
	Databases  map[string]string // role to name
	Cluster    string
	Version    string
	Host       string
	// AfterStatement, when set, runs after each statement; a test uses it to
	// stop a run midway, as a crash would.
	AfterStatement func(migration uint32, statement int) error
	// Sleep waits between re-reads of a lagging replica; nil sleeps.
	Sleep func(ctx context.Context, d time.Duration) error
}

// A replica that differs from the others of its shard is re-read every
// rereadEvery, rereads times, before it counts as a divergence (§5.2).
const (
	rereadEvery = 500 * time.Millisecond
	rereads     = 20
)

// Migrate reads the history and the live schema, decides the starting point,
// and runs what is pending. Every refusal happens before any write.
func (r *Runner) Migrate(ctx context.Context) (Result, error) {
	var res Result
	if err := r.Lease.Valid(ctx); err != nil {
		return res, err
	}
	history, err := r.Store.History(ctx)
	if err != nil {
		return res, err
	}
	done, err := r.checkHistory(history)
	if err != nil {
		return res, err
	}
	live, err := r.Store.ReplicaSnapshots(ctx, r.Databases)
	if err != nil {
		return res, err
	}
	switch {
	case done > 0:
		res.Point = PointManaged
		if err := r.compare(ctx, r.Migrations[done-1], live); err != nil {
			return res, err
		}
		if err := r.holds(ctx, r.Migrations[done-1]); err != nil {
			return res, err
		}
	case empty(live):
		res.Point = PointEmpty
	default:
		base := r.Migrations[0]
		if err := r.compare(ctx, base, live); err != nil {
			return res, err
		}
		res.Point = PointFork
		if err := r.Lease.Valid(ctx); err != nil {
			return res, err
		}
		row := Row{ID: base.ID, Name: base.Name, Checksum: base.Checksum(), Token: r.Lease.Held().Token}
		if err := r.Store.InsertAdopted(ctx, row, r.Version, r.Host); err != nil {
			return res, fmt.Errorf("record %04d %s as adopted: %w", base.ID, base.Name, err)
		}
		done = 1
	}
	for _, m := range r.Migrations[done:] {
		started, err := r.run(ctx, m)
		if err != nil {
			res.Started = started
			return res, fmt.Errorf("%04d %s: %w", m.ID, m.Name, err)
		}
		res.Applied = append(res.Applied, m.ID)
	}
	return res, nil
}

// checkHistory requires the history to be a done prefix of this binary's
// migrations, with their checksums, and returns its length (§7.1–7.3).
func (r *Runner) checkHistory(history []Row) (int, error) {
	for i, h := range history {
		if err := r.checkRow(i, h); err != nil {
			return 0, err
		}
		if !h.Done {
			return 0, fmt.Errorf("%w: %04d %s is still started (lease token %d); see the runbook, §10.1", ErrInterrupted, h.ID, h.Name, h.Token)
		}
	}
	return len(history), nil
}

// checkRow requires history row i to be this binary's migration i, with its
// name and checksum.
func (r *Runner) checkRow(i int, h Row) error {
	if i >= len(r.Migrations) || h.ID != r.Migrations[i].ID {
		return fmt.Errorf("%w: %04d %s", ErrUnknownMigration, h.ID, h.Name)
	}
	if h.Name != r.Migrations[i].Name || h.Checksum != r.Migrations[i].Checksum() {
		return fmt.Errorf("%w: %04d was recorded as %q with checksum %s, this binary has %q with %s",
			ErrChecksum, h.ID, h.Name, h.Checksum, r.Migrations[i].Name, r.Migrations[i].Checksum())
	}
	return nil
}

func empty(live map[string]schema.Snapshot) bool {
	for _, s := range live {
		if len(s) > 0 {
			return false
		}
	}
	return true
}

func (r *Runner) compare(ctx context.Context, m migrations.Migration, live map[string]schema.Snapshot) error {
	diffs, err := schema.Compare(ctx, m.Post, live, r.Store)
	if err != nil {
		return err
	}
	if len(diffs) > 0 {
		return &DriftError{Against: fmt.Sprintf("%04d %s", m.ID, m.Name), Diffs: diffs}
	}
	return nil
}

// run runs one migration (§5.1): started, each statement under a valid lease,
// the post-conditions on every replica, done. It reports whether the
// migration's started row may exist: from the moment the insert is sent,
// unless the store proves it absent.
func (r *Runner) run(ctx context.Context, m migrations.Migration) (started bool, _ error) {
	missing, err := r.admit(ctx, m)
	if err != nil {
		return false, err
	}
	if err := r.Lease.Valid(ctx); err != nil {
		return false, err
	}
	token := r.Lease.Held().Token
	if err := r.Store.InsertStarted(ctx, Row{ID: m.ID, Name: m.Name, Checksum: m.Checksum(), Token: token}, r.Version, r.Host); err != nil {
		return !errors.Is(err, ErrNotWritten), fmt.Errorf("record started: %w", err)
	}
	return true, r.statements(ctx, m, token, len(missing) > 0)
}

// statements runs m's statements; its data statements only when insert, that
// is when a row it owns is missing: with every row there, nothing is written.
func (r *Runner) statements(ctx context.Context, m migrations.Migration, token uint64, insert bool) error {
	for i, stmt := range m.Statements {
		if migrations.IsData(stmt) && !insert {
			continue
		}
		if err := r.Lease.Valid(ctx); err != nil {
			return fmt.Errorf("before statement %d: %w", i+1, err)
		}
		exec, text := r.Store.ExecDDL, stmt
		if migrations.IsData(stmt) {
			exec, text = r.Store.ExecData, strings.TrimPrefix(stmt, migrations.DataMarker)
		}
		if err := exec(ctx, migrations.Render(text, r.Databases, r.Cluster)); err != nil {
			return fmt.Errorf("statement %d: %w", i+1, err)
		}
		if r.AfterStatement != nil {
			if err := r.AfterStatement(m.ID, i+1); err != nil {
				return err
			}
		}
	}
	live, err := r.Store.ReplicaSnapshots(ctx, r.Databases)
	if err != nil {
		return err
	}
	if err := r.compare(ctx, m, live); err != nil {
		return fmt.Errorf("post-conditions: %w", err)
	}
	if err := r.holds(ctx, m); err != nil {
		return fmt.Errorf("post-conditions: %w", err)
	}
	if err := r.Lease.Valid(ctx); err != nil {
		return err
	}
	return r.Store.MarkDone(ctx, m.ID, token)
}

// holds requires m's data conditions to hold (§5.2): a broken one is drift
// (§7.4).
func (r *Runner) holds(ctx context.Context, m migrations.Migration) error {
	missing, problems, err := r.checkData(ctx, m)
	if err != nil {
		return err
	}
	if problems = append(problems, missing...); len(problems) > 0 {
		return &DriftError{Against: fmt.Sprintf("%04d %s's data conditions", m.ID, m.Name), Diffs: problems}
	}
	return nil
}

// admit requires m's data conditions to admit the live rows before m starts
// (§5.2, §7.8), and returns the rows m inserts.
func (r *Runner) admit(ctx context.Context, m migrations.Migration) ([]schema.Diff, error) {
	missing, problems, err := r.checkData(ctx, m)
	if err != nil {
		return nil, err
	}
	if len(problems) > 0 {
		return nil, &DataError{Against: fmt.Sprintf("%04d %s", m.ID, m.Name), Diffs: problems}
	}
	return missing, nil
}

// checkData reads each of m's data conditions and compares the rows: the
// owned rows missing, as Missing diffs, and the problems.
func (r *Runner) checkData(ctx context.Context, m migrations.Migration) (missing, problems []schema.Diff, _ error) {
	for _, c := range m.Data {
		groups, err := r.readData(ctx, m, c)
		if err != nil {
			return nil, nil, err
		}
		absent, wrong := c.Check(groups)
		for _, row := range absent {
			missing = append(missing, schema.Diff{Replica: "cluster", Object: c.Table + " row (" + strings.Join(row.Key, ", ") + ")", Kind: schema.Missing})
		}
		problems = append(problems, wrong...)
	}
	return missing, problems, nil
}

// readData reads c until every replica of each shard shows the same rows,
// re-reading a lagging one for up to 10 s; one that still differs is a
// divergence (§7.4). A read error, or the context ending, is returned as it
// is: retryable, never a divergence.
func (r *Runner) readData(ctx context.Context, m migrations.Migration, c schema.DataCondition) (map[string][]schema.LiveRow, error) {
	merged, err := c.Merged(m.Post)
	if err != nil {
		return nil, err
	}
	sleep := r.Sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	for i := 0; ; i++ {
		read, err := r.Store.DataRows(ctx, c, merged, r.Databases)
		if err != nil {
			return nil, err
		}
		groups, lagging := read.Agree()
		if len(lagging) == 0 {
			return groups, nil
		}
		if i == rereads {
			diffs := make([]schema.Diff, 0, len(lagging))
			for _, l := range lagging {
				diffs = append(diffs, schema.Diff{Replica: l, Object: c.Table, Kind: schema.Changed, Part: "rows",
					Want: "the rows of the other replicas of its shard", Got: "other rows after 10 s"})
			}
			return nil, &DriftError{Against: "its shard", Diffs: diffs}
		}
		if err := sleep(ctx, rereadEvery); err != nil {
			return nil, err
		}
	}
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
