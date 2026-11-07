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
	// row by reading it back; an existing id is an error.
	InsertStarted(ctx context.Context, r Row, version, host string) error
	InsertAdopted(ctx context.Context, r Row, version, host string) error
	// MarkDone marks a started row of this lease generation done, confirmed
	// by reading it back.
	MarkDone(ctx context.Context, id uint32, token uint64) error
	// ExecDDL runs a statement ON CLUSTER and waits for every host.
	ExecDDL(ctx context.Context, statement string) error
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
)

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
}

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
		res.Started = true
		if err := r.run(ctx, m); err != nil {
			return res, fmt.Errorf("%04d %s: %w", m.ID, m.Name, err)
		}
		res.Started = false
		res.Applied = append(res.Applied, m.ID)
	}
	return res, nil
}

// checkHistory requires the history to be a done prefix of this binary's
// migrations, with their checksums, and returns its length (§7.1–7.3).
func (r *Runner) checkHistory(history []Row) (int, error) {
	for i, h := range history {
		if i >= len(r.Migrations) || h.ID != r.Migrations[i].ID {
			return 0, fmt.Errorf("%w: %04d %s", ErrUnknownMigration, h.ID, h.Name)
		}
		if h.Checksum != r.Migrations[i].Checksum() {
			return 0, fmt.Errorf("%w: %04d %s was recorded with checksum %s", ErrChecksum, h.ID, h.Name, h.Checksum)
		}
		if !h.Done {
			return 0, fmt.Errorf("%w: %04d %s is still started (lease token %d); see the runbook, §10.1", ErrInterrupted, h.ID, h.Name, h.Token)
		}
	}
	return len(history), nil
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
// the post-conditions on every replica, done.
func (r *Runner) run(ctx context.Context, m migrations.Migration) error {
	if err := r.Lease.Valid(ctx); err != nil {
		return err
	}
	token := r.Lease.Held().Token
	if err := r.Store.InsertStarted(ctx, Row{ID: m.ID, Name: m.Name, Checksum: m.Checksum(), Token: token}, r.Version, r.Host); err != nil {
		return fmt.Errorf("record started: %w", err)
	}
	for i, stmt := range m.Statements {
		if err := r.Lease.Valid(ctx); err != nil {
			return fmt.Errorf("before statement %d: %w", i+1, err)
		}
		if err := r.Store.ExecDDL(ctx, migrations.Render(stmt, r.Databases, r.Cluster)); err != nil {
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
	if err := r.Lease.Valid(ctx); err != nil {
		return err
	}
	return r.Store.MarkDone(ctx, m.ID, token)
}
