// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package migrate

import (
	"context"
	"fmt"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/coord"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

// Verdict is check's answer (§1).
type Verdict string

const (
	UpToDate   Verdict = "up to date"
	Pending    Verdict = "pending"
	InProgress Verdict = "in progress"
)

// State is what check, plan and status read. Inspect writes nothing and
// holds no lease, so its Point is advisory: migrate decides again under the
// lease.
type State struct {
	Verdict Verdict
	Point   Point
	Pending []uint32
	History []Row
	Diffs   []schema.Diff
}

// Inspect reads the history and the live schema as migrate would, without
// writing. lease is the lease row as read, nil when there is none; a started
// row counts as in progress only under that lease's token, unexpired at
// nowMs, the server's time. A refusal returns the State read so far with the
// error, and the differences in State.Diffs.
func (r *Runner) Inspect(ctx context.Context, lease *coord.Lease, nowMs int64) (State, error) {
	var st State
	history, err := r.Store.History(ctx)
	if err != nil {
		return st, err
	}
	st.History = history
	if n := len(history); n > 0 && !history[n-1].Done && lease != nil &&
		lease.Owner != "" && lease.Token == history[n-1].Token && lease.ExpiresMs > nowMs {
		if _, err := r.checkHistory(history[:n-1]); err != nil {
			return st, err
		}
		if err := r.checkRow(n-1, history[n-1]); err != nil {
			return st, err
		}
		st.Verdict = InProgress
		return st, nil
	}
	done, err := r.checkHistory(history)
	if err != nil {
		return st, err
	}
	live, err := r.Store.ReplicaSnapshots(ctx, r.Databases)
	if err != nil {
		return st, err
	}
	var against int
	switch {
	case done > 0:
		st.Point, against = PointManaged, done-1
	case empty(live):
		st.Point = PointEmpty
	default:
		st.Point = PointFork
	}
	if st.Point != PointEmpty {
		m := r.Migrations[against]
		if st.Diffs, err = schema.Compare(ctx, m.Post, live, r.Store); err != nil {
			return st, err
		}
		if len(st.Diffs) > 0 {
			point := st.Point
			st.Point = ""
			return st, &DriftError{Against: fmt.Sprintf("%04d %s (starting point %s)", m.ID, m.Name, point), Diffs: st.Diffs}
		}
	}
	if st.Point == PointFork {
		done = 1
	}
	for _, m := range r.Migrations[done:] {
		st.Pending = append(st.Pending, m.ID)
	}
	st.Verdict = UpToDate
	if len(st.Pending) > 0 || st.Point == PointFork {
		st.Verdict = Pending
	}
	return st, nil
}
