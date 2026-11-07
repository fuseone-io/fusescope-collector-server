// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package migrate

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
)

// Mark is what repair records (§10.1).
type Mark string

const (
	MarkDone   Mark = "done"
	MarkAbsent Mark = "absent"
)

// ErrRepairRefused: the live schema or the history does not allow the repair.
var ErrRepairRefused = errors.New("repair refused")

// Repair records, under the lease and after checking every replica, that the
// last recorded migration id is done (its post-conditions hold) or absent (it
// is reversible and every object it touches is exactly as its preconditions).
func (r *Runner) Repair(ctx context.Context, id uint32, mark Mark) error {
	if mark != MarkDone && mark != MarkAbsent {
		return fmt.Errorf("%w: mark %q is neither done nor absent", ErrRepairRefused, mark)
	}
	if err := r.Lease.Valid(ctx); err != nil {
		return err
	}
	history, err := r.Store.History(ctx)
	if err != nil {
		return err
	}
	n := len(history)
	if n == 0 || history[n-1].ID != id {
		return fmt.Errorf("%w: %04d is not the last recorded migration", ErrRepairRefused, id)
	}
	if _, err := r.checkHistory(history[:n-1]); err != nil {
		return err
	}
	row := history[n-1]
	if err := r.checkRow(n-1, row); err != nil {
		return err
	}
	m := r.Migrations[n-1]
	live, err := r.Store.ReplicaSnapshots(ctx, r.Databases)
	if err != nil {
		return err
	}
	switch mark {
	case MarkDone:
		if err := r.compare(ctx, m, live); err != nil {
			return fmt.Errorf("%w: %04d's post-conditions do not hold: %w", ErrRepairRefused, id, err)
		}
		if row.Done {
			return nil
		}
		if err := r.Lease.Valid(ctx); err != nil {
			return err
		}
		return r.Store.RepairDone(ctx, id, row.Token, r.Lease.Held().Token)
	default:
		if !m.Reversible {
			return fmt.Errorf("%w: %04d %s is not reversible", ErrRepairRefused, id, m.Name)
		}
		var wrong []string
		for _, replica := range slices.Sorted(maps.Keys(live)) {
			for _, key := range m.Affected() {
				want, existed := m.Pre[key]
				got, exists := live[replica][key]
				switch {
				case existed && !exists:
					wrong = append(wrong, replica+": "+key+" is missing")
				case !existed && exists:
					wrong = append(wrong, replica+": "+key+" still exists")
				case existed && !reflect.DeepEqual(want, got):
					wrong = append(wrong, replica+": "+key+" is not as before the migration")
				}
			}
		}
		if len(wrong) > 0 {
			return fmt.Errorf("%w: %04d's preconditions do not hold exactly:\n  %s", ErrRepairRefused, id, strings.Join(wrong, "\n  "))
		}
		if err := r.Lease.Valid(ctx); err != nil {
			return err
		}
		return r.Store.DeleteRow(ctx, id, row.Token)
	}
}
