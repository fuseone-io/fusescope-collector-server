// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package coord

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"
)

// The re-read window of InspectLayout, as the lease comparison's (§3.2): a
// replica behind a lagging ensemble member may briefly show older rows.
var (
	inspectRereadEvery = 500 * time.Millisecond
	inspectRereadFor   = 10 * time.Second
)

// InspectLayout checks fusescope_schema on every replica without writing,
// for check, plan and status (§1), and reports whether the coordination is
// complete.
//
//   - A table this binary does not know, or a changed one, is refused.
//   - Once meta is on every replica, another coordination version or
//     another cluster identity is refused, whatever else is missing.
//   - A table missing somewhere, or the meta rows or the lease row missing,
//     is incomplete: the bootstrap completes it, and migrate is pending.
//   - Complete, every replica must show the same meta, lease and migrations
//     rows; rows that still differ after the re-read window are a divergence.
func InspectLayout(ctx context.Context, s LayoutStore) (complete bool, _ error) {
	layouts, err := replicaLayouts(ctx, s)
	if err != nil {
		return false, err
	}
	if err := checkStructures(layouts); err != nil {
		return false, err
	}
	everywhere := func(t string) bool { return countWith(layouts, t) == len(layouts) }
	identified := false
	if everywhere("meta") {
		if err := checkVersion(ctx, s, false); err != nil {
			return false, err
		}
		if identified, err = checkIdentities(ctx, s, layouts); err != nil {
			return false, err
		}
	}
	for _, t := range LayoutTables() {
		if !everywhere(t) {
			return false, nil
		}
	}
	rows, err := agreeingRows(ctx, s, slices.Sorted(maps.Keys(layouts)))
	if err != nil {
		return false, err
	}
	switch n := len(rows["lease"]); {
	case n > 1:
		return false, fmt.Errorf("%w: the lease table holds %d rows", ErrUnknownLayout, n)
	case n == 0 || !identified:
		return false, nil
	}
	// The same lease on every replica must also be one the protocol writes,
	// as the bootstrap requires.
	l, ok, err := s.ReadLease(ctx)
	switch {
	case errors.Is(err, ErrUnknownLayout):
		return false, err
	case err != nil:
		return false, fmt.Errorf("%w: reading the lease row: %v", ErrUnavailable, err)
	case !ok:
		return false, nil
	}
	if err := checkLease(l); err != nil {
		return false, fmt.Errorf("%w: %v", ErrUnknownLayout, err)
	}
	return true, nil
}

// checkIdentities refuses two replicas with different cluster identities,
// and reports whether every replica has one.
func checkIdentities(ctx context.Context, s LayoutStore, layouts map[string]map[string]TableLayout) (bool, error) {
	ids, err := s.ReplicaMeta(ctx, "cluster_id")
	if err != nil {
		return false, fmt.Errorf("%w: reading the cluster identity: %v", ErrUnavailable, err)
	}
	versions, err := s.ReplicaMeta(ctx, "coordination")
	if err != nil {
		return false, fmt.Errorf("%w: reading the coordination version: %v", ErrUnavailable, err)
	}
	all, first := true, ""
	for _, r := range slices.Sorted(maps.Keys(layouts)) {
		id := ids[r]
		if _, v := versions[r]; !v || id == "" {
			all = false
			continue
		}
		if first == "" {
			first = id
		}
		if id != first {
			return false, fmt.Errorf("%w: replica %s has cluster identity %q, another has %q", ErrDivergent, r, id, first)
		}
	}
	return all, nil
}

// agreeingRows reads every layout table on every replica until all replicas
// show the same rows, or the re-read window passes. Only the whole window
// establishes a divergence: ctx ending first answers nothing, which is
// ErrUnavailable.
func agreeingRows(ctx context.Context, s LayoutStore, replicas []string) (map[string][]string, error) {
	deadline := time.Now().Add(inspectRereadFor)
	for {
		out := map[string][]string{}
		var differ error
		for _, t := range LayoutTables() {
			rows, err := s.ReplicaRows(ctx, t)
			if err != nil {
				return nil, fmt.Errorf("%w: reading %s.%s: %v", ErrUnavailable, Database, t, err)
			}
			for r := range rows {
				if !slices.Contains(replicas, r) {
					return nil, fmt.Errorf("%w: replica %s answered but is not listed in the cluster", ErrDivergent, r)
				}
			}
			want := rows[replicas[0]]
			for _, r := range replicas[1:] {
				if !slices.Equal(rows[r], want) && differ == nil {
					differ = fmt.Errorf("%w: %s.%s on replica %s holds %v, on %s %v", ErrDivergent, Database, t, r, rows[r], replicas[0], want)
				}
			}
			out[t] = want
		}
		if differ == nil {
			return out, nil
		}
		if !time.Now().Before(deadline) {
			return nil, differ
		}
		if err := sleepCtx(ctx, min(inspectRereadEvery, time.Until(deadline))); err != nil {
			return nil, fmt.Errorf("%w: rows still differ when the check ended (%v): %v", ErrUnavailable, err, differ)
		}
	}
}
