// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package coord

import (
	"context"
	"fmt"
	"maps"
	"slices"
)

// InspectLayout checks fusescope_schema on every replica without writing,
// for check, plan and status (§1). It reports whether the coordination is
// complete. None of it on any replica, or this layout only partly, is
// incomplete, which the bootstrap completes: migrate is pending. A table this
// binary does not know, a changed one, another coordination version or a
// replica with another identity is refused, as the bootstrap and the lease
// would refuse it.
func InspectLayout(ctx context.Context, s LayoutStore) (complete bool, _ error) {
	layouts, err := replicaLayouts(ctx, s)
	if err != nil {
		return false, err
	}
	if err := checkStructures(layouts); err != nil {
		return false, err
	}
	for _, r := range slices.Sorted(maps.Keys(layouts)) {
		if len(layouts[r]) != len(layout) {
			return false, nil
		}
	}
	if err := checkVersion(ctx, s, false); err != nil {
		return false, err
	}
	ids, err := s.ReplicaMeta(ctx, "cluster_id")
	if err != nil {
		return false, fmt.Errorf("%w: reading the cluster identity: %v", ErrUnavailable, err)
	}
	versions, err := s.ReplicaMeta(ctx, "coordination")
	if err != nil {
		return false, fmt.Errorf("%w: reading the coordination version: %v", ErrUnavailable, err)
	}
	var want string
	for _, r := range slices.Sorted(maps.Keys(layouts)) {
		id, ok := ids[r]
		if _, v := versions[r]; !ok || !v || id == "" {
			return false, nil // rows the bootstrap still inserts
		}
		if want == "" {
			want = id
		}
		if id != want {
			return false, fmt.Errorf("%w: replica %s has cluster identity %q, another has %q", ErrDivergent, r, id, want)
		}
	}
	return true, nil
}
