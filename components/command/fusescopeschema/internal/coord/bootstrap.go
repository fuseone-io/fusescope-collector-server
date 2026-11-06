// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package coord

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// Database holds fusescope-schema's own state (§3).
const Database = "fusescope_schema"

// coordinationVersion is the version of the layout below, stored in
// meta.coordination.
const coordinationVersion = "1"

// TableLayout is one table of fusescope_schema as a replica shows it:
// system.tables' engine_full and primary_key, and its columns in order as
// "name Type".
type TableLayout struct {
	EngineFull string
	PrimaryKey string
	Columns    []string
}

func (l TableLayout) equal(o TableLayout) bool {
	return l.EngineFull == o.EngineFull && l.PrimaryKey == o.PrimaryKey && slices.Equal(l.Columns, o.Columns)
}

// layout is fusescope_schema, version 1 (§3): every table is a KeeperMap at
// its own path under the server's keeper_map_path_prefix.
var layout = map[string]TableLayout{
	"meta":  {PrimaryKey: "name", Columns: []string{"name String", "value String"}},
	"lease": {PrimaryKey: "name", Columns: []string{"name String", "owner String", "token UInt64", "expires_at DateTime64(3)"}},
	"migrations": {PrimaryKey: "id", Columns: []string{
		"id UInt32", "name String", "checksum FixedString(64)",
		"state Enum8('started' = 1, 'done' = 2)", "adopted Bool", "token UInt64",
		"started_at DateTime64(3)", "finished_at Nullable(DateTime64(3))",
		"tool_version String", "host String",
	}},
}

// LayoutTables returns the layout's table names, sorted.
func LayoutTables() []string { return slices.Sorted(maps.Keys(layout)) }

// Layout returns one layout table as every replica must show it; its
// EngineFull is also the ENGINE clause that creates it.
func Layout(table string) TableLayout {
	l := layout[table]
	l.EngineFull = fmt.Sprintf("KeeperMap('/%s/%s') PRIMARY KEY %s", Database, table, l.PrimaryKey)
	l.Columns = slices.Clone(l.Columns)
	return l
}

// ErrUnknownLayout: fusescope_schema holds something this binary does not know,
// or did not end up as this layout on every replica (§7.1).
var ErrUnknownLayout = errors.New("fusescope_schema holds a layout this binary does not know")

// LayoutStore is what the bootstrap reads and writes.
type LayoutStore interface {
	// Replicas is Store.Replicas.
	Replicas(ctx context.Context) ([]string, error)
	// ReplicaLayouts reads fusescope_schema's tables on every replica, with an
	// entry, empty without the database, for each one.
	ReplicaLayouts(ctx context.Context) (map[string]map[string]TableLayout, error)
	// CreateLayout creates the database and the named tables, IF NOT EXISTS,
	// on the cluster.
	CreateLayout(ctx context.Context, tables []string) error
	// InsertMetaIfAbsent and InsertLeaseIfAbsent insert in strict mode; an
	// existing key is not an error.
	InsertMetaIfAbsent(ctx context.Context, name, value string) error
	InsertLeaseIfAbsent(ctx context.Context) error
	Meta(ctx context.Context, name string) (string, bool, error)
	// ReadLease is Store.ReadLease.
	ReadLease(ctx context.Context) (Lease, bool, error)
}

// Bootstrap reads before it writes (§3.1): it refuses a layout it does not
// know on any replica, completes this layout where a replica lacks part of
// it, then checks every replica and reads the rows back.
func Bootstrap(ctx context.Context, s LayoutStore) error {
	layouts, err := replicaLayouts(ctx, s)
	if err != nil {
		return err
	}
	if err := checkKnown(ctx, s, layouts); err != nil {
		return err
	}
	var missing []string
	for _, t := range LayoutTables() {
		for _, tables := range layouts {
			if _, ok := tables[t]; !ok {
				missing = append(missing, t)
				break
			}
		}
	}
	if len(missing) > 0 {
		if err := s.CreateLayout(ctx, missing); err != nil {
			return fmt.Errorf("create %s: %w", Database, err)
		}
	}
	id, err := newUUID()
	if err != nil {
		return err
	}
	if err := s.InsertMetaIfAbsent(ctx, "coordination", coordinationVersion); err != nil {
		return err
	}
	if err := s.InsertMetaIfAbsent(ctx, "cluster_id", id); err != nil {
		return err
	}
	if err := s.InsertLeaseIfAbsent(ctx); err != nil {
		return err
	}
	return checkComplete(ctx, s)
}

func replicaLayouts(ctx context.Context, s LayoutStore) (map[string]map[string]TableLayout, error) {
	rs, err := listReplicas(ctx, s)
	if err != nil {
		return nil, err
	}
	layouts, err := s.ReplicaLayouts(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDivergent, err)
	}
	if err := matchReplicas(rs, layouts, func(string, map[string]TableLayout) error { return nil }); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDivergent, err)
	}
	return layouts, nil
}

// checkKnown refuses, on any replica, a table this layout does not have, a
// table that differs from it, and another coordination version.
func checkKnown(ctx context.Context, s LayoutStore, layouts map[string]map[string]TableLayout) error {
	hasMeta := false
	for _, r := range slices.Sorted(maps.Keys(layouts)) {
		for t, got := range layouts[r] {
			if _, known := layout[t]; !known {
				return fmt.Errorf("%w: unknown table %s.%s on replica %s", ErrUnknownLayout, Database, t, r)
			}
			if want := Layout(t); !got.equal(want) {
				return fmt.Errorf("%w: %s.%s on replica %s is %+v, want %+v", ErrUnknownLayout, Database, t, r, got, want)
			}
			hasMeta = hasMeta || t == "meta"
		}
	}
	if !hasMeta {
		return nil
	}
	v, set, err := s.Meta(ctx, "coordination")
	if err != nil {
		return err
	}
	if set && v != coordinationVersion {
		return fmt.Errorf("%w: coordination version %s, this binary knows %s", ErrUnknownLayout, v, coordinationVersion)
	}
	return nil
}

// checkComplete requires every replica to show exactly this layout, the meta
// rows, and the one lease row with consistent fields.
func checkComplete(ctx context.Context, s LayoutStore) error {
	layouts, err := replicaLayouts(ctx, s)
	if err != nil {
		return err
	}
	if err := checkKnown(ctx, s, layouts); err != nil {
		return err
	}
	for _, r := range slices.Sorted(maps.Keys(layouts)) {
		if len(layouts[r]) != len(layout) {
			return fmt.Errorf("%w: replica %s has tables %v after the bootstrap, want %v", ErrUnknownLayout, r, slices.Sorted(maps.Keys(layouts[r])), LayoutTables())
		}
	}
	v, set, err := s.Meta(ctx, "coordination")
	if err != nil {
		return err
	}
	id, idSet, err := s.Meta(ctx, "cluster_id")
	if err != nil {
		return err
	}
	if !set || v != coordinationVersion || !idSet || id == "" {
		return fmt.Errorf("%w: meta rows missing after the bootstrap", ErrUnknownLayout)
	}
	l, ok, err := s.ReadLease(ctx)
	if err != nil {
		return fmt.Errorf("%w: lease row: %v", ErrUnknownLayout, err)
	}
	if !ok {
		return fmt.Errorf("%w: the lease row is missing after the bootstrap", ErrUnknownLayout)
	}
	if err := checkLease(l); err != nil {
		return fmt.Errorf("%w: %v", ErrUnknownLayout, err)
	}
	return nil
}

// checkLease requires the lease row to be one the protocol can write: the
// initial row, or an owner with the token that took it, or a released lease.
func checkLease(l Lease) error {
	switch {
	case l.Token == 0 && (l.Owner != "" || l.ExpiresMs != 0):
		return fmt.Errorf("lease row %+v: token 0 is only the initial, free lease", l)
	case l.ExpiresMs < 0:
		return fmt.Errorf("lease row %+v: negative expiry", l)
	}
	return nil
}

// newUUID is a random version 4 UUID, the cluster identity.
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6], b[8] = b[6]&0x0f|0x40, b[8]&0x3f|0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
