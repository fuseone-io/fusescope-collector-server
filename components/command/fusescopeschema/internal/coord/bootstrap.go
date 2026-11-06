// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package coord

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
)

// coordinationVersion is the version of the layout below, stored in
// meta.coordination.
const coordinationVersion = "1"

// layoutColumns is the layout of fusescope_schema, version 1 (§3): each
// table's columns, in order, as "name Type".
var layoutColumns = map[string][]string{
	"meta":  {"name String", "value String"},
	"lease": {"name String", "owner String", "token UInt64", "expires_at DateTime64(3)"},
	"migrations": {
		"id UInt32", "name String", "checksum FixedString(64)",
		"state Enum8('started' = 1, 'done' = 2)", "adopted Bool", "token UInt64",
		"started_at DateTime64(3)", "finished_at Nullable(DateTime64(3))",
		"tool_version String", "host String",
	},
}

// LayoutTables returns layoutColumns' table names, sorted.
func LayoutTables() []string {
	var out []string
	for t := range layoutColumns {
		out = append(out, t)
	}
	slices.Sort(out)
	return out
}

// LayoutColumns returns the columns of one layout table.
func LayoutColumns(table string) []string { return slices.Clone(layoutColumns[table]) }

// ErrUnknownLayout: fusescope_schema holds something this binary does not know
// (§7.1); nothing is written.
var ErrUnknownLayout = errors.New("fusescope_schema holds a layout this binary does not know")

// LayoutStore is what the bootstrap reads and writes.
type LayoutStore interface {
	// LayoutTables returns the tables of fusescope_schema with their columns,
	// and whether the database exists.
	LayoutTables(ctx context.Context) (map[string][]string, bool, error)
	// CreateLayout creates the database and the named tables, IF NOT EXISTS,
	// on the cluster.
	CreateLayout(ctx context.Context, tables []string) error
	// InsertMetaIfAbsent and InsertLeaseIfAbsent insert in strict mode; an
	// existing key is not an error.
	InsertMetaIfAbsent(ctx context.Context, name, value string) error
	InsertLeaseIfAbsent(ctx context.Context) error
	Meta(ctx context.Context, name string) (string, bool, error)
}

// Bootstrap reads before it writes (§3.1): it refuses a layout it does not
// know, completes this layout where it is absent or partial, then checks it.
func Bootstrap(ctx context.Context, s LayoutStore) error {
	if err := verifyLayout(ctx, s, false); err != nil {
		return err
	}
	tables, _, err := s.LayoutTables(ctx)
	if err != nil {
		return err
	}
	var missing []string
	for _, t := range LayoutTables() {
		if _, ok := tables[t]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		if err := s.CreateLayout(ctx, missing); err != nil {
			return fmt.Errorf("create fusescope_schema: %w", err)
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
	return verifyLayout(ctx, s, true)
}

// verifyLayout refuses unknown tables, changed columns and another
// coordination version; complete also requires every table and row.
func verifyLayout(ctx context.Context, s LayoutStore, complete bool) error {
	tables, exists, err := s.LayoutTables(ctx)
	if err != nil {
		return err
	}
	if !exists {
		if complete {
			return fmt.Errorf("%w: fusescope_schema is missing after the bootstrap", ErrUnknownLayout)
		}
		return nil
	}
	for t, cols := range tables {
		want, known := layoutColumns[t]
		if !known {
			return fmt.Errorf("%w: unknown table fusescope_schema.%s", ErrUnknownLayout, t)
		}
		if !slices.Equal(cols, want) {
			return fmt.Errorf("%w: fusescope_schema.%s has columns %v, want %v", ErrUnknownLayout, t, cols, want)
		}
	}
	if _, ok := tables["meta"]; ok {
		v, set, err := s.Meta(ctx, "coordination")
		if err != nil {
			return err
		}
		if set && v != coordinationVersion {
			return fmt.Errorf("%w: coordination version %s, this binary knows %s", ErrUnknownLayout, v, coordinationVersion)
		}
		if complete {
			id, idSet, err := s.Meta(ctx, "cluster_id")
			if err != nil {
				return err
			}
			if !set || !idSet || id == "" {
				return fmt.Errorf("%w: meta rows missing after the bootstrap", ErrUnknownLayout)
			}
		}
	}
	if complete && len(tables) != len(layoutColumns) {
		return fmt.Errorf("%w: tables missing after the bootstrap", ErrUnknownLayout)
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
