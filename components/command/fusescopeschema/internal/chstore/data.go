// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package chstore

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/ClickHouse/ch-go"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

// dataInsert runs a data statement through a Distributed table: each shard
// has written its rows when it returns. Deduplication is off, since a data
// statement inserts only the keys no shard holds (§5.2) and an identical
// earlier block must not swallow it.
var dataInsert = []ch.Setting{
	{Key: "distributed_foreground_insert", Value: "1"},
	{Key: "insert_deduplicate", Value: "0"},
}

func (s *Store) ExecData(ctx context.Context, statement string) error {
	return s.conn.Exec(ctx, statement, dataInsert...)
}

// DataRows reads c's table in its scope on every replica, grouped by the
// key, each other column with its merge function, and groups the replicas by
// the Keeper path they replicate: the replicas of one shard (§5.2).
func (s *Store) DataRows(ctx context.Context, c schema.DataCondition, merged, databases map[string]string) (schema.DataRead, error) {
	role, table := schema.Split(c.Table)
	db, ok := databases[role]
	if !ok {
		return nil, fmt.Errorf("data condition on %s: no database for role %s", c.Table, role)
	}
	replicas, err := s.Replicas(ctx)
	if err != nil {
		return nil, err
	}
	paths, err := s.conn.Rows(ctx, fmt.Sprintf("SELECT %s, zookeeper_path FROM clusterAllReplicas(%s, system.replicas) WHERE database = %s AND table = %s",
		replicaKey, quote(s.cluster), quote(db), quote(table)), 2)
	if err != nil {
		return nil, err
	}
	group, err := byReplica(paths, func(r []string) (string, error) { return r[0], nil })
	if err != nil {
		return nil, err
	}
	out := schema.DataRead{}
	for _, r := range replicas {
		g, ok := group[r]
		if !ok {
			return nil, fmt.Errorf("replica %s does not replicate %s.%s", r, db, table)
		}
		if out[g] == nil {
			out[g] = map[string][]schema.LiveRow{}
		}
		out[g][r] = nil
	}

	others := slices.Sorted(maps.Keys(merged))
	cols := make([]string, 0, len(c.Key)+len(others))
	keys := make([]string, 0, len(c.Key))
	for _, k := range c.Key {
		keys = append(keys, ident(k))
		cols = append(cols, "toString("+ident(k)+")")
	}
	for _, o := range others {
		cols = append(cols, fmt.Sprintf("toString(%s(%s))", merged[o], ident(o)))
	}
	where := make([]string, 0, len(c.Scope))
	for _, k := range slices.Sorted(maps.Keys(c.Scope)) {
		where = append(where, ident(k)+" = "+quote(c.Scope[k]))
	}
	cond := ""
	if len(where) > 0 {
		cond = " WHERE " + strings.Join(where, " AND ")
	}
	// view() runs the grouping on each replica, so every replica's rows are
	// its own, merged as its engine would merge them.
	inner := fmt.Sprintf("SELECT %s AS replica, %s FROM %s.%s%s GROUP BY %s",
		replicaKey, strings.Join(cols, ", "), ident(db), ident(table), cond, strings.Join(keys, ", "))
	rows, err := s.conn.Rows(ctx, fmt.Sprintf("SELECT * FROM clusterAllReplicas(%s, view(%s)) ORDER BY ALL", quote(s.cluster), inner), 1+len(cols))
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		g, ok := group[r[0]]
		if !ok || out[g] == nil {
			return nil, fmt.Errorf("replica %s answered but is not listed in the cluster", r[0])
		}
		row := schema.LiveRow{Key: r[1 : 1+len(c.Key)], Values: map[string]string{}}
		for i, o := range others {
			row.Values[o] = r[1+len(c.Key)+i]
		}
		out[g][r[0]] = append(out[g][r[0]], row)
	}
	return out, nil
}
