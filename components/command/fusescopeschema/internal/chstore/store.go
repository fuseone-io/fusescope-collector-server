// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package chstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/coord"
)

// Database holds fusescope-schema's own state (§3).
const Database = "fusescope_schema"

// ErrNoKeeperMapPrefix: the server has no keeper_map_path_prefix, which no
// system table shows (§2.1, §11); the bootstrap's first create reports it.
var ErrNoKeeperMapPrefix = errors.New("KeeperMap is disabled: set keeper_map_path_prefix in the ClickHouse server configuration")

var (
	strict = ch.Setting{Key: "keeper_map_strict_mode", Value: "1"}
	ddl    = []ch.Setting{
		{Key: "distributed_ddl_output_mode", Value: "none"}, // like throw, without the per-host result table
		{Key: "distributed_ddl_task_timeout", Value: "180"},
	}
	// primaryKey is each layout table's key.
	primaryKey = map[string]string{"meta": "name", "lease": "name", "migrations": "id"}
)

// Store is the coordination store on ClickHouse, implementing coord.Store and
// coord.LayoutStore.
type Store struct {
	conn    *Conn
	cluster string
}

func NewStore(conn *Conn, cluster string) *Store { return &Store{conn: conn, cluster: cluster} }

var (
	_ coord.Store       = (*Store)(nil)
	_ coord.LayoutStore = (*Store)(nil)
)

func (s *Store) table(t string) string { return ident(Database) + "." + ident(t) }

// all reads table t of every replica of the cluster.
func (s *Store) all(t string) string {
	return fmt.Sprintf("clusterAllReplicas(%s, %s, %s)", quote(s.cluster), quote(Database), quote(t))
}

// tryWrite runs a conditional write in strict mode. A Keeper exception (a
// lost race, or a key that exists) is not an error: the caller learns the
// outcome by reading back (§11).
func (s *Store) tryWrite(ctx context.Context, query string) error {
	err := s.conn.Exec(ctx, query, strict)
	if isCode(err, proto.ErrKeeperException) {
		return nil
	}
	return err
}

func (s *Store) ServerNowMs(ctx context.Context) (int64, error) {
	rows, err := s.conn.Rows(ctx, "SELECT toString(toUnixTimestamp64Milli(now64(3)))", 1)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(rows[0][0], 10, 64)
}

func parseLease(owner, token, expires string) (coord.Lease, error) {
	t, err := strconv.ParseUint(token, 10, 64)
	if err != nil {
		return coord.Lease{}, err
	}
	e, err := strconv.ParseInt(expires, 10, 64)
	if err != nil {
		return coord.Lease{}, err
	}
	return coord.Lease{Owner: owner, Token: t, ExpiresMs: e}, nil
}

const leaseColumns = "owner, toString(token), toString(toUnixTimestamp64Milli(expires_at))"

func (s *Store) ReadLease(ctx context.Context) (coord.Lease, bool, error) {
	rows, err := s.conn.Rows(ctx, "SELECT "+leaseColumns+" FROM "+s.table("lease")+" WHERE name = 'migrate'", 3)
	if err != nil || len(rows) == 0 {
		return coord.Lease{}, false, err
	}
	l, err := parseLease(rows[0][0], rows[0][1], rows[0][2])
	return l, err == nil, err
}

func (s *Store) TryAcquire(ctx context.Context, owner string, readToken uint64, forMs int64) error {
	return s.tryWrite(ctx, fmt.Sprintf(
		"ALTER TABLE %s UPDATE owner = %s, token = token + 1, expires_at = now64(3) + toIntervalMillisecond(%d) "+
			"WHERE name = 'migrate' AND token = %d AND (owner = '' OR expires_at <= now64(3))",
		s.table("lease"), quote(owner), forMs, readToken))
}

func (s *Store) TryRenew(ctx context.Context, owner string, token uint64, forMs int64) error {
	return s.tryWrite(ctx, fmt.Sprintf(
		"ALTER TABLE %s UPDATE expires_at = now64(3) + toIntervalMillisecond(%d) "+
			"WHERE name = 'migrate' AND owner = %s AND token = %d AND expires_at > now64(3)",
		s.table("lease"), forMs, quote(owner), token))
}

func (s *Store) Release(ctx context.Context, owner string, token uint64) error {
	return s.tryWrite(ctx, fmt.Sprintf(
		"ALTER TABLE %s UPDATE owner = '' WHERE name = 'migrate' AND owner = %s AND token = %d",
		s.table("lease"), quote(owner), token))
}

func (s *Store) StartedWithToken(ctx context.Context, token uint64) (bool, error) {
	rows, err := s.conn.Rows(ctx, fmt.Sprintf(
		"SELECT toString(count()) FROM %s WHERE state = 'started' AND token = %d", s.table("migrations"), token), 1)
	if err != nil {
		return false, err
	}
	return rows[0][0] != "0", nil
}

// Replicas lists every replica by hostName(), failing unless each one of the
// cluster's replicas answers.
func (s *Store) Replicas(ctx context.Context) ([]string, error) {
	want, err := s.conn.Rows(ctx, "SELECT toString(count()) FROM system.clusters WHERE cluster = "+quote(s.cluster), 1)
	if err != nil {
		return nil, err
	}
	rows, err := s.conn.Rows(ctx, fmt.Sprintf("SELECT hostName() FROM clusterAllReplicas(%s, system.one)", quote(s.cluster)), 1)
	if err != nil {
		return nil, err
	}
	if strconv.Itoa(len(rows)) != want[0][0] {
		return nil, fmt.Errorf("%d of the cluster's %s replicas answered", len(rows), want[0][0])
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r[0]
	}
	return out, nil
}

func (s *Store) ReplicaLeases(ctx context.Context) (map[string]coord.Lease, error) {
	rows, err := s.conn.Rows(ctx, "SELECT hostName(), "+leaseColumns+" FROM "+s.all("lease")+" WHERE name = 'migrate'", 4)
	if err != nil {
		return nil, err
	}
	out := map[string]coord.Lease{}
	for _, r := range rows {
		l, err := parseLease(r[1], r[2], r[3])
		if err != nil {
			return nil, err
		}
		out[r[0]] = l
	}
	return out, nil
}

func (s *Store) ReplicaIdentities(ctx context.Context) (map[string]string, error) {
	rows, err := s.conn.Rows(ctx, "SELECT hostName(), value FROM "+s.all("meta")+" WHERE name = 'cluster_id'", 2)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, r := range rows {
		out[r[0]] = r[1]
	}
	return out, nil
}

// LayoutTables reads fusescope_schema's tables and columns on this server.
func (s *Store) LayoutTables(ctx context.Context) (map[string][]string, bool, error) {
	db, err := s.conn.Rows(ctx, "SELECT toString(count()) FROM system.databases WHERE name = "+quote(Database), 1)
	if err != nil {
		return nil, false, err
	}
	if db[0][0] == "0" {
		return nil, false, nil
	}
	rows, err := s.conn.Rows(ctx, "SELECT table, name, type FROM system.columns WHERE database = "+quote(Database)+" ORDER BY table, position", 3)
	if err != nil {
		return nil, true, err
	}
	out := map[string][]string{}
	for _, r := range rows {
		out[r[0]] = append(out[r[0]], r[1]+" "+r[2])
	}
	tables, err := s.conn.Rows(ctx, "SELECT name FROM system.tables WHERE database = "+quote(Database), 1)
	if err != nil {
		return nil, true, err
	}
	for _, t := range tables {
		if _, ok := out[t[0]]; !ok {
			out[t[0]] = nil
		}
	}
	return out, true, nil
}

func (s *Store) CreateLayout(ctx context.Context, tables []string) error {
	on := " ON CLUSTER " + ident(s.cluster)
	if err := s.conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+ident(Database)+on, ddl...); err != nil {
		return err
	}
	for _, t := range tables {
		cols := coord.LayoutColumns(t)
		for i, c := range cols {
			name, typ, _ := strings.Cut(c, " ")
			cols[i] = ident(name) + " " + typ
		}
		q := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s%s (%s) ENGINE = KeeperMap(%s) PRIMARY KEY %s",
			s.table(t), on, strings.Join(cols, ", "), quote("/"+Database+"/"+t), ident(primaryKey[t]))
		if err := s.conn.Exec(ctx, q, ddl...); err != nil {
			if isCode(err, proto.ErrBadArguments) && strings.Contains(err.Error(), "keeper_map_path_prefix") {
				return ErrNoKeeperMapPrefix
			}
			return err
		}
	}
	return nil
}

func (s *Store) InsertMetaIfAbsent(ctx context.Context, name, value string) error {
	return s.tryWrite(ctx, fmt.Sprintf("INSERT INTO %s (name, value) SELECT %s, %s", s.table("meta"), quote(name), quote(value)))
}

func (s *Store) InsertLeaseIfAbsent(ctx context.Context) error {
	return s.tryWrite(ctx, fmt.Sprintf("INSERT INTO %s (name, owner, token, expires_at) SELECT 'migrate', '', 0, toDateTime64(0, 3)", s.table("lease")))
}

func (s *Store) Meta(ctx context.Context, name string) (string, bool, error) {
	rows, err := s.conn.Rows(ctx, "SELECT value FROM "+s.table("meta")+" WHERE name = "+quote(name), 1)
	if err != nil || len(rows) == 0 {
		return "", false, err
	}
	return rows[0][0], true, nil
}
