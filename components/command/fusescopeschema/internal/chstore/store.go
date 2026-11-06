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
const Database = coord.Database

// replicaKey identifies the replica a row comes from. serverUUID is unique per
// server, so one server answering for two replicas shows as a duplicate; the
// host name keeps the key readable.
const replicaKey = "hostName() || '/' || toString(serverUUID())"

// ErrNoKeeperMapPrefix: the server has no keeper_map_path_prefix, which no
// system table shows (§2.1, §11); the bootstrap's first create reports it.
var ErrNoKeeperMapPrefix = errors.New("KeeperMap is disabled: set keeper_map_path_prefix in the ClickHouse server configuration")

var (
	strict = ch.Setting{Key: "keeper_map_strict_mode", Value: "1"}
	ddl    = []ch.Setting{
		{Key: "distributed_ddl_output_mode", Value: "none"}, // like throw, without the per-host result table
		{Key: "distributed_ddl_task_timeout", Value: "180"},
	}
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

// ReadLease reads the lease table, which must hold the migrate row only.
func (s *Store) ReadLease(ctx context.Context) (coord.Lease, bool, error) {
	rows, err := s.conn.Rows(ctx, "SELECT name, "+leaseColumns+" FROM "+s.table("lease"), 4)
	if err != nil || len(rows) == 0 {
		return coord.Lease{}, false, err
	}
	if len(rows) != 1 || rows[0][0] != "migrate" {
		return coord.Lease{}, false, fmt.Errorf("%s.lease holds %d rows, want only migrate", Database, len(rows))
	}
	l, err := parseLease(rows[0][1], rows[0][2], rows[0][3])
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

// Replicas lists every replica by replicaKey, failing unless each one of the
// cluster's replicas answers.
func (s *Store) Replicas(ctx context.Context) ([]string, error) {
	want, err := s.conn.Rows(ctx, "SELECT toString(count()) FROM system.clusters WHERE cluster = "+quote(s.cluster), 1)
	if err != nil {
		return nil, err
	}
	rows, err := s.conn.Rows(ctx, fmt.Sprintf("SELECT %s FROM clusterAllReplicas(%s, system.one)", replicaKey, quote(s.cluster)), 1)
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

// byReplica keys rows by their first column, refusing a replica that shows
// twice.
func byReplica[V any](rows [][]string, parse func([]string) (V, error)) (map[string]V, error) {
	out := make(map[string]V, len(rows))
	for _, r := range rows {
		if _, dup := out[r[0]]; dup {
			return nil, fmt.Errorf("replica %s answered twice", r[0])
		}
		v, err := parse(r[1:])
		if err != nil {
			return nil, err
		}
		out[r[0]] = v
	}
	return out, nil
}

func (s *Store) ReplicaLeases(ctx context.Context) (map[string]coord.Lease, error) {
	rows, err := s.conn.Rows(ctx, "SELECT "+replicaKey+", "+leaseColumns+" FROM "+s.all("lease")+" WHERE name = 'migrate'", 4)
	if err != nil {
		return nil, err
	}
	return byReplica(rows, func(r []string) (coord.Lease, error) { return parseLease(r[0], r[1], r[2]) })
}

func (s *Store) ReplicaIdentities(ctx context.Context) (map[string]string, error) {
	return s.ReplicaMeta(ctx, "cluster_id")
}

// ReplicaLayouts reads fusescope_schema's tables on every replica.
func (s *Store) ReplicaLayouts(ctx context.Context) (map[string]map[string]coord.TableLayout, error) {
	seen, err := s.Replicas(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[string]coord.TableLayout, len(seen))
	for _, r := range seen {
		if _, dup := out[r]; dup {
			return nil, fmt.Errorf("replica %s answered twice", r)
		}
		out[r] = map[string]coord.TableLayout{}
	}
	in := " WHERE database = " + quote(Database)
	tables, err := s.conn.Rows(ctx, fmt.Sprintf("SELECT %s, name, engine_full, primary_key FROM clusterAllReplicas(%s, system.tables)%s", replicaKey, quote(s.cluster), in), 4)
	if err != nil {
		return nil, err
	}
	for _, t := range tables {
		r, ok := out[t[0]]
		if !ok {
			return nil, fmt.Errorf("replica %s answered but is not listed in the cluster", t[0])
		}
		r[t[1]] = coord.TableLayout{EngineFull: t[2], PrimaryKey: t[3]}
	}
	cols, err := s.conn.Rows(ctx, fmt.Sprintf("SELECT %s, table, name, type FROM clusterAllReplicas(%s, system.columns)%s ORDER BY 1, table, position", replicaKey, quote(s.cluster), in), 4)
	if err != nil {
		return nil, err
	}
	for _, c := range cols {
		t, ok := out[c[0]][c[1]]
		if !ok {
			return nil, fmt.Errorf("replica %s shows columns of %s.%s but not the table", c[0], Database, c[1])
		}
		t.Columns = append(t.Columns, c[2]+" "+c[3])
		out[c[0]][c[1]] = t
	}
	return out, nil
}

func (s *Store) CreateLayout(ctx context.Context, tables []string) error {
	on := " ON CLUSTER " + ident(s.cluster)
	if err := s.conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+ident(Database)+on, ddl...); err != nil {
		return err
	}
	for _, t := range tables {
		l := coord.Layout(t)
		cols := l.Columns
		for i, c := range cols {
			name, typ, _ := strings.Cut(c, " ")
			cols[i] = ident(name) + " " + typ
		}
		// The ENGINE clause is the engine_full every replica must then show.
		q := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s%s (%s) ENGINE = %s", s.table(t), on, strings.Join(cols, ", "), l.EngineFull)
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

func (s *Store) ReplicaMeta(ctx context.Context, name string) (map[string]string, error) {
	rows, err := s.conn.Rows(ctx, "SELECT "+replicaKey+", value FROM "+s.all("meta")+" WHERE name = "+quote(name), 2)
	if err != nil {
		return nil, err
	}
	return byReplica(rows, func(r []string) (string, error) { return r[0], nil })
}
