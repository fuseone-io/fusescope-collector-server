// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package chstore

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/coord"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/migrate"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/migrations"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

// recorder keeps every query a connection runs.
type recorder struct {
	mu      sync.Mutex
	queries []string
}

func (r *recorder) record(q string) {
	r.mu.Lock()
	r.queries = append(r.queries, q)
	r.mu.Unlock()
}

func (r *recorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.queries...)
}

var (
	writeRe = regexp.MustCompile(`(?is)^\s*(CREATE|ALTER|DROP|INSERT|RENAME|TRUNCATE|EXCHANGE|ATTACH|DETACH|OPTIMIZE|SYSTEM)\b`)
	// historyReadRe is a read of the fork's history: a FROM or JOIN on one of
	// its tables, or a table function over one.
	historyReadRe = regexp.MustCompile("(?is)\\b(FROM|JOIN)\\s+[`\\w.{}:-]*schema_migrations|\\b(cluster|clusterAllReplicas|remote|remoteSecure|merge)\\s*\\([^)]*schema_migrations")
	// historyWriteRe is a write of fusescope_schema's history rows.
	historyWriteRe = regexp.MustCompile("(?is)^\\s*(INSERT INTO|ALTER TABLE)\\s+\\S*migrations")
)

// telemetryWrites returns the queries that change anything outside
// fusescope_schema.
func telemetryWrites(queries []string) []string {
	var out []string
	for _, q := range queries {
		if writeRe.MatchString(q) && !strings.Contains(q, "`fusescope_schema`") && !strings.Contains(q, "fusescope_schema.") {
			out = append(out, q)
		}
	}
	return out
}

// requireNoHistoryRead fails if any query reads one of the fork's history
// tables. Their structure comes from the system tables, and 0001 creates them
// empty; their rows are never read.
func requireNoHistoryRead(t *testing.T, queries []string) {
	t.Helper()
	for _, q := range queries {
		if historyReadRe.MatchString(q) {
			t.Fatalf("a query reads the fork's history: %.200s", q)
		}
	}
}

// emptyCluster drops the six databases and fusescope_schema on cluster.
func emptyCluster(t *testing.T, c *Conn, databases map[string]string, cluster string) {
	t.Helper()
	drop := func() {
		for _, db := range append(mapValues(databases), Database) {
			_ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+ident(db)+" ON CLUSTER "+ident(cluster)+" SYNC", ddl...)
		}
	}
	drop()
	t.Cleanup(drop)
}

func mapValues(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

type run struct {
	store  *Store
	coord  *coord.Coordinator
	runner *migrate.Runner
	rec    *recorder
}

// newRun bootstraps the coordination, takes the lease and builds a runner on
// a fresh, traced connection.
func newRun(t *testing.T, addr string, databases map[string]string, cluster string, cfg coord.Config) *run {
	t.Helper()
	c := dial(t, addr)
	rec := &recorder{}
	c.Trace(rec.record)
	s := NewStore(c, cluster)
	if err := coord.Bootstrap(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if cfg.Owner == "" {
		cfg.Owner = "test-" + addr
	}
	co, err := coord.New(s, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := co.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	all, err := migrations.All()
	if err != nil {
		t.Fatal(err)
	}
	return &run{store: s, coord: co, rec: rec, runner: &migrate.Runner{
		Store: s, Lease: co, Migrations: all, Databases: databases, Cluster: cluster, Version: "test", Host: "integration",
	}}
}

func base(t *testing.T) migrations.Migration {
	t.Helper()
	all, err := migrations.All()
	if err != nil {
		t.Fatal(err)
	}
	return all[0]
}

func requireBase(t *testing.T, s *Store, databases map[string]string) {
	t.Helper()
	live, err := s.ReplicaSnapshots(context.Background(), databases)
	if err != nil {
		t.Fatal(err)
	}
	diffs, err := schema.Compare(context.Background(), base(t).Post, live, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) > 0 {
		t.Fatalf("the schema differs from 0001's post-conditions: %v", diffs)
	}
}

// From scratch, 0001 builds exactly the observed schema and 0002 its five
// base rows, the v1 rows at one instant in nanoseconds; a second run is a
// no-op.
func TestIntegrationFreshInstallRunsBase(t *testing.T) {
	addrs := clusterAddrs(t)
	emptyCluster(t, dial(t, addrs[0]), schema.DefaultDatabases, "cluster")
	r := newRun(t, addrs[0], schema.DefaultDatabases, "cluster", coord.Config{})
	from := time.Now().UnixNano()
	res, err := r.runner.Migrate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Point != migrate.PointEmpty || len(res.Applied) != 2 {
		t.Fatalf("result %+v", res)
	}
	requireBase(t, r.store, schema.DefaultDatabases)
	requireInsertedAt(t, r.store, schema.DefaultDatabases, from, time.Now().UnixNano())
	history, err := r.store.History(context.Background())
	if err != nil || len(history) != 2 || !history[0].Done || history[0].Adopted || history[0].Checksum != base(t).Checksum() || !history[1].Done {
		t.Fatalf("history %+v, %v", history, err)
	}
	requireNoHistoryRead(t, r.rec.all())
	if err := r.coord.Release(context.Background()); err != nil {
		t.Fatal(err)
	}

	again := newRun(t, addrs[3], schema.DefaultDatabases, "cluster", coord.Config{})
	res, err = again.runner.Migrate(context.Background())
	if err != nil || res.Point != migrate.PointManaged || len(res.Applied) != 0 {
		t.Fatalf("second run: %+v, %v", res, err)
	}
	if w := telemetryWrites(again.rec.all()); len(w) > 0 {
		t.Fatalf("the second run wrote: %v", w)
	}
	if h, _ := again.store.History(context.Background()); len(h) != 2 || h[0] != history[0] || h[1] != history[1] {
		t.Fatalf("history changed: %+v", h)
	}
	requireNoHistoryRead(t, again.rec.all())
}

// Custom database names, one of them needing quotes, and a custom cluster.
func TestIntegrationCustomNames(t *testing.T) {
	addrs := clusterAddrs(t)
	databases := map[string]string{}
	for _, role := range schema.Roles {
		databases[role] = "fs_" + role
	}
	databases["traces"] = "fs-traces"
	emptyCluster(t, dial(t, addrs[0]), databases, "fusescope-custom")
	r := newRun(t, addrs[1], databases, "fusescope-custom", coord.Config{})
	if _, err := r.runner.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireBase(t, r.store, databases)
	for _, q := range telemetryWrites(r.rec.all()) {
		if strings.HasPrefix(q, "INSERT INTO `fs_metadata`.distributed_column_evolution_metadata") {
			continue // 0002's data statement, through the Distributed table (§5.2)
		}
		if !strings.Contains(q, "ON CLUSTER `fusescope-custom`") {
			t.Fatalf("a statement outside the custom cluster: %.120s", q)
		}
	}
}

// A fork install is adopted: 0001 recorded done and adopted, 0002 done with
// the fork's rows as they are; no telemetry object or row created or changed.
func TestIntegrationForkSchemaIsAdopted(t *testing.T) {
	addrs := clusterAddrs(t)
	c := dial(t, addrs[0])
	applyObserved(t, c)
	applyObservedRows(t, c)
	emptyCluster(t, c, map[string]string{}, "cluster")
	r := newRun(t, addrs[2], schema.DefaultDatabases, "cluster", coord.Config{})
	before := baseRows(t, r.store, schema.DefaultDatabases)
	res, err := r.runner.Migrate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Point != migrate.PointFork || len(res.Applied) != 1 || res.Applied[0] != 2 {
		t.Fatalf("result %+v", res)
	}
	if w := telemetryWrites(r.rec.all()); len(w) > 0 {
		t.Fatalf("the adoption wrote to the telemetry databases: %v", w)
	}
	if after := baseRows(t, r.store, schema.DefaultDatabases); !reflect.DeepEqual(after, before) {
		t.Fatalf("the fork's rows changed:\n%v\n%v", before, after)
	}
	h, err := r.store.History(context.Background())
	if err != nil || len(h) != 2 || !h[0].Done || !h[0].Adopted || h[0].Checksum != base(t).Checksum() || !h[1].Done || h[1].Adopted {
		t.Fatalf("history %+v, %v", h, err)
	}
	requireBase(t, r.store, schema.DefaultDatabases)
	requireNoHistoryRead(t, r.rec.all())
}

// The fork's schema with one change is refused: nothing written anywhere but
// the lease.
func TestIntegrationDriftIsRefusedWithoutWriting(t *testing.T) {
	addrs := clusterAddrs(t)
	c := dial(t, addrs[0])
	applyObserved(t, c)
	emptyCluster(t, c, map[string]string{}, "cluster")
	exec(t, c, "ALTER TABLE signoz_meter.samples ON CLUSTER cluster ADD COLUMN extra String")
	r := newRun(t, addrs[1], schema.DefaultDatabases, "cluster", coord.Config{})
	res, err := r.runner.Migrate(context.Background())
	var d *migrate.DriftError
	if !errors.As(err, &d) || res.Started {
		t.Fatalf("Migrate = %+v, %v; want a DriftError", res, err)
	}
	if !strings.Contains(d.Error(), "meter.samples column extra is extra") {
		t.Fatalf("the refusal does not name the drift: %v", d)
	}
	if w := telemetryWrites(r.rec.all()); len(w) > 0 {
		t.Fatalf("the refusal wrote to the telemetry databases: %v", w)
	}
	for _, q := range r.rec.all() {
		if historyWriteRe.MatchString(q) {
			t.Fatalf("the refusal wrote the history: %s", q)
		}
	}
	if h, _ := r.store.History(context.Background()); len(h) != 0 {
		t.Fatalf("history %+v", h)
	}
	requireNoHistoryRead(t, r.rec.all())
}

// A run killed midway leaves 0001 started; once its lease expires the next
// run is refused as interrupted and creates nothing more.
func TestIntegrationKilledMidwayIsInterrupted(t *testing.T) {
	addrs := clusterAddrs(t)
	emptyCluster(t, dial(t, addrs[0]), schema.DefaultDatabases, "cluster")
	cfg := coord.Config{LeaseFor: 8 * time.Second, LostAfter: 2 * time.Second, RenewEvery: time.Second, RereadFor: 2 * time.Second, ClockSkew: 2 * time.Second}
	r := newRun(t, addrs[0], schema.DefaultDatabases, "cluster", cfg)
	renewals, stop := context.WithCancel(context.Background())
	go r.coord.RunRenewals(renewals)
	killed := errors.New("killed")
	r.runner.AfterStatement = func(_ uint32, i int) error {
		if i == 40 {
			stop() // the process dies: no more renewals, no release
			return killed
		}
		return nil
	}
	res, err := r.runner.Migrate(context.Background())
	if !errors.Is(err, killed) || !res.Started {
		t.Fatalf("Migrate = %+v, %v", res, err)
	}
	h, err := r.store.History(context.Background())
	if err != nil || len(h) != 1 || h[0].Done {
		t.Fatalf("history %+v, %v; want 0001 started", h, err)
	}
	live, err := r.store.ReplicaSnapshots(context.Background(), schema.DefaultDatabases)
	if err != nil {
		t.Fatal(err)
	}
	created := 0
	for _, s := range live {
		created = len(s)
	}
	if created != 40-6 {
		t.Fatalf("%d objects after 40 statements, want 34", created)
	}

	time.Sleep(cfg.LeaseFor + time.Second)
	c := dial(t, addrs[3])
	s := NewStore(c, "cluster")
	next, err := coord.New(s, coord.Config{Owner: "next"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := next.Acquire(context.Background()); !errors.Is(err, coord.ErrInterrupted) {
		t.Fatalf("Acquire = %v, want ErrInterrupted", err)
	}
	after, err := s.ReplicaSnapshots(context.Background(), schema.DefaultDatabases)
	if err != nil {
		t.Fatal(err)
	}
	for _, snap := range after {
		if len(snap) != created {
			t.Fatalf("%d objects after the refusal, want %d", len(snap), created)
		}
	}
}

// repair --mark absent on the real store: a synthetic reversible 0002,
// injected in the runner only, interrupted after creating its table; refused
// while the table exists, recorded absent once it is dropped by hand.
func TestIntegrationRepairMarkAbsent(t *testing.T) {
	addrs := clusterAddrs(t)
	emptyCluster(t, dial(t, addrs[0]), schema.DefaultDatabases, "cluster")
	ctx := context.Background()
	r := newRun(t, addrs[0], schema.DefaultDatabases, "cluster", coord.Config{})
	r.runner.Migrations = []migrations.Migration{base(t)}
	if _, err := r.runner.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	stmt := "CREATE TABLE IF NOT EXISTS {db:logs}.zz_reversible ON CLUSTER {cluster} (`k` String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY k"
	render := func(s string) string { return migrations.Render(s, schema.DefaultDatabases, "cluster") }
	if err := r.store.ExecDDL(ctx, render(stmt)); err != nil {
		t.Fatal(err)
	}
	live, err := r.store.ReplicaSnapshots(ctx, schema.DefaultDatabases)
	if err != nil {
		t.Fatal(err)
	}
	var post schema.Snapshot
	for _, s := range live {
		post = s
	}
	drop := render("DROP TABLE IF EXISTS {db:logs}.zz_reversible ON CLUSTER {cluster} SYNC")
	if err := r.store.ExecDDL(ctx, drop); err != nil {
		t.Fatal(err)
	}
	base := base(t)
	m2 := migrations.Migration{ID: 2, Name: "reversible", Statements: []string{stmt}, Reversible: true, Pre: base.Post, Post: post}
	if got := m2.Affected(); len(got) != 1 || got[0] != "logs.zz_reversible" {
		t.Fatalf("0002 affects %v", got)
	}
	r.runner.Migrations = []migrations.Migration{base, m2}
	killed := errors.New("killed")
	r.runner.AfterStatement = func(id uint32, _ int) error {
		if id == 2 {
			return killed
		}
		return nil
	}
	if res, err := r.runner.Migrate(ctx); !errors.Is(err, killed) || !res.Started {
		t.Fatalf("Migrate = %+v, %v", res, err)
	}
	if err := r.coord.Release(ctx); err != nil {
		t.Fatal(err)
	}

	rep := newRun(t, addrs[3], schema.DefaultDatabases, "cluster", coord.Config{AllowInterrupted: true})
	rep.runner.Migrations = r.runner.Migrations
	if err := rep.runner.Repair(ctx, 2, migrate.MarkAbsent); !errors.Is(err, migrate.ErrRepairRefused) || !strings.Contains(err.Error(), "logs.zz_reversible still exists") {
		t.Fatalf("Repair with the table there = %v", err)
	}
	if h, _ := rep.store.History(ctx); len(h) != 2 {
		t.Fatalf("a refused repair changed the history: %+v", h)
	}
	if err := rep.store.ExecDDL(ctx, drop); err != nil {
		t.Fatal(err)
	}
	if err := rep.runner.Repair(ctx, 2, migrate.MarkAbsent); err != nil {
		t.Fatalf("Repair = %v", err)
	}
	h, err := rep.store.History(ctx)
	if err != nil || len(h) != 1 || h[0].ID != 1 {
		t.Fatalf("history %+v, %v; want 0002 deleted", h, err)
	}
	deleted := false
	for _, q := range rep.rec.all() {
		deleted = deleted || strings.HasPrefix(q, "DELETE FROM `fusescope_schema`.`migrations` WHERE id = 2")
	}
	if !deleted {
		t.Fatal("no DELETE of 0002's row was issued")
	}
	// 0002 is pending again and runs.
	rep.runner.AfterStatement = nil
	if res, err := rep.runner.Migrate(ctx); err != nil || len(res.Applied) != 1 || res.Applied[0] != 2 {
		t.Fatalf("Migrate after the repair = %+v, %v", res, err)
	}
}
