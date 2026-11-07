// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package chstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/coord"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/migrate"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/migrations"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

// The servers of the 2x2 cluster, in FUSESCOPE_SCHEMA_TEST_ADDRS order.
const (
	shard1a, shard1b, shard2a = 0, 1, 2
)

func baseline(t *testing.T) migrations.Migration {
	t.Helper()
	all, err := migrations.All()
	if err != nil {
		t.Fatal(err)
	}
	return all[1]
}

// baseRows reads 0002's base rows on every replica (§5.2).
func baseRows(t *testing.T, s *Store, databases map[string]string) schema.DataRead {
	t.Helper()
	m := baseline(t)
	c := m.Data[0]
	merged, err := c.Merged(m.Post)
	if err != nil {
		t.Fatal(err)
	}
	read, err := s.DataRows(context.Background(), c, merged, databases)
	if err != nil {
		t.Fatal(err)
	}
	return read
}

// requireInsertedAt requires the five base rows on every replica of their
// shard, each key on one shard, the v1 rows at one instant in nanoseconds
// within [from, to] and the v0 rows at 0.
func requireInsertedAt(t *testing.T, s *Store, databases map[string]string, from, to int64) {
	t.Helper()
	read := baseRows(t, s, databases)
	groups, lagging := read.Agree()
	if len(lagging) > 0 {
		t.Fatalf("replicas %v differ from their shard: %v", lagging, read)
	}
	if missing, problems := baseline(t).Data[0].Check(groups); len(missing)+len(problems) > 0 {
		t.Fatalf("0002's rows: missing %v, problems %v", missing, problems)
	}
	var instant string
	for _, rows := range groups {
		for _, r := range rows {
			v := r.Values["release_time"]
			if r.Key[5] == "0" {
				continue
			}
			if instant == "" {
				instant = v
			}
			if v != instant {
				t.Fatalf("v1 rows at %s and %s, want one instant", instant, v)
			}
		}
	}
	ns, err := strconv.ParseFloat(instant, 64)
	if err != nil || ns < float64(from)-1e6 || ns > float64(to)+1e6 {
		t.Fatalf("release_time %s is not a time in nanoseconds within the run [%d, %d]", instant, from, to)
	}
}

type evolutionRow struct {
	key     []string
	release string
}

func (r evolutionRow) values() string {
	return fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', %s, %s)", r.key[0], r.key[1], r.key[2], r.key[3], r.key[4], r.key[5], r.release)
}

// insertRows inserts through the Distributed table, as the backend does, or
// into the local table of the connected server.
func insertRows(t *testing.T, c *Conn, local bool, rows ...evolutionRow) {
	t.Helper()
	table := "signoz_metadata.distributed_column_evolution_metadata"
	if local {
		table = "signoz_metadata.column_evolution_metadata"
	}
	parts := make([]string, len(rows))
	for i, r := range rows {
		parts[i] = r.values()
	}
	q := "INSERT INTO " + table + " (signal, column_name, column_type, field_context, field_name, version, release_time) VALUES " + strings.Join(parts, ", ")
	if err := c.Exec(context.Background(), q, dataInsert...); err != nil {
		t.Fatal(err)
	}
}

// applyObservedRows inserts the rows a fork install left [obs rows], byte for
// byte.
func applyObservedRows(t *testing.T, c *Conn) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(observed, "rows", "signoz_metadata.column_evolution_metadata.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []evolutionRow
	for _, line := range bytes.Split(bytes.TrimSpace(b), []byte("\n")) {
		d := json.NewDecoder(bytes.NewReader(line))
		d.UseNumber()
		var r map[string]any
		if err := d.Decode(&r); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, evolutionRow{
			key:     []string{r["signal"].(string), r["column_name"].(string), r["column_type"].(string), r["field_context"].(string), r["field_name"].(string), string(r["version"].(json.Number))},
			release: string(r["release_time"].(json.Number)),
		})
	}
	if len(rows) != 5 {
		t.Fatalf("%d observed rows, want 5", len(rows))
	}
	insertRows(t, c, false, rows...)
}

func baseKey(t *testing.T, i int) []string {
	t.Helper()
	return baseline(t).Data[0].Rows[i].Key
}

// atBase leaves the cluster as chart 0.162.0 does: 0001 done, no rows.
func atBase(t *testing.T, addrs []string) {
	t.Helper()
	emptyCluster(t, dial(t, addrs[0]), schema.DefaultDatabases, "cluster")
	r := newRun(t, addrs[0], schema.DefaultDatabases, "cluster", coord.Config{})
	r.runner.Migrations = []migrations.Migration{base(t)}
	if _, err := r.runner.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.coord.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// A cluster made by chart 0.162.0, 0001 done and the table empty: 0002
// inserts the five rows.
func TestIntegrationChart0162ClusterReceivesTheRows(t *testing.T) {
	addrs := clusterAddrs(t)
	atBase(t, addrs)
	r := newRun(t, addrs[3], schema.DefaultDatabases, "cluster", coord.Config{})
	from := time.Now().UnixNano()
	res, err := r.runner.Migrate(context.Background())
	if err != nil || res.Point != migrate.PointManaged || len(res.Applied) != 1 || res.Applied[0] != 2 {
		t.Fatalf("Migrate = %+v, %v", res, err)
	}
	requireInsertedAt(t, r.store, schema.DefaultDatabases, from, time.Now().UnixNano())
	if st, err := r.runner.Inspect(context.Background(), nil, 0); err != nil || st.Verdict != migrate.UpToDate {
		t.Fatalf("Inspect = %+v, %v", st, err)
	}
}

// A partial state receives only the missing rows; the rows there, and a
// promoted path's row, keep their bytes and release_time.
func TestIntegrationPartialRowsAreCompleted(t *testing.T) {
	addrs := clusterAddrs(t)
	atBase(t, addrs)
	c := dial(t, addrs[0])
	promoted := evolutionRow{key: []string{"logs", "body_v2", "JSON()", "body", "user.id", "1"}, release: "5"}
	insertRows(t, c, false,
		evolutionRow{key: baseKey(t, 1), release: "0"},
		evolutionRow{key: baseKey(t, 4), release: "0"},
		evolutionRow{key: baseKey(t, 2), release: "111"},
		promoted)
	r := newRun(t, addrs[1], schema.DefaultDatabases, "cluster", coord.Config{})
	st, err := r.runner.Inspect(context.Background(), nil, 0)
	if err != nil || st.Verdict != migrate.Pending || len(st.Inserts) != 2 {
		t.Fatalf("Inspect = %+v, %v; want two rows to insert", st, err)
	}
	if _, err := r.runner.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	groups, _ := baseRows(t, r.store, schema.DefaultDatabases).Agree()
	if missing, problems := baseline(t).Data[0].Check(groups); len(missing)+len(problems) > 0 {
		t.Fatalf("missing %v, problems %v", missing, problems)
	}
	kept := false
	for _, rows := range groups {
		for _, row := range rows {
			if reflect.DeepEqual(row.Key, baseKey(t, 2)) {
				kept = row.Values["release_time"] == "111"
			}
		}
	}
	if !kept {
		t.Fatalf("the existing v1 row's release_time changed: %v", groups)
	}
	rows, err := c.Rows(context.Background(), "SELECT toString(min(release_time)) FROM signoz_metadata.distributed_column_evolution_metadata WHERE field_name = 'user.id'", 1)
	if err != nil || len(rows) != 1 || rows[0][0] != "5" {
		t.Fatalf("the promoted path's row: %v, %v", rows, err)
	}
}

// Contradictions are refused before started: history and rows unchanged.
func TestIntegrationContradictionsAreRefusedBeforeStarted(t *testing.T) {
	cases := map[string]func(t *testing.T, addrs []string){
		"another base row": func(t *testing.T, addrs []string) {
			k := append([]string(nil), baseKey(t, 2)...)
			k[2] = "Map(LowCardinality(String), String)"
			insertRows(t, dial(t, addrs[0]), false, evolutionRow{key: k, release: "5"})
		},
		"a v0 row not at 0": func(t *testing.T, addrs []string) {
			insertRows(t, dial(t, addrs[0]), false, evolutionRow{key: baseKey(t, 1), release: "7"})
		},
		"a v1 row at 0": func(t *testing.T, addrs []string) {
			insertRows(t, dial(t, addrs[0]), false, evolutionRow{key: baseKey(t, 0), release: "0"})
		},
		"the same key on two shards": func(t *testing.T, addrs []string) {
			insertRows(t, dial(t, addrs[shard1a]), true, evolutionRow{key: baseKey(t, 0), release: "5"})
			insertRows(t, dial(t, addrs[shard2a]), true, evolutionRow{key: baseKey(t, 0), release: "5"})
		},
	}
	for name, add := range cases {
		t.Run(name, func(t *testing.T) {
			addrs := clusterAddrs(t)
			atBase(t, addrs)
			add(t, addrs)
			r := newRun(t, addrs[3], schema.DefaultDatabases, "cluster", coord.Config{})
			before := baseRows(t, r.store, schema.DefaultDatabases)
			st, err := r.runner.Inspect(context.Background(), nil, 0)
			var contra *migrate.DataError
			if !errors.As(err, &contra) || len(st.Diffs) == 0 {
				t.Fatalf("Inspect = %+v, %v; want the contradiction named", st, err)
			}
			res, err := r.runner.Migrate(context.Background())
			if !errors.As(err, &contra) || res.Started {
				t.Fatalf("Migrate = %+v, %v", res, err)
			}
			if w := telemetryWrites(r.rec.all()); len(w) > 0 {
				t.Fatalf("wrote despite the contradiction: %v", w)
			}
			if h, _ := r.store.History(context.Background()); len(h) != 1 {
				t.Fatalf("history %+v", h)
			}
			if after := baseRows(t, r.store, schema.DefaultDatabases); !reflect.DeepEqual(after, before) {
				t.Fatal("the rows changed")
			}
		})
	}
}

// After 0002, a base row deleted by hand is drift: refused, nothing written.
func TestIntegrationADeletedBaseRowIsDrift(t *testing.T) {
	addrs := clusterAddrs(t)
	emptyCluster(t, dial(t, addrs[0]), schema.DefaultDatabases, "cluster")
	r := newRun(t, addrs[0], schema.DefaultDatabases, "cluster", coord.Config{})
	if _, err := r.runner.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	deleteBaseRow(t, dial(t, addrs[0]), baseKey(t, 2))
	_, err := r.runner.Migrate(context.Background())
	var drift *migrate.DriftError
	if !errors.As(err, &drift) || !strings.Contains(err.Error(), "traces, attributes, JSON(), attribute, __all__, 1) is missing") {
		t.Fatalf("Migrate = %v", err)
	}
}

func deleteBaseRow(t *testing.T, c *Conn, key []string) {
	t.Helper()
	q := fmt.Sprintf("ALTER TABLE signoz_metadata.column_evolution_metadata ON CLUSTER cluster DELETE WHERE signal = '%s' AND column_name = '%s' AND column_type = '%s' AND field_context = '%s' AND field_name = '%s' AND version = %s",
		key[0], key[1], key[2], key[3], key[4], key[5])
	if err := c.Exec(context.Background(), q, slices.Concat(ddl, []ch.Setting{{Key: "mutations_sync", Value: "2"}})...); err != nil {
		t.Fatal(err)
	}
}

// The insert's reply lost: exit 4; status reads 0002's post-conditions, and
// repair --mark done follows them.
func TestIntegrationALostInsertReplyIsRepaired(t *testing.T) {
	addrs := clusterAddrs(t)
	atBase(t, addrs)
	ctx := context.Background()
	r := newRun(t, addrs[0], schema.DefaultDatabases, "cluster", coord.Config{})
	lost := errors.New("connection reset by peer")
	r.runner.AfterStatement = func(id uint32, _ int) error {
		if id == 2 {
			return lost
		}
		return nil
	}
	if res, err := r.runner.Migrate(ctx); !errors.Is(err, lost) || !res.Started {
		t.Fatalf("Migrate = %+v, %v", res, err)
	}
	if err := r.coord.Release(ctx); err != nil {
		t.Fatal(err)
	}
	rep := newRun(t, addrs[2], schema.DefaultDatabases, "cluster", coord.Config{AllowInterrupted: true})
	deleteBaseRow(t, dial(t, addrs[0]), baseKey(t, 0))
	st, err := rep.runner.Inspect(ctx, nil, 0)
	if !errors.Is(err, migrate.ErrInterrupted) || st.Unfinished == nil || len(st.Unfinished.Diffs) != 1 {
		t.Fatalf("Inspect with a row gone = %+v, %v", st.Unfinished, err)
	}
	if err := rep.runner.Repair(ctx, 2, migrate.MarkDone); !errors.Is(err, migrate.ErrRepairRefused) {
		t.Fatalf("Repair done with a row gone = %v", err)
	}
	if err := rep.runner.Repair(ctx, 2, migrate.MarkAbsent); !errors.Is(err, migrate.ErrRepairRefused) || !strings.Contains(err.Error(), "not reversible") {
		t.Fatalf("Repair absent = %v", err)
	}
	insertRows(t, dial(t, addrs[0]), false, evolutionRow{key: baseKey(t, 0), release: "9"})
	if st, err = rep.runner.Inspect(ctx, nil, 0); st.Unfinished == nil || len(st.Unfinished.Diffs) != 0 {
		t.Fatalf("Inspect with the rows back = %+v, %v", st.Unfinished, err)
	}
	if err := rep.runner.Repair(ctx, 2, migrate.MarkDone); err != nil {
		t.Fatalf("Repair done = %v", err)
	}
	if h, _ := rep.store.History(ctx); len(h) != 2 || !h[1].Done {
		t.Fatalf("history %+v", h)
	}
}

// A replica of a row's shard behind the others is re-read until it catches
// up; one that never does is a divergence, refused before started.
func TestIntegrationALaggingReplicaIsReRead(t *testing.T) {
	for _, catchUp := range []bool{true, false} {
		t.Run(map[bool]string{true: "catching up", false: "never catching up"}[catchUp], func(t *testing.T) {
			addrs := clusterAddrs(t)
			atBase(t, addrs)
			ctx := context.Background()
			lagging := dial(t, addrs[shard1b])
			stop := "SYSTEM STOP FETCHES signoz_metadata.column_evolution_metadata"
			start := "SYSTEM START FETCHES signoz_metadata.column_evolution_metadata"
			if err := lagging.Exec(ctx, stop); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lagging.Exec(context.Background(), start) })
			insertRows(t, dial(t, addrs[shard1a]), true, evolutionRow{key: baseKey(t, 1), release: "0"})
			if catchUp {
				go func() {
					time.Sleep(2 * time.Second)
					_ = lagging.Exec(context.Background(), start)
				}()
			}
			r := newRun(t, addrs[3], schema.DefaultDatabases, "cluster", coord.Config{})
			began := time.Now()
			res, err := r.runner.Migrate(ctx)
			if catchUp {
				if err != nil {
					t.Fatalf("Migrate = %v", err)
				}
				return
			}
			var drift *migrate.DriftError
			if !errors.As(err, &drift) || res.Started || !strings.Contains(err.Error(), "the rows of the other replicas of its shard") {
				t.Fatalf("Migrate = %+v, %v", res, err)
			}
			if d := time.Since(began); d < 10*time.Second {
				t.Fatalf("refused after %s, before the 10 s of re-reads", d)
			}
		})
	}
}
