// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

// baseChecksum pins 0001. It changes only with a deliberate, reviewed edit
// before 0001 is released; afterwards, never (§5).
const baseChecksum = "5c63a2a2c3f87a9c834f6a54a028de8f63778b5363e08ec1db509d06c421d788"

func base(t *testing.T) Migration {
	t.Helper()
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) < 2 || all[0].ID != 1 || all[0].Name != "base" {
		t.Fatalf("migrations: %d, the first %04d %s", len(all), all[0].ID, all[0].Name)
	}
	return all[0]
}

// baselineChecksum pins 0002 (§5.3).
const baselineChecksum = "5f4cb909199e347087f5fb4354100aff7a94bb79ee479118e04eed77ff157f5b"

func baseline(t *testing.T) Migration {
	t.Helper()
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[1].ID != 2 || all[1].Name != "column_evolution_baseline" {
		t.Fatalf("migrations: %d", len(all))
	}
	return all[1]
}

func TestBaselineChecksumIsPinned(t *testing.T) {
	if got := baseline(t).Checksum(); got != baselineChecksum {
		t.Fatalf("0002's checksum is %s, pinned %s", got, baselineChecksum)
	}
}

// 0002 has one data statement and no DDL, the structure of 0001, and the
// five base rows of §5.3 [obs rows].
func TestBaselineIsTheFiveBaseRows(t *testing.T) {
	m, b := baseline(t), base(t)
	if len(m.Statements) != 1 || !IsData(m.Statements[0]) {
		t.Fatalf("0002's statements: %q", m.Statements)
	}
	if m.Reversible || !reflect.DeepEqual(m.Post, b.Post) || !reflect.DeepEqual(m.Pre, b.Post) || len(m.Affected()) != 0 {
		t.Fatal("0002 must be irreversible and keep 0001's structure")
	}
	if len(b.Data) != 0 || len(m.Data) != 1 {
		t.Fatalf("data conditions: 0001 %d, 0002 %d", len(b.Data), len(m.Data))
	}
	c := m.Data[0]
	if c.Table != "metadata.column_evolution_metadata" || !reflect.DeepEqual(c.Scope, map[string]string{"field_name": "__all__"}) {
		t.Fatalf("condition %+v", c)
	}
	var rows []string
	for _, r := range c.Rows {
		rows = append(rows, strings.Join(r.Key, "|")+" "+r.Values["release_time"].String())
	}
	want := []string{
		"logs|resource|JSON()|resource|__all__|1 > 0",
		"logs|resources_string|Map(LowCardinality(String), Float64)|resource|__all__|0 0",
		"traces|attributes|JSON()|attribute|__all__|1 > 0",
		"traces|resource|JSON()|resource|__all__|1 > 0",
		"traces|resources_string|Map(LowCardinality(String), Float64)|resource|__all__|0 0",
	}
	if !slices.Equal(rows, want) {
		t.Fatalf("rows\n%s\nwant\n%s", strings.Join(rows, "\n"), strings.Join(want, "\n"))
	}
	// The statement inserts the same five rows, v1 at one instant in
	// nanoseconds and v0 at 0, and only those no shard holds.
	stmt := m.Statements[0]
	for _, r := range c.Rows {
		k := r.Key
		tuple := fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', %s)", k[0], k[1], k[2], k[3], k[4], k[5])
		if !strings.Contains(stmt, tuple) {
			t.Errorf("the statement does not insert %s", tuple)
		}
	}
	for _, part := range []string{
		"INSERT INTO {db:metadata}.distributed_column_evolution_metadata",
		"if(version = 0, 0., toFloat64(toUnixTimestamp64Nano(now64(9))))",
		"NOT IN (\n    SELECT signal, column_name, column_type, field_context, field_name, version\n    FROM {db:metadata}.distributed_column_evolution_metadata)",
	} {
		if !strings.Contains(stmt, part) {
			t.Errorf("the statement lacks %q", part)
		}
	}
}

// A migration that owns rows cannot be reversible; data conditions must fit
// the structure (§5.2).
func TestCheckRefusesInconsistentMigrations(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	if err := Check(all); err != nil {
		t.Fatalf("Check = %v", err)
	}
	reversible := func(all []Migration) { all[1].Reversible = true }
	widened := func(all []Migration) {
		next := all[1]
		next.ID, next.Statements, next.Reversible = 3, nil, true
		c := next.Data[0]
		c.Rows = append(slices.Clone(c.Rows), schema.DataRow{Key: []string{"logs", "attributes", "JSON()", "attribute", "__all__", "1"}, Values: c.Rows[0].Values})
		next.Data = []schema.DataCondition{c}
		all[1].Reversible = false
		copy(all, append(all[:2:2], next))
	}
	for name, change := range map[string]func([]Migration){"a reversible 0002": reversible} {
		c := slices.Clone(all)
		change(c)
		if err := Check(c); err == nil {
			t.Errorf("%s: Check accepted it", name)
		}
	}
	c := append(slices.Clone(all), Migration{})
	widened(c)
	c[2].Reversible = true
	if err := Check(c); err == nil || !strings.Contains(err.Error(), "owns rows") {
		t.Errorf("a reversible migration widening the rows: Check = %v", err)
	}
	c[2].Reversible = false
	if err := Check(c); err != nil {
		t.Errorf("an irreversible migration widening the rows: Check = %v", err)
	}
	c = append(slices.Clone(all), all[1])
	c[2].ID, c[2].Statements, c[2].Reversible = 3, []string{"ALTER TABLE x ON CLUSTER {cluster} ADD COLUMN y String"}, true
	if err := Check(c); err != nil {
		t.Errorf("a reversible DDL migration keeping the rows: Check = %v", err)
	}
	c[2].Statements, c[2].Reversible = []string{DataMarker + "INSERT …"}, false
	if err := Check(c); err == nil || !strings.Contains(err.Error(), "no data conditions of its own") {
		t.Errorf("a data statement with 0002's conditions unchanged: Check = %v", err)
	}
	c[2].Data = []schema.DataCondition{{Table: "metadata.nope"}}
	if err := Check(c); err == nil {
		t.Error("a condition on a table not in the structure: Check accepted it")
	}
}

func TestBaseChecksumIsPinned(t *testing.T) {
	m := base(t)
	if got := m.Checksum(); got != baseChecksum {
		t.Fatalf("0001's checksum is %s, pinned %s", got, baseChecksum)
	}
	if m.Checksum() != base(t).Checksum() {
		t.Fatal("the checksum is not stable across loads")
	}
}

func TestChecksumCoversEveryPart(t *testing.T) {
	m := base(t)
	changes := map[string]func(*Migration){
		"id":         func(m *Migration) { m.ID = 2 },
		"name":       func(m *Migration) { m.Name = "other" },
		"reversible": func(m *Migration) { m.Reversible = true },
		"statement":  func(m *Migration) { m.Statements = append(slices.Clone(m.Statements[:1]), m.Statements[2:]...) },
		"post": func(m *Migration) {
			post := schema.Snapshot{}
			for k, o := range m.Post {
				post[k] = o
			}
			delete(post, "logs.logs_v2")
			m.Post = post
		},
	}
	for name, change := range changes {
		c := m
		change(&c)
		if c.Checksum() == m.Checksum() {
			t.Errorf("changing the %s keeps the checksum", name)
		}
	}
}

// 0001 creates the six databases, then local tables, distributed tables and
// materialized views, each after every object it names (§5).
func TestBaseOrderAndDependencies(t *testing.T) {
	m := base(t)
	if len(m.Statements) != 6+135 || len(m.Post) != 135 {
		t.Fatalf("%d statements, %d post-conditions; want 141 and 135", len(m.Statements), len(m.Post))
	}
	create := regexp.MustCompile(`^CREATE (DATABASE|TABLE|MATERIALIZED VIEW) IF NOT EXISTS (\{db:\w+\}(?:\.\w+)?) ON CLUSTER \{cluster\}`)
	ref := regexp.MustCompile(`\{db:(\w+)\}\.(\w+)`)
	distributed := regexp.MustCompile(`ENGINE = Distributed\('\{cluster\}', '\{db:(\w+)\}', '(\w+)'`)
	created := map[string]bool{}
	for i, s := range m.Statements {
		h := create.FindStringSubmatch(s)
		if h == nil {
			t.Fatalf("statement %d is not an idempotent CREATE … ON CLUSTER: %.80s", i, s)
		}
		name := h[2]
		if h[1] != "DATABASE" {
			db, _, _ := strings.Cut(name, ".")
			if !created[db] {
				t.Errorf("%s comes before its database", name)
			}
			var refs [][]string
			refs = append(refs, ref.FindAllStringSubmatch(s[len(h[0]):], -1)...)
			refs = append(refs, distributed.FindAllStringSubmatch(s, -1)...)
			for _, r := range refs {
				dep := schema.RolePlaceholder(r[1]) + "." + r[2]
				if !created[dep] {
					t.Errorf("%s comes before %s, which it names", name, dep)
				}
			}
			role, table := schema.Split(strings.NewReplacer("{db:", "", "}", "").Replace(name))
			if _, ok := m.Post[role+"."+table]; !ok {
				t.Errorf("%s has no post-condition", name)
			}
		}
		created[name] = true
	}
	// Distributed tables after every local table, views after both.
	rankOf := func(s string) int {
		switch {
		case strings.HasPrefix(s, "CREATE DATABASE"):
			return 0
		case strings.HasPrefix(s, "CREATE MATERIALIZED VIEW"):
			return 3
		case distributed.MatchString(s):
			return 2
		}
		return 1
	}
	for i := 1; i < len(m.Statements); i++ {
		if rankOf(m.Statements[i]) < rankOf(m.Statements[i-1]) {
			t.Fatalf("statement %d (%.60s) comes after a later kind", i, m.Statements[i])
		}
	}
}

// 0001 names no database or cluster: every name is a placeholder.
func TestBaseHoldsOnlyPlaceholders(t *testing.T) {
	for _, s := range base(t).Statements {
		for _, db := range schema.DefaultDatabases {
			if strings.Contains(s, db) {
				t.Fatalf("a statement names %s: %.80s", db, s)
			}
		}
		if strings.Contains(s, "'cluster'") || strings.Contains(s, "ON CLUSTER cluster") {
			t.Fatalf("a statement names the cluster: %.80s", s)
		}
	}
}

// Each post-condition is the whole schema after its migration; the
// precondition is the previous one's, and the affected objects their
// difference (§5).
func TestPreconditionsAreThePreviousPost(t *testing.T) {
	m := base(t)
	if len(m.Pre) != 0 {
		t.Fatalf("0001's precondition holds %d objects, want none", len(m.Pre))
	}
	if got := m.Affected(); len(got) != len(m.Post) {
		t.Fatalf("0001 affects %d objects, want all %d", len(got), len(m.Post))
	}
	next := Migration{ID: 2, Pre: m.Post, Post: schema.Snapshot{}}
	for k, o := range m.Post {
		next.Post[k] = o
	}
	changed := next.Post["meter.samples"]
	changed.Columns = append(append([]schema.Column(nil), changed.Columns...), schema.Column{Name: "x", Type: "String"})
	next.Post["meter.samples"] = changed
	delete(next.Post, "logs.usage")
	next.Post["logs.new"] = schema.Object{Kind: schema.KindTable}
	if got := next.Affected(); !slices.Equal(got, []string{"logs.new", "logs.usage", "meter.samples"}) {
		t.Fatalf("Affected = %v", got)
	}
}

func TestRender(t *testing.T) {
	dbs := map[string]string{}
	for _, r := range schema.Roles {
		dbs[r] = "fs-" + r
	}
	dbs["meter"] = "fs`meter'"
	got := Render("CREATE TABLE IF NOT EXISTS {db:meter}.d ON CLUSTER {cluster} ENGINE = Distributed('{cluster}', '{db:meter}', 'samples', rand()) AS SELECT * FROM {db:logs}.t", dbs, "my-cluster")
	want := "CREATE TABLE IF NOT EXISTS `fs``meter'`.d ON CLUSTER `my-cluster` ENGINE = Distributed('my-cluster', 'fs`meter\\'', 'samples', rand()) AS SELECT * FROM `fs-logs`.t"
	if got != want {
		t.Fatalf("Render\n = %s\nwant %s", got, want)
	}
	for _, s := range base(t).Statements {
		if r := Render(s, dbs, "my-cluster"); strings.Contains(r, "{db:") || strings.Contains(r, "{cluster}") {
			t.Fatalf("a placeholder left after rendering: %.80s", r)
		}
	}
}

// The seven views [obs] binds to default render bound to the connected
// user, 0001's text and checksum as released, and every migration's
// post-conditions compare their definer and SQL security (§5.4).
func TestBaseViewsRenderBoundToTheConnectedUser(t *testing.T) {
	var views []string
	for _, w := range []string{"last_30m", "last_5m", "last_60s", "sum_30m", "sum_5m", "sum_60s_cumulative", "sum_60s_delta"} {
		views = append(views, "metrics.samples_v4_reduced_"+w+"_mv")
	}
	slices.Sort(views)
	m := base(t)
	if n := strings.Count(Join(m.Statements), definerDefault); n != len(views) {
		t.Fatalf("0001 holds %q %d times, want %d", definerDefault, n, len(views))
	}
	rendered := Render(Join(m.Statements), schema.DefaultDatabases, "cluster")
	if strings.Contains(rendered, definerDefault) || strings.Count(rendered, definerCurrent) != len(views) || strings.Count(rendered, "DEFINER = ") != len(views) {
		t.Fatalf("rendered 0001 holds %q %d times and %q %d times", definerDefault, strings.Count(rendered, definerDefault), definerCurrent, strings.Count(rendered, definerCurrent))
	}
	all, err := All()
	if err != nil {
		t.Fatal(err)
	}
	for _, mi := range all {
		var bound []string
		for k, o := range mi.Post {
			if o.SQLSecurity != "" || o.Definer != "" {
				if o.SQLSecurity != "DEFINER" || o.Definer != schema.DefinerPlaceholder {
					t.Errorf("%04d: %s is %s, %s", mi.ID, k, o.SQLSecurity, o.Definer)
				}
				bound = append(bound, k)
			}
		}
		slices.Sort(bound)
		if !slices.Equal(bound, views) {
			t.Fatalf("%04d compares the definers of %v, want %v", mi.ID, bound, views)
		}
	}
}

func TestBoundViews(t *testing.T) {
	view := "CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.v_mv ON CLUSTER {cluster}\nTO {db:metrics}.t\n" + definerDefault + "\nAS SELECT 1"
	tests := []struct {
		name       string
		statements []string
		want       []string
		err        bool
	}{
		{"none", []string{"CREATE TABLE t", "CREATE MATERIALIZED VIEW IF NOT EXISTS {db:metrics}.w_mv ON CLUSTER {cluster} TO {db:metrics}.t AS SELECT 1"}, nil, false},
		{"one view", []string{"CREATE TABLE t", view}, []string{"metrics.v_mv"}, false},
		{"the clause outside a view", []string{"CREATE TABLE {db:metrics}.t ON CLUSTER {cluster} COMMENT '" + definerDefault + "'"}, nil, true},
		{"the clause twice", []string{view + " " + definerDefault}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := boundViews(tt.statements)
			if (err != nil) != tt.err || !slices.Equal(got, tt.want) {
				t.Fatalf("boundViews = %v, %v; want %v, error %t", got, err, tt.want, tt.err)
			}
		})
	}
}

func TestSplitJoin(t *testing.T) {
	in := []string{"CREATE DATABASE a", "CREATE TABLE b\n(\n    `x` String DEFAULT ';'\n)"}
	if got := Split(Join(in)); !slices.Equal(got, in) {
		t.Fatalf("Split(Join) = %q", got)
	}
}
