// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package migrations

import (
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
	if len(all) != 1 || all[0].ID != 1 || all[0].Name != "base" {
		t.Fatalf("migrations = %+v", all)
	}
	return all[0]
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

func TestSplitJoin(t *testing.T) {
	in := []string{"CREATE DATABASE a", "CREATE TABLE b\n(\n    `x` String DEFAULT ';'\n)"}
	if got := Split(Join(in)); !slices.Equal(got, in) {
		t.Fatalf("Split(Join) = %q", got)
	}
}
