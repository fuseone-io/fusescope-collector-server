// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package migrations

import (
	"cmp"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

// observedDir is the schema the fork 0.145.0 creates, as SHOW CREATE printed
// it (docs/spec/observed/0.145.0).
var observedDir = filepath.Join("..", "..", "..", "..", "..", "docs", "spec", "observed", "0.145.0")

type observedObject struct{ db, name, engine, ddl string }

func readObserved(t *testing.T) []observedObject {
	t.Helper()
	list, err := os.ReadFile(filepath.Join(observedDir, "tables.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	var out []observedObject
	for _, line := range strings.Split(strings.TrimSpace(string(list)), "\n") {
		f := strings.Split(line, "\t")
		ddl, err := os.ReadFile(filepath.Join(observedDir, "ddl", f[0]+"."+f[1]+".sql"))
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, observedObject{db: f[0], name: f[1], engine: f[2], ddl: strings.TrimSpace(string(ddl))})
	}
	return out
}

// rank orders the objects so each is created after what it reads or writes:
// local tables, then distributed tables, then materialized views (§5).
func rank(engine string) int {
	switch engine {
	case "Distributed":
		return 1
	case "MaterializedView":
		return 2
	}
	return 0
}

var distributedRe = regexp.MustCompile(`ENGINE = Distributed\('([^']*)', '([^']*)', `)

// baseStatements turns the observed DDL into 0001's statements: each object
// IF NOT EXISTS and ON CLUSTER, its database and cluster names placeholders.
func baseStatements(t *testing.T) []string {
	t.Helper()
	roles := map[string]string{}
	for role, db := range schema.DefaultDatabases {
		roles[db] = role
	}
	objects := readObserved(t)
	slices.SortStableFunc(objects, func(a, b observedObject) int {
		return cmp.Or(rank(a.engine)-rank(b.engine), cmp.Compare(roles[a.db], roles[b.db]), cmp.Compare(a.name, b.name))
	})
	var out []string
	for _, role := range schema.Roles {
		out = append(out, "CREATE DATABASE IF NOT EXISTS "+schema.RolePlaceholder(role)+" ON CLUSTER "+schema.ClusterPlaceholder)
	}
	for _, o := range objects {
		kind := "TABLE"
		if o.engine == "MaterializedView" {
			kind = "MATERIALIZED VIEW"
		}
		head := "CREATE " + kind + " " + o.db + "." + o.name
		rest, ok := strings.CutPrefix(o.ddl, head)
		if !ok {
			t.Fatalf("%s.%s does not start with %q", o.db, o.name, head)
		}
		rest = distributedRe.ReplaceAllStringFunc(rest, func(m string) string {
			sub := distributedRe.FindStringSubmatch(m)
			if sub[1] != "cluster" || roles[sub[2]] == "" {
				t.Fatalf("%s.%s: unexpected Distributed arguments %q", o.db, o.name, m)
			}
			return "ENGINE = Distributed('" + schema.ClusterPlaceholder + "', '" + schema.RolePlaceholder(roles[sub[2]]) + "', "
		})
		rest = schema.ReplaceDatabases(rest, roles)
		for db := range roles {
			if strings.Contains(rest, db+".") || strings.Contains(rest, "'"+db+"'") {
				t.Fatalf("%s.%s still names %s", o.db, o.name, db)
			}
		}
		out = append(out, "CREATE "+kind+" IF NOT EXISTS "+schema.RolePlaceholder(roles[o.db])+"."+o.name+" ON CLUSTER "+schema.ClusterPlaceholder+rest)
	}
	return out
}

// TestGenerateBase writes sql/0001_base.sql from the observed DDL, once:
// FUSESCOPE_SCHEMA_GENERATE=1 go test -run TestGenerateBase. 0001 is frozen
// after its release; the checksum test then guards it.
func TestGenerateBase(t *testing.T) {
	if os.Getenv("FUSESCOPE_SCHEMA_GENERATE") != "1" {
		t.Skip("set FUSESCOPE_SCHEMA_GENERATE=1 to write sql/0001_base.sql")
	}
	if err := os.WriteFile(filepath.Join("sql", "0001_base.sql"), []byte(Join(baseStatements(t))), 0o644); err != nil {
		t.Fatal(err)
	}
}
