// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package migrations holds the migrations compiled into fusescope-schema
// (docs/spec/fusescope-schema.md §5). They are never edited once released: a
// correction is a new migration, and each one's checksum is pinned by a test.
package migrations

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

// Migration is one migration (§5). Statements carry the placeholders
// {cluster} and {db:<role>}, bare where an identifier goes and quoted where a
// string literal goes. A statement starting with DataMarker is a data
// statement (§5.2); every other one is DDL.
type Migration struct {
	ID         uint32
	Name       string
	Statements []string
	Reversible bool
	// Post is the whole schema every replica must show afterwards, every
	// object and not only the ones the migration changes. It is a separate
	// artifact, not derived from Statements (§5).
	Post schema.Snapshot
	// Pre is the previous migration's Post, empty for the first: the whole
	// schema before this migration.
	Pre schema.Snapshot
	// Data is every data condition that must hold after the migration,
	// cumulative like Post (§5.2).
	Data []schema.DataCondition
}

// DataMarker starts a data statement in a migration file.
const DataMarker = "-- data\n"

// IsData reports whether a statement is a data statement.
func IsData(statement string) bool { return strings.HasPrefix(statement, DataMarker) }

// OwnsRows reports whether the migration writes or changes rows: it has a
// data statement, or data conditions other than prev's. Such a migration is
// never reversible (§5.2).
func (m Migration) OwnsRows(prev []schema.DataCondition) bool {
	return slices.ContainsFunc(m.Statements, IsData) || !reflect.DeepEqual(m.Data, prev)
}

// Affected lists the objects the migration creates, changes or drops: those
// that differ between Pre and Post, sorted.
func (m Migration) Affected() []string {
	var out []string
	for _, k := range m.Post.Names() {
		if pre, ok := m.Pre[k]; !ok || !reflect.DeepEqual(pre, m.Post[k]) {
			out = append(out, k)
		}
	}
	for _, k := range m.Pre.Names() {
		if _, ok := m.Post[k]; !ok {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

//go:embed sql/*.sql post/*.json data/*.json
var files embed.FS

type entry struct {
	id         uint32
	name       string
	reversible bool
}

var entries = []entry{
	{1, "base", false},
	{2, "column_evolution_baseline", false},
}

// All returns the migrations, ordered by id.
func All() ([]Migration, error) {
	all := make([]Migration, 0, len(entries))
	var prev Migration
	var bound []string
	for _, e := range entries {
		m, err := load(e, prev)
		if err != nil {
			return nil, err
		}
		views, err := boundViews(m.Statements)
		if err != nil {
			return nil, fmt.Errorf("%04d %s: %w", m.ID, m.Name, err)
		}
		for _, v := range views {
			if _, ok := m.Post[v]; !ok {
				return nil, fmt.Errorf("%04d %s binds %s, which its post-conditions do not hold", m.ID, m.Name, v)
			}
		}
		bound = append(bound, views...)
		m.Post = withDefiners(m.Post, bound)
		all = append(all, m)
		prev = m
	}
	return all, Check(all)
}

// definerDefault binds a view to the user default, as seven views of [obs]
// are; Render binds them to the user that runs the statement instead, with
// definerCurrent (§5.4).
const (
	definerDefault = "DEFINER = default SQL SECURITY DEFINER"
	definerCurrent = "DEFINER = CURRENT_USER SQL SECURITY DEFINER"
)

var viewRe = regexp.MustCompile(`^CREATE MATERIALIZED VIEW IF NOT EXISTS \{db:(\w+)\}\.(\w+) `)

// boundViews returns the views, "<role>.<name>", whose statement binds them
// with definerDefault: once, in a materialized view's own statement.
func boundViews(statements []string) ([]string, error) {
	var out []string
	for _, s := range statements {
		switch n := strings.Count(s, definerDefault); {
		case n == 0:
			continue
		case n > 1:
			return nil, fmt.Errorf("%q %d times in one statement: %.80s", definerDefault, n, s)
		}
		m := viewRe.FindStringSubmatch(s)
		if m == nil {
			return nil, fmt.Errorf("%q outside a materialized view: %.80s", definerDefault, s)
		}
		out = append(out, m[1]+"."+m[2])
	}
	return out, nil
}

// withDefiners returns post with each bound view that it holds expecting SQL
// SECURITY DEFINER and DefinerPlaceholder, the fields no post file stores.
func withDefiners(post schema.Snapshot, bound []string) schema.Snapshot {
	if len(bound) == 0 {
		return post
	}
	out := maps.Clone(post)
	for _, v := range bound {
		if o, ok := out[v]; ok {
			o.Definer, o.SQLSecurity = schema.DefinerPlaceholder, "DEFINER"
			out[v] = o
		}
	}
	return out
}

// load reads a migration's files. Without a post-conditions or data file it
// keeps prev's: the migration changes no structure, or no rows.
func load(e entry, prev Migration) (Migration, error) {
	file := fmt.Sprintf("%04d_%s", e.id, e.name)
	sql, err := files.ReadFile("sql/" + file + ".sql")
	if err != nil {
		return Migration{}, err
	}
	m := Migration{ID: e.id, Name: e.name, Reversible: e.reversible, Statements: Split(string(sql)), Pre: prev.Post, Post: prev.Post, Data: prev.Data}
	if m.Pre == nil {
		m.Pre = schema.Snapshot{}
	}
	if post, err := files.ReadFile("post/" + file + ".json"); err == nil {
		m.Post = nil
		if err := json.Unmarshal(post, &m.Post); err != nil {
			return Migration{}, fmt.Errorf("%s post-conditions: %w", file, err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) || prev.Post == nil {
		return Migration{}, err
	}
	if data, err := files.ReadFile("data/" + file + ".json"); err == nil {
		m.Data = nil
		if err := json.Unmarshal(data, &m.Data); err != nil {
			return Migration{}, fmt.Errorf("%s data conditions: %w", file, err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return Migration{}, err
	}
	return m, nil
}

// Check requires the migrations to be consistent: ids in order, each data
// condition valid against its migration's structure, and every migration
// that owns rows irreversible (§5.2).
func Check(all []Migration) error {
	var prev []schema.DataCondition
	for i, m := range all {
		if m.ID != uint32(i+1) {
			return fmt.Errorf("migration %d has id %04d", i+1, m.ID)
		}
		for _, c := range m.Data {
			if err := c.Validate(m.Post); err != nil {
				return fmt.Errorf("%04d %s: %w", m.ID, m.Name, err)
			}
		}
		// A data statement runs only to insert rows its conditions own (§5.2).
		if slices.ContainsFunc(m.Statements, IsData) && reflect.DeepEqual(m.Data, prev) {
			return fmt.Errorf("%04d %s has a data statement but no data conditions of its own", m.ID, m.Name)
		}
		if m.Reversible && m.OwnsRows(prev) {
			return fmt.Errorf("%04d %s owns rows, so it cannot be reversible (§5.2)", m.ID, m.Name)
		}
		prev = m.Data
	}
	return nil
}

// statementEnd ends each statement in a migration file.
const statementEnd = ";\n"

// Split splits a migration file into statements, each ending in ";" on its
// own line.
func Split(sql string) []string {
	var out []string
	for _, s := range strings.Split(sql, "\n"+statementEnd) {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// Join is the inverse of Split: each statement followed by ";" on its own
// line, a blank line between statements.
func Join(statements []string) string {
	return strings.Join(statements, "\n"+statementEnd+"\n") + "\n" + statementEnd
}

// Checksum is the SHA-256 of everything that defines the migration: its id,
// name, reversibility, statements (data statements with their marker),
// post-conditions and, when there are any, data conditions. Without data
// conditions it hashes as before they existed, so 0001's is unchanged.
func (m Migration) Checksum() string {
	post, _ := json.Marshal(m.Post) // a map marshals with sorted keys
	h := sha256.New()
	fmt.Fprintf(h, "%d\n%s\n%t\n", m.ID, m.Name, m.Reversible)
	h.Write([]byte(Join(m.Statements)))
	h.Write(post)
	if len(m.Data) > 0 {
		data, _ := json.Marshal(m.Data)
		h.Write([]byte("\ndata\n"))
		h.Write(data)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Render replaces the placeholders with the configured names: '{cluster}' and
// '{db:<role>}' as string literals, {cluster} and {db:<role>} as identifiers.
// It also binds the views definerDefault binds to the user that runs it.
func Render(statement string, databases map[string]string, cluster string) string {
	pairs := []string{"'" + schema.ClusterPlaceholder + "'", literal(cluster), schema.ClusterPlaceholder, identifier(cluster), definerDefault, definerCurrent}
	for _, role := range schema.Roles {
		p := schema.RolePlaceholder(role)
		pairs = append(pairs, "'"+p+"'", literal(databases[role]), p, identifier(databases[role]))
	}
	return strings.NewReplacer(pairs...).Replace(statement)
}

func literal(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

func identifier(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

// MarshalPost writes post-conditions as the artifact stores them.
func MarshalPost(s schema.Snapshot) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
