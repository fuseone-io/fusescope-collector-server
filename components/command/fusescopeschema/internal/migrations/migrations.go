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
	"fmt"
	"strings"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

// Migration is one migration (§5). Statements carry the placeholders
// {cluster} and {db:<role>}, bare where an identifier goes and quoted where a
// string literal goes.
type Migration struct {
	ID         uint32
	Name       string
	Statements []string
	Reversible bool
	// Post is the structure every replica must show afterwards. It is a
	// separate artifact, not derived from Statements (§9).
	Post schema.Snapshot
}

//go:embed sql/*.sql post/*.json
var files embed.FS

// All returns the migrations, ordered by id.
func All() ([]Migration, error) {
	base, err := load(1, "base", false)
	if err != nil {
		return nil, err
	}
	return []Migration{base}, nil
}

func load(id uint32, name string, reversible bool) (Migration, error) {
	file := fmt.Sprintf("%04d_%s", id, name)
	sql, err := files.ReadFile("sql/" + file + ".sql")
	if err != nil {
		return Migration{}, err
	}
	post, err := files.ReadFile("post/" + file + ".json")
	if err != nil {
		return Migration{}, err
	}
	m := Migration{ID: id, Name: name, Reversible: reversible, Statements: Split(string(sql))}
	if err := json.Unmarshal(post, &m.Post); err != nil {
		return Migration{}, fmt.Errorf("%s post-conditions: %w", file, err)
	}
	return m, nil
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

// Join is the inverse of Split.
func Join(statements []string) string {
	var b strings.Builder
	for _, s := range statements {
		b.WriteString(s)
		b.WriteString("\n" + statementEnd + "\n")
	}
	return b.String()
}

// Checksum is the SHA-256 of everything that defines the migration: its id,
// name, reversibility, statements and post-conditions.
func (m Migration) Checksum() string {
	post, _ := json.Marshal(m.Post) // a map marshals with sorted keys
	h := sha256.New()
	fmt.Fprintf(h, "%d\n%s\n%t\n", m.ID, m.Name, m.Reversible)
	h.Write([]byte(Join(m.Statements)))
	h.Write(post)
	return hex.EncodeToString(h.Sum(nil))
}

// Render replaces the placeholders with the configured names: '{cluster}' and
// '{db:<role>}' as string literals, {cluster} and {db:<role>} as identifiers.
func Render(statement string, databases map[string]string, cluster string) string {
	pairs := []string{"'" + schema.ClusterPlaceholder + "'", literal(cluster), schema.ClusterPlaceholder, identifier(cluster)}
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
