// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package chstore

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

// Canonical forms (§4.1): the server formats each text with
// formatQuerySingleLine inside a statement that parses it, and the wrapper is
// cut off again.
type form struct{ prefix, cut, suffix string }

var (
	formExpression = form{"SELECT ", "SELECT ", ""}
	formQuery      = form{"", "", ""}
	formTTL        = form{"ALTER TABLE t MODIFY TTL ", "ALTER TABLE t (MODIFY TTL", ")"}
	formSettings   = form{"ALTER TABLE t MODIFY SETTING ", "ALTER TABLE t (MODIFY SETTING", ")"}
)

// canonBatch bounds the text of one formatting query, well under the
// default max_query_size of 256 KiB.
const canonBatch = 64 << 10

// canonical formats texts in f's form, in order.
func (s *Store) canonical(ctx context.Context, f form, texts []string) ([]string, error) {
	out := make([]string, 0, len(texts))
	for start := 0; start < len(texts); {
		end, size := start, 0
		for end < len(texts) && (end == start || size+len(texts[end]) < canonBatch) {
			size += len(texts[end]) + len(f.prefix) + 8
			end++
		}
		lits := make([]string, end-start)
		for i, t := range texts[start:end] {
			lits[i] = quote(f.prefix + t)
		}
		rows, err := s.conn.Rows(ctx, "SELECT toString(i), formatQuerySingleLine(a[i]) FROM (SELECT ["+strings.Join(lits, ", ")+"] AS a) ARRAY JOIN arrayEnumerate(a) AS i ORDER BY i", 2)
		if err != nil {
			return nil, fmt.Errorf("formatQuery: %w", err)
		}
		formatted, err := byIndex(rows, end-start)
		if err != nil {
			return nil, err
		}
		for i, got := range formatted {
			c, err := f.unwrap(got)
			if err != nil {
				return nil, fmt.Errorf("formatQuery of %q: %w", texts[start+i], err)
			}
			out = append(out, c)
		}
		start = end
	}
	return out, nil
}

// unwrap cuts the form's wrapper off a formatted text, refusing one that
// does not carry it.
func (f form) unwrap(formatted string) (string, error) {
	c, ok := strings.CutPrefix(formatted, f.cut)
	if !ok {
		return "", fmt.Errorf("%q does not start with %q", formatted, f.cut)
	}
	if c, ok = strings.CutSuffix(c, f.suffix); !ok {
		return "", fmt.Errorf("%q does not end with %q", formatted, f.suffix)
	}
	// The suffix must close the wrapper, not the text's own last call.
	if _, err := schema.SplitTop(c, ","); err != nil {
		return "", fmt.Errorf("%q is not wrapped as %q…%q: %w", formatted, f.cut, f.suffix, err)
	}
	return strings.TrimSpace(c), nil
}

// byIndex orders rows of (1-based index, text), requiring each index of 1..n
// exactly once.
func byIndex(rows [][]string, n int) ([]string, error) {
	if len(rows) != n {
		return nil, fmt.Errorf("formatQuery returned %d rows for %d texts", len(rows), n)
	}
	out := make([]string, n)
	seen := make([]bool, n)
	for _, r := range rows {
		i, err := strconv.Atoi(r[0])
		if err != nil || i < 1 || i > n || seen[i-1] {
			return nil, fmt.Errorf("formatQuery returned index %q for %d texts", r[0], n)
		}
		seen[i-1], out[i-1] = true, r[1]
	}
	return out, nil
}

var _ schema.Canon = (*Store)(nil)

func (s *Store) Expressions(ctx context.Context, exprs []string) ([]string, error) {
	return s.canonical(ctx, formExpression, exprs)
}

func (s *Store) TTLs(ctx context.Context, ttls []string) ([]string, error) {
	return s.canonical(ctx, formTTL, ttls)
}

// canonSet collects texts to canonicalise and hands out their results.
type canonSet struct {
	byForm map[form]map[string]string
}

func (c *canonSet) add(f form, text string) {
	if text == "" {
		return
	}
	if c.byForm[f] == nil {
		c.byForm[f] = map[string]string{}
	}
	c.byForm[f][text] = ""
}

func (c *canonSet) get(f form, text string) string {
	if text == "" {
		return ""
	}
	return c.byForm[f][text]
}

func (c *canonSet) run(ctx context.Context, s *Store) error {
	for f, texts := range c.byForm {
		in := slices.Sorted(maps.Keys(texts))
		out, err := s.canonical(ctx, f, in)
		if err != nil {
			return err
		}
		for i, t := range in {
			texts[t] = out[i]
		}
	}
	return nil
}

// mvRe reads a materialized view's refresh clause, if refreshable, and its
// TO target from create_table_query.
var mvRe = regexp.MustCompile("^CREATE MATERIALIZED VIEW (?:`[^`]*`|[^\\s.`]+)\\.(?:`[^`]*`|[^\\s`]+) (?:(REFRESH .*?) )?TO ((?:`[^`]*`|[^\\s.`]+)\\.(?:`[^`]*`|[^\\s(`]+))")

// rawObject is one object as the system tables show it, before
// canonicalisation.
type rawObject struct {
	db, name                                        string
	engine, engineFull, sorting, primary, partition string
	sampling, asSelect, create                      string
	columns                                         []schema.Column
	indexes                                         []schema.Index
	projections                                     []schema.Projection
}

// ReplicaSnapshots reads the telemetry databases, role to name, on every
// replica, canonicalised by the connected server (§4.1).
func (s *Store) ReplicaSnapshots(ctx context.Context, databases map[string]string) (map[string]schema.Snapshot, error) {
	roles := map[string]string{}
	var lits []string
	for role, db := range databases {
		if _, dup := roles[db]; dup {
			return nil, fmt.Errorf("database %s has two roles", db)
		}
		roles[db] = role
		lits = append(lits, quote(db))
	}
	slices.Sort(lits)
	in := " WHERE database IN (" + strings.Join(lits, ", ") + ")"
	from := func(table string) string {
		return fmt.Sprintf(" FROM clusterAllReplicas(%s, system.%s)", quote(s.cluster), table)
	}
	replicas, err := s.Replicas(ctx)
	if err != nil {
		return nil, err
	}
	raw := map[string]map[string]*rawObject{}
	for _, r := range replicas {
		if _, dup := raw[r]; dup {
			return nil, fmt.Errorf("replica %s answered twice", r)
		}
		raw[r] = map[string]*rawObject{}
	}
	tables, err := s.conn.Rows(ctx, "SELECT "+replicaKey+", database, name, engine, engine_full, sorting_key, primary_key, partition_key, sampling_key, as_select, create_table_query"+from("tables")+in, 11)
	if err != nil {
		return nil, err
	}
	for _, t := range tables {
		objs, ok := raw[t[0]]
		if !ok {
			return nil, fmt.Errorf("replica %s answered but is not listed in the cluster", t[0])
		}
		objs[t[1]+"."+t[2]] = &rawObject{db: t[1], name: t[2], engine: t[3], engineFull: t[4], sorting: t[5], primary: t[6], partition: t[7], sampling: t[8], asSelect: t[9], create: t[10]}
	}
	object := func(r []string) (*rawObject, error) {
		o := raw[r[0]][r[1]+"."+r[2]]
		if o == nil {
			return nil, fmt.Errorf("replica %s shows parts of %s.%s but not the table", r[0], r[1], r[2])
		}
		return o, nil
	}
	cols, err := s.conn.Rows(ctx, "SELECT "+replicaKey+", database, table, name, type, default_kind, default_expression, compression_codec, comment"+from("columns")+in+" ORDER BY 1, database, table, position", 9)
	if err != nil {
		return nil, err
	}
	for _, c := range cols {
		o, err := object(c)
		if err != nil {
			return nil, err
		}
		o.columns = append(o.columns, schema.Column{Name: c[3], Type: c[4], DefaultKind: c[5], DefaultExpression: c[6], Codec: c[7], Comment: c[8]})
	}
	idx, err := s.conn.Rows(ctx, "SELECT "+replicaKey+", database, table, name, type_full, expr, toString(granularity)"+from("data_skipping_indices")+in, 7)
	if err != nil {
		return nil, err
	}
	for _, i := range idx {
		o, err := object(i)
		if err != nil {
			return nil, err
		}
		g, err := strconv.ParseUint(i[6], 10, 64)
		if err != nil {
			return nil, err
		}
		o.indexes = append(o.indexes, schema.Index{Name: i[3], Type: i[4], Expression: i[5], Granularity: g})
	}
	projs, err := s.conn.Rows(ctx, "SELECT "+replicaKey+", database, table, name, query"+from("projections")+in, 5)
	if err != nil {
		return nil, err
	}
	for _, p := range projs {
		o, err := object(p)
		if err != nil {
			return nil, err
		}
		o.projections = append(o.projections, schema.Projection{Name: p[3], Query: p[4]})
	}
	return s.snapshots(ctx, raw, roles)
}

// snapshots canonicalises every replica's objects in one pass and replaces
// database and cluster names with their placeholders.
func (s *Store) snapshots(ctx context.Context, raw map[string]map[string]*rawObject, roles map[string]string) (map[string]schema.Snapshot, error) {
	type parsed struct {
		raw     *rawObject
		engine  schema.Engine
		target  string
		refresh string
	}
	all := map[string]map[string]parsed{}
	set := &canonSet{byForm: map[form]map[string]string{}}
	for r, objs := range raw {
		all[r] = map[string]parsed{}
		for key, o := range objs {
			p := parsed{raw: o}
			if o.engineFull != "" {
				e, err := schema.ParseEngine(o.engineFull)
				if err != nil {
					return nil, fmt.Errorf("%s on %s: %w", key, r, err)
				}
				p.engine = e
			}
			if o.engine == "MaterializedView" {
				m := mvRe.FindStringSubmatch(o.create)
				if m == nil {
					return nil, fmt.Errorf("%s on %s: a materialized view without a TO target", key, r)
				}
				p.refresh, p.target = m[1], m[2]
			}
			for _, a := range p.engine.Args {
				set.add(formExpression, a)
			}
			for _, k := range []string{o.sorting, o.primary, o.partition, o.sampling} {
				set.add(formExpression, k)
			}
			set.add(formTTL, p.engine.TTL)
			set.add(formSettings, strings.Join(p.engine.Settings, ", "))
			set.add(formQuery, o.asSelect)
			for _, c := range o.columns {
				set.add(formExpression, c.DefaultExpression)
			}
			for _, i := range o.indexes {
				set.add(formExpression, i.Expression)
			}
			for _, pr := range o.projections {
				set.add(formQuery, pr.Query)
			}
			all[r][key] = p
		}
	}
	if err := set.run(ctx, s); err != nil {
		return nil, err
	}
	sub := func(f form, text string) string { return schema.ReplaceDatabases(set.get(f, text), roles) }
	out := map[string]schema.Snapshot{}
	for r, objs := range all {
		snap := schema.Snapshot{}
		for _, p := range objs {
			o := p.raw
			obj := schema.Object{
				Kind: schema.KindTable, Engine: p.engine.Name,
				SortingKey: sub(formExpression, o.sorting), PrimaryKey: sub(formExpression, o.primary),
				PartitionKey: sub(formExpression, o.partition), SamplingKey: sub(formExpression, o.sampling),
				TTL: sub(formTTL, p.engine.TTL), Query: sub(formQuery, o.asSelect),
			}
			if settings := set.get(formSettings, strings.Join(p.engine.Settings, ", ")); settings != "" {
				split, err := schema.SplitTop(settings, ",")
				if err != nil {
					return nil, err
				}
				obj.Settings = split
			}
			for i, a := range p.engine.Args {
				a = sub(formExpression, a)
				if p.engine.Name == "Distributed" {
					switch {
					case i == 0 && a == quote(s.cluster):
						a = quote(schema.ClusterPlaceholder)
					case i == 1:
						for db, role := range roles {
							if a == quote(db) {
								a = quote(schema.RolePlaceholder(role))
							}
						}
					}
				}
				obj.EngineArgs = append(obj.EngineArgs, a)
			}
			switch o.engine {
			case "Dictionary":
				obj.Kind = schema.KindDictionary
			case "Distributed":
				obj.Kind = schema.KindDistributed
			case "MaterializedView":
				obj.Kind, obj.Engine = schema.KindMaterializedView, ""
				obj.Target = schema.ReplaceDatabases(p.target, roles)
				obj.Refresh = p.refresh
			}
			if obj.Kind != schema.KindMaterializedView {
				for _, c := range o.columns {
					c.DefaultExpression = sub(formExpression, c.DefaultExpression)
					obj.Columns = append(obj.Columns, c)
				}
			}
			for _, i := range o.indexes {
				i.Expression = sub(formExpression, i.Expression)
				obj.Indexes = append(obj.Indexes, i)
			}
			for _, pr := range o.projections {
				pr.Query = sub(formQuery, pr.Query)
				obj.Projections = append(obj.Projections, pr)
			}
			snap[roles[o.db]+"."+o.name] = obj
		}
		out[r] = snap
	}
	return out, nil
}
