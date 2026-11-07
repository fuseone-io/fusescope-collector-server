// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// DiffKind is how an object or a part of it differs.
type DiffKind string

const (
	Missing DiffKind = "missing"
	Extra   DiffKind = "extra"
	Changed DiffKind = "changed"
)

// Diff is one difference on one replica. Part is empty for a whole object,
// else names what differs, such as "column body" or "ttl".
type Diff struct {
	Replica string
	Object  string
	Kind    DiffKind
	Part    string
	Want    string
	Got     string
}

func (d Diff) String() string {
	what := d.Object
	if d.Part != "" {
		what += " " + d.Part
	}
	switch d.Kind {
	case Missing:
		return fmt.Sprintf("%s: %s is missing", d.Replica, what)
	case Extra:
		return fmt.Sprintf("%s: %s is extra: %s", d.Replica, what, d.Got)
	}
	return fmt.Sprintf("%s: %s differs: want %s, got %s", d.Replica, what, d.Want, d.Got)
}

// Canon canonicalises text with the server's formatQuery, one result per
// input, in order: expressions as in a SELECT, TTLs as in MODIFY TTL.
type Canon interface {
	Expressions(ctx context.Context, exprs []string) ([]string, error)
	TTLs(ctx context.Context, ttls []string) ([]string, error)
}

// check is one condition of an allowance. An expression check holds when the
// expression the backend's pattern builds canonicalises to the live text; a
// TTL check when the live TTL matches the canonical form of a template,
// whose sentinels (ttlSentinels) stand for any number or volume.
type check struct {
	ttl  bool
	want string
	got  string
}

// pending is a difference that §8 may allow: it is allowed when every check
// of one of its alternatives holds.
type pending struct {
	diff Diff
	alts [][]check
}

// Compare compares every replica's snapshot with want and returns every
// difference §8 does not allow, sorted. A difference on one replica counts.
func Compare(ctx context.Context, want Snapshot, live map[string]Snapshot, canon Canon) ([]Diff, error) {
	var all []pending
	for _, r := range slices.Sorted(maps.Keys(live)) {
		got := live[r]
		for _, name := range want.Names() {
			g, ok := got[name]
			if !ok {
				all = append(all, pending{diff: Diff{Replica: r, Object: name, Kind: Missing}})
				continue
			}
			all = append(all, compareObject(r, name, want[name], g)...)
		}
		for _, name := range got.Names() {
			if _, ok := want[name]; !ok {
				all = append(all, pending{diff: Diff{Replica: r, Object: name, Kind: Extra, Got: string(got[name].Kind)}})
			}
		}
	}
	return settle(ctx, all, canon)
}

// settle canonicalises every allowance's texts in one call per kind and
// keeps the differences no alternative allows.
func settle(ctx context.Context, all []pending, canon Canon) ([]Diff, error) {
	exprs, ttls := map[string]string{}, map[string]string{}
	for _, p := range all {
		for _, alt := range p.alts {
			for _, c := range alt {
				if c.ttl {
					ttls[c.want] = ""
				} else {
					exprs[c.want] = ""
				}
			}
		}
	}
	if err := canonicalise(ctx, exprs, canon.Expressions); err != nil {
		return nil, err
	}
	if err := canonicalise(ctx, ttls, canon.TTLs); err != nil {
		return nil, err
	}
	patterns := map[string]*regexp.Regexp{}
	for template, c := range ttls {
		patterns[template] = ttlPattern(c)
	}
	var diffs []Diff
	for _, p := range all {
		if !slices.ContainsFunc(p.alts, func(alt []check) bool { return holds(alt, exprs, patterns) }) {
			diffs = append(diffs, p.diff)
		}
	}
	return diffs, nil
}

func canonicalise(ctx context.Context, texts map[string]string, f func(context.Context, []string) ([]string, error)) error {
	if len(texts) == 0 {
		return nil
	}
	in := slices.Sorted(maps.Keys(texts))
	out, err := f(ctx, in)
	if err != nil {
		return fmt.Errorf("canonicalise the allowed patterns: %w", err)
	}
	if len(out) != len(in) {
		return fmt.Errorf("canonicalise the allowed patterns: %d results for %d inputs", len(out), len(in))
	}
	for i, t := range in {
		texts[t] = out[i]
	}
	return nil
}

func holds(alt []check, exprs map[string]string, patterns map[string]*regexp.Regexp) bool {
	for _, c := range alt {
		if c.ttl && !patterns[c.want].MatchString(c.got) || !c.ttl && exprs[c.want] != c.got {
			return false
		}
	}
	return true
}

func compareObject(replica, name string, want, got Object) []pending {
	var out []pending
	diff := func(part, w, g string) {
		if w != g {
			out = append(out, pending{diff: Diff{Replica: replica, Object: name, Kind: Changed, Part: part, Want: w, Got: g}})
		}
	}
	diff("kind", string(want.Kind), string(got.Kind))
	diff("engine", want.Engine, got.Engine)
	diff("engine arguments", list(want.EngineArgs), list(got.EngineArgs))
	diff("sorting key", want.SortingKey, got.SortingKey)
	diff("primary key", want.PrimaryKey, got.PrimaryKey)
	diff("partition key", want.PartitionKey, got.PartitionKey)
	diff("sampling key", want.SamplingKey, got.SamplingKey)
	diff("settings", list(want.Settings), list(got.Settings))
	diff("target", want.Target, got.Target)
	diff("refresh", want.Refresh, got.Refresh)
	diff("query", want.Query, got.Query)
	if want.TTL != got.TTL {
		out = append(out, pending{diff: Diff{Replica: replica, Object: name, Kind: Changed, Part: "ttl", Want: want.TTL, Got: got.TTL}, alts: allowedTTL(name, got.TTL)})
	}
	out = append(out, compareColumns(replica, name, want, got)...)
	out = append(out, compareIndexes(replica, name, want, got)...)
	wantP, gotP := map[string]string{}, map[string]string{}
	for _, p := range want.Projections {
		wantP[p.Name] = p.Query
	}
	for _, p := range got.Projections {
		gotP[p.Name] = p.Query
	}
	out = append(out, compareNamed(replica, name, "projection", wantP, gotP)...)
	return out
}

func list(xs []string) string { return "(" + strings.Join(xs, ", ") + ")" }

func (c Column) String() string {
	s := c.Name + " " + c.Type
	if c.DefaultKind != "" {
		s += " " + c.DefaultKind + " " + c.DefaultExpression
	}
	if c.Codec != "" {
		s += " " + c.Codec
	}
	if c.Comment != "" {
		s += " COMMENT " + strconv.Quote(c.Comment)
	}
	return s
}

func (i Index) String() string {
	return fmt.Sprintf("%s TYPE %s GRANULARITY %d", i.Expression, i.Type, i.Granularity)
}

// compareColumns compares the columns in order. A live column the backend may
// add (§8) is set aside if it matches its pattern; the remaining columns must
// then be want's, in want's order.
func compareColumns(replica, name string, want, got Object) []pending {
	var out []pending
	wantByName := map[string]Column{}
	for _, c := range want.Columns {
		wantByName[c.Name] = c
	}
	// The backend adds columns with ADD COLUMN, at the end: an extra column
	// before an expected one is drift whatever its shape.
	lastExpected := -1
	for i, c := range got.Columns {
		if _, expected := wantByName[c.Name]; expected {
			lastExpected = i
		}
	}
	var kept []Column
	for i, c := range got.Columns {
		if _, expected := wantByName[c.Name]; expected {
			kept = append(kept, c)
			continue
		}
		p := pending{diff: Diff{Replica: replica, Object: name, Kind: Extra, Part: "column " + c.Name, Got: c.String()}}
		if i > lastExpected {
			p.alts = allowedColumn(name, c, got)
		}
		out = append(out, p)
	}
	gotByName := map[string]Column{}
	for _, c := range kept {
		gotByName[c.Name] = c
	}
	for _, w := range want.Columns {
		g, ok := gotByName[w.Name]
		if !ok {
			out = append(out, pending{diff: Diff{Replica: replica, Object: name, Kind: Missing, Part: "column " + w.Name, Want: w.String()}})
			continue
		}
		if g != w {
			out = append(out, pending{diff: Diff{Replica: replica, Object: name, Kind: Changed, Part: "column " + w.Name, Want: w.String(), Got: g.String()}, alts: allowedColumnChange(name, w, g)})
		}
	}
	// The expected columns that remain must keep want's order.
	var order []string
	for _, c := range kept {
		order = append(order, c.Name)
	}
	var wantOrder []string
	for _, c := range want.Columns {
		if _, ok := gotByName[c.Name]; ok {
			wantOrder = append(wantOrder, c.Name)
		}
	}
	if !slices.Equal(order, wantOrder) {
		out = append(out, pending{diff: Diff{Replica: replica, Object: name, Kind: Changed, Part: "column order", Want: list(wantOrder), Got: list(order)}})
	}
	return out
}

func compareIndexes(replica, name string, want, got Object) []pending {
	var out []pending
	wantI := map[string]Index{}
	for _, i := range want.Indexes {
		wantI[i.Name] = i
	}
	for _, g := range got.Indexes {
		w, ok := wantI[g.Name]
		switch {
		case !ok:
			out = append(out, pending{diff: Diff{Replica: replica, Object: name, Kind: Extra, Part: "index " + g.Name, Got: g.String()}, alts: allowedIndex(name, g, got)})
		case w != g:
			out = append(out, pending{diff: Diff{Replica: replica, Object: name, Kind: Changed, Part: "index " + g.Name, Want: w.String(), Got: g.String()}})
		}
	}
	gotI := map[string]bool{}
	for _, g := range got.Indexes {
		gotI[g.Name] = true
	}
	for _, w := range want.Indexes {
		if !gotI[w.Name] {
			out = append(out, pending{diff: Diff{Replica: replica, Object: name, Kind: Missing, Part: "index " + w.Name, Want: w.String()}})
		}
	}
	return out
}

func compareNamed(replica, name, what string, want, got map[string]string) []pending {
	var out []pending
	for _, n := range slices.Sorted(maps.Keys(want)) {
		g, ok := got[n]
		switch {
		case !ok:
			out = append(out, pending{diff: Diff{Replica: replica, Object: name, Kind: Missing, Part: what + " " + n, Want: want[n]}})
		case g != want[n]:
			out = append(out, pending{diff: Diff{Replica: replica, Object: name, Kind: Changed, Part: what + " " + n, Want: want[n], Got: g}})
		}
	}
	for _, n := range slices.Sorted(maps.Keys(got)) {
		if _, ok := want[n]; !ok {
			out = append(out, pending{diff: Diff{Replica: replica, Object: name, Kind: Extra, Part: what + " " + n, Got: got[n]}})
		}
	}
	return out
}
