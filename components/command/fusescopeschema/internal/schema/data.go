// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// DataCondition is a set of rows a migration owns (§5.2): in Table, the rows
// whose key is in Scope must be exactly Rows, each on one shard.
type DataCondition struct {
	// Table is a replicated local table, "<role>.<name>".
	Table string `json:"table"`
	// Key is the table's whole sorting key.
	Key []string `json:"key"`
	// Scope selects the rows the condition owns: each key column named
	// equals its value.
	Scope map[string]string `json:"scope"`
	Rows  []DataRow         `json:"rows"`
}

// DataRow is one owned row: its key, in Key's order, and a constraint on
// every other column.
type DataRow struct {
	Key    []string              `json:"key"`
	Values map[string]Constraint `json:"values"`
}

// Constraint is one column's value: exactly Equals, or a number greater than
// zero.
type Constraint struct {
	Equals   *string `json:"equals,omitempty"`
	Positive bool    `json:"positive,omitempty"`
}

func (c Constraint) String() string {
	if c.Equals != nil {
		return *c.Equals
	}
	return "> 0"
}

func (c Constraint) holds(got string) bool {
	if c.Equals != nil {
		return got == *c.Equals
	}
	f, err := strconv.ParseFloat(got, 64)
	return err == nil && f > 0
}

// LiveRow is a row as read: its key and every other column, as text.
type LiveRow struct {
	Key    []string
	Values map[string]string
}

// DataRead is a condition's table as read, in its scope: replication group
// (the replicas of one shard), then replica, then its rows sorted by key.
type DataRead map[string]map[string][]LiveRow

var mergedRe = regexp.MustCompile(`^SimpleAggregateFunction\((\w+), `)

// Merged returns, for each column outside the key, the function the engine
// merges it with, as read (§5.2).
func (c DataCondition) Merged(post Snapshot) (map[string]string, error) {
	o, ok := post[c.Table]
	if !ok || o.Kind != KindTable {
		return nil, fmt.Errorf("data condition on %s: no such table in the post-conditions", c.Table)
	}
	out := map[string]string{}
	for _, col := range o.Columns {
		if slices.Contains(c.Key, col.Name) {
			continue
		}
		m := mergedRe.FindStringSubmatch(col.Type)
		if m == nil {
			return nil, fmt.Errorf("data condition on %s: column %s (%s) has no merge function", c.Table, col.Name, col.Type)
		}
		out[col.Name] = m[1]
	}
	return out, nil
}

// Validate checks the condition against the structure it is read from.
func (c DataCondition) Validate(post Snapshot) error {
	merged, err := c.Merged(post)
	if err != nil {
		return err
	}
	if got := strings.Join(c.Key, ", "); got != post[c.Table].SortingKey {
		return fmt.Errorf("data condition on %s: key (%s) is not the sorting key (%s)", c.Table, got, post[c.Table].SortingKey)
	}
	for col := range c.Scope {
		if !slices.Contains(c.Key, col) {
			return fmt.Errorf("data condition on %s: scope column %s is not in the key", c.Table, col)
		}
	}
	if len(c.Rows) == 0 {
		return fmt.Errorf("data condition on %s owns no rows", c.Table)
	}
	seen := map[string]bool{}
	for _, r := range c.Rows {
		if len(r.Key) != len(c.Key) {
			return fmt.Errorf("data condition on %s: row %v has %d key values, want %d", c.Table, r.Key, len(r.Key), len(c.Key))
		}
		if !c.inScope(r.Key) {
			return fmt.Errorf("data condition on %s: row %v is outside the scope", c.Table, r.Key)
		}
		k := keyText(r.Key)
		if seen[k] {
			return fmt.Errorf("data condition on %s: row %s twice", c.Table, k)
		}
		seen[k] = true
		if !slices.Equal(slices.Sorted(maps.Keys(r.Values)), slices.Sorted(maps.Keys(merged))) {
			return fmt.Errorf("data condition on %s: row %s constrains %v, want every column outside the key: %v",
				c.Table, k, slices.Sorted(maps.Keys(r.Values)), slices.Sorted(maps.Keys(merged)))
		}
		for col, v := range r.Values {
			if (v.Equals != nil) == v.Positive {
				return fmt.Errorf("data condition on %s: row %s column %s needs exactly one of equals and positive", c.Table, k, col)
			}
		}
	}
	return nil
}

func (c DataCondition) inScope(key []string) bool {
	for col, want := range c.Scope {
		if key[slices.Index(c.Key, col)] != want {
			return false
		}
	}
	return true
}

func keyText(key []string) string { return "(" + strings.Join(key, ", ") + ")" }

// Agree returns each group's rows when every replica of the group shows the
// same ones, and else the replicas that differ from the group's first,
// sorted.
func (d DataRead) Agree() (map[string][]LiveRow, []string) {
	groups := map[string][]LiveRow{}
	var lagging []string
	for _, g := range slices.Sorted(maps.Keys(d)) {
		replicas := slices.Sorted(maps.Keys(d[g]))
		if len(replicas) == 0 {
			groups[g] = nil
			continue
		}
		first := d[g][replicas[0]]
		for _, r := range replicas[1:] {
			if !reflect.DeepEqual(normal(d[g][r]), normal(first)) {
				lagging = append(lagging, r)
			}
		}
		groups[g] = first
	}
	slices.Sort(lagging)
	return groups, lagging
}

func normal(rows []LiveRow) []LiveRow {
	if len(rows) == 0 {
		return nil
	}
	return rows
}

// Check unites the groups and compares them with the condition: the owned
// rows not there, and every row that is extra, on more than one shard, or
// breaks a constraint. The condition holds when both are empty, and admits
// the rows when the second is.
func (c DataCondition) Check(groups map[string][]LiveRow) (missing []DataRow, problems []Diff) {
	type found struct {
		row    LiveRow
		groups []string
	}
	live := map[string]*found{}
	var order []string
	for _, g := range slices.Sorted(maps.Keys(groups)) {
		for _, r := range groups[g] {
			k := keyText(r.Key)
			if live[k] == nil {
				live[k] = &found{row: r}
				order = append(order, k)
			}
			live[k].groups = append(live[k].groups, g)
		}
	}
	want := map[string]DataRow{}
	for _, r := range c.Rows {
		k := keyText(r.Key)
		want[k] = r
		if live[k] == nil {
			missing = append(missing, r)
		}
	}
	for _, k := range order {
		f := live[k]
		object := c.Table + " row " + k
		where := strings.Join(f.groups, ", ")
		w, owned := want[k]
		switch {
		case !owned:
			problems = append(problems, Diff{Replica: where, Object: object, Kind: Extra, Got: valuesText(f.row.Values)})
			continue
		case len(f.groups) > 1:
			problems = append(problems, Diff{Replica: "cluster", Object: object, Kind: Changed, Part: "shards",
				Want: "one shard", Got: where})
		}
		for _, col := range slices.Sorted(maps.Keys(w.Values)) {
			if got := f.row.Values[col]; !w.Values[col].holds(got) {
				problems = append(problems, Diff{Replica: where, Object: object, Kind: Changed, Part: col, Want: w.Values[col].String(), Got: got})
			}
		}
	}
	return missing, problems
}

func valuesText(v map[string]string) string {
	parts := make([]string, 0, len(v))
	for _, k := range slices.Sorted(maps.Keys(v)) {
		parts = append(parts, k+" = "+v[k])
	}
	return strings.Join(parts, ", ")
}
