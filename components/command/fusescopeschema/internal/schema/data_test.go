// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"slices"
	"strings"
	"testing"
)

func ptr(s string) *string { return &s }

// cond owns two base rows: a v1 row whose release_time must be positive and
// a v0 row whose release_time must be 0.
var cond = DataCondition{
	Table: "metadata.t",
	Key:   []string{"signal", "field_name", "version"},
	Scope: map[string]string{"field_name": "__all__"},
	Rows: []DataRow{
		{Key: []string{"logs", "__all__", "1"}, Values: map[string]Constraint{"release_time": {Positive: true}}},
		{Key: []string{"logs", "__all__", "0"}, Values: map[string]Constraint{"release_time": {Equals: ptr("0")}}},
	},
}

func live(key string, release string) LiveRow {
	return LiveRow{Key: strings.Split(key, ","), Values: map[string]string{"release_time": release}}
}

func TestDataConditionCheck(t *testing.T) {
	v1, v0 := live("logs,__all__,1", "1794010640653742600"), live("logs,__all__,0", "0")
	cases := map[string]struct {
		groups   map[string][]LiveRow
		missing  []string // keys
		problems []string // Diff strings, each containing this text
	}{
		"both rows":                  {groups: map[string][]LiveRow{"s1": {v1}, "s2": {v0}}},
		"none":                       {groups: map[string][]LiveRow{"s1": nil}, missing: []string{"logs,__all__,1", "logs,__all__,0"}},
		"one":                        {groups: map[string][]LiveRow{"s1": {v0}}, missing: []string{"logs,__all__,1"}},
		"an extra base row":          {groups: map[string][]LiveRow{"s1": {v1, v0, live("logs,__all__,2", "5")}}, problems: []string{"(logs, __all__, 2) is extra"}},
		"a v0 row not at 0":          {groups: map[string][]LiveRow{"s1": {v1, live("logs,__all__,0", "7")}}, problems: []string{"release_time differs: want 0, got 7"}},
		"a v1 row at 0":              {groups: map[string][]LiveRow{"s1": {live("logs,__all__,1", "0"), v0}}, problems: []string{"release_time differs: want > 0, got 0"}},
		"a v1 row not a number":      {groups: map[string][]LiveRow{"s1": {live("logs,__all__,1", "x"), v0}}, problems: []string{"want > 0, got x"}},
		"a key on two shards":        {groups: map[string][]LiveRow{"s1": {v1, v0}, "s2": {v1}}, problems: []string{"(logs, __all__, 1) shards differs: want one shard, got s1, s2"}},
		"a missing row and an extra": {groups: map[string][]LiveRow{"s1": {v1, live("logs,__all__,9", "5")}}, missing: []string{"logs,__all__,0"}, problems: []string{"is extra"}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			missing, problems := cond.Check(c.groups)
			var keys []string
			for _, m := range missing {
				keys = append(keys, strings.Join(m.Key, ","))
			}
			if !slices.Equal(keys, c.missing) {
				t.Errorf("missing = %v, want %v", keys, c.missing)
			}
			if len(problems) != len(c.problems) {
				t.Fatalf("problems = %v, want %d", problems, len(c.problems))
			}
			for i, p := range problems {
				if !strings.Contains(p.String(), c.problems[i]) {
					t.Errorf("problem %q, want it to contain %q", p.String(), c.problems[i])
				}
			}
		})
	}
}

func TestDataConditionAgree(t *testing.T) {
	v1, v0 := live("logs,__all__,1", "5"), live("logs,__all__,0", "0")
	read := DataRead{
		"s1": {"r1": {v1, v0}, "r2": {v1, v0}},
		"s2": {"r3": nil, "r4": nil},
	}
	groups, lagging := read.Agree()
	if len(lagging) != 0 || len(groups["s1"]) != 2 || len(groups["s2"]) != 0 {
		t.Fatalf("Agree = %v, %v", groups, lagging)
	}
	read["s1"]["r2"] = []LiveRow{v1}
	read["s2"]["r4"] = []LiveRow{live("logs,__all__,0", "1")}
	if _, lagging = read.Agree(); !slices.Equal(lagging, []string{"r2", "r4"}) {
		t.Fatalf("lagging = %v, want r2 and r4", lagging)
	}
}

func TestDataConditionValidate(t *testing.T) {
	post := Snapshot{"metadata.t": {Kind: KindTable, SortingKey: "signal, field_name, version", Columns: []Column{
		{Name: "signal", Type: "String"}, {Name: "field_name", Type: "String"}, {Name: "version", Type: "UInt32"},
		{Name: "release_time", Type: "SimpleAggregateFunction(min, Float64)"},
	}}}
	if err := cond.Validate(post); err != nil {
		t.Fatalf("Validate = %v", err)
	}
	bad := map[string]func(*DataCondition){
		"another table":       func(c *DataCondition) { c.Table = "metadata.u" },
		"part of the key":     func(c *DataCondition) { c.Key = c.Key[:2] },
		"a scope off the key": func(c *DataCondition) { c.Scope = map[string]string{"release_time": "0"} },
		"a row off the scope": func(c *DataCondition) { c.Rows = []DataRow{{Key: []string{"logs", "x", "1"}}} },
		"a short row key":     func(c *DataCondition) { c.Rows = []DataRow{{Key: []string{"logs", "__all__"}}} },
		"a duplicate row":     func(c *DataCondition) { c.Rows = []DataRow{c.Rows[0], c.Rows[0]} },
		"an unknown column": func(c *DataCondition) {
			c.Rows = []DataRow{{Key: c.Rows[0].Key, Values: map[string]Constraint{"x": {Positive: true}}}}
		},
		"an empty constraint": func(c *DataCondition) {
			c.Rows = []DataRow{{Key: c.Rows[0].Key, Values: map[string]Constraint{"release_time": {}}}}
		},
		"a constraint on keys": func(c *DataCondition) {
			c.Rows = []DataRow{{Key: c.Rows[0].Key, Values: map[string]Constraint{"signal": {Positive: true}}}}
		},
		"no rows": func(c *DataCondition) { c.Rows = nil },
	}
	for name, change := range bad {
		c := cond
		change(&c)
		if err := c.Validate(post); err == nil {
			t.Errorf("%s: Validate accepted it", name)
		}
	}
	plain := Snapshot{"metadata.t": post["metadata.t"]}
	o := plain["metadata.t"]
	o.Columns = slices.Clone(o.Columns)
	o.Columns[3].Type = "Float64"
	plain["metadata.t"] = o
	if err := cond.Validate(plain); err == nil {
		t.Error("Validate accepted a column with no merge function")
	}
}
