// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package chstore

import (
	"slices"
	"testing"
)

func TestFormUnwrap(t *testing.T) {
	tests := []struct {
		form form
		in   string
		want string
		ok   bool
	}{
		{formExpression, "SELECT a + 1", "a + 1", true},
		{formExpression, "a + 1", "", false},
		{formExpression, "WITH x SELECT a", "", false},
		{formTTL, "ALTER TABLE t (MODIFY TTL  toDateTime(end) + toIntervalSecond(5))", "toDateTime(end) + toIntervalSecond(5)", true},
		{formTTL, "ALTER TABLE t (MODIFY TTL  toDateTime(end)", "", false},
		{formTTL, "toDateTime(end) + toIntervalSecond(5))", "", false},
		{formTTL, "ALTER TABLE u (MODIFY TTL x)", "", false},
		{formSettings, "ALTER TABLE t (MODIFY SETTING index_granularity = 8192)", "index_granularity = 8192", true},
		{formSettings, "ALTER TABLE t (MODIFY TTL x)", "", false},
		{formQuery, "SELECT * ORDER BY timestamp", "SELECT * ORDER BY timestamp", true},
	}
	for _, tt := range tests {
		got, err := tt.form.unwrap(tt.in)
		if tt.ok != (err == nil) || got != tt.want {
			t.Errorf("unwrap(%q) = %q, %v; want %q, ok %v", tt.in, got, err, tt.want, tt.ok)
		}
	}
}

func TestByIndex(t *testing.T) {
	got, err := byIndex([][]string{{"2", "b"}, {"1", "a"}, {"3", "c"}}, 3)
	if err != nil || !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("byIndex = %v, %v", got, err)
	}
	for _, rows := range [][][]string{
		{{"1", "a"}, {"2", "b"}},             // a row short
		{{"1", "a"}, {"1", "b"}, {"3", "c"}}, // an index twice
		{{"0", "a"}, {"1", "b"}, {"2", "c"}}, // out of range
		{{"1", "a"}, {"x", "b"}, {"3", "c"}}, // not a number
	} {
		if _, err := byIndex(rows, 3); err == nil {
			t.Errorf("byIndex(%v) accepted", rows)
		}
	}
}
