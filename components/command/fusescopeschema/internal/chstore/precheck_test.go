// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package chstore

import (
	"errors"
	"testing"
	"time"
)

func TestEvaluatePrecheck(t *testing.T) {
	ok := []replicaFacts{{"a", "25.12.5.44", 1_000}, {"b", "25.12.5.44", 1_500}}
	tests := []struct {
		name  string
		facts []replicaFacts
		fails bool
	}{
		{"supported, clocks close", ok, false},
		{"a newer version", []replicaFacts{ok[0], {"b", "26.1.1.1", 1_000}}, true},
		{"an older version", []replicaFacts{{"a", "25.8.1.1", 1_000}}, true},
		{"a prefix that is not the minor", []replicaFacts{{"a", "25.120.1.1", 1_000}}, true},
		{"clocks 30 s apart", []replicaFacts{ok[0], {"b", "25.12.5.44", 31_000}}, true},
		{"clocks just under 30 s apart", []replicaFacts{ok[0], {"b", "25.12.5.44", 30_999}}, false},
		{"no replica", nil, true},
	}
	for _, tt := range tests {
		err := evaluatePrecheck(tt.facts, SupportedVersions, 30*time.Second)
		if tt.fails != (err != nil) || err != nil && !errors.Is(err, ErrPrecheck) {
			t.Errorf("%s: %v", tt.name, err)
		}
	}
}
