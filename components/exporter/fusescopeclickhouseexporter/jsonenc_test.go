// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"encoding/json"
	"testing"
)

func TestAppendJSONStringMatchesEncodingJSON(t *testing.T) {
	tests := []string{
		"",
		"plain",
		`<a href="x">&amp;</a>`,
		"ção ✓",
		"  ",
		"\b\f\n\r\t\x00\x1f\x7f",
		`back\slash "quote"`,
		"invalid \xff\xfe utf8",
		"truncated \xe2\x82",
	}
	for _, s := range tests {
		want, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		if got := appendJSONString(nil, s); string(got) != string(want) {
			t.Errorf("appendJSONString(%q) = %q, want %q", s, got, want)
		}
	}
}
