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

// Invalid UTF-8 is pinned to the reference's output, not to the toolchain's
// encoding/json, whose handling of it has changed between Go releases.
func TestAppendJSONStringReplacesInvalidUTF8PerByte(t *testing.T) {
	got := appendJSONString(nil, "bad \xff\xfe utf8 \xe2\x82")
	if want := `"bad \ufffd\ufffd utf8 \ufffd\ufffd"`; string(got) != want {
		t.Errorf("got %s, want %s (observed events of span 7574663875746638)", got, want)
	}
}
