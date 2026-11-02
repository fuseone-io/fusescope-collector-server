// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package grok

import (
	"os"
	"testing"
)

func TestPatternFilesMatchDirectory(t *testing.T) {
	entries, err := os.ReadDir("patterns")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("no pattern files")
	}
	for _, e := range entries {
		want, err := os.ReadFile("patterns/" + e.Name())
		if err != nil {
			t.Fatal(err)
		}
		got, err := PatternFiles.ReadFile("patterns/" + e.Name())
		if err != nil {
			t.Fatalf("%s is not embedded: %v", e.Name(), err)
		}
		if string(got) != string(want) {
			t.Errorf("%s differs from the embedded copy", e.Name())
		}
	}
}
