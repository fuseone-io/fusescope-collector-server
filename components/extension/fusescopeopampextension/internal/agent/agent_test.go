// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"strings"
	"testing"

	"go.uber.org/zap"

	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/state"
)

type pushRecord = state.Push

const base = `
receivers: {nop: {}}
exporters: {nop: {endpoint: local}}
extensions: {fusescope_opamp: {}}
service:
  extensions: [fusescope_opamp]
  pipelines:
    logs: {receivers: [nop], processors: [], exporters: [nop]}
`

const push = `
receivers: {nop: {}}
exporters: {nop: {endpoint: local}}
extensions: {fusescope_opamp: {}}
processors: {signozlogspipeline/pipeline_a: {}}
service:
  extensions: [fusescope_opamp]
  pipelines:
    logs: {receivers: [nop], processors: [signozlogspipeline/pipeline_a], exporters: [nop]}
`

func mustParse(t *testing.T, s string) map[string]any {
	t.Helper()
	m, err := parseYAML([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// A cached push is merged again with the current base at start; one the
// base no longer accepts is dropped and the base runs alone.
func TestCacheRevalidatedAgainstNewBase(t *testing.T) {
	dir := t.TempDir()
	a, err := New(zap.NewNop(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Start(mustParse(t, base)); err != nil {
		t.Fatal(err)
	}
	// Record an applied push without a build of its processor.
	if err := a.store.Begin(&pushRecord{Hash: []byte("h1"), Revision: "r1", Config: []byte(push)}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.store.Confirm("r1"); err != nil {
		t.Fatal(err)
	}

	same, _ := New(zap.NewNop(), dir, nil)
	if _, err := same.Start(mustParse(t, base)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fmtConfig(same.Effective()), "pipeline_a") {
		t.Error("the cached push was not merged on the unchanged base")
	}
	same.Confirm("r1") // that start reached Ready

	upgraded := strings.Replace(base, "endpoint: local", "endpoint: upgraded", 1)
	next, _ := New(zap.NewNop(), dir, nil)
	if _, err := next.Start(mustParse(t, upgraded)); err != nil {
		t.Fatal(err)
	}
	eff := fmtConfig(next.Effective())
	if strings.Contains(eff, "pipeline_a") || !strings.Contains(eff, "upgraded") {
		t.Errorf("a push the new base rejects must not revert it; effective:\n%s", eff)
	}
}

func fmtConfig(m map[string]any) string {
	b, _ := MarshalYAML(m)
	return string(b)
}
