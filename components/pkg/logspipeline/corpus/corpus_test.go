// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package corpus

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Each call decodes anew: what a caller changes does not reach the next one.
func TestFreshData(t *testing.T) {
	a, err := Observed()
	require.NoError(t, err)
	require.NotEmpty(t, a.Cases)
	name := a.Cases[0].Name
	a.Cases[0].Name = "changed"
	a.Cases[0].ProcessorNames[0] = "changed"
	for k := range a.Cases[0].Processors {
		a.Cases[0].Processors[k][0] = 'X'
	}
	a.Inputs["text"][0][0] = 'X'
	delete(a.Inputs, "json")

	b, err := Observed()
	require.NoError(t, err)
	assert.Equal(t, name, b.Cases[0].Name)
	assert.NotEqual(t, "changed", b.Cases[0].ProcessorNames[0])
	for _, raw := range b.Cases[0].Processors {
		assert.Equal(t, byte('{'), raw[0])
	}
	assert.Equal(t, byte('{'), b.Inputs["text"][0][0])
	assert.Contains(t, b.Inputs, "json")

	m, err := LibraryMessages()
	require.NoError(t, err)
	for k := range m.Cases {
		m.Cases[k] = nil
	}
	m2, err := LibraryMessages()
	require.NoError(t, err)
	assert.NotEqual(t, m, m2)

	f, err := File("observed/run.yaml")
	require.NoError(t, err)
	f[0] = 'X'
	f2, err := File("observed/run.yaml")
	require.NoError(t, err)
	assert.NotEqual(t, f[0], f2[0])
}

// Numbers in processor configurations survive as written: decoded with
// UseNumber they are json.Number, not float64.
func TestNumbersKept(t *testing.T) {
	c, err := Observed()
	require.NoError(t, err)
	found := false
	for _, k := range c.Cases {
		for _, raw := range k.Processors {
			d := json.NewDecoder(bytes.NewReader(raw))
			d.UseNumber()
			var v any
			require.NoError(t, d.Decode(&v))
			if hasNumber(v) {
				found = true
			}
		}
	}
	assert.True(t, found, "some processor configuration holds a number")
}

func hasNumber(v any) bool {
	switch v := v.(type) {
	case json.Number:
		return true
	case map[string]any:
		for _, item := range v {
			if hasNumber(item) {
				return true
			}
		}
	case []any:
		for _, item := range v {
			if hasNumber(item) {
				return true
			}
		}
	}
	return false
}

// The observed files are what the observation recorded, and run.yaml counts
// only them: nothing the library derived is counted as observed.
func TestObservationCounts(t *testing.T) {
	c, err := Observed()
	require.NoError(t, err)
	startup, err := ObservedStartup()
	require.NoError(t, err)
	refused := 0
	for _, r := range startup {
		if !r.Starts {
			refused++
		}
	}
	run, err := File("observed/run.yaml")
	require.NoError(t, err)
	for _, line := range []string{
		"reference_image: ghcr.io/fuseone-io/fusescope-otel-collector:0.145.0",
		"cases: " + itoa(len(c.Cases)),
		"startup_checks: " + itoa(len(startup)),
		"refused_at_startup: " + itoa(refused),
	} {
		assert.Contains(t, strings.Split(strings.TrimSpace(string(run)), "\n"), line)
	}
	assert.Equal(t, 223, len(c.Cases))
	assert.Equal(t, 88, len(startup))
	assert.Equal(t, 24, refused)

	outputs, err := ObservedOutputs()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, k := range append(c.Cases, c.Startup...) {
		names[k.Name] = true
	}
	for name := range outputs {
		assert.True(t, names[name], "output %s has a case", name)
	}
	for _, r := range startup {
		assert.True(t, names[r.Name], "startup result %s has a case", r.Name)
	}
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}
