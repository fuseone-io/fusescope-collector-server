// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozspanmapperprocessor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/fuseone-io/fusescope-collector-server/components/processor/signozspanmapperprocessor/internal/metadata"
)

var observed = filepath.Join("..", "..", "..", "docs", "spec", "observed", "signozspanmapper")

type observedCases struct {
	Accepted []struct {
		Name   string          `json:"name"`
		Config map[string]any  `json:"config"`
		Input  json.RawMessage `json:"input"`
	} `json:"accepted"`
	Startup map[string]map[string]any `json:"startup"`
}

func readCases(t *testing.T) observedCases {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(observed, "cases.json"))
	require.NoError(t, err)
	var c observedCases
	require.NoError(t, json.Unmarshal(b, &c))
	return c
}

func decode(cfg map[string]any) (*Config, error) {
	c := NewFactory().CreateDefaultConfig().(*Config)
	if err := confmap.NewFromStringMap(cfg).Unmarshal(c); err != nil {
		return nil, err
	}
	return c, confmap.Validate(c)
}

// canonical round-trips OTLP/JSON through pdata, so both sides compare in
// one encoding with attribute order kept.
func canonical(t *testing.T, td ptrace.Traces) string {
	t.Helper()
	b, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
	require.NoError(t, err)
	var out bytes.Buffer
	require.NoError(t, json.Indent(&out, b, "", " "))
	return out.String()
}

func unmarshal(t *testing.T, b []byte) ptrace.Traces {
	t.Helper()
	td, err := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(b)
	require.NoError(t, err)
	return td
}

// TestGolden replays every observed case and expects the reference's output
// byte for byte, attribute order included (spec §3).
func TestGolden(t *testing.T) {
	cases := readCases(t)
	b, err := os.ReadFile(filepath.Join(observed, "outputs.json"))
	require.NoError(t, err)
	var outputs map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(b, &outputs))
	require.Len(t, outputs, len(cases.Accepted))

	for _, c := range cases.Accepted {
		t.Run(c.Name, func(t *testing.T) {
			cfg, err := decode(c.Config)
			require.NoError(t, err)
			sink := new(consumertest.TracesSink)
			p, err := NewFactory().CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
			require.NoError(t, err)
			require.NoError(t, p.ConsumeTraces(context.Background(), unmarshal(t, c.Input)))
			require.Len(t, sink.AllTraces(), 1)
			want, ok := outputs[c.Name]
			require.True(t, ok)
			assert.Equal(t, canonical(t, unmarshal(t, want)), canonical(t, sink.AllTraces()[0]))
		})
	}
}

// TestGoldenStartup expects a config refused exactly when the reference
// refused to start with it (spec §2).
func TestGoldenStartup(t *testing.T) {
	cases := readCases(t)
	f, err := os.Open(filepath.Join(observed, "startup.jsonl"))
	require.NoError(t, err)
	defer f.Close()
	seen := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var line struct {
			Name   string `json:"name"`
			Result string `json:"result"`
		}
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &line))
		require.Contains(t, []string{"accepted", "refused"}, line.Result, line.Name)
		seen++
		t.Run(line.Name, func(t *testing.T) {
			conf, ok := cases.Startup[line.Name]
			require.True(t, ok)
			_, err := decode(conf)
			if line.Result == "refused" {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
	require.NoError(t, scanner.Err())
	assert.Equal(t, len(cases.Startup), seen)
}
