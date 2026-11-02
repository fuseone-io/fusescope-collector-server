// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozspanmapperprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.yaml.in/yaml/v3"
)

// load decodes a processor block as the collector does: into the default
// config, unknown keys refused, then Validate.
func load(t *testing.T, block string) (*Config, error) {
	t.Helper()
	var raw map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(block), &raw))
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	if err := confmap.NewFromStringMap(raw).Unmarshal(cfg); err != nil {
		return nil, err
	}
	return cfg, confmap.Validate(cfg)
}

// The block the backend pushes (fusescope pkg/types/spantypes/testdata/collector_with_groups.yaml).
const backendBlock = `
groups:
  - id: llm
    exists_any:
      attributes: [model]
      resource: [service.name]
    attributes:
      - target: gen_ai.request.model
        context: resource
        sources:
          - key: gen_ai.llm.model
          - key: llm.model
          - key: resource.service.name
      - target: gen_ai.request.input
        context: attribute
        sources:
          - key: gen_ai.input
            action: move
          - key: llm.input
            action: move
  - id: agent
    exists_any:
      attributes: [agent.]
    attributes:
      - target: gen_ai.agent.name
        context: attribute
        sources:
          - key: agent.name
`

func TestConfigBackendBlock(t *testing.T) {
	cfg, err := load(t, backendBlock)
	require.NoError(t, err)
	require.Len(t, cfg.Groups, 2)
	g := cfg.Groups[0]
	assert.Equal(t, "llm", g.ID)
	assert.Equal(t, []string{"model"}, g.ExistsAny.Attributes)
	assert.Equal(t, []string{"service.name"}, g.ExistsAny.Resource)
	assert.Equal(t, Rule{Target: "gen_ai.request.model", Context: ContextResource, Sources: []Source{
		{Key: "gen_ai.llm.model"}, {Key: "llm.model"}, {Key: "resource.service.name"},
	}}, g.Attributes[0])
	assert.Equal(t, ActionMove, g.Attributes[1].Sources[0].Action)
}

// Mirrors the startup configs of docs/spec/observed/signozspanmapper (spec §2).
func TestConfigValidate(t *testing.T) {
	rule := func(r string) string {
		return "groups: [{id: g, exists_any: {attributes: [src]}, attributes: [" + r + "]}]"
	}
	tests := []struct {
		name    string
		block   string
		wantErr string
	}{
		{"empty", "", ""},
		{"groups empty", "groups: []", ""},
		{"groups null", "groups:", ""},
		{"valid", rule("{target: t, context: attribute, sources: [{key: src.a}]}"), ""},
		{"unknown top key", "extra: 1", "extra"},
		{"unknown group key", "groups: [{id: g, extra: 1, exists_any: {attributes: [src]}}]", "extra"},
		{"unknown exists_any key", "groups: [{id: g, exists_any: {attributes: [src], extra: [x]}}]", "extra"},
		{"unknown rule key", rule("{target: t, extra: 1, sources: [{key: k}]}"), "extra"},
		{"unknown source key", rule("{target: t, sources: [{key: k, extra: 1}]}"), "extra"},
		{"groups not a list", "groups: {id: g}", "groups"},
		{"id missing, empty, repeated", "groups: [{exists_any: {attributes: [a]}}, {id: '', exists_any: {attributes: [a]}}, {id: '', exists_any: {resource: [a]}}]", ""},
		{"exists_any missing", "groups: [{id: g}]", "exists_any"},
		{"exists_any empty lists", "groups: [{id: g, exists_any: {attributes: [], resource: []}}]", "exists_any"},
		{"exists_any empty substring", "groups: [{id: g, exists_any: {attributes: [src, '']}}]", "exists_any.attributes"},
		{"exists_any empty resource substring", "groups: [{id: g, exists_any: {resource: ['']}}]", "exists_any.resource"},
		{"rules missing", "groups: [{id: g, exists_any: {attributes: [src]}}]", ""},
		{"target missing", rule("{sources: [{key: k}]}"), "target"},
		{"target repeated", rule("{target: t, sources: [{key: a}]}, {target: t, sources: [{key: b}]}"), ""},
		{"sources missing", rule("{target: t}"), "sources"},
		{"source key missing", rule("{target: t, sources: [{action: move}]}"), "key"},
		{"source key resource prefix only", rule("{target: t, sources: [{key: resource.}]}"), ""},
		{"context empty", rule("{target: t, context: '', sources: [{key: k}]}"), ""},
		{"context resource", rule("{target: t, context: resource, sources: [{key: k}]}"), ""},
		{"context span", rule("{target: t, context: span, sources: [{key: k}]}"), `"span"`},
		{"context upper case", rule("{target: t, context: Resource, sources: [{key: k}]}"), `"Resource"`},
		{"action copy", rule("{target: t, sources: [{key: k, action: copy}]}"), ""},
		{"action empty", rule("{target: t, sources: [{key: k, action: ''}]}"), ""},
		{"action delete", rule("{target: t, sources: [{key: k, action: delete}]}"), `"delete"`},
		{"action upper case", rule("{target: t, sources: [{key: k, action: MOVE}]}"), `"MOVE"`},
		{"error names the place", "groups: [{id: a, exists_any: {attributes: [x]}}, {id: b, exists_any: {attributes: [x]}, attributes: [{target: t, sources: [{key: ''}]}]}]",
			`groups[1] (id "b"): attributes[0] (target "t"): sources[0]: key must not be empty`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := load(t, tt.block)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
