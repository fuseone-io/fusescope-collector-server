// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozllmpricingprocessor

import (
	"strings"
	"testing"

	"go.opentelemetry.io/collector/confmap"
)

// backendBlock is the block the backend pushes for one rule
// (pkg/types/llmpricingruletypes/testdata/collector_with_rule.yaml).
const backendBlock = `
attrs:
    model: gen_ai.request.model
    in: gen_ai.usage.input_tokens
    out: gen_ai.usage.output_tokens
    cache_read: gen_ai.usage.cache_read.input_tokens
    cache_write: gen_ai.usage.cache_creation.input_tokens
default_pricing:
    rules:
        - name: gpt-4o
          pattern:
            - gpt-4o*
          cache:
            mode: subtract
            read: 2.5
            write: 0
          in: 5
          out: 15
output_attrs:
    in: signoz.gen_ai.usage.input_tokens.cost
    out: signoz.gen_ai.usage.output_tokens.cost
    cache_read: signoz.gen_ai.usage.cache_read.input_tokens.cost
    cache_write: signoz.gen_ai.usage.cache_write.input_tokens.cost
    total: signoz.gen_ai.usage.tokens.cost
`

const rulesBlock = `default_pricing:
    rules:
        - name: gpt-4o
          pattern:
            - gpt-4o*
          cache:
            mode: subtract
            read: 2.5
            write: 0
          in: 5
          out: 15
`

func loadConfig(t *testing.T, yaml string) (*Config, error) {
	t.Helper()
	retrieved, err := confmap.NewRetrievedFromYAML([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	conf, err := retrieved.AsConf()
	if err != nil {
		t.Fatal(err)
	}
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	if err := conf.Unmarshal(cfg); err != nil {
		return nil, err
	}
	return cfg, confmap.Validate(cfg)
}

func TestBackendBlock(t *testing.T) {
	cfg, err := loadConfig(t, backendBlock)
	if err != nil {
		t.Fatal(err)
	}
	want := Rule{Name: "gpt-4o", Pattern: []string{"gpt-4o*"}, Cache: &Cache{Mode: "subtract", Read: 2.5}, In: 5, Out: 15}
	got := cfg.DefaultPricing.Rules
	if len(got) != 1 || got[0].Name != want.Name || got[0].Pattern[0] != want.Pattern[0] || *got[0].Cache != *want.Cache ||
		got[0].In != want.In || got[0].Out != want.Out {
		t.Fatalf("rules = %+v", got)
	}
	if cfg.Attrs.CacheWrite != "gen_ai.usage.cache_creation.input_tokens" || cfg.OutputAttrs.Total != "signoz.gen_ai.usage.tokens.cost" {
		t.Fatalf("attribute names = %+v %+v", cfg.Attrs, cfg.OutputAttrs)
	}
}

// Every case of spec §2.1, as edits of the backend's block.
func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name, from, to string
		wantErr        string
	}{
		{"backend block", "", "", ""},
		{"no rules", rulesBlock, "default_pricing:\n    rules: []\n", ""},
		{"no default_pricing", rulesBlock, "", ""},
		{"unknown top-level key", "default_pricing:", "unused:", "invalid keys: unused"},
		{"unknown rule key", "in: 5", "in: 5\n          unit: per_million_tokens", "invalid keys: unit"},
		{"string price", "in: 5", `in: "5"`, "expected type 'float64'"},
		{"empty model", "model: gen_ai.request.model", "model: ''", "attrs.model must not be empty"},
		{"empty total", "total: signoz.gen_ai.usage.tokens.cost", "total: ''", "output_attrs.total must not be empty"},
		{"empty other names", "in: gen_ai.usage.input_tokens", "in: ''", ""},
		{"empty pattern list", "pattern:\n            - gpt-4o*", "pattern: []", "at least one pattern"},
		{"empty pattern", "- gpt-4o*", "- gpt-4o*\n            - ''", "pattern[1] must not be empty"},
		{"bad glob", "- gpt-4o*", "- gpt-[", "not a valid glob"},
		{"negative in", "in: 5", "in: -1", "must not be negative"},
		{"negative out", "out: 15", "out: -1", "must not be negative"},
		{"negative cache read", "read: 2.5", "read: -1", "must not be negative"},
		{"negative cache write", "write: 0", "write: -0.5", "must not be negative"},
		{"unknown mode", "mode: subtract", "mode: foo", "cache.mode"},
		{"mode is case-sensitive", "mode: subtract", "mode: Subtract", "cache.mode"},
		{"additive", "mode: subtract", "mode: additive", ""},
		{"no mode", "mode: subtract", "mode: ''", ""},
		{"empty cache", "cache:\n            mode: subtract\n            read: 2.5\n            write: 0", "cache: {}", ""},
		{"no name", "- name: gpt-4o\n          pattern", "- pattern", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			yaml := backendBlock
			if tt.from != "" {
				if !strings.Contains(yaml, tt.from) {
					t.Fatalf("%q is not in the block", tt.from)
				}
				yaml = strings.Replace(yaml, tt.from, tt.to, 1)
			}
			_, err := loadConfig(t, yaml)
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultConfigIsInvalid(t *testing.T) {
	// A null block is the default configuration, which the reference refuses.
	if err := confmap.Validate(NewFactory().CreateDefaultConfig()); err == nil {
		t.Fatal("the default configuration validated")
	}
}
