// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/preflight"
)

// builtManaged is this build's allow-list, taken before other tests replace it.
var builtManaged = managedFactories

// The block the backend pushes for LLM pricing passes preflight in this build
// (pkg/types/llmpricingruletypes/testdata/collector_with_rule.yaml), and a
// negative price, which the backend never sends, fails it.
func TestLLMPricingPreflight(t *testing.T) {
	block := func(in float64) map[string]any {
		return map[string]any{
			"attrs": map[string]any{
				"model": "gen_ai.request.model", "in": "gen_ai.usage.input_tokens", "out": "gen_ai.usage.output_tokens",
				"cache_read": "gen_ai.usage.cache_read.input_tokens", "cache_write": "gen_ai.usage.cache_creation.input_tokens",
			},
			"default_pricing": map[string]any{"rules": []any{map[string]any{
				"name": "gpt-4o", "pattern": []any{"gpt-4o*"},
				"cache": map[string]any{"mode": "subtract", "read": 2.5, "write": 0},
				"in":    in, "out": 15,
			}}},
			"output_attrs": map[string]any{
				"in": "signoz.gen_ai.usage.input_tokens.cost", "out": "signoz.gen_ai.usage.output_tokens.cost",
				"cache_read":  "signoz.gen_ai.usage.cache_read.input_tokens.cost",
				"cache_write": "signoz.gen_ai.usage.cache_write.input_tokens.cost",
				"total":       "signoz.gen_ai.usage.tokens.cost",
			},
		}
	}
	pipelines := map[string][]string{"traces": {"memory_limiter", "signozllmpricing", "batch"}}
	check := func(in float64) error {
		return preflight.Check(context.Background(), zap.NewNop(), builtManaged,
			map[string]any{"signozllmpricing": block(in)}, pipelines, 10*time.Second)
	}
	if err := check(5); err != nil {
		t.Fatalf("the backend's block fails preflight: %v", err)
	}
	if err := check(-1); err == nil {
		t.Fatal("a negative price passed preflight")
	}
}
