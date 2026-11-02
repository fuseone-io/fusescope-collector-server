// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
	"go.yaml.in/yaml/v3"

	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/preflight"
)

// buildManaged is the build's map, kept before lifecycle tests replace it.
var buildManaged = managedFactories

// The build's preflight knows signozspanmapper: it passes the block the
// backend pushes and fails one the processor refuses.
func TestManagedSignozSpanMapper(t *testing.T) {
	if _, ok := buildManaged[component.MustNewType("signozspanmapper")]; !ok {
		t.Fatal("signozspanmapper is not a managed processor")
	}
	pipelines := map[string][]string{"traces": {"signozspanmapper"}}
	for block, wantErr := range map[string]bool{
		"groups: [{id: llm, exists_any: {attributes: [model]}, attributes: [{target: gen_ai.request.model, context: attribute, sources: [{key: llm.model}]}]}]": false,
		"groups: []": false,
		"groups: [{id: llm, exists_any: {attributes: [model]}, attributes: [{target: t, context: span, sources: [{key: k}]}]}]": true,
	} {
		var raw map[string]any
		if err := yaml.Unmarshal([]byte(block), &raw); err != nil {
			t.Fatal(err)
		}
		err := preflight.Check(context.Background(), zap.NewNop(), buildManaged,
			map[string]any{"signozspanmapper": raw}, pipelines, 5*time.Second)
		if (err != nil) != wantErr {
			t.Errorf("%s: err = %v, want error %v", block, err, wantErr)
		}
	}
}
