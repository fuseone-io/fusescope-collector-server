// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
	"go.yaml.in/yaml/v3"

	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/preflight"
)

// The build's preflight runs the real signozlogspipeline: a grok pipeline as
// the backend pushes it passes, and env() fails in every expression position
// with the operator named and the expression not echoed (opamp.md §6).
func TestManagedSignozLogsPipeline(t *testing.T) {
	if _, ok := buildManaged[component.MustNewType("signozlogspipeline")]; !ok {
		t.Fatal("signozlogspipeline is not a managed processor")
	}
	const name = "signozlogspipeline/pipeline_nginx"
	pipelines := map[string][]string{"logs": {name}}
	pipeline := func(filter, op string) string {
		return `operators:
  - {id: router_signoz, type: router, routes: [{expr: '` + filter + `', output: op1}], default: noop}
  - ` + op + `
  - {id: noop, type: noop}`
	}
	const grok = `{id: op1, type: grok_parser, if: 'body != nil', parse_from: body, parse_to: attributes, pattern: '%{WORD:method} %{URIPATH:path}', output: noop}`
	tests := []struct {
		name    string
		block   string
		wantErr string
	}{
		{"grok", pipeline(`body != nil`, grok), ""},
		{"grok unknown pattern", pipeline(`body != nil`, `{id: op1, type: grok_parser, parse_from: body, pattern: '%{NOSUCHPATTERN:x}', output: noop}`), `"op1"`},
		{"env in if", pipeline(`body != nil`, `{id: op1, type: add, field: attributes.a, value: x, if: 'env("SECRET_NAME") != nil', output: noop}`), `"op1"`},
		{"env in a route", pipeline(`env("SECRET_NAME") == "x"`, grok), `"router_signoz"`},
		{"env in a value", pipeline(`body != nil`, `{id: op1, type: add, field: attributes.a, value: 'EXPR(env("SECRET_NAME"))', output: noop}`), `"op1"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var raw map[string]any
			if err := yaml.Unmarshal([]byte(tt.block), &raw); err != nil {
				t.Fatal(err)
			}
			err := preflight.Check(context.Background(), zap.NewNop(), buildManaged,
				map[string]any{name: raw}, pipelines, 5*time.Second)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Check = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Check = %v, want an error naming %s", err, tt.wantErr)
			}
			if strings.Contains(err.Error(), "SECRET_NAME") {
				t.Errorf("the error echoes the expression: %v", err)
			}
		})
	}
}
