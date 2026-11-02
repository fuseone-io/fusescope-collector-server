// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozlogspipelineprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
)

func TestConfigUnmarshal(t *testing.T) {
	tests := []struct {
		name    string
		raw     map[string]any
		wantErr string
	}{
		{
			name: "backend shape",
			raw: map[string]any{"operators": []any{
				map[string]any{"id": "router_signoz", "type": "router", "default": "noop",
					"routes": []any{map[string]any{"expr": "body != nil", "output": "op1"}}},
				map[string]any{"id": "op1", "type": "add", "field": "attributes.a", "value": "1"},
				map[string]any{"id": "noop", "type": "noop"},
			}},
		},
		{name: "unknown top-level key", raw: map[string]any{"operators": []any{}, "extra": 1}, wantErr: "extra"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewFactory().CreateDefaultConfig().(*Config)
			err := confmap.NewFromStringMap(tt.raw).Unmarshal(cfg)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NoError(t, cfg.Validate())
		})
	}
}

func TestConfigValidate(t *testing.T) {
	add := func(extra map[string]any) map[string]any {
		op := map[string]any{"id": "a", "type": "add", "field": "attributes.a", "value": "1"}
		for k, v := range extra {
			op[k] = v
		}
		return op
	}
	tests := []struct {
		name      string
		operators []map[string]any
		wantErr   string
	}{
		{name: "no operators", wantErr: "no operators"},
		{name: "empty operators", operators: []map[string]any{}, wantErr: "no operators"},
		{name: "noop only", operators: []map[string]any{{"type": "noop"}}},
		{name: "id defaults to type", operators: []map[string]any{{"type": "noop"}, {"type": "add", "field": "attributes.a", "value": 1}}},
		{name: "missing type", operators: []map[string]any{{"id": "x"}}, wantErr: "missing required field 'type'"},
		{name: "unsupported type", operators: []map[string]any{{"type": "key_value_parser"}}, wantErr: `unsupported type "key_value_parser"`},
		{name: "retain is unsupported", operators: []map[string]any{{"type": "retain", "fields": []any{"attributes.a"}}}, wantErr: "unsupported type"},
		{name: "grok is not in this build", operators: []map[string]any{{"type": "grok_parser", "pattern": "%{WORD:w}"}}, wantErr: "not supported by this build"},
		{name: "unknown operator field", operators: []map[string]any{add(map[string]any{"foo": "bar"})}, wantErr: "foo"},
		{name: "unknown nested field", operators: []map[string]any{{"type": "trace_parser", "trace_id": map[string]any{"parse_from": "attributes.t", "x": 1}}}, wantErr: "x"},
		{name: "duplicate id", operators: []map[string]any{add(nil), add(nil)}, wantErr: "used twice"},
		{name: "unknown output", operators: []map[string]any{add(map[string]any{"output": "missing"})}, wantErr: `output "missing"`},
		{name: "cycle", operators: []map[string]any{add(map[string]any{"output": "b"}), add(map[string]any{"id": "b", "output": "a"})}, wantErr: "cycle"},
		{name: "self loop", operators: []map[string]any{add(map[string]any{"output": "a"})}, wantErr: "cycle"},
		{name: "bad if", operators: []map[string]any{add(map[string]any{"if": "body =="})}, wantErr: "compile if"},
		{name: "if not a string", operators: []map[string]any{add(map[string]any{"if": true})}, wantErr: "if must be a string"},
		{name: "bad on_error", operators: []map[string]any{add(map[string]any{"on_error": "bogus"})}, wantErr: "on_error"},
		{name: "bad field prefix", operators: []map[string]any{add(map[string]any{"field": "nowhere.a"})}, wantErr: "unrecognized prefix"},
		{name: "add without value", operators: []map[string]any{{"type": "add", "field": "attributes.a"}}, wantErr: "value"},
		{name: "add bad expression", operators: []map[string]any{add(map[string]any{"value": "EXPR(1 +)"})}, wantErr: "compile value expression"},
		{name: "regex without named group", operators: []map[string]any{{"type": "regex_parser", "regex": "^a"}}, wantErr: "named capture"},
		{name: "epoch layout", operators: []map[string]any{{"type": "time_parser", "parse_from": "attributes.t", "layout_type": "epoch", "layout": "x"}}, wantErr: "layout"},
		{name: "json depth type", operators: []map[string]any{{"type": "json_parser", "max_flattening_depth": "deep"}}, wantErr: "max_flattening_depth"},
		{name: "copy from attributes root", operators: []map[string]any{{"type": "copy", "from": "attributes", "to": "resource.a"}}, wantErr: "without subfield"},
		{
			name: "router",
			operators: []map[string]any{
				{"id": "r", "type": "router", "routes": []any{map[string]any{"expr": "true", "output": "a"}}, "default": "a"},
				add(nil),
			},
		},
		{name: "router bad expr", operators: []map[string]any{{"id": "r", "type": "router", "routes": []any{map[string]any{"expr": "==", "output": "r"}}}}, wantErr: "compile route"},
		{name: "router unknown default", operators: []map[string]any{{"id": "r", "type": "router", "default": "x"}}, wantErr: `output "x"`},
		{name: "router with if", operators: []map[string]any{{"id": "r", "type": "router", "if": "true"}}, wantErr: "if"},
		{name: "router cycle", operators: []map[string]any{{"id": "r", "type": "router", "routes": []any{map[string]any{"expr": "true", "output": "r"}}}}, wantErr: "cycle"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := (&Config{Operators: tt.operators}).Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}
