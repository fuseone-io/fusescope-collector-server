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
		{name: "grok", operators: []map[string]any{{"type": "grok_parser", "parse_from": "body", "parse_to": "attributes", "pattern": "%{WORD:w} %{NUMBER:n:int}"}}},
		{name: "grok inline group", operators: []map[string]any{{"type": "grok_parser", "pattern": "(?<verb>[A-Z]+) %{GREEDYDATA:rest}"}}},
		{name: "grok without pattern", operators: []map[string]any{{"type": "grok_parser", "parse_from": "body"}}, wantErr: "missing required field 'pattern'"},
		// A pattern that does not compile is accepted and fails each record (spec §3.2).
		{name: "grok unknown pattern name", operators: []map[string]any{{"type": "grok_parser", "pattern": "%{NOSUCHPATTERN:x}"}}},
		{name: "grok invalid regex", operators: []map[string]any{{"type": "grok_parser", "pattern": "(?<x>[a-"}}},
		{name: "grok invalid type", operators: []map[string]any{{"type": "grok_parser", "pattern": "%{WORD:w:bool}"}}},
		{name: "grok unknown field", operators: []map[string]any{{"type": "grok_parser", "pattern": "%{WORD:w}", "regex": "x"}}, wantErr: "regex"},
		{name: "grok bad parse_from", operators: []map[string]any{{"type": "grok_parser", "pattern": "%{WORD:w}", "parse_from": "nowhere.a"}}, wantErr: "unrecognized prefix"},
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
		{name: "add without field", operators: []map[string]any{{"type": "add", "value": "x", "on_error": "drop"}}, wantErr: "'field'"},
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

// env() would read the collector's environment into exported logs, so a
// configuration naming it is refused in every expression position (spec
// §4.1), and the error names the operator, never the expression.
func TestEnvIsRefused(t *testing.T) {
	const secret = "FUSESCOPE_TEST_SECRET_NAME"
	route := func(expr string) []map[string]any {
		return []map[string]any{
			{"id": "router_signoz", "type": "router", "default": "noop", "routes": []any{map[string]any{"expr": expr, "output": "noop"}}},
			{"id": "noop", "type": "noop"},
		}
	}
	add := func(key, value string) []map[string]any {
		op := map[string]any{"id": "op1", "type": "add", "field": "attributes.a", "value": "1"}
		op[key] = value
		return []map[string]any{op}
	}
	tests := []struct {
		name      string
		operators []map[string]any
		wantID    string
	}{
		{"if", add("if", `env("`+secret+`") != nil`), `"op1"`},
		{"route expr", route(`env("` + secret + `") == "x"`), `"router_signoz"`},
		{"second route expr", []map[string]any{
			{"id": "r", "type": "router", "routes": []any{
				map[string]any{"expr": "true", "output": "noop"},
				map[string]any{"expr": `env("` + secret + `") == "x"`, "output": "noop"},
			}},
			{"id": "noop", "type": "noop"},
		}, `"r"`},
		{"EXPR value", add("value", `EXPR(env("`+secret+`"))`), `"op1"`},
		{"EXPR value inside a call", add("value", `EXPR(lower(env("`+secret+`")))`), `"op1"`},
		{"aliased with let", add("value", `EXPR(let f = env; f("`+secret+`"))`), `"op1"`},
		{"bare identifier", add("value", `EXPR(env)`), `"op1"`},
		{"the internal alias", add("if", `os_env_func("`+secret+`") != nil`), `"op1"`},
		{"grok if", []map[string]any{{"id": "g", "type": "grok_parser", "pattern": "%{WORD:w}", "if": `env("` + secret + `") == ""`}}, `"g"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := (&Config{Operators: tt.operators}).Validate()
			require.Error(t, err)
			assert.ErrorContains(t, err, "env()")
			assert.ErrorContains(t, err, tt.wantID)
			assert.NotContains(t, err.Error(), secret, "the error must not echo the expression")
		})
	}
}

// Fields and attributes named env are not the function.
func TestEnvAsAFieldName(t *testing.T) {
	for _, expr := range []string{`attributes.env == "prod"`, `attributes["env"] == "prod"`, `resource?.env != nil`, `body.env == "x"`, `"env" in attributes`} {
		t.Run(expr, func(t *testing.T) {
			err := (&Config{Operators: []map[string]any{{"type": "add", "field": "attributes.a", "value": "1", "if": expr}}}).Validate()
			assert.NoError(t, err)
		})
	}
}
