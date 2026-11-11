// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package logspipeline

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
)

// Every configuration New and Validate refuse is a *ConfigError that tells
// the stage it failed at and the operator, so a caller such as the backend's
// preview can classify it; its message is the one the processor always gave.
func TestConfigError(t *testing.T) {
	add := func(fields map[string]any) map[string]any {
		op := map[string]any{"type": "add", "id": "a", "field": "attributes.x", "value": "v"}
		for k, v := range fields {
			op[k] = v
		}
		return op
	}
	tests := []struct {
		name      string
		operators []map[string]any
		stage     Stage
		id, typ   string
		message   string // exact, or a prefix when prefix is set
		prefix    bool
	}{
		{"none", nil, StageOperators, "", "", "no operators were configured for signozlogspipeline processor", false},
		{"missing type", []map[string]any{{"id": "a"}}, StageOperators, "", "", "operators[0]: missing required field 'type'", false},
		{"unsupported type", []map[string]any{{"type": "retain", "fields": []any{"a"}}}, StageOperators, "", "", `operators[0]: unsupported type "retain"`, false},
		{"id not a string", []map[string]any{add(map[string]any{"id": 1})}, StageOperators, "", "", "operators[0]: id must be a string", false},
		{"id used twice", []map[string]any{add(nil), add(nil)}, StageOperators, "", "", `operators[1]: operator id "a" is used twice`, false},
		{"unknown field", []map[string]any{add(map[string]any{"nope": 1})}, StageDecode, "a", "add", `operator "a" (add): `, true},
		{"if does not compile", []map[string]any{add(map[string]any{"if": "body =="})}, StageCompile, "a", "add", `operator "a" (add): compile if: `, true},
		{"regex without named group", []map[string]any{{"type": "regex_parser", "id": "r", "regex": "^x$"}}, StageBuild, "r", "regex_parser", `operator "r" (regex_parser): no named capture groups in regex pattern`, false},
		{"epoch with a bad layout", []map[string]any{{"type": "time_parser", "id": "t", "parse_from": "attributes.ts", "layout_type": "epoch", "layout": "x"}}, StageBuild, "t", "time_parser", "operator \"t\" (time_parser): invalid `layout` for `epoch` type", false},
		{"unknown output", []map[string]any{add(map[string]any{"output": "nope"})}, StageGraph, "a", "add", `operator "a": output "nope" is not an operator of this processor`, false},
		{"route does not compile", []map[string]any{{"type": "router", "id": "r", "routes": []any{map[string]any{"expr": "==", "output": "r"}}}}, StageCompile, "r", "router", `operator "r" (router): compile route 0: `, true},
		{"route to an unknown output", []map[string]any{{"type": "router", "id": "r", "default": "nope"}}, StageGraph, "r", "router", `operator "r" (router): operator "r": output "nope" is not an operator of this processor`, false},
		{"cycle", []map[string]any{add(map[string]any{"output": "b"}), {"type": "add", "id": "b", "field": "attributes.y", "value": "v", "output": "a"}}, StageGraph, "", "", "operators form a cycle", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, err := range []error{Validate(tt.operators), func() error { _, err := New(tt.operators, componenttest.NewNopTelemetrySettings()); return err }()} {
				require.Error(t, err)
				var ce *ConfigError
				require.True(t, errors.As(err, &ce), "%T: %v", err, err)
				assert.Equal(t, tt.stage, ce.Stage)
				assert.Equal(t, tt.id, ce.OperatorID)
				assert.Equal(t, tt.typ, ce.OperatorType)
				if tt.prefix {
					assert.True(t, strings.HasPrefix(err.Error(), tt.message), "%q does not start with %q", err.Error(), tt.message)
				} else {
					assert.Equal(t, tt.message, err.Error())
				}
			}
		})
	}
}

// A ConfigError keeps the cause it wraps.
func TestConfigErrorUnwraps(t *testing.T) {
	err := Validate([]map[string]any{{"type": "add", "id": "a", "field": "attributes.x", "value": "v", "if": "body =="}})
	var ce *ConfigError
	require.True(t, errors.As(err, &ce))
	assert.NotNil(t, errors.Unwrap(ce))
}
