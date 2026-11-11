// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package logspipeline // import "github.com/fuseone-io/fusescope-collector-server/components/pkg/logspipeline"

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
)

// parsedBodyKey caches, in the entry's attributes, a string or []byte body
// parsed as a JSON object for body.<field> access (spec §4.3). It is removed
// when the entry leaves the processor.
const parsedBodyKey = "__signozlogspipelineprocessor_internal__parsedBodyJson"

const (
	bodyVar      = "body"
	bodyFieldVar = "body_map"
)

// errEnv refuses env(), which would read the collector's environment into
// the logs of a pipeline the backend pushes (spec §4.1). os_env_func is the
// name the reference gives it inside its expression environment.
var errEnv = errors.New("env() is not allowed: a pipeline may not read the collector's environment")

// program is a compiled pipeline expression (spec §4).
type program struct {
	vm *vm.Program
	// bodyFields is set when the expression reads fields of the body, which
	// then reads a JSON string body as an object.
	bodyFields bool
}

func compileBool(input string) (*program, error) {
	return compile(input, expr.AsBool())
}

func compile(input string, options ...expr.Option) (*program, error) {
	patch := &bodyFieldPatcher{}
	guard := &envGuard{}
	options = append([]expr.Option{
		expr.AllowUndefinedVariables(),
		expr.Patch(guard),
		expr.Patch(patch),
	}, options...)
	p, err := expr.Compile(input, options...)
	if guard.found {
		return nil, errEnv
	}
	if err != nil {
		return nil, err
	}
	return &program{vm: p, bodyFields: patch.found}, nil
}

var envPool = sync.Pool{New: func() any { return make(map[string]any, 16) }}

// run evaluates p against e. Panics inside expression functions are
// returned as errors.
func (p *program) run(e *entry.Entry) (result any, err error) {
	env := envPool.Get().(map[string]any)
	defer func() {
		clear(env)
		envPool.Put(env)
		if r := recover(); r != nil {
			err = fmt.Errorf("expression panicked: %v", r)
		}
	}()
	env["isJSON"] = isJSON
	env["unquote"] = unquote
	env["$"] = e.Body
	env[bodyVar] = e.Body
	env["attributes"] = e.Attributes
	env["resource"] = e.Resource
	env["timestamp"] = e.Timestamp
	env["severity_text"] = e.SeverityText
	env["severity_number"] = int(e.Severity)
	env["trace_id"] = e.TraceID
	env["span_id"] = e.SpanID
	env["trace_flags"] = e.TraceFlags
	if p.bodyFields {
		env[bodyFieldVar] = bodyFields(e)
	}
	return vm.Run(p.vm, env)
}

func (p *program) matches(e *entry.Entry) (bool, error) {
	result, err := p.run(e)
	if err != nil {
		return false, err
	}
	b, _ := result.(bool)
	return b, nil
}

// envGuard records whether an expression names env or os_env_func, called
// or not, so that neither a call nor an alias (let f = env) gets through.
// Neither is in the expression environment either.
type envGuard struct {
	found bool
}

func (g *envGuard) Visit(node *ast.Node) {
	if id, ok := (*node).(*ast.IdentifierNode); ok && (id.Value == "env" || id.Value == "os_env_func") {
		g.found = true
	}
}

// bodyFieldPatcher makes member access on body (body.a, body?.a, body["a"])
// read the body as a JSON object.
type bodyFieldPatcher struct {
	found bool
}

func (p *bodyFieldPatcher) Visit(node *ast.Node) {
	member, ok := (*node).(*ast.MemberNode)
	if !ok {
		return
	}
	if target, ok := member.Node.(*ast.IdentifierNode); ok && target.Value == bodyVar {
		target.Value = bodyFieldVar
		p.found = true
	}
}

func bodyFields(e *entry.Entry) map[string]any {
	var raw []byte
	switch body := e.Body.(type) {
	case map[string]any:
		return body
	case string:
		raw = []byte(body)
	case []byte:
		raw = body
	default:
		return nil
	}
	if cached, ok := e.Attributes[parsedBodyKey].(map[string]any); ok {
		return cached
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil
	}
	if e.Attributes == nil {
		e.Attributes = map[string]any{}
	}
	e.Attributes[parsedBodyKey] = fields
	return fields
}

func isJSON(value any) bool {
	switch v := value.(type) {
	case string:
		return json.Valid([]byte(v))
	case []byte:
		return json.Valid(v)
	default:
		return false
	}
}

// unquote strips Go string quotes, returning a string that is not a quoted
// literal unchanged.
func unquote(value any) (string, error) {
	s, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("unquote expects a string, got %T", value)
	}
	if unquoted, err := strconv.Unquote(s); err == nil {
		return unquoted, nil
	}
	return s, nil
}
