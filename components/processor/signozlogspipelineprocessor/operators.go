// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozlogspipelineprocessor // import "github.com/fuseone-io/fusescope-collector-server/components/processor/signozlogspipelineprocessor"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/helper"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/parser/regex"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/parser/severity"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/parser/timeparser"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/parser/trace"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/transformer/copy"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/transformer/move"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/transformer/noop"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/transformer/remove"
	"go.opentelemetry.io/collector/component"
)

const (
	routerType = "router"
	noopType   = "noop"
	grokType   = "grok_parser"
)

// builders are the stanza-backed operator types (spec §3). router and noop
// are run by the pipeline itself.
var builders = map[string]func() operator.Builder{
	"add": func() operator.Builder {
		return &addConfig{TransformerConfig: helper.NewTransformerConfig("add", "add")}
	},
	"copy": func() operator.Builder { return &copyConfig{Config: *copy.NewConfig()} },
	"json_parser": func() operator.Builder {
		return &jsonConfig{ParserConfig: helper.NewParserConfig("json_parser", "json_parser")}
	},
	"move": func() operator.Builder { return move.NewConfig() },
	"normalize": func() operator.Builder {
		return &normalizeConfig{TransformerConfig: helper.NewTransformerConfig("normalize", "normalize")}
	},
	"regex_parser":    func() operator.Builder { return regex.NewConfig() },
	"remove":          func() operator.Builder { return remove.NewConfig() },
	"severity_parser": func() operator.Builder { return &severityConfig{Config: *severity.NewConfig()} },
	"time_parser":     func() operator.Builder { return &timeConfig{Config: *timeparser.NewConfig()} },
	"trace_parser":    func() operator.Builder { return trace.NewConfig() },
	noopType:          func() operator.Builder { return noop.NewConfig() },
}

var errUnparsable = errors.New("value cannot be parsed")

// addConfig is stanza's add with values EXPR(...) evaluated in the
// pipeline expression environment (spec §3.5).
type addConfig struct {
	helper.TransformerConfig `mapstructure:",squash"`
	Field                    entry.Field `mapstructure:"field"`
	Value                    any         `mapstructure:"value"`
}

func (c *addConfig) Build(set component.TelemetrySettings) (operator.Operator, error) {
	t, err := c.TransformerConfig.Build(set)
	if err != nil {
		return nil, err
	}
	if c.Value == nil {
		return nil, errors.New("add: missing required field 'value'")
	}
	if c.Field.FieldInterface == nil {
		return nil, errors.New("add: missing required field 'field'")
	}
	op := &addOperator{TransformerOperator: t, field: c.Field, value: c.Value}
	if s, ok := c.Value.(string); ok && strings.HasPrefix(s, "EXPR(") && strings.HasSuffix(s, ")") {
		if op.program, err = compile(strings.TrimSuffix(strings.TrimPrefix(s, "EXPR("), ")")); err != nil {
			return nil, fmt.Errorf("add: compile value expression: %w", err)
		}
	}
	return op, nil
}

type addOperator struct {
	helper.TransformerOperator
	field   entry.Field
	value   any
	program *program
}

func (o *addOperator) Process(ctx context.Context, e *entry.Entry) error {
	return o.ProcessWith(ctx, e, o.transform)
}

func (o *addOperator) ProcessBatch(ctx context.Context, entries []*entry.Entry) error {
	return o.ProcessBatchWith(ctx, entries, o.Process)
}

func (o *addOperator) transform(e *entry.Entry) error {
	if o.program == nil {
		return e.Set(o.field, deepCopy(o.value))
	}
	result, err := o.program.run(e)
	if err != nil {
		return fmt.Errorf("evaluate value expression: %w", err)
	}
	return e.Set(o.field, deepCopy(result))
}

// jsonConfig parses a JSON object, optionally flattening nested objects
// (spec §3.3).
type jsonConfig struct {
	helper.ParserConfig `mapstructure:",squash"`
	EnableFlattening    bool   `mapstructure:"enable_flattening"`
	MaxFlatteningDepth  int    `mapstructure:"max_flattening_depth"`
	EnablePaths         bool   `mapstructure:"enable_paths"`
	PathPrefix          string `mapstructure:"path_prefix"`
}

func (c *jsonConfig) Build(set component.TelemetrySettings) (operator.Operator, error) {
	p, err := c.ParserConfig.Build(set)
	if err != nil {
		return nil, err
	}
	return &jsonOperator{ParserOperator: p, config: *c}, nil
}

type jsonOperator struct {
	helper.ParserOperator
	config jsonConfig
}

func (o *jsonOperator) Process(ctx context.Context, e *entry.Entry) error {
	return o.ProcessWith(ctx, e, o.parse)
}

func (o *jsonOperator) ProcessBatch(ctx context.Context, entries []*entry.Entry) error {
	return o.ProcessBatchWith(ctx, entries, o.parse)
}

func (o *jsonOperator) parse(value any) (any, error) {
	var parsed map[string]any
	switch v := value.(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &parsed); err != nil {
			return nil, err
		}
		if parsed == nil {
			return nil, fmt.Errorf("%w: JSON null is not an object", errUnparsable)
		}
	case map[string]any:
		parsed = deepCopy(v).(map[string]any)
	default:
		return nil, fmt.Errorf("%w: type %T is not a JSON string", errUnparsable, value)
	}
	if !o.config.EnableFlattening {
		return parsed, nil
	}
	prefix := ""
	if o.config.EnablePaths {
		prefix = o.config.PathPrefix
	}
	return o.flatten(parsed, 0, prefix), nil
}

// flatten lifts the members of nested objects up to MaxFlatteningDepth
// levels; a member already present at the outer level keeps its value.
func (o *jsonOperator) flatten(m map[string]any, depth int, prefix string) map[string]any {
	out := make(map[string]any, len(m))
	var lifted []string
	for k, v := range m {
		if _, isMap := v.(map[string]any); isMap && depth < o.config.MaxFlatteningDepth {
			lifted = append(lifted, k)
			continue
		}
		out[o.path(prefix, k)] = v
	}
	sort.Strings(lifted)
	for _, k := range lifted {
		childPrefix := prefix
		if o.config.EnablePaths {
			childPrefix = o.path(prefix, k)
		}
		for ck, cv := range o.flatten(m[k].(map[string]any), depth+1, childPrefix) {
			if _, exists := out[ck]; !exists {
				out[ck] = cv
			}
		}
	}
	return out
}

func (o *jsonOperator) path(prefix, key string) string {
	if !o.config.EnablePaths || prefix == "" {
		return key
	}
	return prefix + "." + key
}

// normalizeConfig turns the body into an object (spec §3.9).
type normalizeConfig struct {
	helper.TransformerConfig `mapstructure:",squash"`
}

func (c *normalizeConfig) Build(set component.TelemetrySettings) (operator.Operator, error) {
	t, err := c.TransformerConfig.Build(set)
	if err != nil {
		return nil, err
	}
	return &normalizeOperator{TransformerOperator: t}, nil
}

type normalizeOperator struct {
	helper.TransformerOperator
}

func (o *normalizeOperator) Process(ctx context.Context, e *entry.Entry) error {
	return o.ProcessWith(ctx, e, normalizeBody)
}

func (o *normalizeOperator) ProcessBatch(ctx context.Context, entries []*entry.Entry) error {
	return o.ProcessBatchWith(ctx, entries, o.Process)
}

func normalizeBody(e *entry.Entry) error {
	switch body := e.Body.(type) {
	case map[string]any:
		return nil
	case string:
		if object, ok := jsonObject(body); ok {
			if msg, hasMsg := object["msg"]; hasMsg {
				if _, hasMessage := object["message"]; !hasMessage {
					delete(object, "msg")
					object["message"] = msg
				}
			}
			e.Body = object
			return nil
		}
	}
	e.Body = map[string]any{"message": e.Body}
	return nil
}

// jsonObject parses s when it is exactly one JSON object with no
// surrounding whitespace; integers become int64, other numbers float64.
func jsonObject(s string) (map[string]any, bool) {
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return nil, false
	}
	d := json.NewDecoder(bytes.NewReader([]byte(s)))
	d.UseNumber()
	var object map[string]any
	if err := d.Decode(&object); err != nil || d.Decode(new(json.RawMessage)) != io.EOF {
		return nil, false
	}
	return numbers(object).(map[string]any), true
}

func numbers(value any) any {
	switch v := value.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
		f, _ := v.Float64()
		return f
	case map[string]any:
		for k, item := range v {
			v[k] = numbers(item)
		}
	case []any:
		for i, item := range v {
			v[i] = numbers(item)
		}
	}
	return value
}

// severityConfig is stanza's severity_parser, except that a 64-bit integer
// value is an error, as an integer attribute is in the reference (spec §3.6).
type severityConfig struct {
	severity.Config `mapstructure:",squash"`
}

func (c *severityConfig) Build(set component.TelemetrySettings) (operator.Operator, error) {
	t, err := c.TransformerConfig.Build(set)
	if err != nil {
		return nil, err
	}
	p, err := c.SeverityConfig.Build(set)
	if err != nil {
		return nil, err
	}
	return &severityOperator{TransformerOperator: t, parser: p}, nil
}

type severityOperator struct {
	helper.TransformerOperator
	parser helper.SeverityParser
}

func (o *severityOperator) Process(ctx context.Context, e *entry.Entry) error {
	return o.ProcessWith(ctx, e, o.parse)
}

func (o *severityOperator) ProcessBatch(ctx context.Context, entries []*entry.Entry) error {
	return o.ProcessBatchWith(ctx, entries, o.Process)
}

func (o *severityOperator) parse(e *entry.Entry) error {
	if v, ok := e.Get(o.parser.ParseFrom); ok {
		if _, isInt64 := v.(int64); isInt64 {
			return fmt.Errorf("%w: type int64 cannot be a severity", errUnparsable)
		}
	}
	return o.parser.Parse(e)
}

// copyConfig is stanza's copy writing a deep copy, so copying a map into
// one of its own members cannot make the record cyclic.
type copyConfig struct {
	copy.Config `mapstructure:",squash"`
}

func (c *copyConfig) Build(set component.TelemetrySettings) (operator.Operator, error) {
	op, err := c.Config.Build(set)
	if err != nil {
		return nil, err
	}
	t := op.(*copy.Transformer)
	return &copyOperator{TransformerOperator: t.TransformerOperator, from: t.From, to: t.To}, nil
}

type copyOperator struct {
	helper.TransformerOperator
	from, to entry.Field
}

func (o *copyOperator) Process(ctx context.Context, e *entry.Entry) error {
	return o.ProcessWith(ctx, e, o.transform)
}

func (o *copyOperator) ProcessBatch(ctx context.Context, entries []*entry.Entry) error {
	return o.ProcessBatchWith(ctx, entries, o.Process)
}

func (o *copyOperator) transform(e *entry.Entry) error {
	v, ok := o.from.Get(e)
	if !ok {
		return fmt.Errorf("copy: from field does not exist in this entry: %s", o.from.String())
	}
	return o.to.Set(e, deepCopy(v))
}

// deepCopy copies maps, lists and bytes, so a value written into a record
// shares nothing with the configuration, another record or itself.
func deepCopy(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			out[k] = deepCopy(item)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = deepCopy(item)
		}
		return out
	case []byte:
		return append([]byte(nil), v...)
	}
	return value
}

// timeConfig parses layouts without a zone in UTC (spec §3.7).
type timeConfig struct {
	timeparser.Config `mapstructure:",squash"`
}

func (c *timeConfig) Build(set component.TelemetrySettings) (operator.Operator, error) {
	if c.Location == "" {
		c.Location = "UTC"
	}
	return c.Config.Build(set)
}
