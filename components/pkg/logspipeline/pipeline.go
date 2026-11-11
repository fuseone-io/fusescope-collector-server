// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package logspipeline // import "github.com/fuseone-io/fusescope-collector-server/components/pkg/logspipeline"

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/operator/helper"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.uber.org/zap"
)

// exit is the next hop of an entry that leaves the processor.
const exit = -1

// pipeline is a processor's operators as a graph (spec §2).
type pipeline struct {
	first int
	nodes []node
	// writesResource is set when an operator may change the resource;
	// otherwise a resource's records share one resource map.
	writesResource bool
}

// resourceWriteKeys are the operator fields that name where it writes; a
// move also deletes its from field.
var resourceWriteKeys = []string{"parse_to", "to", "field", "from"}

func writesResource(specs []spec) bool {
	for _, s := range specs {
		for _, key := range resourceWriteKeys {
			if key == "from" && s.typ != "move" {
				continue
			}
			if v, _ := s.config[key].(string); strings.HasPrefix(v, "resource") {
				return true
			}
		}
	}
	return false
}

type node interface {
	// process runs e and appends where it goes next to out; an entry the
	// node drops appends nothing.
	process(ctx context.Context, r *run, e *entry.Entry, out []hop) []hop
}

type hop struct {
	e    *entry.Entry
	next int
}

type spec struct {
	id     string
	typ    string
	output string
	cond   string
	config map[string]any
}

func newPipeline(operators []map[string]any, set component.TelemetrySettings) (*pipeline, error) {
	if len(operators) == 0 {
		return nil, errors.New("no operators were configured for signozlogspipeline processor")
	}
	specs, err := parseSpecs(operators)
	if err != nil {
		return nil, err
	}
	ids := make(map[string]int, len(specs))
	for i, s := range specs {
		ids[s.id] = i
	}
	resolve := func(from, id string) (int, error) {
		if id == "" {
			return exit, nil
		}
		i, ok := ids[id]
		if !ok {
			return 0, fmt.Errorf("operator %q: output %q is not an operator of this processor", from, id)
		}
		return i, nil
	}

	out, err := helper.NewOutputConfig(sinkID, "sink").Build(set)
	if err != nil {
		return nil, err
	}
	output := &sink{OutputOperator: out}

	p := &pipeline{nodes: make([]node, len(specs)), writesResource: writesResource(specs)}
	for i, s := range specs {
		next := exit
		switch {
		case s.output != "":
			if next, err = resolve(s.id, s.output); err != nil {
				return nil, err
			}
		case i+1 < len(specs):
			next = i + 1
		}
		if p.nodes[i], err = buildNode(s, next, resolve, output, set); err != nil {
			return nil, fmt.Errorf("operator %q (%s): %w", s.id, s.typ, err)
		}
	}
	if err := checkCycles(p.nodes); err != nil {
		return nil, err
	}
	return p, nil
}

func parseSpecs(operators []map[string]any) ([]spec, error) {
	specs := make([]spec, 0, len(operators))
	seen := map[string]bool{}
	for i, raw := range operators {
		config := maps.Clone(raw)
		typ, _ := config["type"].(string)
		if typ == "" {
			return nil, fmt.Errorf("operators[%d]: missing required field 'type'", i)
		}
		if _, ok := builders[typ]; !ok && typ != routerType {
			return nil, fmt.Errorf("operators[%d]: unsupported type %q", i, typ)
		}
		s := spec{typ: typ, id: typ, config: config}
		for _, field := range []struct {
			key string
			dst *string
		}{{"id", &s.id}, {"output", &s.output}, {"if", &s.cond}} {
			v, ok := config[field.key]
			if !ok || typ == routerType && field.key != "id" {
				continue
			}
			str, isStr := v.(string)
			if !isStr {
				return nil, fmt.Errorf("operators[%d]: %s must be a string", i, field.key)
			}
			if str != "" {
				*field.dst = str
			}
			if field.key != "id" {
				delete(config, field.key)
			}
		}
		if seen[s.id] {
			return nil, fmt.Errorf("operators[%d]: operator id %q is used twice", i, s.id)
		}
		seen[s.id] = true
		config["id"] = s.id
		specs = append(specs, s)
	}
	return specs, nil
}

func buildNode(s spec, next int, resolve func(from, id string) (int, error), output operator.Operator, set component.TelemetrySettings) (node, error) {
	if s.typ == routerType {
		return buildRouter(s, resolve)
	}
	reads, err := redirectBodyReads(s)
	if err != nil {
		return nil, err
	}
	builder := builders[s.typ]()
	if err := confmap.NewFromStringMap(s.config).Unmarshal(builder); err != nil {
		return nil, err
	}
	var when *program
	if s.cond != "" {
		if when, err = compileBool(s.cond); err != nil {
			return nil, fmt.Errorf("compile if: %w", err)
		}
	}
	if s.typ == noopType {
		return &noopNode{next: next}, nil
	}
	op, err := builder.Build(set)
	if err != nil {
		return nil, err
	}
	onError := helper.SendOnError
	if v, ok := s.config["on_error"].(string); ok && v != "" {
		onError = v
	}
	n := &stanzaNode{id: s.id, op: op, when: when, onError: onError, next: next, reads: reads, logger: set.Logger}
	op.SetOutputIDs([]string{sinkID})
	if err := op.SetOutputs([]operator.Operator{output}); err != nil {
		return nil, err
	}
	return n, nil
}

// bodyReadFields are, per operator type, the config paths of the fields the
// operator reads; move and remove are absent because they delete their field.
var bodyReadFields = map[string][][]string{
	"copy":            {{"from"}},
	"grok_parser":     {{"parse_from"}},
	"json_parser":     {{"parse_from"}},
	"regex_parser":    {{"parse_from"}},
	"severity_parser": {{"parse_from"}},
	"time_parser":     {{"parse_from"}},
	"trace_parser":    {{"trace_id", "parse_from"}, {"span_id", "parse_from"}, {"trace_flags", "parse_from"}},
}

// scratchPrefix names the scratch attributes of body reads. Its random part,
// drawn once per process, keeps a record's own attributes and an operator's
// output from ever colliding with them.
var scratchPrefix = func() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "__signozlogspipeline_bodyfield_" + hex.EncodeToString(b[:]) + "_"
}()

// bodyRead is a body.<field> the operator reads, redirected to a scratch
// attribute that holds the field's value while the operator runs.
type bodyRead struct {
	keys    []string
	scratch string
}

// redirectBodyReads points every body.<field> the operator reads at a
// scratch attribute, so the read sees a JSON string or bytes body as an
// object, as expressions do (spec §4.3).
func redirectBodyReads(s spec) ([]bodyRead, error) {
	var reads []bodyRead
	for _, path := range bodyReadFields[s.typ] {
		parent := s.config
		for _, key := range path[:len(path)-1] {
			child, ok := parent[key].(map[string]any)
			if !ok {
				parent = nil
				break
			}
			child = maps.Clone(child)
			parent[key] = child
			parent = child
		}
		last := path[len(path)-1]
		raw, ok := parent[last].(string)
		if parent == nil || !ok {
			continue
		}
		field, err := entry.NewField(raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", strings.Join(path, "."), err)
		}
		body, ok := field.FieldInterface.(entry.BodyField)
		if !ok || len(body.Keys) == 0 {
			continue
		}
		scratch := fmt.Sprintf("%s%d__", scratchPrefix, len(reads))
		parent[last] = fmt.Sprintf("attributes[%q]", scratch)
		reads = append(reads, bodyRead{keys: body.Keys, scratch: scratch})
	}
	return reads, nil
}

func (r bodyRead) resolve(e *entry.Entry) (any, bool) {
	var object map[string]any
	if m, ok := e.Body.(map[string]any); ok {
		object = m
	} else if object = bodyFields(e); object == nil {
		return nil, false
	}
	return entry.NewBodyField(r.keys...).Get(&entry.Entry{Body: object})
}

type routerConfig struct {
	ID      string        `mapstructure:"id"`
	Type    string        `mapstructure:"type"`
	Routes  []routeConfig `mapstructure:"routes"`
	Default string        `mapstructure:"default"`
}

type routeConfig struct {
	Expr   string `mapstructure:"expr"`
	Output string `mapstructure:"output"`
}

func buildRouter(s spec, resolve func(from, id string) (int, error)) (node, error) {
	var cfg routerConfig
	if err := confmap.NewFromStringMap(s.config).Unmarshal(&cfg); err != nil {
		return nil, err
	}
	n := &routerNode{defaultNext: exit}
	if cfg.Default != "" {
		next, err := resolve(s.id, cfg.Default)
		if err != nil {
			return nil, err
		}
		n.defaultNext, n.hasDefault = next, true
	}
	for i, r := range cfg.Routes {
		when, err := compileBool(r.Expr)
		if err != nil {
			return nil, fmt.Errorf("compile route %d: %w", i, err)
		}
		next, err := resolve(s.id, r.Output)
		if err != nil {
			return nil, err
		}
		n.routes = append(n.routes, route{when: when, next: next})
	}
	return n, nil
}

func checkCycles(nodes []node) error {
	const (
		unvisited = iota
		visiting
		done
	)
	state := make([]int, len(nodes))
	var visit func(i int) error
	visit = func(i int) error {
		switch state[i] {
		case visiting:
			return errors.New("operators form a cycle")
		case done:
			return nil
		}
		state[i] = visiting
		for _, next := range successors(nodes[i]) {
			if next == exit {
				continue
			}
			if err := visit(next); err != nil {
				return err
			}
		}
		state[i] = done
		return nil
	}
	for i := range nodes {
		if err := visit(i); err != nil {
			return err
		}
	}
	return nil
}

func successors(n node) []int {
	switch n := n.(type) {
	case *routerNode:
		next := make([]int, 0, len(n.routes)+1)
		for _, r := range n.routes {
			next = append(next, r.next)
		}
		if n.hasDefault {
			next = append(next, n.defaultNext)
		}
		return next
	case *noopNode:
		return []int{n.next}
	case *stanzaNode:
		return []int{n.next}
	}
	return nil
}

// run is the state of one ConsumeLogs call; nodes share nothing else, so a
// processor serves concurrent calls.
type run struct {
	ctx     context.Context
	emitted []*entry.Entry
	pending []hop
}

type runKey struct{}

func newRun(ctx context.Context) *run {
	r := &run{}
	r.ctx = context.WithValue(ctx, runKey{}, r)
	return r
}

// process runs e through the graph and calls leave for each entry that
// leaves it, with the parsed body cache removed.
func (p *pipeline) process(r *run, e *entry.Entry, leave func(*entry.Entry)) {
	r.pending = append(r.pending[:0], hop{e: e, next: p.first})
	for len(r.pending) > 0 {
		h := r.pending[len(r.pending)-1]
		r.pending = r.pending[:len(r.pending)-1]
		if h.next == exit {
			delete(h.e.Attributes, parsedBodyKey)
			leave(h.e)
			continue
		}
		r.pending = p.nodes[h.next].process(r.ctx, r, h.e, r.pending)
	}
}

// stanzaNode runs a stanza operator whose output is the sink, evaluating
// the operator's if itself so it sees the pipeline expression environment.
type stanzaNode struct {
	id      string
	op      operator.Operator
	when    *program
	onError string
	next    int
	reads   []bodyRead
	logger  *zap.Logger
}

func (n *stanzaNode) process(ctx context.Context, r *run, e *entry.Entry, out []hop) []hop {
	if n.when != nil {
		ok, err := n.when.matches(e)
		if err != nil {
			return n.failed(e, fmt.Errorf("running if expr: %w", err), out)
		}
		if !ok {
			return append(out, hop{e: e, next: n.next})
		}
	}
	for _, read := range n.reads {
		if v, ok := read.resolve(e); ok {
			if e.Attributes == nil {
				e.Attributes = map[string]any{}
			}
			e.Attributes[read.scratch] = v
		}
	}
	r.emitted = r.emitted[:0]
	err := n.safeProcess(ctx, e)
	for _, read := range n.reads {
		delete(e.Attributes, read.scratch)
	}
	if errors.Is(err, errPanicked) {
		return n.failed(e, err, out)
	}
	for _, emitted := range r.emitted {
		out = append(out, hop{e: emitted, next: n.next})
	}
	return out
}

var errPanicked = errors.New("operator panicked")

// safeProcess runs the operator, turning a panic into an error; stanza has
// already applied on_error to any other error it returns.
func (n *stanzaNode) safeProcess(ctx context.Context, e *entry.Entry) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%w: %v", errPanicked, p)
		}
	}()
	return n.op.Process(ctx, e)
}

func (n *stanzaNode) failed(e *entry.Entry, err error, out []hop) []hop {
	quiet := n.onError == helper.SendOnErrorQuiet || n.onError == helper.DropOnErrorQuiet
	if quiet {
		n.logger.Debug("Failed to process entry", zap.String("operator_id", n.id), zap.Error(err), zap.String("action", n.onError))
	} else {
		n.logger.Error("Failed to process entry", zap.String("operator_id", n.id), zap.Error(err), zap.String("action", n.onError))
	}
	if n.onError == helper.SendOnError || n.onError == helper.SendOnErrorQuiet {
		return append(out, hop{e: e, next: n.next})
	}
	return out
}

type route struct {
	when *program
	next int
}

// routerNode sends an entry to the first route whose expression is true; an
// expression that fails is false. Without a match it takes the default, or
// is dropped when there is none.
type routerNode struct {
	routes      []route
	defaultNext int
	hasDefault  bool
}

func (n *routerNode) process(_ context.Context, _ *run, e *entry.Entry, out []hop) []hop {
	for _, r := range n.routes {
		if ok, err := r.when.matches(e); err == nil && ok {
			return append(out, hop{e: e, next: r.next})
		}
	}
	if n.hasDefault {
		return append(out, hop{e: e, next: n.defaultNext})
	}
	return out
}

type noopNode struct {
	next int
}

func (n *noopNode) process(_ context.Context, _ *run, e *entry.Entry, out []hop) []hop {
	return append(out, hop{e: e, next: n.next})
}

const sinkID = "__signozlogspipeline_sink__"

// sink is every stanza operator's output: it hands what the operator writes
// to the run in the context.
type sink struct {
	helper.OutputOperator
}

func (*sink) Process(ctx context.Context, e *entry.Entry) error {
	if r, ok := ctx.Value(runKey{}).(*run); ok {
		r.emitted = append(r.emitted, e)
	}
	return nil
}

func (s *sink) ProcessBatch(ctx context.Context, entries []*entry.Entry) error {
	for _, e := range entries {
		_ = s.Process(ctx, e)
	}
	return nil
}
