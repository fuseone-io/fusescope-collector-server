// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozspanmapperprocessor // import "github.com/fuseone-io/fusescope-collector-server/components/processor/signozspanmapperprocessor"

import (
	"context"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type source struct {
	key          string
	fromResource bool
	move         bool
}

type rule struct {
	target     string
	toResource bool
	sources    []source
}

type group struct {
	attributes, resource []string
	rules                []rule
}

// mapper is the validated config, with source prefixes and contexts resolved
// once instead of per span.
type mapper struct {
	groups []group
}

func newMapper(cfg *Config) *mapper {
	m := &mapper{groups: make([]group, 0, len(cfg.Groups))}
	for _, g := range cfg.Groups {
		cg := group{attributes: g.ExistsAny.Attributes, resource: g.ExistsAny.Resource}
		for _, r := range g.Attributes {
			cr := rule{target: r.Target, toResource: r.Context == ContextResource}
			for _, s := range r.Sources {
				key, fromResource := strings.CutPrefix(s.Key, resourcePrefix)
				cr.sources = append(cr.sources, source{key: key, fromResource: fromResource, move: s.Action == ActionMove})
			}
			cg.rules = append(cg.rules, cr)
		}
		m.groups = append(m.groups, cg)
	}
	return m
}

// maxStackGroups bounds the per-resource match flags kept on the stack.
const maxStackGroups = 64

func (m *mapper) processTraces(_ context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	if len(m.groups) == 0 {
		return td, nil
	}
	var stack [maxStackGroups]bool
	resourceMatch := stack[:0]
	if len(m.groups) > maxStackGroups {
		resourceMatch = make([]bool, 0, len(m.groups))
	}
	for _, rs := range td.ResourceSpans().All() {
		res := rs.Resource().Attributes()
		// The resource part of a condition is fixed before any span is mapped,
		// as the reference does (spec G4).
		resourceMatch = resourceMatch[:0]
		for i := range m.groups {
			resourceMatch = append(resourceMatch, anyKeyContains(res, m.groups[i].resource))
		}
		for _, ss := range rs.ScopeSpans().All() {
			for _, span := range ss.Spans().All() {
				attrs := span.Attributes()
				for i := range m.groups {
					g := &m.groups[i]
					if !resourceMatch[i] && !anyKeyContains(attrs, g.attributes) {
						continue
					}
					for j := range g.rules {
						applyRule(&g.rules[j], attrs, res)
					}
				}
			}
		}
	}
	return td, nil
}

func anyKeyContains(m pcommon.Map, substrings []string) bool {
	if len(substrings) == 0 {
		return false
	}
	for k := range m.All() {
		for _, s := range substrings {
			if strings.Contains(k, s) {
				return true
			}
		}
	}
	return false
}

func applyRule(r *rule, attrs, res pcommon.Map) {
	target := attrs
	if r.toResource {
		target = res
	}
	if _, exists := target.Get(r.target); exists {
		return
	}
	for i := range r.sources {
		s := &r.sources[i]
		from := attrs
		if s.fromResource {
			from = res
		}
		if _, ok := from.Get(s.key); !ok {
			continue
		}
		dst := target.PutEmpty(r.target)
		// Looked up after PutEmpty: appending to the same map may move the
		// source's storage.
		value, _ := from.Get(s.key)
		value.CopyTo(dst)
		if s.move {
			from.Remove(s.key)
		}
		return
	}
}
