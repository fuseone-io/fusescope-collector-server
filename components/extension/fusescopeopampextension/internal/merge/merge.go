// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package merge applies the allow-list of ADR 0008 §2: it merges a pushed
// configuration into the local base, or names the paths where the push
// differs from the base in ways it may not.
package merge // import "github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/merge"

import (
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
)

const (
	// LogsPipelinePrefix names the processors the backend places in the logs pipeline.
	LogsPipelinePrefix = "signozlogspipeline/pipeline_"
	// LogsPipeline is the only pipeline log pipelines may be placed in.
	LogsPipeline = "logs"
	// maxPaths bounds the paths a rejection names.
	maxPaths = 20
)

// blockOnly are processors whose block may change, but not their position.
var blockOnly = []string{"signozspanmapper", "signozllmpricing"}

// Config is a collector configuration as nested maps.
type Config = map[string]any

// Rejection lists the configuration paths a push may not change. It never
// holds values (ADR 0008 §2).
type Rejection struct{ Paths []string }

func (r *Rejection) Error() string {
	return "the pushed configuration changes what the allow-list keeps local, or holds a ${...} reference: " + strings.Join(r.Paths, ", ")
}

// Merge returns base with the allow-listed parts of push, or a *Rejection.
func Merge(base, push Config) (Config, error) {
	d := &differ{}
	merged := Config{}
	for k := range union(base, push) {
		bm, pm, sections := maps(base[k], push[k])
		switch {
		case k == "processors" && sections:
			merged[k] = d.processors(bm, pm)
		case k == "service" && sections:
			merged[k] = d.service(bm, pm)
		default:
			d.equal(k, base[k], push[k])
			if v, ok := base[k]; ok {
				merged[k] = v
			}
		}
	}
	if len(d.paths) > 0 {
		sort.Strings(d.paths)
		return nil, &Rejection{Paths: d.paths}
	}
	d.checkWiring(merged)
	if len(d.paths) > 0 {
		return nil, &Rejection{Paths: d.paths}
	}
	return merged, nil
}

// AllowListed reports whether a processor name belongs to the allow-list.
func AllowListed(name string) bool {
	return strings.HasPrefix(name, LogsPipelinePrefix) || slices.Contains(blockOnly, name)
}

type differ struct{ paths []string }

func (d *differ) add(path string) {
	if len(d.paths) < maxPaths && !slices.Contains(d.paths, path) {
		d.paths = append(d.paths, path)
	}
}

// equal records the leaf paths where a and b differ.
func (d *differ) equal(path string, a, b any) {
	if reflect.DeepEqual(a, b) {
		return
	}
	am, aok := a.(Config)
	bm, bok := b.(Config)
	if !aok || !bok {
		d.add(path)
		return
	}
	for k := range union(am, bm) {
		d.equal(path+"."+k, am[k], bm[k])
	}
}

func (d *differ) processors(base, push Config) Config {
	out := Config{}
	for name := range union(base, push) {
		bv, inBase := base[name]
		pv, inPush := push[name]
		switch {
		case strings.HasPrefix(name, LogsPipelinePrefix):
			if inPush {
				d.substitutions("processors."+name, bv, pv)
				out[name] = pv
			}
		case slices.Contains(blockOnly, name):
			switch {
			case !inBase:
				d.add("processors." + name) // the base must declare and wire it
			case inPush:
				d.substitutions("processors."+name, bv, pv)
				out[name] = pv
			default:
				out[name] = bv
			}
		default:
			d.equal("processors."+name, bv, pv)
			if inBase {
				out[name] = bv
			}
		}
	}
	return out
}

// substitutions records where push holds a confmap reference ("${...}")
// that base does not hold at the same path: the collector would resolve it,
// so a push could read local files or environment into exported values.
func (d *differ) substitutions(path string, base, push any) {
	switch p := push.(type) {
	case string:
		if b, ok := base.(string); hasReference(p) && (!ok || b != p) {
			d.add(path)
		}
	case Config:
		bm, _ := base.(Config)
		for k, v := range p {
			if _, inBase := bm[k]; hasReference(k) && !inBase {
				d.add(path)
			}
			d.substitutions(path+"."+k, bm[k], v)
		}
	case []any:
		bl, _ := base.([]any)
		for i, v := range p {
			var b any
			if i < len(bl) {
				b = bl[i]
			}
			d.substitutions(fmt.Sprintf("%s[%d]", path, i), b, v)
		}
	}
}

// hasReference reports a "${" whose "$" is not escaped as "$$".
func hasReference(s string) bool {
	for i := 0; i+1 < len(s); i++ {
		if s[i] != '$' {
			continue
		}
		if s[i+1] == '{' {
			return true
		}
		if s[i+1] == '$' {
			i++
		}
	}
	return false
}

func (d *differ) service(base, push Config) Config {
	out := Config{}
	for k := range union(base, push) {
		if k != "pipelines" {
			d.equal("service."+k, base[k], push[k])
			if v, ok := base[k]; ok {
				out[k] = v
			}
			continue
		}
		bp, pp, ok := maps(base[k], push[k])
		if !ok {
			d.equal("service."+k, base[k], push[k])
			out[k] = base[k]
			continue
		}
		pipelines := Config{}
		for name := range union(bp, pp) {
			path := "service.pipelines." + name
			bv, inBase := bp[name]
			pv, inPush := pp[name]
			if !inBase || !inPush {
				d.add(path)
				continue
			}
			b, p, ok := maps(bv, pv)
			if !ok {
				d.equal(path, bv, pv)
				pipelines[name] = bv
				continue
			}
			pipelines[name] = d.pipeline(path, name, b, p)
		}
		out[k] = pipelines
	}
	return out
}

func (d *differ) pipeline(path, name string, base, push Config) Config {
	out := Config{}
	for k := range union(base, push) {
		if k != "processors" {
			d.equal(path+"."+k, base[k], push[k])
			if v, ok := base[k]; ok {
				out[k] = v
			}
			continue
		}
		bl, bok := asList(base[k])
		pl, pok := asList(push[k])
		if !bok || !pok {
			// A reference the collector resolves later: kept only unchanged.
			d.equal(path+".processors", base[k], push[k])
			out[k] = base[k]
			continue
		}
		if name != LogsPipeline {
			// Span mapping and LLM pricing keep the base's position (ADR 0008 §2).
			if !slices.Equal(bl, pl) {
				d.add(path + ".processors")
			}
			out[k] = toAny(bl)
			continue
		}
		if !slices.Equal(withoutLogsPipelines(bl), withoutLogsPipelines(pl)) {
			d.add(path + ".processors")
			continue
		}
		out[k] = toAny(pl)
	}
	return out
}

// checkWiring rejects log pipelines listed without a block, and blocks
// listed in a pipeline other than logs (which the comparison already refuses).
func (d *differ) checkWiring(merged Config) {
	procs := asMap(merged["processors"])
	for name, p := range asMap(asMap(merged["service"])["pipelines"]) {
		list, _ := asList(asMap(p)["processors"])
		for _, proc := range list {
			if strings.HasPrefix(proc, LogsPipelinePrefix) {
				if _, ok := procs[proc]; !ok {
					d.add(fmt.Sprintf("service.pipelines.%s.processors", name))
				}
			}
		}
	}
}

func withoutLogsPipelines(l []string) []string {
	return slices.DeleteFunc(slices.Clone(l), func(s string) bool { return strings.HasPrefix(s, LogsPipelinePrefix) })
}

func union(a, b Config) map[string]struct{} {
	out := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		out[k] = struct{}{}
	}
	for k := range b {
		out[k] = struct{}{}
	}
	return out
}

// maps returns a and b as maps, absent ones as empty, or false when either is
// something else, such as a ${file:...} reference the collector resolves
// later: such a section is compared whole and kept as the base has it, never
// read as empty.
func maps(a, b any) (Config, Config, bool) {
	am, aok := a.(Config)
	bm, bok := b.(Config)
	if (!aok && a != nil) || (!bok && b != nil) {
		return nil, nil, false
	}
	return asMap(am), asMap(bm), true
}

func asMap(v any) Config {
	m, _ := v.(Config)
	if m == nil {
		return Config{}
	}
	return m
}

func asList(v any) ([]string, bool) {
	if v == nil {
		return nil, true
	}
	raw, ok := v.([]any)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(raw))
	for _, e := range raw {
		s, ok := e.(string)
		if !ok {
			return nil, false
		}
		out = append(out, s)
	}
	return out, true
}

func toAny(l []string) []any {
	out := make([]any, len(l))
	for i, s := range l {
		out[i] = s
	}
	return out
}
