// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozllmpricingprocessor // import "github.com/fuseone-io/fusescope-collector-server/components/processor/signozllmpricingprocessor"

import (
	"context"
	"path"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// reservedPrefix marks the attributes only this processor writes (spec §5).
const reservedPrefix = "signoz.gen_ai."

type matchKind uint8

const (
	matchLiteral matchKind = iota
	// matchPrefix is "<literal>*": the literal, then anything without '/'.
	matchPrefix
	matchGlob
)

type pattern struct {
	kind matchKind
	// lit is the pattern's text before its first metacharacter, which every
	// match starts with.
	lit, text string
}

func compilePattern(p string) pattern {
	const meta = `*?[\`
	i := strings.IndexAny(p, meta)
	switch {
	case i < 0:
		return pattern{matchLiteral, p, p}
	case i == len(p)-1 && p[i] == '*':
		return pattern{matchPrefix, p[:i], p}
	}
	return pattern{matchGlob, p[:i], p}
}

// matches is path.Match, with fast paths for the common shapes.
func (p pattern) matches(model string) bool {
	if !strings.HasPrefix(model, p.lit) {
		return false
	}
	switch p.kind {
	case matchLiteral:
		return len(model) == len(p.lit)
	case matchPrefix:
		return !strings.Contains(model[len(p.lit):], "/")
	}
	ok, err := path.Match(p.text, model)
	return ok && err == nil
}

type rule struct {
	patterns             []pattern
	in, out, read, write float64
}

type pricer struct {
	attrs  Attrs
	output OutputAttrs
	rules  []rule
}

func newPricer(cfg *Config) *pricer {
	p := &pricer{attrs: cfg.Attrs, output: cfg.OutputAttrs}
	for _, r := range cfg.DefaultPricing.Rules {
		compiled := rule{in: r.In, out: r.Out}
		if r.Cache != nil {
			compiled.read, compiled.write = r.Cache.Read, r.Cache.Write
		}
		for _, s := range r.Pattern {
			compiled.patterns = append(compiled.patterns, compilePattern(s))
		}
		p.rules = append(p.rules, compiled)
	}
	return p
}

func (p *pricer) processTraces(_ context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		removeReserved(rs.Resource().Attributes())
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				p.price(spans.At(k).Attributes())
			}
		}
	}
	return td, nil
}

func removeReserved(attrs pcommon.Map) {
	attrs.RemoveIf(func(k string, v pcommon.Value) bool {
		return strings.HasPrefix(k, reservedPrefix) || reservedNested(k, v)
	})
}

// reservedNested removes, from a map value at path, the entries whose
// flattened keys (the ClickHouse exporter joins kvlist keys with '.') fall
// under the prefix, and reports whether all of the value does. The reference
// keeps them; spec §5 deviates so no cost can be planted this way.
func reservedNested(path string, v pcommon.Value) bool {
	n := len(path)
	if v.Type() != pcommon.ValueTypeMap || n >= len(reservedPrefix) || reservedPrefix[n] != '.' || !strings.HasPrefix(reservedPrefix, path) {
		return false
	}
	if n == len(reservedPrefix)-1 {
		return true
	}
	v.Map().RemoveIf(func(k string, nested pcommon.Value) bool {
		full := path + "." + k
		return strings.HasPrefix(full, reservedPrefix) || reservedNested(full, nested)
	})
	return false
}

func (p *pricer) match(model string) *rule {
	for i := range p.rules {
		for _, pat := range p.rules[i].patterns {
			if pat.matches(model) {
				return &p.rules[i]
			}
		}
	}
	return nil
}

// count reads a token count: an int or a double, anything else is 0 (spec §4).
func count(attrs pcommon.Map, name string) float64 {
	if name == "" {
		return 0
	}
	v, ok := attrs.Get(name)
	if !ok {
		return 0
	}
	switch v.Type() {
	case pcommon.ValueTypeInt:
		return float64(v.Int())
	case pcommon.ValueTypeDouble:
		return v.Double()
	}
	return 0
}

func (p *pricer) price(attrs pcommon.Map) {
	removeReserved(attrs)
	if p.attrs.Model == "" {
		return
	}
	m, ok := attrs.Get(p.attrs.Model)
	if !ok || m.Type() != pcommon.ValueTypeStr {
		return
	}
	r := p.match(m.Str())
	if r == nil {
		return
	}
	in, out := count(attrs, p.attrs.In), count(attrs, p.attrs.Out)
	read, write := count(attrs, p.attrs.CacheRead), count(attrs, p.attrs.CacheWrite)
	if in == 0 && out == 0 && read == 0 && write == 0 {
		return
	}
	// Input tokens include the cache tokens unless these exceed them (spec §4).
	if read+write <= in {
		in = in - read - write
	}
	costIn, costOut := in*r.in/1e6, out*r.out/1e6
	costRead, costWrite := read*r.read/1e6, write*r.write/1e6
	total := ((costIn + costOut) + costRead) + costWrite

	attrs.EnsureCapacity(attrs.Len() + 5)
	put(attrs, p.output.In, costIn)
	put(attrs, p.output.Out, costOut)
	put(attrs, p.output.CacheRead, costRead)
	put(attrs, p.output.CacheWrite, costWrite)
	put(attrs, p.output.Total, total)
}

func put(attrs pcommon.Map, name string, v float64) {
	if name != "" {
		attrs.PutDouble(name, v)
	}
}
