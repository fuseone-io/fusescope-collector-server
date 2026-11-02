// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozllmpricingprocessor

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/fuseone-io/fusescope-collector-server/components/processor/signozllmpricingprocessor/internal/metadata"
)

const (
	model      = "gen_ai.request.model"
	in         = "gen_ai.usage.input_tokens"
	out        = "gen_ai.usage.output_tokens"
	cacheRead  = "gen_ai.usage.cache_read.input_tokens"
	cacheWrite = "gen_ai.usage.cache_creation.input_tokens"
	costIn     = "signoz.gen_ai.usage.input_tokens.cost"
	costOut    = "signoz.gen_ai.usage.output_tokens.cost"
	costRead   = "signoz.gen_ai.usage.cache_read.input_tokens.cost"
	costWrite  = "signoz.gen_ai.usage.cache_write.input_tokens.cost"
	costTotal  = "signoz.gen_ai.usage.tokens.cost"
)

func testConfig(rules ...Rule) *Config {
	return &Config{
		Attrs:          Attrs{Model: model, In: in, Out: out, CacheRead: cacheRead, CacheWrite: cacheWrite},
		DefaultPricing: DefaultPricing{Rules: rules},
		OutputAttrs:    OutputAttrs{In: costIn, Out: costOut, CacheRead: costRead, CacheWrite: costWrite, Total: costTotal},
	}
}

type kv struct {
	k string
	v any
}

func putAll(m pcommon.Map, attrs []kv) {
	for _, a := range attrs {
		switch v := a.v.(type) {
		case string:
			m.PutStr(a.k, v)
		case int:
			m.PutInt(a.k, int64(v))
		case float64:
			m.PutDouble(a.k, v)
		case bool:
			m.PutBool(a.k, v)
		default:
			panic(fmt.Sprintf("unsupported %T", v))
		}
	}
}

// render lists a map in order, so tests check the order attributes are written in.
func render(m pcommon.Map) string {
	var b strings.Builder
	for k, v := range m.All() {
		fmt.Fprintf(&b, "%s=%s:%s ", k, v.Type(), v.AsString())
	}
	return strings.TrimSpace(b.String())
}

func run(t *testing.T, cfg *Config, resource, span []kv) ptrace.Traces {
	t.Helper()
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	putAll(rs.Resource().Attributes(), resource)
	putAll(rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty().Attributes(), span)
	sink := new(consumertest.TracesSink)
	p, err := NewFactory().CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	if len(sink.AllTraces()) != 1 {
		t.Fatalf("got %d batches", len(sink.AllTraces()))
	}
	return sink.AllTraces()[0]
}

func spanAttrs(td ptrace.Traces) pcommon.Map {
	return td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes()
}

// costs renders the five default cost attributes as the processor writes them.
func costs(i, o, r, w float64) string {
	return fmt.Sprintf("%s=Double:%s %s=Double:%s %s=Double:%s %s=Double:%s %s=Double:%s",
		costIn, d(i), costOut, d(o), costRead, d(r), costWrite, d(w), costTotal, d(((i+o)+r)+w))
}

// c is a cost computed at run time, as the processor does; a constant
// expression would be rounded once, exactly.
func c(tokens, price float64) float64 { return tokens * price / 1e6 }

func d(f float64) string {
	v := pcommon.NewValueDouble(f)
	return v.AsString()
}

var gpt4o = Rule{Name: "gpt-4o", Pattern: []string{"gpt-4o*"}, Cache: &Cache{Mode: "subtract", Read: 2.5, Write: 3.75}, In: 5, Out: 15}

// Behaviour of spec §3-§5; the golden test replays the full observation.
func TestPricing(t *testing.T) {
	usage := func(m any, i, o, r, w any) []kv {
		var attrs []kv
		for _, a := range []kv{{model, m}, {in, i}, {out, o}, {cacheRead, r}, {cacheWrite, w}} {
			if a.v != nil {
				attrs = append(attrs, a)
			}
		}
		return attrs
	}
	tests := []struct {
		name  string
		rules []Rule
		span  []kv
		want  string // after the span's own attributes
	}{
		{"subtract cache from input", nil, usage("gpt-4o", 1000, 200, 300, 50),
			costs(c(650, 5), c(200, 15), c(300, 2.5), c(50, 3.75))},
		{"cache equal to input", nil, usage("gpt-4o", 350, 0, 300, 50),
			costs(0, 0, c(300, 2.5), c(50, 3.75))},
		{"cache above input keeps input whole", nil, usage("gpt-4o", 320, 0, 300, 50),
			costs(c(320, 5), 0, c(300, 2.5), c(50, 3.75))},
		{"negative cache is subtracted", nil, usage("gpt-4o", 1000, 0, -100, nil),
			costs(c(1100, 5), 0, c(-100, 2.5), 0)},
		{"doubles count as they are", nil, usage("gpt-4o", 1000.0, 200.5, 300.25, 50.75),
			costs(c(649, 5), c(200.5, 15), c(300.25, 2.5), c(50.75, 3.75))},
		{"strings and bools count as zero", nil, usage("gpt-4o", "1000", 200, true, nil),
			costs(0, c(200, 15), 0, 0)},
		{"all counts zero is unpriced", nil, usage("gpt-4o", 0, 0.0, 0, 0), ""},
		{"no counts is unpriced", nil, usage("gpt-4o", nil, nil, nil, nil), ""},
		{"cache only is priced", nil, usage("gpt-4o", nil, nil, 300, nil), costs(0, 0, c(300, 2.5), 0)},
		{"cancelling counts are priced", nil, usage("gpt-4o", -5, 5, nil, nil), costs(c(-5, 5), c(5, 15), 0, 0)},
		{"NaN propagates", nil, usage("gpt-4o", math.NaN(), 200, nil, nil), costs(math.NaN(), c(200, 15), 0, 0)},
		{"unknown model", nil, usage("foo", 1000, 200, nil, nil), ""},
		{"int model is not matched", []Rule{{Pattern: []string{"42"}, In: 1}}, usage(42, 1000, 200, nil, nil), ""},
		{"case-sensitive", nil, usage("GPT-4O", 1000, 200, nil, nil), ""},
		{"star does not cross a slash", []Rule{{Pattern: []string{"meta/*/llama"}, In: 1}},
			usage("meta/a/b/llama", 1, nil, nil, nil), ""},
		{"star within a segment", []Rule{{Pattern: []string{"meta/*/llama"}, In: 1}},
			usage("meta/a/llama", 1, nil, nil, nil), costs(c(1, 1), 0, 0, 0)},
		{"prefix star does not cross a slash", nil, usage("gpt-4o/x", 1, nil, nil, nil), ""},
		{"bang is not a negation", []Rule{{Pattern: []string{"g-[!3]"}, In: 1}}, usage("g-3", 1, nil, nil, nil), costs(c(1, 1), 0, 0, 0)},
		{"escaped star is literal", []Rule{{Pattern: []string{`a\*b`}, In: 1}}, usage("aXb", 1, nil, nil, nil), ""},
		{"literal pattern", []Rule{{Pattern: []string{"o1"}, In: 1}}, usage("o1-mini", 1, nil, nil, nil), ""},
		{"first match wins", []Rule{gpt4o, {Pattern: []string{"gpt-4o-mini*"}, In: 1}}, usage("gpt-4o-mini", 1000, nil, nil, nil),
			costs(c(1000, 5), 0, 0, 0)},
		{"second pattern of a rule", []Rule{{Pattern: []string{"x", "y*"}, In: 1}}, usage("yes", 1, nil, nil, nil), costs(c(1, 1), 0, 0, 0)},
		{"rule without cache prices cache at zero", []Rule{{Pattern: []string{"m"}, In: 1, Out: 2}}, usage("m", 1000, 10, 300, 50),
			costs(c(650, 1), c(10, 2), 0, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rules := tt.rules
			if rules == nil {
				rules = []Rule{gpt4o}
			}
			got := render(spanAttrs(run(t, testConfig(rules...), nil, tt.span)))
			own := pcommon.NewMap()
			putAll(own, tt.span)
			want := strings.TrimSpace(render(own) + " " + tt.want)
			if got != want {
				t.Fatalf("got  %s\nwant %s", got, want)
			}
		})
	}
}

// The fast paths agree with path.Match.
func TestPatternMatchesPathMatch(t *testing.T) {
	patterns := []string{"gpt-4o", "gpt-4o*", "*", "", "a/*", "gpt-4o-*-mini", "o1?", "model\\*star", "a*b*", "[ab]*", "x\\", "gpt-4o**"}
	models := []string{"", "gpt-4o", "gpt-4o-mini", "gpt-4o/x", "gpt-4", "a/b", "a/b/c", "o1x", "o1", "model*star", "ab", "acb", "bz", "x\\"}
	for _, ps := range patterns {
		p := compilePattern(ps)
		for _, m := range models {
			want, err := path.Match(ps, m)
			if got := p.matches(m); got != (want && err == nil) {
				t.Errorf("%q against %q: %v, path.Match %v (%v)", ps, m, got, want, err)
			}
		}
	}
}

func TestReservedPrefixIsRemoved(t *testing.T) {
	resource := []kv{{"service.name", "a"}, {"signoz.gen_ai.x", 1.0}, {"signoz.other", 1.0}}
	span := []kv{{model, "foo"}, {in, 1}, {costTotal, 99.0}, {"signoz.gen_ai", 1.0}, {"Signoz.gen_ai.x", 1.0}, {"x.cost", 1.0}}
	td := run(t, testConfig(gpt4o), resource, span)
	if got, want := render(td.ResourceSpans().At(0).Resource().Attributes()), "service.name=Str:a signoz.other=Double:1"; got != want {
		t.Errorf("resource: %s, want %s", got, want)
	}
	want := "gen_ai.request.model=Str:foo gen_ai.usage.input_tokens=Int:1 signoz.gen_ai=Double:1 Signoz.gen_ai.x=Double:1 x.cost=Double:1"
	if got := render(spanAttrs(td)); got != want {
		t.Errorf("span: %s, want %s", got, want)
	}
}

// Maps whose flattened keys, as the ClickHouse exporter writes them, fall
// under the prefix lose those entries too (spec §5, a deliberate deviation).
func TestReservedPrefixInNestedMaps(t *testing.T) {
	td := ptrace.NewTraces()
	attrs := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().Attributes()
	attrs.PutEmptyMap("signoz.gen_ai").PutDouble("usage.tokens.cost", 999)
	s := attrs.PutEmptyMap("signoz")
	s.PutDouble("gen_ai.usage.input_tokens.cost", 999)
	s.PutEmptyMap("gen_ai").PutDouble("z", 1)
	s.PutEmptyMap("gen").PutDouble("ai", 1)
	s.PutDouble("other", 1)
	attrs.PutEmptyMap("other").PutDouble("signoz.gen_ai.w", 1)
	if _, err := newPricer(testConfig(gpt4o)).processTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	want := `{"other":{"signoz.gen_ai.w":1},"signoz":{"gen":{"ai":1},"other":1}}`
	if got, _ := json.Marshal(attrs.AsRaw()); string(got) != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

// The exporter joins an empty prefix transparently: {"": {"": {k: v}}}
// lands as k, while "signoz." + "" + "." + "gen_ai.x" does not reach the prefix.
func TestReservedPrefixUnderEmptyKeys(t *testing.T) {
	td := ptrace.NewTraces()
	attrs := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().Attributes()
	e := attrs.PutEmptyMap("")
	e.PutDouble("signoz.gen_ai.usage.tokens.cost", 999)
	e.PutEmptyMap("").PutEmptyMap("signoz").PutDouble("gen_ai.x", 1)
	e.PutDouble("keep", 1)
	attrs.PutEmptyMap("signoz").PutEmptyMap("").PutDouble("gen_ai.x", 1)
	if _, err := newPricer(testConfig(gpt4o)).processTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	want := `{"":{"":{"signoz":{}},"keep":1},"signoz":{"":{"gen_ai.x":1}}}`
	if got, _ := json.Marshal(attrs.AsRaw()); string(got) != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestExistingCostsAreReplaced(t *testing.T) {
	td := run(t, testConfig(gpt4o), nil, []kv{{model, "gpt-4o"}, {costTotal, 99.0}, {in, 1000}, {costIn, "x"}})
	want := "gen_ai.request.model=Str:gpt-4o gen_ai.usage.input_tokens=Int:1000 " + costs(c(1000, 5), 0, 0, 0)
	if got := render(spanAttrs(td)); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestOutputNames(t *testing.T) {
	cfg := testConfig(gpt4o)
	cfg.OutputAttrs = OutputAttrs{In: "", Out: "c.out", CacheRead: "c.read", Total: "c.total"}
	cfg.Attrs.CacheRead = ""
	td := run(t, cfg, nil, []kv{{model, "gpt-4o"}, {in, 1000}, {out, 10}, {cacheRead, 300}, {"c.out", 1.0}})
	// c.out keeps its place; the unread cache count is not subtracted; the input cost is in the total.
	want := fmt.Sprintf("gen_ai.request.model=Str:gpt-4o gen_ai.usage.input_tokens=Int:1000 gen_ai.usage.output_tokens=Int:10 "+
		"gen_ai.usage.cache_read.input_tokens=Int:300 c.out=Double:%s c.read=Double:0 c.total=Double:%s", d(c(10, 15)), d(c(1000, 5)+c(10, 15)))
	if got := render(spanAttrs(td)); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestOnlyTraces(t *testing.T) {
	f := NewFactory()
	if _, err := f.CreateLogs(context.Background(), processortest.NewNopSettings(metadata.Type), testConfig(), consumertest.NewNop()); err == nil {
		t.Error("logs are supported")
	}
	if _, err := f.CreateMetrics(context.Background(), processortest.NewNopSettings(metadata.Type), testConfig(), consumertest.NewNop()); err == nil {
		t.Error("metrics are supported")
	}
}
