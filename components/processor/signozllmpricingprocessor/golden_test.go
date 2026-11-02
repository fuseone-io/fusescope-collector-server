// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozllmpricingprocessor

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/fuseone-io/fusescope-collector-server/components/processor/signozllmpricingprocessor/internal/metadata"
)

var observed = filepath.Join("..", "..", "..", "docs", "spec", "observed", "signozllmpricing")

func readConf(t *testing.T, path string) *confmap.Conf {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	retrieved, err := confmap.NewRetrievedFromYAML(b)
	if err != nil {
		t.Fatal(err)
	}
	conf, err := retrieved.AsConf()
	if err != nil {
		t.Fatal(err)
	}
	return conf
}

func readCorpus(t *testing.T, path string) ptrace.Traces {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	td, err := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(b)
	if err != nil {
		t.Fatal(err)
	}
	return td
}

// readTraces reads the file exporter's output, one request per line.
func readTraces(t *testing.T, path string) ptrace.Traces {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	all := ptrace.NewTraces()
	for _, line := range bytes.Split(b, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		td, err := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(line)
		if err != nil {
			t.Fatal(err)
		}
		td.ResourceSpans().MoveAndAppendTo(all.ResourceSpans())
	}
	return all
}

func canonical(t *testing.T, td ptrace.Traces) string {
	t.Helper()
	b, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// withDeviations applies to the observed output the one rule where this
// processor deliberately differs from the reference: reserved entries of
// nested maps (spec §5).
func withDeviations(td ptrace.Traces) ptrace.Traces {
	nested := func(attrs pcommon.Map) {
		attrs.RemoveIf(func(k string, v pcommon.Value) bool { return reservedNested(k, v) })
	}
	for _, rs := range td.ResourceSpans().All() {
		nested(rs.Resource().Attributes())
		for _, ss := range rs.ScopeSpans().All() {
			for _, s := range ss.Spans().All() {
				nested(s.Attributes())
			}
		}
	}
	return td
}

// loadCase builds the processor configuration of one observed case the way
// the collector does: unmarshal into the default configuration, then validate.
func loadCase(conf *confmap.Conf) (*Config, error) {
	sub, err := conf.Sub("processors::" + metadata.Type.String())
	if err != nil {
		return nil, err
	}
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	if err := sub.Unmarshal(cfg); err != nil {
		return nil, err
	}
	return cfg, confmap.Validate(cfg)
}

// Every observed configuration starts, or is refused, as it was by the
// reference, and every started one turns the corpus into the observed output
// byte for byte (spec, [obs]).
func TestMatchesObservation(t *testing.T) {
	results := readConf(t, filepath.Join(observed, "results.yaml"))
	cases, err := results.Sub("cases")
	if err != nil {
		t.Fatal(err)
	}
	names := cases.AllKeys()
	var caseNames []string
	for _, k := range names {
		if name, ok := strings.CutSuffix(k, "::start"); ok {
			caseNames = append(caseNames, name)
		}
	}
	sort.Strings(caseNames)
	if len(caseNames) < 20 {
		t.Fatalf("only %d cases in results.yaml", len(caseNames))
	}
	corpus := filepath.Join(observed, "corpus", "traces.json")
	for _, name := range caseNames {
		t.Run(name, func(t *testing.T) {
			conf := readConf(t, filepath.Join(observed, "configs", name+".yaml"))
			pipelines, err := conf.Sub("service::pipelines")
			if err != nil {
				t.Fatal(err)
			}
			var signals []string
			used := false
			for id, p := range pipelines.ToStringMap() {
				signals = append(signals, strings.SplitN(id, "/", 2)[0])
				procs, _ := p.(map[string]any)["processors"].([]any)
				used = used || len(procs) > 0
			}
			if !used {
				t.Skip("control case without the processor")
			}

			cfg, err := loadCase(conf)
			for _, s := range signals {
				if err == nil && s != "traces" {
					err = errors.New("signal not supported")
				}
			}
			want := cases.Get(name + "::start")
			if got := map[bool]string{true: "started", false: "refused"}[err == nil]; got != want {
				t.Fatalf("%s, the reference %s (error: %v)", got, want, err)
			}
			if err != nil {
				return
			}

			sink := new(consumertest.TracesSink)
			p, err := NewFactory().CreateTraces(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
			if err != nil {
				t.Fatal(err)
			}
			if err := p.ConsumeTraces(context.Background(), readCorpus(t, corpus)); err != nil {
				t.Fatal(err)
			}
			got := ptrace.NewTraces()
			for _, td := range sink.AllTraces() {
				td.ResourceSpans().MoveAndAppendTo(got.ResourceSpans())
			}
			if g, w := canonical(t, got), canonical(t, withDeviations(readTraces(t, filepath.Join(observed, "outputs", name+".jsonl")))); g != w {
				t.Errorf("output differs from the observation\ngot  %s\nwant %s", g, w)
			}
		})
	}
}
