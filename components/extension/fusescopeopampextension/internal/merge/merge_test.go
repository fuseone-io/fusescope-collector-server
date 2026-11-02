// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package merge

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/confmap"
)

func parse(t *testing.T, yaml string) Config {
	t.Helper()
	r, err := confmap.NewRetrievedFromYAML([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := r.AsRaw()
	if err != nil {
		t.Fatal(err)
	}
	return raw.(Config)
}

const base = `
receivers: {otlp: {protocols: {grpc: {endpoint: 0.0.0.0:4317}}}}
processors:
  batch: {}
  memory_limiter: {limit_mib: 512}
  signozspanmapper: {groups: []}
  signozllmpricing: {}
exporters: {fusescope_clickhouse: {endpoint: clickhouse:9440}}
service:
  telemetry: {logs: {level: info}}
  pipelines:
    logs: {receivers: [otlp], processors: [memory_limiter, batch], exporters: [fusescope_clickhouse]}
    traces: {receivers: [otlp], processors: [memory_limiter, signozspanmapper, batch, signozllmpricing], exporters: [fusescope_clickhouse]}
`

func TestMerge(t *testing.T) {
	tests := []struct {
		name      string
		push      string
		wantPaths []string
		check     func(t *testing.T, m Config)
	}{
		{"unchanged push", base, nil, nil},
		{"log pipelines added in logs", `
receivers: {otlp: {protocols: {grpc: {endpoint: 0.0.0.0:4317}}}}
processors:
  batch: {}
  memory_limiter: {limit_mib: 512}
  signozspanmapper: {groups: []}
  signozllmpricing: {}
  signozlogspipeline/pipeline_nginx: {operators: []}
exporters: {fusescope_clickhouse: {endpoint: clickhouse:9440}}
service:
  telemetry: {logs: {level: info}}
  pipelines:
    logs: {receivers: [otlp], processors: [memory_limiter, signozlogspipeline/pipeline_nginx, batch], exporters: [fusescope_clickhouse]}
    traces: {receivers: [otlp], processors: [memory_limiter, signozspanmapper, batch, signozllmpricing], exporters: [fusescope_clickhouse]}
`, nil, func(t *testing.T, m Config) {
			got := m["service"].(Config)["pipelines"].(Config)["logs"].(Config)["processors"]
			if want := []any{"memory_limiter", "signozlogspipeline/pipeline_nginx", "batch"}; !reflect.DeepEqual(got, want) {
				t.Errorf("logs processors = %v, want %v", got, want)
			}
			if _, ok := m["processors"].(Config)["signozlogspipeline/pipeline_nginx"]; !ok {
				t.Error("log pipeline block missing")
			}
		}},
		{"span mapper block changes, position kept", `
receivers: {otlp: {protocols: {grpc: {endpoint: 0.0.0.0:4317}}}}
processors:
  batch: {}
  memory_limiter: {limit_mib: 512}
  signozspanmapper: {groups: [{id: g1}]}
  signozllmpricing: {}
exporters: {fusescope_clickhouse: {endpoint: clickhouse:9440}}
service:
  telemetry: {logs: {level: info}}
  pipelines:
    logs: {receivers: [otlp], processors: [memory_limiter, batch], exporters: [fusescope_clickhouse]}
    traces: {receivers: [otlp], processors: [memory_limiter, signozspanmapper, batch, signozllmpricing], exporters: [fusescope_clickhouse]}
`, nil, func(t *testing.T, m Config) {
			if !reflect.DeepEqual(m["processors"].(Config)["signozspanmapper"], Config{"groups": []any{Config{"id": "g1"}}}) {
				t.Error("span mapper block not taken from the push")
			}
		}},
		{"span mapper moved", replace(base, "[memory_limiter, signozspanmapper, batch, signozllmpricing]", "[signozspanmapper, memory_limiter, batch, signozllmpricing]"),
			[]string{"service.pipelines.traces.processors"}, nil},
		{"log pipeline outside logs", replace(base, "[memory_limiter, signozspanmapper, batch, signozllmpricing]", "[memory_limiter, signozspanmapper, signozlogspipeline/pipeline_x, batch, signozllmpricing]"),
			[]string{"service.pipelines.traces.processors"}, nil},
		{"log pipeline listed without a block", replace(base, "[memory_limiter, batch]", "[memory_limiter, signozlogspipeline/pipeline_x, batch]"),
			[]string{"service.pipelines.logs.processors"}, nil},
		{"other processor reordered in logs", replace(base, "[memory_limiter, batch]", "[batch, memory_limiter]"),
			[]string{"service.pipelines.logs.processors"}, nil},
		{"exporter endpoint", replace(base, "endpoint: clickhouse:9440", "endpoint: evil:9440"),
			[]string{"exporters.fusescope_clickhouse.endpoint"}, nil},
		{"receiver added", replace(base, "receivers: {otlp:", "receivers: {zipkin: {}, otlp:"),
			[]string{"receivers.zipkin"}, nil},
		{"pipeline exporters", replace(base, "processors: [memory_limiter, batch], exporters: [fusescope_clickhouse]", "processors: [memory_limiter, batch], exporters: [debug]"),
			[]string{"service.pipelines.logs.exporters"}, nil},
		{"new pipeline", replace(base, "    logs: {", "    logs/2: {receivers: [otlp], exporters: [fusescope_clickhouse]}\n    logs: {"),
			[]string{"service.pipelines.logs/2"}, nil},
		{"other processor block", replace(base, "limit_mib: 512", "limit_mib: 1"),
			[]string{"processors.memory_limiter.limit_mib"}, nil},
		{"telemetry", replace(base, "level: info", "level: debug"),
			[]string{"service.telemetry.logs.level"}, nil},
		{"extension added", base + "extensions: {pprof: {}}\n", []string{"extensions"}, nil},
		// The resolver would expand a reference in a pushed block, reading a
		// local file or variable into telemetry (ADR 0004).
		{"provider reference in the span mapper", replace(base, "signozspanmapper: {groups: []}",
			"signozspanmapper: {groups: [{id: g, attributes: [{target: '${file:/run/secrets/token}'}]}]}"),
			[]string{"processors.signozspanmapper.groups[0].attributes[0].target"}, nil},
		{"reference after an escaped dollar", replace(base, "signozllmpricing: {}", "signozllmpricing: {rules: ['$$${env:TOKEN}']}"),
			[]string{"processors.signozllmpricing.rules[0]"}, nil},
		{"embedded reference in a log pipeline", replace(base, "  batch: {}\n",
			"  batch: {}\n  signozlogspipeline/pipeline_x: {operators: [{value: 'a ${env:TOKEN} b'}]}\n"),
			[]string{"processors.signozlogspipeline/pipeline_x.operators[0].value"}, nil},
		{"escaped reference is literal", replace(base, "signozspanmapper: {groups: []}",
			"signozspanmapper: {groups: [{id: '$${file:/x}', attributes: [{target: 'cost $$5 {}'}]}]}"), nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := Merge(parse(t, base), parse(t, tt.push))
			var rej *Rejection
			switch {
			case tt.wantPaths == nil && err != nil:
				t.Fatalf("Merge rejected: %v", err)
			case tt.wantPaths != nil && !errors.As(err, &rej):
				t.Fatalf("Merge = %v, want a rejection of %v", err, tt.wantPaths)
			case tt.wantPaths != nil && !slices.Equal(rej.Paths, tt.wantPaths):
				t.Fatalf("rejected paths %v, want %v", rej.Paths, tt.wantPaths)
			}
			if tt.check != nil && m != nil {
				tt.check(t, m)
			}
		})
	}
}

// A rejection names paths, never values.
func TestRejectionHidesValues(t *testing.T) {
	_, err := Merge(parse(t, base), parse(t, replace(base, "endpoint: clickhouse:9440", "endpoint: secret-host:9440")))
	if err == nil || contains(err.Error(), "secret-host") || contains(err.Error(), "clickhouse:9440") {
		t.Errorf("error %q should name the path only", err)
	}
}

// Stale log pipelines go away when the push no longer has them.
func TestStaleLogPipelinesRemoved(t *testing.T) {
	b := replace(base, "  batch: {}\n", "  batch: {}\n  signozlogspipeline/pipeline_old: {operators: []}\n")
	b = replace(b, "[memory_limiter, batch]", "[memory_limiter, signozlogspipeline/pipeline_old, batch]")
	m, err := Merge(parse(t, b), parse(t, base))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := m["processors"].(Config)["signozlogspipeline/pipeline_old"]; ok {
		t.Error("stale log pipeline kept")
	}
}

func replace(s, old, repl string) string {
	if !contains(s, old) {
		panic("test fixture: " + old)
	}
	return stringsReplace(s, old, repl)
}

// A section the collector resolves later, such as a ${file:...} reference, is
// kept as the base has it when the push echoes it, never read as empty, and a
// push that changes it is rejected.
func TestReferenceSectionsKept(t *testing.T) {
	const refBase = `
receivers: {otlp: {}}
processors: ${file:/etc/otel/processors.yaml}
exporters: {nop: {}}
service:
  pipelines: ${file:/etc/otel/pipelines.yaml}
`
	const nestedBase = `
receivers: {otlp: {}}
processors: {batch: {}}
exporters: {nop: {}}
service:
  pipelines:
    metrics: ${file:/etc/otel/metrics.yaml}
    traces:
      receivers: [otlp]
      processors: ${file:/etc/otel/traces.yaml}
      exporters: [nop]
`
	tests := []struct {
		name      string
		base      string
		push      string
		wantPaths []string
	}{
		{"echoed", refBase, refBase, nil},
		{"processors changed", refBase, strings.Replace(refBase, "processors.yaml", "other.yaml", 1), []string{"processors"}},
		{"processors expanded", refBase, strings.Replace(refBase, "${file:/etc/otel/processors.yaml}", "{batch: {}}", 1), []string{"processors"}},
		{"pipelines expanded", refBase, strings.Replace(refBase, "${file:/etc/otel/pipelines.yaml}", "{logs: {receivers: [otlp], exporters: [nop]}}", 1), []string{"service.pipelines"}},
		{"nested echoed", nestedBase, nestedBase, nil},
		{"pipeline reference changed", nestedBase, strings.Replace(nestedBase, "metrics.yaml", "other.yaml", 1), []string{"service.pipelines.metrics"}},
		{"processors reference changed", nestedBase, strings.Replace(nestedBase, "traces.yaml", "other.yaml", 1), []string{"service.pipelines.traces.processors"}},
		{"processors reference expanded", nestedBase, strings.Replace(nestedBase, "${file:/etc/otel/traces.yaml}", "[batch]", 1), []string{"service.pipelines.traces.processors"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := Merge(parse(t, tt.base), parse(t, tt.push))
			if tt.wantPaths == nil {
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(m, parse(t, tt.base)) {
					t.Errorf("merged = %v; want the base unchanged", m)
				}
				return
			}
			var rej *Rejection
			if !errors.As(err, &rej) || !slices.Equal(rej.Paths, tt.wantPaths) {
				t.Errorf("err = %v; want rejection of %v", err, tt.wantPaths)
			}
		})
	}
}
