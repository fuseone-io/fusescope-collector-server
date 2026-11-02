// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package merge

import (
	"errors"
	"reflect"
	"slices"
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
