// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopespanmetricsconnector

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/fuseone-io/fusescope-collector-server/components/connector/fusescopespanmetricsconnector/internal/metadata"
)

var observed = filepath.Join("..", "..", "..", "docs", "spec", "observed", "0.145.0")

// sample is one stored sample of a span metric: the series as ClickHouse
// labels it, without the labels the exporter adds, and the summed value.
type sample struct {
	name, labels string
	minute       int64
}

// withoutGenerated drops the labels the exporter generates, and the collector
// identity, which differs by design (apm-metrics §6).
func withoutGenerated(labels map[string]string) string {
	out := map[string]string{}
	for k, v := range labels {
		switch k {
		case "__name__", "__temporality__", "__scope.name__", "__scope.schema_url__", "__scope.version__":
			continue
		case collectorIDKey, "resource_" + collectorIDKey:
			v = "<collector>"
		}
		// Invalid UTF-8 is compared as one replacement per run on both sides.
		out[k] = strings.ToValidUTF8(v, "\ufffd")
	}
	b, _ := json.Marshal(out)
	return string(b)
}

func readJSONL(t *testing.T, table string) []map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(observed, "rows", table+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []map[string]any
	for _, line := range strings.Split(string(b), "\n") {
		if line == "" {
			continue
		}
		var r map[string]any
		if err := json.Unmarshal([]byte(strings.ToValidUTF8(line, "�")), &r); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r)
	}
	return rows
}

// observedSamples sums the reference's span-metric samples per series and minute.
func observedSamples(t *testing.T) map[sample]float64 {
	t.Helper()
	series := map[string]sample{}
	for _, r := range readJSONL(t, "signoz_metrics.time_series_v4") {
		name := r["metric_name"].(string)
		if !strings.HasPrefix(name, "signoz_") {
			continue
		}
		var labels map[string]string
		if err := json.Unmarshal([]byte(r["labels"].(string)), &labels); err != nil {
			t.Fatal(err)
		}
		series[fmt.Sprint(r["fingerprint"])] = sample{name: name, labels: withoutGenerated(labels)}
	}
	out := map[sample]float64{}
	for _, r := range readJSONL(t, "signoz_metrics.samples_v4") {
		s, ok := series[fmt.Sprint(r["fingerprint"])]
		if !ok {
			continue
		}
		s.minute = int64(r["unix_milli"].(float64))
		out[s] += r["value"].(float64)
	}
	return out
}

// storedSamples turns the connector's metrics into the samples the exporter
// stores: a histogram becomes cumulative .bucket rows, .count and .sum
// (clickhouse-metrics §2).
func storedSamples(md pmetric.Metrics) map[sample]float64 {
	out := map[sample]float64{}
	add := func(name string, labels map[string]string, ts int64, v float64) {
		out[sample{name: name, labels: withoutGenerated(labels), minute: ts / 1e6}] += v
	}
	for _, rm := range md.ResourceMetrics().All() {
		for _, sm := range rm.ScopeMetrics().All() {
			for _, m := range sm.Metrics().All() {
				switch m.Type() {
				case pmetric.MetricTypeSum:
					for _, p := range m.Sum().DataPoints().All() {
						add(m.Name(), stringMap(p.Attributes().AsRaw()), int64(p.Timestamp()), p.DoubleValue())
					}
				case pmetric.MetricTypeHistogram:
					for _, p := range m.Histogram().DataPoints().All() {
						labels := stringMap(p.Attributes().AsRaw())
						ts := int64(p.Timestamp())
						add(m.Name()+".count", labels, ts, float64(p.Count()))
						add(m.Name()+".sum", labels, ts, p.Sum())
						var cumulative uint64
						for i := range p.BucketCounts().Len() {
							cumulative += p.BucketCounts().At(i)
							le := "+Inf"
							if i < p.ExplicitBounds().Len() {
								le = strconv.FormatFloat(p.ExplicitBounds().At(i), 'f', -1, 64)
							}
							bucket := maps.Clone(labels)
							bucket["le"] = le
							add(m.Name()+".bucket", bucket, ts, float64(cumulative))
						}
					}
				}
			}
		}
	}
	return out
}

func stringMap(raw map[string]any) map[string]string {
	out := map[string]string{}
	for k, v := range raw {
		out[k] = fmt.Sprint(v)
	}
	return out
}

func readObservedTraces(t *testing.T) []ptrace.Traces {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(observed, "corpus", "traces.json"))
	if err != nil {
		t.Fatal(err)
	}
	td, err := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(b)
	if err != nil {
		t.Fatal(err)
	}
	pb, err := os.ReadFile(filepath.Join(observed, "corpus", "traces-invalid-utf8.pb"))
	if err != nil {
		t.Fatal(err)
	}
	probes, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(pb)
	if err != nil {
		t.Fatal(err)
	}
	return []ptrace.Traces{td, probes}
}

// The connector reproduces every span-metric sample the reference stored for
// the observed corpus, with the same labels and values (apm-metrics).
func TestSpanMetricsMatchObservedRows(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	c, err := NewFactory().CreateTracesToMetrics(context.Background(), connectortest.NewNopSettings(metadata.Type), defaultConfig(), sink)
	if err != nil {
		t.Fatal(err)
	}
	for _, td := range readObservedTraces(t) {
		if err := c.ConsumeTraces(context.Background(), td); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := map[sample]float64{}
	for _, md := range sink.AllMetrics() {
		for k, v := range storedSamples(md) {
			got[k] += v
		}
	}
	want := observedSamples(t)
	if len(want) == 0 {
		t.Fatal("the observation has no span metrics")
	}
	keys := slices.SortedFunc(maps.Keys(want), func(a, b sample) int { return strings.Compare(a.name+a.labels, b.name+b.labels) })
	for _, k := range keys {
		if g, ok := got[k]; !ok {
			t.Errorf("missing %s %s @%d", k.name, k.labels, k.minute)
		} else if diff := g - want[k]; diff > 1e-9 || diff < -1e-9 {
			t.Errorf("%s %s @%d = %v, want %v", k.name, k.labels, k.minute, g, want[k])
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("unexpected %s %s @%d", k.name, k.labels, k.minute)
		}
	}
}
