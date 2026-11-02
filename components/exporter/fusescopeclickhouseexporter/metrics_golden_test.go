// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// The observed run also holds the reference's span-metrics output (phase 4).
func isSpanMetric(r row) bool {
	return strings.HasPrefix(fmt.Sprint(r["metric_name"]), "signoz_")
}

// byLabels replaces each row's fingerprint with the labels and attrs of its
// series, since fingerprints differ by design (metrics §4). Series with equal
// labels differ in attrs (probe.collide_resource).
func byLabels(t *testing.T, rows, series []row) []row {
	t.Helper()
	labels := map[string]string{}
	for _, s := range series {
		fp, id := fmt.Sprint(s["fingerprint"]), fmt.Sprint(s["labels"], s["attrs"])
		if l, ok := labels[fp]; ok && l != id {
			t.Fatalf("fingerprint %s used by two series", fp)
		}
		labels[fp] = id
	}
	var out []row
	for _, r := range rows {
		if isSpanMetric(r) {
			continue
		}
		fp := fmt.Sprint(r["fingerprint"])
		l, ok := labels[fp]
		if !ok {
			t.Fatalf("row %v references no series", r)
		}
		c := row{}
		for k, v := range r {
			c[k] = v
		}
		c["fingerprint"] = l
		out = append(out, c)
	}
	return out
}

// metadataCap is the most metadata rows the reference was seen to keep; at
// the cap it drops the rest, which the exporter writes (metrics §5).
const metadataCap = 1000

func TestMetricsMatchObservedRows(t *testing.T) {
	enc := encodeObservedMetrics(t)
	atCap := len(readObserved(t, "signoz_metrics", "metadata")) == metadataCap
	wantSeries := readObserved(t, "signoz_metrics", "time_series_v4")
	gotSeries := blockRows(enc.series.input())

	tests := []struct {
		table     string
		want, got []row
		key       func(row) string
		ignore    []string
	}{
		{"time_series_v4", byLabels(t, wantSeries, wantSeries), byLabels(t, gotSeries, gotSeries),
			func(r row) string { return fmt.Sprint(r["fingerprint"]) }, []string{"inserted_at_unix_milli"}},
		{"samples_v4", byLabels(t, readObserved(t, "signoz_metrics", "samples_v4"), wantSeries), byLabels(t, blockRows(enc.samples.input()), gotSeries),
			func(r row) string { return fmt.Sprint(r["fingerprint"], r["unix_milli"]) }, []string{"inserted_at_unix_milli"}},
		{"metadata", withoutSpanMetrics(readObserved(t, "signoz_metrics", "metadata")), withoutSpanMetrics(blockRows(enc.meta.input())),
			func(r row) string {
				return fmt.Sprint(r["temporality"], r["metric_name"], r["attr_name"], r["attr_type"], r["attr_datatype"], r["attr_string_value"])
			}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			want, got := indexRows(t, tt.want, tt.key, tt.ignore), indexRows(t, tt.got, tt.key, tt.ignore)
			if tt.table == "metadata" && atCap {
				for k := range got {
					if _, ok := want[k]; !ok {
						delete(got, k)
					}
				}
			}
			compareRows(t, want, got)
		})
	}
}

// withoutSpanMetrics also drops the unit of .count metadata, which the
// reference writes arbitrarily (metrics §5).
func withoutSpanMetrics(rows []row) []row {
	var out []row
	for _, r := range rows {
		if isSpanMetric(r) {
			continue
		}
		if strings.HasSuffix(fmt.Sprint(r["metric_name"]), ".count") {
			delete(r, "unit")
		}
		out = append(out, r)
	}
	return out
}

func encodeObservedMetrics(t *testing.T) *metricsEncoder {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(observed, "corpus", "metrics.json"))
	if err != nil {
		t.Fatal(err)
	}
	md, err := (&pmetric.JSONUnmarshaler{}).UnmarshalMetrics(b)
	if err != nil {
		t.Fatal(err)
	}
	enc := newMetricsEncoder()
	if err := enc.encode(md, nil); err != nil {
		t.Fatal(err)
	}
	return enc
}
