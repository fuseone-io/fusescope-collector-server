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

// byLabels replaces each row's fingerprint with the labels of its series, since
// fingerprints differ by design (metrics §4).
func byLabels(t *testing.T, rows, series []row) []row {
	t.Helper()
	labels := map[string]string{}
	for _, s := range series {
		fp := fmt.Sprint(s["fingerprint"])
		if l, ok := labels[fp]; ok && l != s["labels"] {
			t.Fatalf("fingerprint %s used by two label sets", fp)
		}
		labels[fp] = s["labels"].(string)
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

func TestMetricsMatchObservedRows(t *testing.T) {
	enc := encodeObservedMetrics(t)
	wantSeries := readObserved(t, "signoz_metrics", "time_series_v4")
	gotSeries := blockRows(enc.series.input())

	tests := []struct {
		table     string
		want, got []row
		key       func(row) string
		ignore    []string
	}{
		{"time_series_v4", byLabels(t, wantSeries, wantSeries), byLabels(t, gotSeries, gotSeries),
			func(r row) string { return fmt.Sprint(r["labels"]) }, []string{"inserted_at_unix_milli"}},
		{"samples_v4", byLabels(t, readObserved(t, "signoz_metrics", "samples_v4"), wantSeries), byLabels(t, blockRows(enc.samples.input()), gotSeries),
			func(r row) string { return fmt.Sprint(r["fingerprint"], r["unix_milli"]) }, []string{"inserted_at_unix_milli"}},
		{"metadata", withoutSpanMetrics(readObserved(t, "signoz_metrics", "metadata")), withoutSpanMetrics(blockRows(enc.meta.input())),
			func(r row) string {
				return fmt.Sprint(r["temporality"], r["metric_name"], r["attr_name"], r["attr_type"], r["attr_datatype"], r["attr_string_value"])
			}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			compareRows(t, indexRows(t, tt.want, tt.key, tt.ignore), indexRows(t, tt.got, tt.key, tt.ignore))
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
	if err := enc.encode(md); err != nil {
		t.Fatal(err)
	}
	return enc
}
