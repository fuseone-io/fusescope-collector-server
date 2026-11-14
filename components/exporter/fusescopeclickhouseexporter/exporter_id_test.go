// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter/internal/metadatatest"
)

// exporterOf is the exporter label of a data point's attributes, "" without one.
func exporterOf(attrs attribute.Set) string {
	v, _ := attrs.Value("exporter")
	return v.AsString()
}

// The insert metrics of two instances in one process, as the chart runs
// fusescope_clickhouse and fusescope_clickhouse/meter, carry each instance's
// own id (#97).
func TestInsertMetricsNameTheExporterInstance(t *testing.T) {
	tel := componenttest.NewTelemetry()
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })

	signals := metadatatest.NewSettings(tel)
	meter := metadatatest.NewSettings(tel)
	meter.ID = component.MustNewIDWithName("fusescope_clickhouse", "meter")
	meterCfg := createDefaultConfig()
	meterCfg.Mode = modeMeter

	logsExp, err := newLogsExporter(createDefaultConfig(), signals)
	if err != nil {
		t.Fatal(err)
	}
	meterExp, err := newMeterExporter(meterCfg, meter)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	retryable := errors.New("connection reset")
	for _, it := range []*insertTelemetry{logsExp.telemetry, meterExp.telemetry} {
		it.record(ctx, 0, 10*time.Millisecond, 3, nil)
		it.record(ctx, 0, 10*time.Millisecond, 3, retryable)
	}

	want := map[string]string{
		logsTables[0].name:  "fusescope_clickhouse",
		meterTables[0].name: "fusescope_clickhouse/meter",
	}
	sums := func(name string) map[string]string {
		m, err := tel.GetMetric(name)
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]string{}
		for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
			table, _ := dp.Attributes.Value("table")
			got[table.AsString()] = exporterOf(dp.Attributes)
		}
		return got
	}
	for _, name := range []string{errorsMetric, rowsMetric} {
		got := sums(name)
		if len(got) != 2 || got[logsTables[0].name] != want[logsTables[0].name] || got[meterTables[0].name] != want[meterTables[0].name] {
			t.Errorf("%s: exporter by table = %v, want %v", name, got, want)
		}
	}

	m, err := tel.GetMetric(durationMetric)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, dp := range m.Data.(metricdata.Histogram[float64]).DataPoints {
		table, _ := dp.Attributes.Value("table")
		got[table.AsString()] = exporterOf(dp.Attributes)
	}
	if got[logsTables[0].name] != want[logsTables[0].name] || got[meterTables[0].name] != want[meterTables[0].name] {
		t.Errorf("%s: exporter by table = %v, want %v", durationMetric, got, want)
	}
}

// Two ordinary instances drop attribute metadata rows for the same signal
// and reason; only the exporter label keeps their counts apart (#97). The
// meter instance keeps no attribute metadata, so it has no such series.
func TestDroppedAttributesMetadataNameTheExporterInstance(t *testing.T) {
	tel := componenttest.NewTelemetry()
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
	cfg := createDefaultConfig()
	cfg.AttributesMetadata.Enabled = true

	a, err := newAttrsMetadata(cfg, "logs", tel.NewTelemetrySettings(), "fusescope_clickhouse/a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := newAttrsMetadata(cfg, "logs", tel.NewTelemetrySettings(), "fusescope_clickhouse/b")
	if err != nil {
		t.Fatal(err)
	}
	a.dropped(dropQueue, 2)
	b.dropped(dropQueue, 5)

	m, err := tel.GetMetric("otelcol_exporter_fusescope_clickhouse_attributes_metadata_dropped")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
		got[exporterOf(dp.Attributes)] = dp.Value
	}
	if want := map[string]int64{"fusescope_clickhouse/a": 2, "fusescope_clickhouse/b": 5}; len(got) != 2 || got["fusescope_clickhouse/a"] != 2 || got["fusescope_clickhouse/b"] != 5 {
		t.Errorf("dropped by exporter = %v, want %v", got, want)
	}
}
