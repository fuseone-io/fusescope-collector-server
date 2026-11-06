// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// meterMetrics is what fusescope_meter emits for one set: a count and a size.
func meterMetrics(ts time.Time, labels map[string]string, count, size int64) pmetric.Metrics {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("signozmeterconnector")
	for _, m := range []struct {
		name, unit, desc string
		value            int64
	}{
		{"signoz.meter.span.count", "1", "The number of spans observed.", count},
		{"signoz.meter.span.size", "By", "The size of spans observed.", size},
	} {
		mt := sm.Metrics().AppendEmpty()
		mt.SetName(m.name)
		mt.SetUnit(m.unit)
		mt.SetDescription(m.desc)
		sum := mt.SetEmptySum()
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		sum.SetIsMonotonic(true)
		p := sum.DataPoints().AppendEmpty()
		p.SetStartTimestamp(pcommon.NewTimestampFromTime(ts.Add(-time.Hour)))
		p.SetTimestamp(pcommon.NewTimestampFromTime(ts))
		p.SetIntValue(m.value)
		for k, v := range labels {
			p.Attributes().PutStr(k, v)
		}
	}
	return md
}

type meterRow struct {
	temporality, name, description, unit, typ string
	monotonic                                 bool
	labels                                    string
	fingerprint                               uint64
	unixMilli                                 int64
	value                                     float64
}

func encodeMeter(t *testing.T, md pmetric.Metrics) []meterRow {
	t.Helper()
	e := newMeterEncoder()
	if err := e.encode(md, func() error { t.Fatal("one block per request"); return nil }); err != nil {
		t.Fatal(err)
	}
	c := e.rows
	var rows []meterRow
	for i := range c.value.Rows() {
		rows = append(rows, meterRow{
			temporality: c.temporality.Row(i), name: c.metricName.Row(i), description: c.description.Row(i),
			unit: c.unit.Row(i), typ: c.metricType.Row(i), monotonic: c.isMonotonic.Row(i), labels: c.labels.Row(i),
			fingerprint: c.fingerprint.Row(i), unixMilli: c.unixMilli.Row(i), value: c.value.Row(i),
		})
	}
	return rows
}

// The rows of signoz_meter.samples (meter §6), with labels as the reference
// wrote them [obs: samples.tsv].
func TestMeterRows(t *testing.T) {
	ts := time.Date(2026, 11, 6, 3, 10, 50, 0, time.UTC)
	rows := encodeMeter(t, meterMetrics(ts, map[string]string{
		"service.name": "obs-keyid", "host.name": "obs-host", "connector_id": "d19a3b03-7f4b-40ff-80aa-e25063b2cbff",
	}, 1, 346))
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	want := []meterRow{
		{"Delta", "signoz.meter.span.count", "The number of spans observed.", "1", "Sum", true,
			`{"__name__":"signoz.meter.span.count","__scope.name__":"signozmeterconnector","__scope.schema_url__":"","__scope.version__":"","__temporality__":"Delta","connector_id":"d19a3b03-7f4b-40ff-80aa-e25063b2cbff","host.name":"obs-host","service.name":"obs-keyid"}`,
			0, ts.UnixMilli(), 1},
		{"Delta", "signoz.meter.span.size", "The size of spans observed.", "By", "Sum", true,
			`{"__name__":"signoz.meter.span.size","__scope.name__":"signozmeterconnector","__scope.schema_url__":"","__scope.version__":"","__temporality__":"Delta","connector_id":"d19a3b03-7f4b-40ff-80aa-e25063b2cbff","host.name":"obs-host","service.name":"obs-keyid"}`,
			0, ts.UnixMilli(), 346},
	}
	for i := range want {
		got := rows[i]
		if got.fingerprint == 0 {
			t.Errorf("row %d has no fingerprint", i)
		}
		got.fingerprint = 0
		if got != want[i] {
			t.Errorf("row %d\n got %+v\nwant %+v", i, got, want[i])
		}
	}
}

func TestMeterFingerprint(t *testing.T) {
	ts := time.Unix(1_800_000_000, 0)
	fp := func(labels map[string]string, at time.Time) uint64 {
		return encodeMeter(t, meterMetrics(at, labels, 1, 1))[0].fingerprint
	}
	a := map[string]string{"service.name": "a", "connector_id": "x"}
	if fp(a, ts) != fp(a, ts.Add(time.Hour)) {
		t.Error("the fingerprint depends on time")
	}
	if fp(a, ts) == fp(map[string]string{"service.name": "b", "connector_id": "x"}, ts) {
		t.Error("two label sets share a fingerprint")
	}
	rows := encodeMeter(t, meterMetrics(ts, a, 1, 1))
	if rows[0].fingerprint == rows[1].fingerprint {
		t.Error("the count and the size share a fingerprint")
	}
}

// Only number points are rows; a meter instance sees nothing else, so others
// are dropped rather than refused.
func TestMeterSkipsOtherTypes(t *testing.T) {
	md := pmetric.NewMetrics()
	ms := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	h := ms.AppendEmpty()
	h.SetName("h")
	h.SetEmptyHistogram().DataPoints().AppendEmpty()
	g := ms.AppendEmpty()
	g.SetName("g")
	g.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(2.5)
	rows := encodeMeter(t, md)
	if len(rows) != 1 || rows[0].name != "g" || rows[0].typ != "Gauge" || rows[0].temporality != "Unspecified" || rows[0].value != 2.5 {
		t.Fatalf("rows = %+v", rows)
	}
}

func TestMeterModeWritesOnlyMetrics(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.Endpoint = "clickhouse:9440"
	cfg.Mode = modeMeter
	f := NewFactory()
	set := exportertest.NewNopSettings(f.Type())
	if _, err := f.CreateTraces(context.Background(), set, cfg); err == nil || !strings.Contains(err.Error(), "mode meter writes only metrics") {
		t.Errorf("traces: err = %v", err)
	}
	if _, err := f.CreateLogs(context.Background(), set, cfg); err == nil || !strings.Contains(err.Error(), "mode meter writes only metrics") {
		t.Errorf("logs: err = %v", err)
	}
	if _, err := f.CreateMetrics(context.Background(), set, cfg); err != nil {
		t.Errorf("metrics: err = %v", err)
	}
	m, err := newMeterExporter(cfg, componenttest.NewNopTelemetrySettings())
	if err != nil {
		t.Fatal(err)
	}
	if len(m.bodies) != 1 || m.bodies[0] != `INSERT INTO signoz_meter.distributed_samples `+
		`("temporality","metric_name","description","unit","type","is_monotonic","labels","fingerprint","unix_milli","value") VALUES` {
		t.Errorf("bodies = %q", m.bodies)
	}
}

// Every row the reference wrote to signoz_meter.samples is reproduced from the
// point fusescope_meter would emit for it: the same columns and, byte for byte,
// the same labels [obs: docs/spec/observed/meter-metadata/samples.tsv].
func TestMeterRowsMatchObserved(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "spec", "observed", "meter-metadata", "samples.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 50 {
		t.Fatalf("observed rows = %d", len(lines)-1)
	}
	for _, line := range lines[1:] {
		f := strings.Split(line, "\t")
		name, temporality, typ, monotonic, unit, description, at, value, labels := f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8]
		ts, err := time.Parse(time.DateTime, at)
		if err != nil {
			t.Fatal(err)
		}
		var all map[string]string
		if err := json.Unmarshal([]byte(labels), &all); err != nil {
			t.Fatal(err)
		}
		md := pmetric.NewMetrics()
		sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
		sm.Scope().SetName(all["__scope.name__"])
		m := sm.Metrics().AppendEmpty()
		m.SetName(name)
		m.SetUnit(unit)
		m.SetDescription(description)
		sum := m.SetEmptySum()
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		sum.SetIsMonotonic(true)
		p := sum.DataPoints().AppendEmpty()
		p.SetTimestamp(pcommon.NewTimestampFromTime(ts))
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		p.SetIntValue(v)
		for k, v := range all {
			if !strings.HasPrefix(k, "__") {
				p.Attributes().PutStr(k, v)
			}
		}
		rows := encodeMeter(t, md)
		if len(rows) != 1 {
			t.Fatalf("rows = %d", len(rows))
		}
		got := rows[0]
		if got.labels != labels || got.name != name || got.temporality != temporality || got.typ != typ ||
			strconv.FormatBool(got.monotonic) != monotonic || got.unit != unit || got.description != description ||
			got.unixMilli != ts.UnixMilli() || got.value != float64(v) {
			t.Errorf("observed %s\n got %+v", line, got)
		}
	}
}
