// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"context"
	"errors"
	"maps"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter/internal/metadata"
	"github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter/internal/metadatatest"
)

// metaNow is 2026-11-06 02:35 UTC, in the window of 00:00.
var metaNow = time.Date(2026, 11, 6, 2, 35, 0, 0, time.UTC)

func window(h int) int64 {
	return time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC).Add(time.Duration(h) * time.Hour).UnixMilli()
}

type metaTestRow struct {
	window   int64
	source   string
	resource map[string]string
	attrs    map[string]string
	resFP    uint64
	attrsFP  uint64
}

// metaFake answers the metadata inserts with reply and records their rows.
type metaFake struct {
	mu    sync.Mutex
	rows  []metaTestRow
	reply error
}

func (f *metaFake) do(_ context.Context, q ch.Query) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reply != nil {
		return f.reply
	}
	in := q.Input
	unix := in[0].Data.(*proto.ColUInt64)
	source := in[1].Data.(*proto.ColStr)
	resFP := in[2].Data.(*proto.ColUInt64)
	attrsFP := in[3].Data.(*proto.ColUInt64)
	res := in[4].Data.(*strMap)
	attrs := in[5].Data.(*strMap)
	for i := range unix.Rows() {
		f.rows = append(f.rows, metaTestRow{
			window: int64(unix.Row(i)), source: source.Row(i), resource: res.Row(i), attrs: attrs.Row(i),
			resFP: resFP.Row(i), attrsFP: attrsFP.Row(i),
		})
	}
	return nil
}

func (f *metaFake) take() []metaTestRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	rows := f.rows
	f.rows = nil
	return rows
}

type metaFixture struct {
	m    *attrsMetadata
	fake *metaFake
	tel  *componenttest.Telemetry
	now  time.Time
}

func newMetaFixture(t testing.TB, signal string, edit func(*AttributesMetadataConfig)) *metaFixture {
	t.Helper()
	cfg := createDefaultConfig()
	cfg.Endpoint = "testing.invalid:9440"
	cfg.AttributesMetadata.Enabled = true
	if edit != nil {
		edit(&cfg.AttributesMetadata)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	tel := componenttest.NewTelemetry()
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
	m, err := newAttrsMetadata(cfg, signal, metadatatest.NewSettings(tel).TelemetrySettings)
	if err != nil {
		t.Fatal(err)
	}
	f := &metaFixture{m: m, fake: &metaFake{}, tel: tel, now: metaNow}
	m.client.do = f.fake.do
	m.now = func() time.Time { return f.now }
	return f
}

// flush inserts the queue and returns what was written.
func (f *metaFixture) flush() []metaTestRow {
	f.m.flush(context.Background())
	return f.fake.take()
}

func (f *metaFixture) assertDropped(t *testing.T, want map[string]int64) {
	t.Helper()
	if len(want) == 0 {
		assertAbsent(t, f.tel, "otelcol_exporter_fusescope_clickhouse_attributes_metadata_dropped")
		return
	}
	var dps []metricdata.DataPoint[int64]
	for reason, n := range want {
		dps = append(dps, metricdata.DataPoint[int64]{
			Attributes: attribute.NewSet(attribute.String("signal", f.m.signal), attribute.String("reason", reason)), Value: n,
		})
	}
	metadatatest.AssertEqualExporterFusescopeClickhouseAttributesMetadataDropped(t, f.tel, dps,
		metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreExemplars())
}

func putAll(m pcommon.Map, kv map[string]string) {
	for k, v := range kv {
		m.PutStr(k, v)
	}
}

func spans(res map[string]string, start time.Time, sets ...map[string]string) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	putAll(rs.Resource().Attributes(), res)
	ss := rs.ScopeSpans().AppendEmpty()
	for i, attrs := range sets {
		s := ss.Spans().AppendEmpty()
		s.SetName("op" + strconv.Itoa(i))
		s.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
		putAll(s.Attributes(), attrs)
	}
	return td
}

func logRecords(res map[string]string, ts, observed time.Time, sets ...map[string]string) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	putAll(rl.Resource().Attributes(), res)
	sl := rl.ScopeLogs().AppendEmpty()
	for _, attrs := range sets {
		l := sl.LogRecords().AppendEmpty()
		if !ts.IsZero() {
			l.SetTimestamp(pcommon.NewTimestampFromTime(ts))
		}
		if !observed.IsZero() {
			l.SetObservedTimestamp(pcommon.NewTimestampFromTime(observed))
		}
		l.Body().SetStr("the body is not metadata")
		putAll(l.Attributes(), attrs)
	}
	return ld
}

// Rows per signal (attributes-metadata §2).
func TestMetadataRows(t *testing.T) {
	old := metaNow.Add(-7 * time.Hour) // window 18:00 of the day before
	t.Run("spans: attributes plus the span name", func(t *testing.T) {
		f := newMetaFixture(t, "traces", nil)
		f.m.observeTraces(spans(map[string]string{"service.name": "a", "host.name": "h"}, old,
			map[string]string{"peer.service": "p", "name": "overridden"}, nil))
		rows := f.flush()
		want := []metaTestRow{
			{window: window(-6), source: "traces", resource: map[string]string{"service.name": "a", "host.name": "h"}, attrs: map[string]string{"peer.service": "p", "name": "op0"}},
			{window: window(-6), source: "traces", resource: map[string]string{"service.name": "a", "host.name": "h"}, attrs: map[string]string{"name": "op1"}},
		}
		assertRows(t, rows, want)
	})
	t.Run("logs: attributes, not the body; a resource without attributes", func(t *testing.T) {
		f := newMetaFixture(t, "logs", nil)
		f.m.observeLogs(logRecords(nil, old, time.Time{}, map[string]string{"marker": "m"}, nil))
		assertRows(t, f.flush(), []metaTestRow{
			{window: window(-6), source: "logs", resource: map[string]string{}, attrs: map[string]string{"marker": "m"}},
			{window: window(-6), source: "logs", resource: map[string]string{}, attrs: map[string]string{}},
		})
	})
	t.Run("metrics: point attributes, not the metric name, at arrival", func(t *testing.T) {
		f := newMetaFixture(t, "metrics", nil)
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		putAll(rm.Resource().Attributes(), map[string]string{"service.name": "a"})
		ms := rm.ScopeMetrics().AppendEmpty().Metrics()
		g := ms.AppendEmpty()
		g.SetName("g")
		p := g.SetEmptyGauge().DataPoints().AppendEmpty()
		p.SetTimestamp(pcommon.NewTimestampFromTime(old))
		p.Attributes().PutStr("marker", "m")
		h := ms.AppendEmpty()
		h.SetName("h")
		h.SetEmptyHistogram().DataPoints().AppendEmpty()
		f.m.observeMetrics(md)
		assertRows(t, f.flush(), []metaTestRow{
			{window: window(0), source: "metrics", resource: map[string]string{"service.name": "a"}, attrs: map[string]string{"marker": "m"}},
			{window: window(0), source: "metrics", resource: map[string]string{"service.name": "a"}, attrs: map[string]string{}},
		})
	})
}

func assertRows(t *testing.T, got, want []metaTestRow) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("rows = %d %+v, want %d", len(got), got, len(want))
	}
	for i := range want {
		g := got[i]
		if g.resFP == 0 || g.attrsFP == 0 {
			t.Errorf("row %d lacks a fingerprint", i)
		}
		if g.window != want[i].window || g.source != want[i].source || !maps.Equal(g.resource, want[i].resource) || !maps.Equal(g.attrs, want[i].attrs) {
			t.Errorf("row %d\n got %+v\nwant %+v", i, g, want[i])
		}
	}
}

func TestMetadataValuesAndFingerprints(t *testing.T) {
	f := newMetaFixture(t, "traces", nil)
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	add := func(set func(pcommon.Map)) {
		s := ss.Spans().AppendEmpty()
		s.SetName("fp")
		s.SetStartTimestamp(pcommon.NewTimestampFromTime(metaNow))
		set(s.Attributes())
	}
	add(func(m pcommon.Map) { m.PutStr("a", "1"); m.PutStr("b", "x") })
	add(func(m pcommon.Map) { m.PutStr("b", "x"); m.PutStr("a", "1") }) // order
	add(func(m pcommon.Map) { m.PutInt("a", 1); m.PutStr("b", "x") })   // type
	add(func(m pcommon.Map) { m.PutStr("b", "x") })                     // a absent
	add(func(m pcommon.Map) { m.PutStr("a", ""); m.PutStr("b", "x") })  // a empty
	add(func(m pcommon.Map) { m.PutEmptyMap("a").PutStr("k", "<v>") })  // JSON, not HTML-escaped
	f.m.observeTraces(td)
	rows := f.flush()
	if len(rows) != 4 {
		t.Fatalf("rows = %+v, want 4: order and type make no difference, absence and emptiness do", rows)
	}
	if got := rows[3].attrs["a"]; got != `{"k":"<v>"}` {
		t.Errorf("map value = %q", got)
	}
}

func TestMetadataWindows(t *testing.T) {
	tests := []struct {
		name         string
		ts, observed time.Time
		want         int64
		dropped      bool
	}{
		{"record time, 6 h UTC", metaNow.Add(-3 * time.Hour), time.Time{}, window(-6), false},
		{"observed time when no record time", time.Time{}, metaNow.Add(-5 * time.Hour), window(-6), false},
		{"arrival when neither", time.Time{}, time.Time{}, window(0), false},
		{"the future is clamped to the current window", metaNow.Add(48 * time.Hour), time.Time{}, window(0), false},
		{"the oldest admitted window", metaNow.Add(-18 * time.Hour), time.Time{}, window(-18), false},
		{"older is not recorded", metaNow.Add(-25 * time.Hour), time.Time{}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newMetaFixture(t, "logs", nil)
			f.m.observeLogs(logRecords(map[string]string{"service.name": "a"}, tt.ts, tt.observed, nil))
			rows := f.flush()
			if tt.dropped {
				if len(rows) != 0 {
					t.Fatalf("rows = %+v", rows)
				}
				f.assertDropped(t, map[string]int64{"too_old": 1})
				return
			}
			if len(rows) != 1 || rows[0].window != tt.want {
				t.Fatalf("rows = %+v, want window %d", rows, tt.want)
			}
		})
	}
}

func TestMetadataDeduplicationPerWindow(t *testing.T) {
	f := newMetaFixture(t, "traces", nil)
	set := map[string]string{"k": "v"}
	f.m.observeTraces(spans(map[string]string{"service.name": "a"}, metaNow, set))
	f.m.observeTraces(spans(map[string]string{"service.name": "a"}, metaNow, set))
	if rows := f.flush(); len(rows) != 1 {
		t.Fatalf("rows = %+v, want one per set and window", rows)
	}
	f.m.observeTraces(spans(map[string]string{"service.name": "a"}, metaNow, set))
	if rows := f.flush(); len(rows) != 0 {
		t.Fatalf("rows = %+v: written again in the same window", rows)
	}
	// The next window writes it again, the deliberate fix of §5.
	f.now = metaNow.Add(6 * time.Hour)
	f.m.observeTraces(spans(map[string]string{"service.name": "a"}, f.now, set))
	if rows := f.flush(); len(rows) != 1 || rows[0].window != window(6) {
		t.Fatalf("rows = %+v, want one in the next window", rows)
	}
	f.assertDropped(t, nil)
}

// Every limit drops only the metadata row, and a refused row is not admitted,
// so its next occurrence is tried again (attributes-metadata §5).
func TestMetadataLimits(t *testing.T) {
	distinct := func(n int) []map[string]string {
		var sets []map[string]string
		for i := range n {
			sets = append(sets, map[string]string{"i": strconv.Itoa(i)})
		}
		return sets
	}
	many := func(n int) map[string]string {
		m := map[string]string{}
		for i := range n {
			m[strconv.Itoa(i)] = "v"
		}
		return m
	}
	tests := []struct {
		name   string
		edit   func(*AttributesMetadataConfig)
		res    map[string]string
		sets   []map[string]string
		rows   int
		reason string
	}{
		{"too many attributes", nil, map[string]string{"service.name": "a"}, []map[string]string{many(256)}, 0, "row_bytes"},
		{"a resource of too many attributes", nil, many(257), []map[string]string{nil}, 0, "row_bytes"},
		{"too many bytes", nil, nil, []map[string]string{{"big": strings.Repeat("x", 16<<10)}}, 0, "row_bytes"},
		{"sets per window", func(c *AttributesMetadataConfig) { c.MaxSetsPerWindow = 2 }, nil, distinct(3), 2, "window"},
		{"sets per resource", func(c *AttributesMetadataConfig) { c.MaxSetsPerResource = 2 }, nil, distinct(3), 2, "resource"},
		{"state bytes", func(c *AttributesMetadataConfig) { c.MaxStateBytes = amResourceCost + 2*amKeyCost }, nil, distinct(3), 2, "state_bytes"},
		{"queue rows", func(c *AttributesMetadataConfig) { c.MaxQueueRows = 2 }, nil, distinct(3), 2, "queue"},
		{"queue bytes", func(c *AttributesMetadataConfig) {
			c.MaxQueueBytes = 2 * (amRowOverhead + amAttributeOverhead + 3)
			c.MaxRowBytes = c.MaxQueueBytes
		}, nil, distinct(3), 2, "queue"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newMetaFixture(t, "logs", tt.edit)
			ld := logRecords(tt.res, metaNow, time.Time{}, tt.sets...)
			f.m.observeLogs(ld)
			f.m.observeLogs(ld) // the refused set is tried, and refused, again
			if rows := f.flush(); len(rows) != tt.rows {
				t.Fatalf("rows = %d, want %d", len(rows), tt.rows)
			}
			f.assertDropped(t, map[string]int64{tt.reason: 2})
		})
	}
}

// A row over max_attributes_per_row or max_row_bytes is refused before its
// values are converted, sorted or hashed: refusing it allocates nothing, even
// for map, list and bytes values far over the budget.
func TestMetadataRefusalIsCheap(t *testing.T) {
	huge := func(m pcommon.Map) {
		mv := m.PutEmptyMap("map")
		for i := range 50_000 {
			mv.PutStr(strconv.Itoa(i), "v")
		}
		lv := m.PutEmptySlice("list")
		for range 50_000 {
			lv.AppendEmpty().SetInt(1)
		}
		m.PutEmptyBytes("bytes").FromRaw(make([]byte, 1<<20))
	}
	tests := []struct {
		name  string
		attrs func(pcommon.Map)
	}{
		{"too many attributes", func(m pcommon.Map) {
			for i := range 300 {
				m.PutStr(strconv.Itoa(i), "v")
			}
			huge(m)
		}},
		{"values over the byte budget", huge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newMetaFixture(t, "logs", nil)
			ld := logRecords(nil, metaNow, time.Time{}, nil)
			tt.attrs(ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes())
			f.m.observeLogs(ld) // warm the scratch
			if allocs := testing.AllocsPerRun(20, func() { f.m.observeLogs(ld) }); allocs != 0 {
				t.Errorf("allocations per refused record = %v", allocs)
			}
			if rows := f.flush(); len(rows) != 0 {
				t.Fatalf("rows = %+v", rows)
			}
		})
	}
}

func TestMetadataQueueFullThenRetry(t *testing.T) {
	f := newMetaFixture(t, "logs", func(c *AttributesMetadataConfig) { c.MaxQueueRows = 1 })
	first, second := map[string]string{"i": "1"}, map[string]string{"i": "2"}
	f.m.observeLogs(logRecords(nil, metaNow, time.Time{}, first, second))
	if rows := f.flush(); len(rows) != 1 {
		t.Fatalf("rows = %+v", rows)
	}
	f.m.observeLogs(logRecords(nil, metaNow, time.Time{}, first, second))
	rows := f.flush()
	if len(rows) != 1 || rows[0].attrs["i"] != "2" {
		t.Fatalf("rows = %+v, want the set the full queue refused", rows)
	}
	f.assertDropped(t, map[string]int64{"queue": 1})
}

// Many distinct empty sets fill the row limit, not memory: with the
// defaults, and with each queue limit reached alone.
func TestMetadataEmptyRows(t *testing.T) {
	emptyRows := func(n int) plog.Logs {
		ld := plog.NewLogs()
		for i := range n {
			rl := ld.ResourceLogs().AppendEmpty()
			rl.Resource().Attributes().PutStr("s", strconv.Itoa(i))
			rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().SetTimestamp(pcommon.NewTimestampFromTime(metaNow))
		}
		return ld
	}
	tests := []struct {
		name string
		edit func(*AttributesMetadataConfig)
		send int
		want int
	}{
		{"defaults", nil, 20_001, 20_000},
		{"rows alone", func(c *AttributesMetadataConfig) { c.MaxQueueRows = 10; c.MaxQueueBytes = 64 << 20 }, 11, 10},
		{"bytes alone", func(c *AttributesMetadataConfig) {
			c.MaxQueueRows = 1_000_000
			c.MaxQueueBytes = 10 * (amRowOverhead + amAttributeOverhead + 1 + 4)
			c.MaxRowBytes = c.MaxQueueBytes
		}, 11, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newMetaFixture(t, "logs", tt.edit)
			f.m.observeLogs(emptyRows(tt.send))
			f.m.mu.Lock()
			queued, bytes := len(f.m.queue), f.m.queueBytes
			f.m.mu.Unlock()
			if queued != tt.want {
				t.Fatalf("queued = %d, want %d", queued, tt.want)
			}
			if bytes > f.m.cfg.MaxQueueBytes {
				t.Errorf("queue bytes = %d over %d", bytes, f.m.cfg.MaxQueueBytes)
			}
			f.assertDropped(t, map[string]int64{"queue": int64(tt.send - tt.want)})
		})
	}
}

func TestMetadataInsertFailureReleasesKeys(t *testing.T) {
	f := newMetaFixture(t, "logs", nil)
	set := map[string]string{"k": "v"}
	f.m.observeLogs(logRecords(nil, metaNow, time.Time{}, set))
	f.fake.reply = errors.New("clickhouse down")
	if rows := f.flush(); len(rows) != 0 {
		t.Fatalf("rows = %+v", rows)
	}
	f.assertDropped(t, map[string]int64{"insert": 1})
	f.fake.reply = nil
	f.m.observeLogs(logRecords(nil, metaNow, time.Time{}, set))
	if rows := f.flush(); len(rows) != 1 {
		t.Fatalf("rows = %+v, want the set written again in the same window", rows)
	}
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if f.m.stateBytes != amKeyCost+amResourceCost {
		t.Errorf("state bytes = %d after a release and a new admission", f.m.stateBytes)
	}
}

// Metadata follows only a request whose every table and attempt succeeded.
func TestMetadataOnlyAfterWholeSuccess(t *testing.T) {
	tests := []struct {
		name  string
		reply func(table string, n int) error
		rows  int
	}{
		{"success", func(string, int) error { return nil }, 1},
		{"success after a retried table", func(table string, n int) error {
			if table == "distributed_logs_v2" && n == 1 {
				return errors.New("timeout")
			}
			return nil
		}, 1},
		{"a table refused for good", func(table string, _ int) error {
			if table == "distributed_logs_v2" {
				return consumererror.NewPermanent(errors.New("bad"))
			}
			return nil
		}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := createDefaultConfig()
			cfg.AttributesMetadata.Enabled = true
			cfg.RetryConfig.InitialInterval = time.Millisecond
			cfg.Endpoint = "testing.invalid:9440"
			cfg.QueueConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
			fake := &clickhouseFake{reply: tt.reply}
			meta := &metaFake{}
			set := exportertest.NewNopSettings(metadata.Type)
			x, err := newLogsExporter(cfg, set.TelemetrySettings)
			if err != nil {
				t.Fatal(err)
			}
			x.do, x.meta.client.do = fake.do, meta.do
			ctx := context.Background()
			exp, err := newLogs(ctx, set, cfg, x)
			if err != nil {
				t.Fatal(err)
			}
			if err := exp.Start(ctx, componenttest.NewNopHost()); err != nil {
				t.Fatal(err)
			}
			_ = exp.ConsumeLogs(ctx, richLog())
			x.meta.flush(ctx)
			if err := exp.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			if got := len(meta.take()); got != tt.rows {
				t.Errorf("metadata rows = %d, want %d", got, tt.rows)
			}
		})
	}
}

func TestMetadataConfig(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
		err  string
	}{
		{"enabled", func(c *Config) { c.AttributesMetadata.Enabled = true }, ""},
		{"disabled ignores its fields", func(c *Config) { c.AttributesMetadata.MaxWindows = 0 }, ""},
		{"not in mode meter", func(c *Config) { c.Mode = modeMeter; c.AttributesMetadata.Enabled = true }, "attributes_metadata must not be enabled in mode meter"},
		{"database", func(c *Config) { c.AttributesMetadata.Enabled = true; c.AttributesMetadata.Database = "a b" }, "attributes_metadata.database"},
		{"windows", func(c *Config) { c.AttributesMetadata.Enabled = true; c.AttributesMetadata.MaxWindows = 0 }, "max_windows"},
		{"row bytes", func(c *Config) { c.AttributesMetadata.Enabled = true; c.AttributesMetadata.MaxRowBytes = amRowOverhead }, "max_row_bytes"},
		{"queue bytes", func(c *Config) {
			c.AttributesMetadata.Enabled = true
			c.AttributesMetadata.MaxQueueBytes = c.AttributesMetadata.MaxRowBytes - 1
		}, "max_queue_bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := createDefaultConfig()
			cfg.Endpoint = "clickhouse:9440"
			tt.edit(cfg)
			err := cfg.Validate()
			switch {
			case tt.err == "" && err != nil:
				t.Fatal(err)
			case tt.err != "" && (err == nil || !strings.Contains(err.Error(), tt.err)):
				t.Fatalf("err = %v, want %q", err, tt.err)
			}
		})
	}
}

func heapAfterGC() uint64 {
	runtime.GC()
	runtime.GC()
	var s runtime.MemStats
	runtime.ReadMemStats(&s)
	return s.HeapAlloc
}

// The costs bound the memory they stand for: the measured heap of a full
// queue, or of a full state, is within its budget (attributes-metadata §5).
// The slack only covers the garbage collector's noise; each case also checks
// the heap per row against the cost per row, so an undercount fails.
func TestMetadataMemoryBudgets(t *testing.T) {
	if testing.Short() {
		t.Skip("measures the heap")
	}
	shortAttrs := map[string]string{}
	for i := range 199 {
		shortAttrs[string(rune('A'+i%26))+strconv.Itoa(i/26)] = "v"
	}
	tests := []struct {
		name string
		fill func(f *metaFixture) int // returns the rows it queued
	}{
		{"queue of rows of many one-byte attributes", func(f *metaFixture) int {
			n := 0
			for i := 0; ; i++ {
				attrs := maps.Clone(shortAttrs)
				attrs["z"] = strconv.Itoa(i)
				f.m.observeLogs(logRecords(nil, metaNow, time.Time{}, attrs))
				f.m.mu.Lock()
				q := len(f.m.queue)
				f.m.mu.Unlock()
				if q == n {
					return n
				}
				n = q
			}
		}},
		{"queue of empty rows", func(f *metaFixture) int {
			ld := plog.NewLogs()
			for i := range 25_000 {
				rl := ld.ResourceLogs().AppendEmpty()
				rl.Resource().Attributes().PutStr("s", strconv.Itoa(i))
				rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().SetTimestamp(pcommon.NewTimestampFromTime(metaNow))
			}
			f.m.observeLogs(ld)
			f.m.mu.Lock()
			defer f.m.mu.Unlock()
			return len(f.m.queue)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newMetaFixture(t, "logs", nil)
			before := heapAfterGC()
			rows := tt.fill(f)
			f.m.mu.Lock()
			cost := f.m.queueBytes
			f.m.mu.Unlock()
			// Drop the inputs' memory: only the queue and state remain.
			used := int64(heapAfterGC()) - int64(before)
			state := int64(f.m.stateBytes)
			if rows == 0 {
				t.Fatal("nothing queued")
			}
			t.Logf("rows %d, cost %d, state %d, heap %d", rows, cost, state, used)
			if used > int64(cost)+state {
				t.Errorf("heap %d exceeds the queue cost %d plus the state %d it accounts for", used, cost, state)
			}
			if cost > f.m.cfg.MaxQueueBytes {
				t.Errorf("queue cost %d over max_queue_bytes", cost)
			}
			runtime.KeepAlive(f)
		})
	}
}

// The state's real footprint per key, from distinct resources so the quota
// index counts too, stays within amKeyCost + amResourceCost.
func TestMetadataStateFootprint(t *testing.T) {
	if testing.Short() {
		t.Skip("measures the heap")
	}
	f := newMetaFixture(t, "logs", func(c *AttributesMetadataConfig) { c.MaxQueueRows = 1 << 30; c.MaxQueueBytes = 1 << 30 })
	const n = 50_000
	before := heapAfterGC()
	f.m.mu.Lock()
	for i := range n {
		ws := f.m.windows[window(0)]
		if ws == nil {
			ws = &amWindow{keys: map[amKey]struct{}{}, perResource: map[uint64]int{}}
			f.m.windows[window(0)] = ws
		}
		k := amKey{resource: uint64(i) + 1, attrs: uint64(i) + 7}
		ws.keys[k] = struct{}{}
		ws.perResource[k.resource]++
	}
	f.m.mu.Unlock()
	used := heapAfterGC() - before
	per := float64(used) / n
	t.Logf("state heap per key with its resource: %.1f bytes", per)
	if per > amKeyCost+amResourceCost {
		t.Errorf("state per key = %.1f bytes, over the accounted %d", per, amKeyCost+amResourceCost)
	}
	runtime.KeepAlive(f)
}

// Observing sets already recorded allocates nothing.
func BenchmarkMetadataObserve(b *testing.B) {
	f := newMetaFixture(b, "traces", nil)
	td := ptrace.NewTraces()
	for r := range 10 {
		rs := td.ResourceSpans().AppendEmpty()
		putAll(rs.Resource().Attributes(), map[string]string{"service.name": "svc-" + strconv.Itoa(r), "host.name": "h", "k8s.pod.name": "p"})
		ss := rs.ScopeSpans().AppendEmpty()
		for i := range 100 {
			s := ss.Spans().AppendEmpty()
			s.SetName("GET /items/{id}")
			s.SetStartTimestamp(pcommon.NewTimestampFromTime(metaNow))
			s.Attributes().PutStr("http.route", "/items/{id}")
			s.Attributes().PutInt("http.status_code", int64(200+i%5))
			s.Attributes().PutStr("peer.service", "db")
		}
	}
	f.m.observeTraces(td)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		f.m.observeTraces(td)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*td.SpanCount()), "ns/span")
}
