// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter/internal/metadatatest"
)

const (
	durationMetric = "otelcol_exporter_fusescope_clickhouse_insert_duration"
	errorsMetric   = "otelcol_exporter_fusescope_clickhouse_insert_errors"
	rowsMetric     = "otelcol_exporter_fusescope_clickhouse_inserted_rows"
)

// richLog has rows for every logs table.
func richLog() plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "shop")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr("served")
	lr.Attributes().PutStr("http.route", "/items/{id}")
	return ld
}

// queryTable is the table of an INSERT body, without its database.
func queryTable(body string) string {
	target := strings.Fields(body)[2]
	return target[strings.IndexByte(target, '.')+1:]
}

// attempt is one insert the fake ClickHouse answered.
type attempt struct {
	table string
	rows  int64
	err   error
}

// clickhouseFake answers each insert with reply(table, n), n counting the
// attempts into that table from 1, and records what it was sent.
type clickhouseFake struct {
	mu       sync.Mutex
	attempts []attempt
	reply    func(table string, n int) error
}

func (f *clickhouseFake) do(_ context.Context, q ch.Query) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	table := queryTable(q.Body)
	n := 1
	for _, a := range f.attempts {
		if a.table == table {
			n++
		}
	}
	err := f.reply(table, n)
	f.attempts = append(f.attempts, attempt{table, int64(q.Input[0].Data.Rows()), err})
	return err
}

// sentRows is the rows the fake was sent into table, from its first attempt.
func (f *clickhouseFake) sentRows(t *testing.T, table string) int64 {
	t.Helper()
	for _, a := range f.attempts {
		if a.table == table {
			return a.rows
		}
	}
	t.Fatalf("nothing was inserted into %s", table)
	return 0
}

var allLogsTables = []string{
	"distributed_logs_v2_resource", "distributed_tag_attributes_v2", "distributed_logs_attribute_keys",
	"distributed_logs_resource_keys", "distributed_logs_v2",
}

func clickhouseException(code proto.Error) error {
	return &ch.Exception{Code: code, Name: "DB::Exception", Message: "row 1: cannot parse"}
}

// attemptCount is how many attempts into a table had an outcome.
type attemptCount struct {
	table, outcome string
	count          uint64
}

func TestInsertTelemetry(t *testing.T) {
	const first = "distributed_logs_v2_resource"
	tests := []struct {
		name  string
		cfg   func() *Config
		reply func(table string, n int) error
		// wantErr is "" for success, else the class of the returned error.
		wantErr string
		// attempts are the duration observations, per table and outcome.
		attempts []attemptCount
		// errors are the failed attempts per table and outcome.
		errors []attemptCount
		// rows are the tables whose rows are counted, once each.
		rows []string
	}{
		{
			name:  "success records duration and rows",
			cfg:   fastRetries,
			reply: func(string, int) error { return nil },
			attempts: []attemptCount{
				{"distributed_logs_v2_resource", "ok", 1}, {"distributed_tag_attributes_v2", "ok", 1},
				{"distributed_logs_attribute_keys", "ok", 1}, {"distributed_logs_resource_keys", "ok", 1},
				{"distributed_logs_v2", "ok", 1},
			},
			rows: allLogsTables,
		},
		{
			name: "retryable failure records its outcome and no rows",
			cfg: func() *Config {
				cfg := fastRetries()
				cfg.RetryConfig.Enabled = false
				return cfg
			},
			reply:    func(string, int) error { return errors.New("connection refused") },
			wantErr:  "retryable",
			attempts: []attemptCount{{first, "retryable", 1}},
			errors:   []attemptCount{{first, "retryable", 1}},
		},
		{
			name:     "permanent failure records its outcome and no rows",
			cfg:      fastRetries,
			reply:    func(string, int) error { return clickhouseException(proto.ErrCannotParseText) },
			wantErr:  "permanent",
			attempts: []attemptCount{{first, "permanent", 1}},
			errors:   []attemptCount{{first, "permanent", 1}},
		},
		{
			name: "a schema error stays retryable",
			cfg: func() *Config {
				cfg := fastRetries()
				cfg.RetryConfig.Enabled = false
				return cfg
			},
			reply:    func(string, int) error { return clickhouseException(proto.ErrUnknownTable) },
			wantErr:  "retryable",
			attempts: []attemptCount{{first, "retryable", 1}},
			errors:   []attemptCount{{first, "retryable", 1}},
		},
		{
			name: "retried batch records two attempts",
			cfg:  fastRetries,
			reply: func(table string, n int) error {
				if table == first && n == 1 {
					return errors.New("connection reset")
				}
				return nil
			},
			attempts: []attemptCount{
				{first, "retryable", 1}, {first, "ok", 1}, {"distributed_tag_attributes_v2", "ok", 1},
				{"distributed_logs_attribute_keys", "ok", 1}, {"distributed_logs_resource_keys", "ok", 1},
				{"distributed_logs_v2", "ok", 1},
			},
			errors: []attemptCount{{first, "retryable", 1}},
			rows:   allLogsTables,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tel := componenttest.NewTelemetry()
			t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
			fake := &clickhouseFake{reply: tt.reply}
			exp := helperLogsWith(t, metadatatest.NewSettings(tel), tt.cfg(), false, fake.do)

			err := exp.ConsumeLogs(context.Background(), richLog())
			if got := errorClass(err); got != tt.wantErr {
				t.Fatalf("ConsumeLogs = %v (%q), want %q", err, got, tt.wantErr)
			}

			var durations []metricdata.HistogramDataPoint[float64]
			for _, o := range tt.attempts {
				durations = append(durations, metricdata.HistogramDataPoint[float64]{
					Attributes: attribute.NewSet(attribute.String("table", o.table), attribute.String("outcome", o.outcome)),
				})
			}
			metadatatest.AssertEqualExporterFusescopeClickhouseInsertDuration(t, tel, durations,
				metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreValue(), metricdatatest.IgnoreExemplars())
			assertDurationCounts(t, tel, tt.attempts)

			if len(tt.errors) == 0 {
				assertAbsent(t, tel, errorsMetric)
			} else {
				var dps []metricdata.DataPoint[int64]
				for _, o := range tt.errors {
					dps = append(dps, metricdata.DataPoint[int64]{
						Attributes: attribute.NewSet(attribute.String("table", o.table), attribute.String("outcome", o.outcome)),
						Value:      int64(o.count),
					})
				}
				metadatatest.AssertEqualExporterFusescopeClickhouseInsertErrors(t, tel, dps, metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreExemplars())
			}

			if len(tt.rows) == 0 {
				assertAbsent(t, tel, rowsMetric)
				return
			}
			var dps []metricdata.DataPoint[int64]
			for _, table := range tt.rows {
				dps = append(dps, metricdata.DataPoint[int64]{
					Attributes: attribute.NewSet(attribute.String("table", table)),
					Value:      fake.sentRows(t, table),
				})
			}
			metadatatest.AssertEqualExporterFusescopeClickhouseInsertedRows(t, tel, dps, metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreExemplars())
		})
	}
}

// The rows counted are those of the inserts, also across the chunks of one
// request: one observation per insert, none per row.
func TestInsertedRowsCountsEveryChunk(t *testing.T) {
	tel := componenttest.NewTelemetry()
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
	x, err := newLogsExporter(createDefaultConfig(), metadatatest.NewSettings(tel).TelemetrySettings)
	if err != nil {
		t.Fatal(err)
	}
	fake := &clickhouseFake{reply: func(string, int) error { return nil }}
	x.do = fake.do
	ld := plog.NewLogs()
	records := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for range 3 {
		records.AppendEmpty().Body().SetStr("chunked")
	}
	// Every record fills a chunk, so each goes in an insert of its own.
	x.encoders.New = func() any {
		e := newLogsEncoder()
		e.maxChunkBytes = 1
		return e
	}
	if err := x.export(context.Background(), ld, newRequestID(time.Now())); err != nil {
		t.Fatal(err)
	}
	inserts := map[string]uint64{}
	rows := map[string]int64{}
	for _, a := range fake.attempts {
		inserts[a.table]++
		rows[a.table] += a.rows
	}
	if inserts["distributed_logs_v2"] < 2 || rows["distributed_logs_v2"] != 3 {
		t.Fatalf("sent %d rows in %d inserts, want 3 rows in several", rows["distributed_logs_v2"], inserts["distributed_logs_v2"])
	}
	var dps []metricdata.DataPoint[int64]
	var attempts []attemptCount
	for table, n := range inserts {
		dps = append(dps, metricdata.DataPoint[int64]{Attributes: attribute.NewSet(attribute.String("table", table)), Value: rows[table]})
		attempts = append(attempts, attemptCount{table, "ok", n})
	}
	metadatatest.AssertEqualExporterFusescopeClickhouseInsertedRows(t, tel, dps, metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreExemplars())
	assertDurationCounts(t, tel, attempts)
}

// Recording an attempt allocates nothing once its series exists, as the
// attribute sets are built with the exporter; a failure only pays for testing
// whether its error is permanent.
func TestRecordAllocations(t *testing.T) {
	retryable := errors.New("connection refused")
	tests := []struct {
		name string
		err  error
		want float64
	}{
		{"ok", nil, 0},
		{"retryable", retryable, 1},
		{"permanent", consumererror.NewPermanent(retryable), 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tel := componenttest.NewTelemetry()
			t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
			it, err := newInsertTelemetry(tel.NewTelemetrySettings(), allLogsTables)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			record := func() { it.record(ctx, 0, time.Millisecond, 1000, tt.err) }
			record()
			// Allocations by other goroutines only add, so the least of three
			// runs is the recording's own.
			n := testing.AllocsPerRun(100, record)
			for range 2 {
				n = min(n, testing.AllocsPerRun(100, record))
			}
			if n > tt.want {
				t.Errorf("recording allocates %v times, want at most %v", n, tt.want)
			}
		})
	}
}

// Every table the exporter writes is a declared value of the table attribute
// (metadata.yaml, rendered in documentation.md).
func TestTablesAreDeclared(t *testing.T) {
	doc, err := os.ReadFile("documentation.md")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tb := range tracesTables {
		names = append(names, tb.name)
	}
	for _, tb := range logsTables {
		names = append(names, tb.name)
	}
	for _, tb := range metricsTables {
		names = append(names, tb.name)
	}
	for _, name := range names {
		if !strings.Contains(string(doc), "``"+name+"``") {
			t.Errorf("table %s is not a value of the table attribute in metadata.yaml", name)
		}
	}
}

// The recorded outcome follows the class of the error returned, never its text.
func TestInsertOutcome(t *testing.T) {
	tests := []struct {
		err  error
		want insertOutcome
	}{
		{nil, outcomeOK},
		{errors.New("connection refused"), outcomeRetryable},
		{context.DeadlineExceeded, outcomeRetryable},
		{consumererror.NewPermanent(errors.New("bad data")), outcomePermanent},
		{classify(clickhouseException(proto.ErrTypeMismatch)), outcomePermanent},
		{classify(clickhouseException(proto.ErrUnknownTable)), outcomeRetryable},
	}
	for _, tt := range tests {
		if got := outcomeOf(tt.err); got != tt.want {
			t.Errorf("outcomeOf(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

func errorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case consumererror.IsPermanent(err):
		return "permanent"
	default:
		return "retryable"
	}
}

func assertDurationCounts(t *testing.T, tel *componenttest.Telemetry, want []attemptCount) {
	t.Helper()
	m, err := tel.GetMetric(durationMetric)
	if err != nil {
		t.Fatal(err)
	}
	got := map[attemptCount]bool{}
	for _, dp := range m.Data.(metricdata.Histogram[float64]).DataPoints {
		table, _ := dp.Attributes.Value("table")
		outcome, _ := dp.Attributes.Value("outcome")
		got[attemptCount{table.AsString(), outcome.AsString(), dp.Count}] = true
		if dp.Sum < 0 {
			t.Errorf("%s/%s: negative duration %v", table.AsString(), outcome.AsString(), dp.Sum)
		}
	}
	for _, o := range want {
		if !got[o] {
			t.Errorf("no %d observation(s) of %s/%s in %v", o.count, o.table, o.outcome, got)
		}
	}
}

func assertAbsent(t *testing.T, tel *componenttest.Telemetry, name string) {
	t.Helper()
	if m, err := tel.GetMetric(name); err == nil {
		t.Errorf("%s recorded %+v, want nothing", name, m.Data)
	}
}
