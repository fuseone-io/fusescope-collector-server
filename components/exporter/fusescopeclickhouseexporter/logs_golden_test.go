// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestLogsMatchObservedRows(t *testing.T) {
	batches := encodeObservedLogs(t)
	retention := []string{"_retention_days", "_retention_days_cold"}

	tests := []struct {
		table  string
		input  func(*logsEncoder) proto.Input
		key    func(row) string
		ignore []string
	}{
		{"logs_v2", func(e *logsEncoder) proto.Input { return e.rows.input() }, func(r row) string { return fmt.Sprint(r["body"]) }, append([]string{"id", "inserted_at"}, retention...)},
		{"logs_v2_resource", func(e *logsEncoder) proto.Input { return e.resources.cols.input() }, func(r row) string { return fmt.Sprint(r["fingerprint"], r["seen_at_ts_bucket_start"]) }, retention},
		{"tag_attributes_v2", func(e *logsEncoder) proto.Input { return e.tags.cols.input() }, func(r row) string { return fmt.Sprint(r) }, nil},
		{"logs_attribute_keys", func(e *logsEncoder) proto.Input { return e.attrKeys.input() }, func(r row) string { return fmt.Sprint(r["name"], r["datatype"]) }, []string{"timestamp"}},
		{"logs_resource_keys", func(e *logsEncoder) proto.Input { return e.resKeys.input() }, func(r row) string { return fmt.Sprint(r["name"], r["datatype"]) }, []string{"timestamp"}},
	}
	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			observedRows := readObserved(t, "signoz_logs", tt.table)
			for _, r := range observedRows {
				// body_v2 has a typed path, so ClickHouse renders the "{}" we send as {"message":""}.
				if reflect.DeepEqual(r["body_v2"], map[string]any{"message": ""}) {
					r["body_v2"] = map[string]any{}
				}
			}
			var rows []row
			for _, enc := range batches {
				rows = append(rows, blockRows(tt.input(enc))...)
			}
			want := indexRows(t, observedRows, tt.key, tt.ignore)
			got := indexRows(t, rows, tt.key, tt.ignore)
			compareRows(t, want, got)
		})
	}
}

func TestLogIDsCarryTheRecordTime(t *testing.T) {
	enc := encodeObservedLogs(t)[0]
	for i := range enc.rows.id.Rows() {
		id := enc.rows.id.Row(i)
		if len(id) != 27 {
			t.Fatalf("id %q has %d characters, want 27", id, len(id))
		}
		raw := decodeBase62(t, id)
		if got, want := binary.BigEndian.Uint64(raw[:8])+logIDEpochNanos, enc.rows.timestamp.Row(i); got != want {
			t.Errorf("id %s carries %d, want the record timestamp %d", id, got, want)
		}
	}
}

func TestBase62MatchesObservedIDs(t *testing.T) {
	for _, r := range readObserved(t, "signoz_logs", "logs_v2") {
		id := r["id"].(string)
		if got := base62(decodeBase62(t, id)); string(got[:]) != id {
			t.Errorf("base62 round trip of %s = %s", id, got)
		}
	}
}

func decodeBase62(t *testing.T, s string) (raw [20]byte) {
	t.Helper()
	var parts [5]uint32
	for _, ch := range []byte(s) {
		d := strings.IndexByte(base62Alphabet, ch)
		if d < 0 {
			t.Fatalf("invalid base62 %q", s)
		}
		carry := uint64(d)
		for j := len(parts) - 1; j >= 0; j-- {
			acc := uint64(parts[j])*62 + carry
			parts[j] = uint32(acc)
			carry = acc >> 32
		}
	}
	for i, p := range parts {
		binary.BigEndian.PutUint32(raw[i*4:], p)
	}
	return raw
}

// encodeObservedLogs replays the observed corpus as the reference received it:
// the JSON corpus, then the protobuf probe as a second batch (test/observe),
// each with the ingest time the reference gave it.
func encodeObservedLogs(t *testing.T) []*logsEncoder {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(observed, "corpus", "logs.json"))
	if err != nil {
		t.Fatal(err)
	}
	ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs(b)
	if err != nil {
		t.Fatal(err)
	}
	pb, err := os.ReadFile(filepath.Join(observed, "corpus", "logs-invalid-utf8.pb"))
	if err != nil {
		t.Fatal(err)
	}
	probe, err := (&plog.ProtoUnmarshaler{}).UnmarshalLogs(pb)
	if err != nil {
		t.Fatal(err)
	}
	// The JSON record without any time and the probe without an observed time
	// carry their batch's ingest time.
	var jsonClock, probeClock float64
	for _, r := range readObserved(t, "signoz_logs", "logs_v2") {
		switch body := fmt.Sprint(r["body"]); {
		case body == "no time at all":
			jsonClock = r["timestamp"].(float64)
		case strings.HasPrefix(body, "bad "):
			probeClock = r["observed_timestamp"].(float64)
		}
	}
	if jsonClock == 0 || probeClock == 0 {
		t.Fatal("observed corpus lacks the records that show the ingest times")
	}
	var batches []*logsEncoder
	for _, batch := range []struct {
		ld    plog.Logs
		clock float64
	}{{ld, jsonClock}, {probe, probeClock}} {
		now := time.Unix(0, int64(batch.clock))
		enc := newLogsEncoder()
		enc.now = func() time.Time { return now }
		enc.encode(batch.ld)
		batches = append(batches, enc)
	}
	return batches
}

func TestLogTimestamps(t *testing.T) {
	now := time.Unix(0, 9_000)
	tests := []struct {
		name                 string
		ts, observed         uint64
		wantTS, wantObserved uint64
	}{
		{"both", 1_000, 2_000, 1_000, 2_000},
		{"observed only", 0, 2_000, 2_000, 2_000},
		{"record time only", 1_000, 0, 1_000, 9_000},
		{"neither", 0, 0, 9_000, 9_000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ld := plog.NewLogs()
			lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			lr.SetTimestamp(pcommon.Timestamp(tt.ts))
			lr.SetObservedTimestamp(pcommon.Timestamp(tt.observed))
			enc := newLogsEncoder()
			enc.now = func() time.Time { return now }
			enc.encode(ld)
			if got := enc.rows.timestamp.Row(0); got != tt.wantTS {
				t.Errorf("timestamp = %d, want %d", got, tt.wantTS)
			}
			if got := enc.rows.observedTimestamp.Row(0); got != tt.wantObserved {
				t.Errorf("observed_timestamp = %d, want %d", got, tt.wantObserved)
			}
		})
	}
}
