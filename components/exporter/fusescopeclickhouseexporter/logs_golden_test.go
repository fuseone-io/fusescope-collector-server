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
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestLogsMatchObservedRows(t *testing.T) {
	enc := encodeObservedLogs(t)
	retention := []string{"_retention_days", "_retention_days_cold"}

	tests := []struct {
		table  string
		input  proto.Input
		key    func(row) string
		ignore []string
	}{
		{"logs_v2", enc.rows.input(), func(r row) string { return fmt.Sprint(r["body"]) }, append([]string{"id", "inserted_at"}, retention...)},
		{"logs_v2_resource", enc.resources.cols.input(), func(r row) string { return fmt.Sprint(r["fingerprint"], r["seen_at_ts_bucket_start"]) }, retention},
		{"tag_attributes_v2", enc.tags.cols.input(), func(r row) string { return fmt.Sprint(r) }, nil},
		{"logs_attribute_keys", enc.attrKeys.input(), func(r row) string { return fmt.Sprint(r["name"], r["datatype"]) }, []string{"timestamp"}},
		{"logs_resource_keys", enc.resKeys.input(), func(r row) string { return fmt.Sprint(r["name"], r["datatype"]) }, []string{"timestamp"}},
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
			want := indexRows(t, observedRows, tt.key, tt.ignore)
			got := indexRows(t, blockRows(tt.input), tt.key, tt.ignore)
			compareRows(t, want, got)
		})
	}
}

func TestLogIDsCarryTheRecordTime(t *testing.T) {
	enc := encodeObservedLogs(t)
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

// encodeObservedLogs replays the observed corpus with the clock of the observed
// run, taken from the record that had no timestamp at all.
func encodeObservedLogs(t *testing.T) *logsEncoder {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(observed, "corpus", "logs.json"))
	if err != nil {
		t.Fatal(err)
	}
	ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs(b)
	if err != nil {
		t.Fatal(err)
	}
	// The protobuf probe reached the reference in the same batch, after the JSON corpus.
	pb, err := os.ReadFile(filepath.Join(observed, "corpus", "logs-invalid-utf8.pb"))
	if err != nil {
		t.Fatal(err)
	}
	extra, err := (&plog.ProtoUnmarshaler{}).UnmarshalLogs(pb)
	if err != nil {
		t.Fatal(err)
	}
	extra.ResourceLogs().MoveAndAppendTo(ld.ResourceLogs())
	var now time.Time
	for _, r := range readObserved(t, "signoz_logs", "logs_v2") {
		if r["body"] == "no time at all" {
			now = time.Unix(0, int64(r["timestamp"].(float64)))
		}
	}
	if now.IsZero() {
		t.Fatal("observed corpus has no record without timestamps")
	}
	enc := newLogsEncoder()
	enc.now = func() time.Time { return now }
	enc.encode(ld)
	return enc
}
