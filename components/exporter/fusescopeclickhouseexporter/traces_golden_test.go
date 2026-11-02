// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// observed is the reference run the encoder must reproduce
// (docs/spec/observed/<version>, written by test/observe).
var observed = filepath.Join("..", "..", "..", "docs", "spec", "observed", "0.145.0")

type row = map[string]any

func TestTracesMatchObservedRows(t *testing.T) {
	batches := encodeObservedCorpus(t)

	tests := []struct {
		table  string
		input  func(*tracesEncoder) proto.Input
		key    func(row) string
		ignore []string
	}{
		{"signoz_index_v3", func(e *tracesEncoder) proto.Input { return e.index.input() }, func(r row) string { return fmt.Sprint(r["trace_id"], r["span_id"]) }, []string{"inserted_at"}},
		{"signoz_error_index_v2", func(e *tracesEncoder) proto.Input { return e.errors.input() }, func(r row) string { return fmt.Sprint(r["spanID"], r["timestamp"], r["exceptionMessage"]) }, []string{"errorID"}},
		{"traces_v3_resource", func(e *tracesEncoder) proto.Input { return e.resources.cols.input() }, func(r row) string { return fmt.Sprint(r["fingerprint"], r["seen_at_ts_bucket_start"]) }, nil},
		{"tag_attributes_v2", func(e *tracesEncoder) proto.Input { return e.tags.cols.input() }, func(r row) string { return fmt.Sprint(r) }, nil},
		{"span_attributes_keys", func(e *tracesEncoder) proto.Input { return e.keys.input() }, func(r row) string { return fmt.Sprint(r["tagKey"], r["tagType"], r["dataType"]) }, []string{"timestamp"}},
	}
	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			var rows []row
			for _, enc := range batches {
				rows = append(rows, blockRows(tt.input(enc))...)
			}
			want := indexRows(t, readObserved(t, "signoz_traces", tt.table), tt.key, tt.ignore)
			got := indexRows(t, rows, tt.key, tt.ignore)
			compareRows(t, want, got)
		})
	}
}

func compareRows(t *testing.T, want, got map[string]row) {
	t.Helper()
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Errorf("missing row %s", k)
			continue
		}
		for col, wv := range w {
			if gv := g[col]; !reflect.DeepEqual(gv, wv) {
				t.Errorf("row %s column %s:\n got  %#v\n want %#v", k, col, gv, wv)
			}
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("unexpected row %s", k)
		}
	}
}

func TestErrorIDsAreUUIDv4(t *testing.T) {
	enc := encodeObservedCorpus(t)[0]
	uuid := regexp.MustCompile(`^[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}$`)
	for i := range enc.errors.errorID.Rows() {
		id := enc.errors.errorID.Row(i)
		if !uuid.Match(id[:]) {
			t.Errorf("errorID %s is not a UUIDv4", id)
		}
	}
}

// encodeObservedCorpus replays the observed corpus as the reference received
// it: the JSON corpus, then the protobuf probes as a second batch (test/observe).
func encodeObservedCorpus(t *testing.T) []*tracesEncoder {
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
	var batches []*tracesEncoder
	for _, batch := range []ptrace.Traces{td, probes} {
		enc := newTracesEncoder()
		enc.now = func() time.Time { return time.Unix(0, 0) }
		_ = enc.encode(batch, nil)
		batches = append(batches, enc)
	}
	return batches
}

func readObserved(t *testing.T, db, table string) []row {
	t.Helper()
	f, err := os.Open(filepath.Join(observed, "rows", db+"."+table+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var rows []row
	s := bufio.NewScanner(f)
	s.Buffer(nil, 1<<24)
	for s.Scan() {
		if len(s.Bytes()) == 0 {
			continue
		}
		var r row
		if err := json.Unmarshal(s.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r)
	}
	return rows
}

// indexRows keys rows and keeps only the columns both sides can compare.
func indexRows(t *testing.T, rows []row, key func(row) string, ignore []string) map[string]row {
	t.Helper()
	out := map[string]row{}
	for _, r := range rows {
		for c := range r {
			if slices.Contains(ignore, c) || strings.Contains(c, "$$") || isAlias(c) {
				delete(r, c)
			}
		}
		// A row repeated by a later batch (test/observe sends two) is merged, as a
		// ReplacingMergeTree does: the reference skips keys it already wrote,
		// the exporter repeats them (traces §7).
		k := key(r)
		if prev, dup := out[k]; dup {
			if !reflect.DeepEqual(prev, r) {
				t.Fatalf("two rows with key %s:\n %v\n %v", k, prev, r)
			}
			continue
		}
		out[k] = r
	}
	return out
}

// isAlias reports columns ClickHouse computes (DEFAULT/ALIAS) that the
// exporter does not send.
func isAlias(c string) bool {
	return c == "created_at"
}

func blockRows(input proto.Input) []row {
	if len(input) == 0 {
		return nil
	}
	rows := make([]row, input[0].Data.Rows())
	for i := range rows {
		rows[i] = row{}
		for _, col := range input {
			rows[i][col.Name] = cell(col.Data, i)
		}
	}
	return rows
}

func cell(c proto.ColInput, i int) any {
	switch c := c.(type) {
	case *proto.ColUInt64:
		return float64(c.Row(i))
	case *proto.ColUInt32:
		return float64(c.Row(i))
	case *proto.ColUInt8:
		return float64(c.Row(i))
	case *proto.ColInt64:
		return float64(c.Row(i))
	case *proto.ColInt16:
		return float64(c.Row(i))
	case *proto.ColInt8:
		return float64(c.Row(i))
	case *proto.ColFloat64:
		return jsonFloat(c.Row(i))
	case *proto.ColBool:
		return c.Row(i)
	case *proto.ColStr:
		return asJSONText(c.Row(i))
	case *proto.ColLowCardinality[string]:
		return asJSONText(c.Row(i))
	case *proto.ColEnum:
		return c.Row(i)
	case *proto.ColFixedStr32:
		v := c.Row(i)
		return string(v[:])
	case *proto.ColDateTime64:
		return c.Row(i).UTC().Format("2006-01-02 15:04:05.000000000")
	case *proto.ColDateTime:
		return c.Row(i).UTC().Format(time.DateTime)
	case *proto.ColNullable[float64]:
		if v := c.Row(i); v.Set {
			return jsonFloat(v.Value)
		}
		return nil
	case *proto.ColJSONStr:
		return nestedJSON(c.Str.Row(i))
	case *proto.ColArr[string]:
		out := []any{}
		for _, s := range c.Row(i) {
			out = append(out, asJSONText(s))
		}
		return out
	case *proto.ColMap[string, string]:
		out := map[string]any{}
		for k, v := range c.Row(i) {
			out[asJSONText(k)] = asJSONText(v)
		}
		return out
	case *proto.ColMap[string, float64]:
		out := map[string]any{}
		for k, v := range c.Row(i) {
			out[k] = jsonFloat(v)
		}
		return out
	case *proto.ColMap[string, bool]:
		return mapRow(c.Row(i))
	}
	panic(fmt.Sprintf("cell: unsupported column %T", c))
}

// asJSONText replaces each invalid UTF-8 byte with U+FFFD, as encoding/json
// does when it reads the observed rows.
func asJSONText(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			b.WriteRune(utf8.RuneError)
		} else {
			b.WriteString(s[i : i+size])
		}
		i += size
	}
	return b.String()
}

// jsonFloat renders NaN and infinities as ClickHouse's JSON output does: null.
func jsonFloat(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}
	return f
}

func mapRow[V any](m map[string]V) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// nestedJSON parses a flat JSON object the way a ClickHouse JSON column
// stores it: dotted keys become nested paths and an object wins over a leaf at
// the same path.
func nestedJSON(s string) any {
	var flat map[string]any
	if err := json.Unmarshal([]byte(s), &flat); err != nil {
		panic(fmt.Sprintf("invalid JSON %q: %v", s, err))
	}
	return nest(flat)
}

func nest(flat map[string]any) map[string]any {
	out := map[string]any{}
	keys := make([]string, 0, len(flat))
	for k := range flat {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b string) int { return strings.Count(b, ".") - strings.Count(a, ".") })
	for _, k := range keys {
		v := flat[k]
		if m, ok := v.(map[string]any); ok {
			v = nest(m)
		}
		parts := strings.Split(k, ".")
		node := out
		for _, p := range parts[:len(parts)-1] {
			child, ok := node[p].(map[string]any)
			if !ok {
				child = map[string]any{}
				node[p] = child
			}
			node = child
		}
		last := parts[len(parts)-1]
		if existing, ok := node[last].(map[string]any); ok {
			if m, ok := v.(map[string]any); ok {
				for mk, mv := range m {
					existing[mk] = mv
				}
			}
			continue
		}
		node[last] = v
	}
	return out
}

func TestInsertedColumnsExistInDistributedTables(t *testing.T) {
	traces, logs := newTracesEncoder(), newLogsEncoder()
	for _, tbl := range tracesTables {
		checkColumnsExist(t, "signoz_traces", tbl.name, tbl.input(traces))
	}
	for _, tbl := range logsTables {
		checkColumnsExist(t, "signoz_logs", tbl.name, tbl.input(logs))
	}
	metrics := newMetricsEncoder()
	for _, tbl := range metricsTables {
		checkColumnsExist(t, "signoz_metrics", tbl.name, tbl.input(metrics))
	}
}

func checkColumnsExist(t *testing.T, db, table string, input proto.Input) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(observed, "ddl", db+"."+table+".sql"))
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, m := range regexp.MustCompile("(?m)^\\s+`([^`]+)` ").FindAllStringSubmatch(string(b), -1) {
		have[m[1]] = true
	}
	for _, col := range input {
		if !have[col.Name] {
			t.Errorf("%s.%s has no column %q", db, table, col.Name)
		}
	}
}
