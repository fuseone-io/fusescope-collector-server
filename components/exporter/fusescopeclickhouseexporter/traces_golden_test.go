// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// observed is the reference run the encoder must reproduce
// (docs/spec/observed/<version>, written by test/observe).
var observed = filepath.Join("..", "..", "..", "docs", "spec", "observed", "0.145.0")

type row = map[string]any

func TestTracesMatchObservedRows(t *testing.T) {
	enc := encodeObservedCorpus(t)

	tests := []struct {
		table  string
		input  proto.Input
		key    func(row) string
		ignore []string
	}{
		{"signoz_index_v3", enc.index.input(), func(r row) string { return fmt.Sprint(r["trace_id"], r["span_id"]) }, []string{"inserted_at"}},
		{"signoz_error_index_v2", enc.errors.input(), func(r row) string { return fmt.Sprint(r["spanID"], r["timestamp"], r["exceptionMessage"]) }, []string{"errorID"}},
		{"traces_v3_resource", enc.resources.input(), func(r row) string { return fmt.Sprint(r["fingerprint"], r["seen_at_ts_bucket_start"]) }, nil},
		{"tag_attributes_v2", enc.tags.input(), func(r row) string { return fmt.Sprint(r) }, nil},
		{"span_attributes_keys", enc.keys.input(), func(r row) string { return fmt.Sprint(r["tagKey"], r["tagType"], r["dataType"]) }, []string{"timestamp"}},
	}
	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			want := indexRows(t, readObserved(t, tt.table), tt.key, tt.ignore)
			got := indexRows(t, blockRows(tt.input), tt.key, tt.ignore)
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
		})
	}
}

func TestErrorIDsAreUUIDv4(t *testing.T) {
	enc := encodeObservedCorpus(t)
	uuid := regexp.MustCompile(`^[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}$`)
	for i := range enc.errors.errorID.Rows() {
		id := enc.errors.errorID.Row(i)
		if !uuid.Match(id[:]) {
			t.Errorf("errorID %s is not a UUIDv4", id)
		}
	}
}

func encodeObservedCorpus(t *testing.T) *tracesEncoder {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(observed, "corpus", "traces.json"))
	if err != nil {
		t.Fatal(err)
	}
	td, err := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(b)
	if err != nil {
		t.Fatal(err)
	}
	enc := newTracesEncoder()
	enc.now = func() time.Time { return time.Unix(0, 0) }
	enc.encode(td)
	return enc
}

func readObserved(t *testing.T, table string) []row {
	t.Helper()
	f, err := os.Open(filepath.Join(observed, "rows", "signoz_traces."+table+".jsonl"))
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
		k := key(r)
		if _, dup := out[k]; dup {
			t.Fatalf("duplicate row key %s", k)
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
	case *proto.ColInt64:
		return float64(c.Row(i))
	case *proto.ColInt16:
		return float64(c.Row(i))
	case *proto.ColInt8:
		return float64(c.Row(i))
	case *proto.ColBool:
		return c.Row(i)
	case *proto.ColStr:
		return c.Row(i)
	case *proto.ColLowCardinality[string]:
		return c.Row(i)
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
			return v.Value
		}
		return nil
	case *proto.ColJSONStr:
		return nestedJSON(c.Str.Row(i))
	case *proto.ColArr[string]:
		out := []any{}
		for _, s := range c.Row(i) {
			out = append(out, s)
		}
		return out
	case *proto.ColMap[string, string]:
		return mapRow(c.Row(i))
	case *proto.ColMap[string, float64]:
		return mapRow(c.Row(i))
	case *proto.ColMap[string, bool]:
		return mapRow(c.Row(i))
	}
	panic(fmt.Sprintf("cell: unsupported column %T", c))
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
