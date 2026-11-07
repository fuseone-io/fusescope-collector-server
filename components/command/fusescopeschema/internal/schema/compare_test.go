// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"strings"
	"testing"
)

func sample() Snapshot {
	return Snapshot{
		"meter.samples": {
			Kind: KindTable, Engine: "ReplicatedMergeTree", EngineArgs: []string{"'/clickhouse/tables/{uuid}/{shard}'", "'{replica}'"},
			SortingKey: "fingerprint, unix_milli", PrimaryKey: "fingerprint, unix_milli", PartitionKey: "toDate(unix_milli / 1000)",
			TTL: "toDateTime(unix_milli / 1000) + toIntervalSecond(2592000)", Settings: []string{"index_granularity = 8192"},
			Columns: []Column{
				{Name: "fingerprint", Type: "UInt64", Codec: "CODEC(Delta(8), ZSTD(1))"},
				{Name: "unix_milli", Type: "Int64", Codec: "CODEC(DoubleDelta, ZSTD(1))"},
				{Name: "value", Type: "Float64", Codec: "CODEC(Gorilla, ZSTD(1))"},
			},
			Indexes:     []Index{{Name: "idx_fp", Type: "minmax", Expression: "fingerprint", Granularity: 1}},
			Projections: []Projection{{Name: "by_time", Query: "SELECT * ORDER BY unix_milli"}},
		},
		"meter.distributed_samples": {
			Kind: KindDistributed, Engine: "Distributed",
			EngineArgs: []string{"'{cluster}'", "'{db:meter}'", "'samples'", "cityHash64(fingerprint)"},
			Columns:    []Column{{Name: "fingerprint", Type: "UInt64"}},
		},
		"meter.samples_agg_1d_mv": {
			Kind: KindMaterializedView, Target: "{db:meter}.samples_agg_1d", Definer: DefinerPlaceholder, SQLSecurity: "DEFINER",
			Query: "SELECT fingerprint, intDiv(unix_milli, 86400000) * 86400000 AS unix_milli FROM {db:meter}.samples",
		},
	}
}

func TestCompare(t *testing.T) {
	type change func(Snapshot)
	set := func(name string, f func(*Object)) change {
		return func(s Snapshot) {
			o := s[name]
			o.Columns = append([]Column(nil), o.Columns...)
			o.Indexes = append([]Index(nil), o.Indexes...)
			f(&o)
			s[name] = o
		}
	}
	tests := []struct {
		name   string
		change change
		want   []string // "<kind> <object> <part>" of each difference
	}{
		{"identical", func(Snapshot) {}, nil},
		{"an object missing", func(s Snapshot) { delete(s, "meter.distributed_samples") }, []string{"missing meter.distributed_samples "}},
		{"an extra object", func(s Snapshot) { s["meter.extra"] = Object{Kind: KindTable} }, []string{"extra meter.extra "}},
		{"another kind", set("meter.distributed_samples", func(o *Object) { o.Kind = KindTable }), []string{"changed meter.distributed_samples kind"}},
		{"another engine", set("meter.samples", func(o *Object) { o.Engine = "ReplicatedReplacingMergeTree" }), []string{"changed meter.samples engine"}},
		{"another Keeper path", set("meter.samples", func(o *Object) { o.EngineArgs = []string{"'/clickhouse/tables/{shard}/samples'", "'{replica}'"} }), []string{"changed meter.samples engine arguments"}},
		{"another sharding key", set("meter.distributed_samples", func(o *Object) { o.EngineArgs[3] = "rand()" }), []string{"changed meter.distributed_samples engine arguments"}},
		{"a distributed table over another database", set("meter.distributed_samples", func(o *Object) {
			o.EngineArgs = []string{"'{cluster}'", "'{db:metrics}'", "'samples'", "cityHash64(fingerprint)"}
		}), []string{"changed meter.distributed_samples engine arguments"}},
		{"another sorting key", set("meter.samples", func(o *Object) { o.SortingKey = "unix_milli" }), []string{"changed meter.samples sorting key"}},
		{"another partition key", set("meter.samples", func(o *Object) { o.PartitionKey = "toYYYYMM(unix_milli)" }), []string{"changed meter.samples partition key"}},
		{"a sampling key added", set("meter.samples", func(o *Object) { o.SamplingKey = "fingerprint" }), []string{"changed meter.samples sampling key"}},
		{"another setting", set("meter.samples", func(o *Object) { o.Settings = []string{"index_granularity = 1024"} }), []string{"changed meter.samples settings"}},
		{"a TTL on a table no retention targets", set("meter.samples", func(o *Object) { o.TTL = "toDateTime(unix_milli / 1000) + toIntervalSecond(1)" }), []string{"changed meter.samples ttl"}},
		{"a column missing", set("meter.samples", func(o *Object) { o.Columns = o.Columns[:2] }), []string{"missing meter.samples column value"}},
		{"an extra column", set("meter.samples", func(o *Object) { o.Columns = append(o.Columns, Column{Name: "x", Type: "String"}) }), []string{"extra meter.samples column x"}},
		{"a column's type", set("meter.samples", func(o *Object) { o.Columns[2].Type = "Float32" }), []string{"changed meter.samples column value"}},
		{"a column's codec", set("meter.samples", func(o *Object) { o.Columns[2].Codec = "CODEC(ZSTD(1))" }), []string{"changed meter.samples column value"}},
		{"a column's default", set("meter.samples", func(o *Object) { o.Columns[2].DefaultKind, o.Columns[2].DefaultExpression = "DEFAULT", "0" }), []string{"changed meter.samples column value"}},
		{"a column's comment", set("meter.samples", func(o *Object) { o.Columns[2].Comment = "v" }), []string{"changed meter.samples column value"}},
		{"columns reordered", set("meter.samples", func(o *Object) { o.Columns[0], o.Columns[1] = o.Columns[1], o.Columns[0] }), []string{"changed meter.samples column order"}},
		{"an index missing", set("meter.samples", func(o *Object) { o.Indexes = nil }), []string{"missing meter.samples index idx_fp"}},
		{"an index's granularity", set("meter.samples", func(o *Object) { o.Indexes[0].Granularity = 4 }), []string{"changed meter.samples index idx_fp"}},
		{"an index's type arguments", set("meter.samples", func(o *Object) { o.Indexes[0].Type = "set(10)" }), []string{"changed meter.samples index idx_fp"}},
		{"a projection's query", set("meter.samples", func(o *Object) {
			o.Projections = []Projection{{Name: "by_time", Query: "SELECT * ORDER BY fingerprint"}}
		}), []string{"changed meter.samples projection by_time"}},
		{"a materialized view's query", set("meter.samples_agg_1d_mv", func(o *Object) { o.Query = strings.Replace(o.Query, "86400000", "3600000", 2) }), []string{"changed meter.samples_agg_1d_mv query"}},
		{"a materialized view's refresh", set("meter.samples_agg_1d_mv", func(o *Object) { o.Refresh = "REFRESH EVERY 5 MINUTE APPEND" }), []string{"changed meter.samples_agg_1d_mv refresh"}},
		{"a materialized view's target", set("meter.samples_agg_1d_mv", func(o *Object) { o.Target = "{db:meter}.samples" }), []string{"changed meter.samples_agg_1d_mv target"}},
		{"a view bound to a third user", set("meter.samples_agg_1d_mv", func(o *Object) { o.Definer = "intruder" }), []string{"changed meter.samples_agg_1d_mv definer"}},
		{"a view with no definer", set("meter.samples_agg_1d_mv", func(o *Object) { o.Definer = "" }), []string{"changed meter.samples_agg_1d_mv definer"}},
		{"a view with another SQL security", set("meter.samples_agg_1d_mv", func(o *Object) { o.SQLSecurity = "NONE" }), []string{"changed meter.samples_agg_1d_mv sql security"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := sample()
			// One replica differs: the difference counts, on that replica only.
			live := map[string]Snapshot{"r1": sample(), "r2": sample(), "r3": sample(), "r4": sample()}
			tt.change(live["r3"])
			canon := &fakeCanon{}
			diffs, err := Compare(context.Background(), want, live, canon)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, d := range diffs {
				if d.Replica != "r3" {
					t.Errorf("difference on %s: %v", d.Replica, d)
				}
				got = append(got, string(d.Kind)+" "+d.Object+" "+d.Part)
			}
			if strings.Join(got, "|") != strings.Join(tt.want, "|") {
				t.Fatalf("diffs %q\nwant %q", got, tt.want)
			}
			if canon.calls > 2 {
				t.Errorf("%d canonicalisation calls, want at most one per kind", canon.calls)
			}
		})
	}
}

// A view whose expected structure names no SQL security has its definer and
// mode left uncompared (§5.4).
func TestCompareLeavesUnnamedDefinersAlone(t *testing.T) {
	want := sample()
	mv := want["meter.samples_agg_1d_mv"]
	mv.Definer, mv.SQLSecurity = "", ""
	want["meter.samples_agg_1d_mv"] = mv
	live := map[string]Snapshot{"r1": sample()}
	o := live["r1"]["meter.samples_agg_1d_mv"]
	o.Definer, o.SQLSecurity = "intruder", "NONE"
	live["r1"]["meter.samples_agg_1d_mv"] = o
	diffs, err := Compare(context.Background(), want, live, &fakeCanon{})
	if err != nil || len(diffs) != 0 {
		t.Fatalf("Compare = %v, %v; want no difference", diffs, err)
	}
}

// The allowances of every replica are canonicalised in one call per kind.
func TestCompareCanonicalisesInOneCallPerKind(t *testing.T) {
	want := base()
	live := map[string]Snapshot{}
	for _, r := range []string{"r1", "r2", "r3", "r4"} {
		s := withColumns(clone(want), "logs.logs_v2", promotedString("k."+r)...)
		o := s["traces.trace_summary"]
		o.TTL = "toDateTime(end) + toIntervalSecond(86400)"
		s["traces.trace_summary"] = o
		live[r] = s
	}
	canon := &fakeCanon{}
	diffs, err := Compare(context.Background(), want, live, canon)
	if err != nil || len(diffs) > 0 {
		t.Fatalf("Compare = %v, %v", diffs, err)
	}
	if canon.calls != 2 {
		t.Fatalf("%d canonicalisation calls, want 2", canon.calls)
	}
}

func TestDiffString(t *testing.T) {
	d := Diff{Replica: "ch-1-1/uuid", Object: "logs.logs_v2", Kind: Changed, Part: "column body", Want: "body String", Got: "body LowCardinality(String)"}
	if got := d.String(); got != "ch-1-1/uuid: logs.logs_v2 column body differs: want body String, got body LowCardinality(String)" {
		t.Fatal(got)
	}
}
