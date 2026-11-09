// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// fakeCanon imitates what ClickHouse 25.12's formatQuery does to the texts
// these tests use (observed in §11's cluster): x::String becomes
// CAST(x, 'String'), INTERVAL n SECOND becomes toIntervalSecond(n), a TTL
// rule loses its DELETE, and runs of spaces collapse.
type fakeCanon struct{ calls int }

var (
	castRe     = regexp.MustCompile("((?:[A-Za-z_][A-Za-z0-9_$]*|`[^`]*`)(?:\\.(?:[A-Za-z_][A-Za-z0-9_$]*|`[^`]*`))*)::String")
	intervalRe = regexp.MustCompile(`INTERVAL (\d+) SECOND`)
	spacesRe   = regexp.MustCompile(` +`)
)

func fakeFormat(s string) string {
	s = castRe.ReplaceAllString(s, "CAST($1, 'String')")
	s = intervalRe.ReplaceAllString(s, "toIntervalSecond($1)")
	return spacesRe.ReplaceAllString(strings.TrimSpace(s), " ")
}

func (f *fakeCanon) Expressions(_ context.Context, in []string) ([]string, error) {
	f.calls++
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = fakeFormat(s)
	}
	return out, nil
}

func (f *fakeCanon) TTLs(_ context.Context, in []string) ([]string, error) {
	f.calls++
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = fakeFormat(strings.ReplaceAll(s, " DELETE", ""))
	}
	return out, nil
}

// base is a fragment of the observed tables §8 names.
func base() Snapshot {
	retention := []Column{
		{Name: "_retention_days", Type: "UInt16", DefaultKind: "DEFAULT", DefaultExpression: "15"},
		{Name: "_retention_days_cold", Type: "UInt16", DefaultKind: "DEFAULT", DefaultExpression: "0"},
	}
	logs := Object{Kind: KindTable, Engine: "ReplicatedMergeTree", TTL: "toDateTime(timestamp / 1000000000) + toIntervalDay(_retention_days)", Settings: logsSettings, Columns: append([]Column{
		{Name: "attributes_string", Type: "Map(LowCardinality(String), String)", Codec: "CODEC(ZSTD(1))"},
		{Name: "body_v2", Type: "JSON(max_dynamic_paths=0, message String)", Codec: "CODEC(ZSTD(1))"},
	}, retention...)}
	spans := Object{Kind: KindTable, Engine: "ReplicatedMergeTree", TTL: "toDateTime(timestamp) + toIntervalSecond(1296000)", Columns: []Column{
		{Name: "attributes_number", Type: "Map(LowCardinality(String), Float64)", Codec: "CODEC(ZSTD(1))"},
		{Name: "attributes", Type: "JSON(max_dynamic_paths=0)", Codec: "CODEC(ZSTD(1))"},
	}}
	return Snapshot{
		"logs.logs_v2":                   logs,
		"logs.distributed_logs_v2":       {Kind: KindDistributed, Engine: "Distributed", Columns: logs.Columns},
		"logs.logs_v2_resource":          {Kind: KindTable, Engine: "ReplicatedReplacingMergeTree", TTL: "(toDateTime(seen_at_ts_bucket_start) + toIntervalSecond(1800)) + toIntervalDay(_retention_days)", Columns: retention},
		"logs.tag_attributes_v2":         {Kind: KindTable, Engine: "ReplicatedReplacingMergeTree", TTL: "toDateTime(unix_milli / 1000) + toIntervalSecond(172800)", Settings: logsSettings, Columns: retention},
		"traces.signoz_index_v3":         spans,
		"traces.trace_summary":           {Kind: KindTable, Engine: "ReplicatedAggregatingMergeTree", TTL: "toDateTime(end) + toIntervalSecond(1296000)"},
		"traces.span_attributes_keys":    {Kind: KindTable, Engine: "ReplicatedReplacingMergeTree", TTL: "toDateTime(timestamp) + toIntervalSecond(1296000)"},
		"metrics.samples_v4":             {Kind: KindTable, Engine: "ReplicatedMergeTree", TTL: "toDateTime(toUInt32(unix_milli / 1000), 'UTC') + toIntervalSecond(2592000)"},
		"metrics.distributed_samples_v4": {Kind: KindDistributed, Engine: "Distributed"},
	}
}

var logsSettings = []string{"index_granularity = 8192", "ttl_only_drop_parts = 1"}

// tiered is the setting the backend's cold storage writes (reader.go about line 1929), as the
// server shows it in engine_full: appended after the table's own settings (observed on
// ClickHouse 25.12.5, 2026-11-09).
const tiered = "storage_policy = 'tiered'"

// withSettings replaces the tables' settings with their expected ones followed by extra.
func withSettings(extra []string, tables ...string) func(Snapshot) Snapshot {
	return func(s Snapshot) Snapshot {
		for _, table := range tables {
			o := s[table]
			o.Settings = append(slices.Clone(o.Settings), extra...)
			s[table] = o
		}
		return s
	}
}

func clone(s Snapshot) Snapshot {
	out := Snapshot{}
	for k, o := range s {
		o.Columns = append([]Column(nil), o.Columns...)
		o.Indexes = append([]Index(nil), o.Indexes...)
		out[k] = o
	}
	return out
}

func withColumns(s Snapshot, table string, cols ...Column) Snapshot {
	o := s[table]
	o.Columns = append(o.Columns, cols...)
	s[table] = o
	return s
}

func withIndexes(s Snapshot, table string, idx ...Index) Snapshot {
	o := s[table]
	o.Indexes = append(o.Indexes, idx...)
	s[table] = o
	return s
}

func promotedString(key string) []Column {
	name := "attribute_string_" + strings.ReplaceAll(key, ".", "$$")
	return []Column{
		{Name: name, Type: "String", DefaultKind: "DEFAULT", DefaultExpression: "attributes_string['" + key + "']", Codec: "CODEC(ZSTD(1))"},
		{Name: name + "_exists", Type: "Bool", DefaultKind: "DEFAULT", DefaultExpression: "if(mapContains(attributes_string, '" + key + "') != 0, true, false)", Codec: "CODEC(ZSTD(1))"},
	}
}

func retention(table, column, def string) func(Snapshot) Snapshot {
	return func(s Snapshot) Snapshot {
		o := s[table]
		i := slices.IndexFunc(o.Columns, func(c Column) bool { return c.Name == column })
		o.Columns[i].DefaultExpression = def
		s[table] = o
		return s
	}
}

// Each §8 pattern, with a nearly identical change that must still count.
func TestAllowList(t *testing.T) {
	number := Column{Name: "attribute_number_http$$status", Type: "Float64", DefaultKind: "DEFAULT", DefaultExpression: "attributes_number['http.status']", Codec: "CODEC(ZSTD(1))"}
	tests := []struct {
		name    string
		live    func(Snapshot) Snapshot
		allowed bool
	}{
		{"nothing changed", func(s Snapshot) Snapshot { return s }, true},

		{"promoted string column and its _exists, on logs_v2 and distributed_logs_v2", func(s Snapshot) Snapshot {
			s = withColumns(s, "logs.logs_v2", promotedString("http.route")...)
			return withColumns(s, "logs.distributed_logs_v2", promotedString("http.route")...)
		}, true},
		{"promoted column with another codec", func(s Snapshot) Snapshot {
			c := promotedString("http.route")[0]
			c.Codec = "CODEC(ZSTD(3))"
			return withColumns(s, "logs.logs_v2", c)
		}, false},
		{"promoted column whose DEFAULT reads another key", func(s Snapshot) Snapshot {
			c := promotedString("http.route")[0]
			c.DefaultExpression = "attributes_string['http.url']"
			return withColumns(s, "logs.logs_v2", c)
		}, false},
		{"promoted column whose DEFAULT reads another map", func(s Snapshot) Snapshot {
			c := promotedString("http.route")[0]
			c.DefaultExpression = "resources_string['http.route']"
			return withColumns(s, "logs.logs_v2", c)
		}, false},
		{"promoted string column with type Float64", func(s Snapshot) Snapshot {
			c := promotedString("http.route")[0]
			c.Type = "Float64"
			return withColumns(s, "logs.logs_v2", c)
		}, false},
		{"promoted column as MATERIALIZED", func(s Snapshot) Snapshot {
			c := promotedString("http.route")[0]
			c.DefaultKind = "MATERIALIZED"
			return withColumns(s, "logs.logs_v2", c)
		}, false},
		{"promoted column on a table §8 does not name", func(s Snapshot) Snapshot {
			return withColumns(s, "logs.tag_attributes_v2", promotedString("http.route")[0])
		}, false},
		{"promoted columns of two keys, interleaved with their _exists", func(s Snapshot) Snapshot {
			return withColumns(s, "logs.logs_v2", append(promotedString("a"), promotedString("b")...)...)
		}, true},
		{"a promoted column moved before a base column", func(s Snapshot) Snapshot {
			o := s["logs.logs_v2"]
			o.Columns = append(promotedString("http.route"), o.Columns...)
			s["logs.logs_v2"] = o
			return s
		}, false},
		{"a promoted column between base columns", func(s Snapshot) Snapshot {
			o := s["logs.logs_v2"]
			cols := append([]Column{o.Columns[0]}, promotedString("http.route")...)
			o.Columns = append(cols, o.Columns[1:]...)
			s["logs.logs_v2"] = o
			return s
		}, false},
		{"an _exists column before its promoted column", func(s Snapshot) Snapshot {
			cols := promotedString("http.route")
			return withColumns(s, "logs.logs_v2", cols[1], cols[0])
		}, false},
		{"an _exists column without its promoted column", func(s Snapshot) Snapshot {
			return withColumns(s, "logs.logs_v2", promotedString("http.route")[1])
		}, false},
		{"an _exists column testing another key", func(s Snapshot) Snapshot {
			cols := promotedString("http.route")
			cols[1].DefaultExpression = "if(mapContains(attributes_string, 'http.url') != 0, true, false)"
			return withColumns(s, "logs.logs_v2", cols...)
		}, false},
		{"RUM resource columns on signoz_index_v3", func(s Snapshot) Snapshot {
			return withColumns(s, "traces.signoz_index_v3",
				Column{Name: "resource_string_fusescope$$rum$$platform", Type: "String", DefaultKind: "DEFAULT", DefaultExpression: "resources_string['fusescope.rum.platform']", Codec: "CODEC(ZSTD(1))"},
				Column{Name: "resource_string_fusescope$$rum$$platform_exists", Type: "Bool", DefaultKind: "DEFAULT", DefaultExpression: "if(mapContains(resources_string, 'fusescope.rum.platform') != 0, true, false)", Codec: "CODEC(ZSTD(1))"})
		}, true},
		{"a column that is not a pattern", func(s Snapshot) Snapshot {
			return withColumns(s, "logs.logs_v2", Column{Name: "extra", Type: "String"})
		}, false},

		{"_idx and _minmax_idx over a promoted number column", func(s Snapshot) Snapshot {
			s = withColumns(s, "traces.signoz_index_v3", number)
			return withIndexes(s, "traces.signoz_index_v3",
				Index{Name: number.Name + "_idx", Type: "bloom_filter(0.01)", Expression: "`" + number.Name + "`", Granularity: 64},
				Index{Name: number.Name + "_minmax_idx", Type: "minmax", Expression: "`" + number.Name + "`", Granularity: 1})
		}, true},
		{"_idx over a column that is not promoted", func(s Snapshot) Snapshot {
			return withIndexes(s, "traces.signoz_index_v3", Index{Name: "attributes_number_idx", Type: "bloom_filter(0.01)", Expression: "attributes_number", Granularity: 64})
		}, false},
		{"_idx over another column than its name says", func(s Snapshot) Snapshot {
			s = withColumns(s, "traces.signoz_index_v3", number)
			return withIndexes(s, "traces.signoz_index_v3", Index{Name: number.Name + "_idx", Type: "bloom_filter(0.01)", Expression: "lower(`" + number.Name + "`)", Granularity: 64})
		}, false},
		{"_idx of a type the backend does not accept", func(s Snapshot) Snapshot {
			s = withColumns(s, "traces.signoz_index_v3", number)
			return withIndexes(s, "traces.signoz_index_v3", Index{Name: number.Name + "_idx", Type: "full_text", Expression: "`" + number.Name + "`", Granularity: 64})
		}, false},
		{"_minmax_idx with granularity 4", func(s Snapshot) Snapshot {
			s = withColumns(s, "traces.signoz_index_v3", number)
			return withIndexes(s, "traces.signoz_index_v3", Index{Name: number.Name + "_minmax_idx", Type: "minmax", Expression: "`" + number.Name + "`", Granularity: 4})
		}, false},
		{"_minmax_idx over a promoted string column", func(s Snapshot) Snapshot {
			c := promotedString("http.route")[0]
			s = withColumns(s, "logs.logs_v2", c)
			return withIndexes(s, "logs.logs_v2", Index{Name: c.Name + "_minmax_idx", Type: "minmax", Expression: "`" + c.Name + "`", Granularity: 1})
		}, false},
		{"_idx on a distributed table", func(s Snapshot) Snapshot {
			c := promotedString("http.route")[0]
			s = withColumns(s, "logs.distributed_logs_v2", c)
			return withIndexes(s, "logs.distributed_logs_v2", Index{Name: c.Name + "_idx", Type: "bloom_filter(0.01)", Expression: "`" + c.Name + "`", Granularity: 64})
		}, false},
		{"_idx over a promoted bool column", func(s Snapshot) Snapshot {
			c := Column{Name: "attribute_bool_retry", Type: "Bool", DefaultKind: "DEFAULT", DefaultExpression: "attributes_bool['retry']", Codec: "CODEC(ZSTD(1))"}
			s = withColumns(s, "logs.logs_v2", c)
			return withIndexes(s, "logs.logs_v2", Index{Name: c.Name + "_idx", Type: "bloom_filter(0.01)", Expression: "`" + c.Name + "`", Granularity: 64})
		}, false},

		{"logs JSON subcolumn indexes, string and number", func(s Snapshot) Snapshot {
			return withIndexes(s, "logs.logs_v2",
				Index{Name: "body_v2.user.e-mail_string_ngrambf_v1", Type: "ngrambf_v1(4, 15000, 3, 0)", Expression: "lower(assumeNotNull(dynamicElement(body_v2.user.`e-mail`, 'String')))", Granularity: 1},
				Index{Name: "body_v2.http.status_int64_minmax", Type: "minmax", Expression: "assumeNotNull(dynamicElement(body_v2.http.status, 'Int64'))", Granularity: 8})
		}, true},
		{"logs JSON string index without lower()", func(s Snapshot) Snapshot {
			return withIndexes(s, "logs.logs_v2", Index{Name: "body_v2.message_string_tokenbf_v1", Type: "tokenbf_v1(10000, 2, 0)", Expression: "assumeNotNull(dynamicElement(body_v2.message, 'String'))", Granularity: 1})
		}, false},
		{"logs JSON index whose type differs from its name", func(s Snapshot) Snapshot {
			return withIndexes(s, "logs.logs_v2", Index{Name: "body_v2.message_string_tokenbf_v1", Type: "ngrambf_v1(4, 15000, 3, 0)", Expression: "lower(assumeNotNull(dynamicElement(body_v2.message, 'String')))", Granularity: 1})
		}, false},
		{"JSON index on a column that is not a target", func(s Snapshot) Snapshot {
			return withIndexes(s, "logs.logs_v2", Index{Name: "resource.host_string_tokenbf_v1", Type: "tokenbf_v1(10000, 2, 0)", Expression: "lower(assumeNotNull(dynamicElement(resource.host, 'String')))", Granularity: 1})
		}, false},
		{"traces JSON subcolumn indexes, string and number", func(s Snapshot) Snapshot {
			return withIndexes(s, "traces.signoz_index_v3",
				Index{Name: "attributes.http.route_string_bloom_filter", Type: "bloom_filter(0.01)", Expression: "CAST(attributes.`http.route`, 'String')", Granularity: 4},
				Index{Name: "attributes_promoted.http.status_float64_minmax", Type: "minmax", Expression: "toFloat64OrNull(CAST(attributes_promoted.`http.status`, 'String'))", Granularity: 1})
		}, true},
		{"traces JSON index in the logs spelling", func(s Snapshot) Snapshot {
			return withIndexes(s, "traces.signoz_index_v3", Index{Name: "attributes.http.route_string_bloom_filter", Type: "bloom_filter(0.01)", Expression: "lower(assumeNotNull(dynamicElement(attributes.http.route, 'String')))", Granularity: 4})
		}, false},
		{"traces JSON index on Int64, which traces fold into Float64", func(s Snapshot) Snapshot {
			return withIndexes(s, "traces.signoz_index_v3", Index{Name: "attributes.http.status_int64_minmax", Type: "minmax", Expression: "toFloat64OrNull(CAST(attributes.`http.status`, 'String'))", Granularity: 1})
		}, false},

		{"_retention_days DEFAULT changed on the logs and logs-resource tables", func(s Snapshot) Snapshot {
			for _, table := range []string{"logs.logs_v2", "logs.distributed_logs_v2", "logs.logs_v2_resource"} {
				o := s[table]
				o.Columns[len(o.Columns)-2].DefaultExpression = "multiIf(resources_string['service.name'] IN ('api'), 30, 15)"
				if table == "logs.logs_v2_resource" {
					o.Columns[len(o.Columns)-2].DefaultExpression = "multiIf(JSONExtractString(labels, 'service.name') IN ('api'), 30, 15)"
				}
				o.Columns[len(o.Columns)-1].DefaultExpression = "7"
				s[table] = o
			}
			return s
		}, true},
		{"_retention_days as a plain number", retention("logs.logs_v2", "_retention_days", "30"), true},
		{"_retention_days as the backend's multiIf on a resource table", retention("logs.logs_v2_resource", "_retention_days", "multiIf(JSONExtractString(labels, 'service.name') IN ('api'), 30, 15)"), true},
		{"_retention_days as a multiIf with several rules and conditions", retention("logs.distributed_logs_v2", "_retention_days",
			"multiIf(resources_string['service.name'] IN ('api', 'web') AND resources_string['env'] IN ('prod'), 30, resources_string['team'] IN ('a'), 7, 15)"), true},
		{"_retention_days as arithmetic", retention("logs.logs_v2", "_retention_days", "7 + 1"), false},
		{"_retention_days as an arbitrary function", retention("logs.logs_v2", "_retention_days", "toUInt16(rand() % 30)"), false},
		{"_retention_days as a multiIf over attributes", retention("logs.logs_v2", "_retention_days", "multiIf(attributes_string['service.name'] IN ('api'), 30, 15)"), false},
		{"_retention_days as a multiIf with labels on the logs table", retention("logs.logs_v2", "_retention_days", "multiIf(JSONExtractString(labels, 'service.name') IN ('api'), 30, 15)"), false},
		{"_retention_days as a multiIf with = instead of IN", retention("logs.logs_v2", "_retention_days", "multiIf(resources_string['service.name'] = 'api', 30, 15)"), false},
		{"_retention_days as a multiIf with OR", retention("logs.logs_v2", "_retention_days", "multiIf(resources_string['a'] IN ('x') OR resources_string['b'] IN ('y'), 30, 15)"), false},
		{"_retention_days as a multiIf whose days are computed", retention("logs.logs_v2", "_retention_days", "multiIf(resources_string['a'] IN ('x'), 30 * 2, 15)"), false},
		{"_retention_days as a multiIf without a default", retention("logs.logs_v2", "_retention_days", "multiIf(resources_string['a'] IN ('x'), 30)"), false},
		{"_retention_days as a multiIf matching a computed value", retention("logs.logs_v2", "_retention_days", "multiIf(resources_string['a'] IN (lower('X')), 30, 15)"), false},
		{"_retention_days_cold as a number", retention("logs.logs_v2", "_retention_days_cold", "7"), true},
		{"_retention_days_cold as a multiIf", retention("logs.logs_v2", "_retention_days_cold", "multiIf(resources_string['a'] IN ('x'), 30, 15)"), false},
		{"_retention_days_cold as arithmetic", retention("logs.logs_v2", "_retention_days_cold", "7 + 1"), false},
		{"_retention_days_cold negative", retention("logs.logs_v2", "_retention_days_cold", "-1"), false},
		{"_retention_days with another type", func(s Snapshot) Snapshot {
			o := s["logs.logs_v2"]
			o.Columns[len(o.Columns)-2].Type = "UInt32"
			s["logs.logs_v2"] = o
			return s
		}, false},
		{"_retention_days DEFAULT changed on a table §8 does not name", func(s Snapshot) Snapshot {
			o := s["logs.tag_attributes_v2"]
			o.Columns[0].DefaultExpression = "30"
			s["logs.tag_attributes_v2"] = o
			return s
		}, false},
		{"_retention_days turned MATERIALIZED", func(s Snapshot) Snapshot {
			o := s["logs.logs_v2"]
			o.Columns[len(o.Columns)-2].DefaultKind = "MATERIALIZED"
			s["logs.logs_v2"] = o
			return s
		}, false},

		{"retention TTLs, with and without a cold volume", func(s Snapshot) Snapshot {
			set := func(table, ttl string) {
				o := s[table]
				o.TTL = ttl
				s[table] = o
			}
			set("logs.logs_v2", "toDateTime(timestamp / 1000000000) + toIntervalDay(_retention_days), toDateTime(timestamp / 1000000000) + toIntervalDay(if(_retention_days_cold = 0, 7, _retention_days_cold)) TO VOLUME 'cold'")
			set("logs.logs_v2_resource", "toDateTime(seen_at_ts_bucket_start) + toIntervalSecond(1800) + toIntervalSecond(604800)") // the server adds parentheses; the integration test covers them
			set("traces.trace_summary", "toDateTime(end) + toIntervalSecond(2592000), toDateTime(end) + toIntervalSecond(86400) TO VOLUME 's3'")
			set("traces.span_attributes_keys", "toDateTime(timestamp) + toIntervalSecond(2592000)")
			set("metrics.samples_v4", "toDateTime(toUInt32(unix_milli / 1000), 'UTC') + toIntervalSecond(7776000)")
			return s
		}, true},
		{"a TTL on another time column", func(s Snapshot) Snapshot {
			o := s["traces.trace_summary"]
			o.TTL = "toDateTime(start) + toIntervalSecond(2592000)"
			s["traces.trace_summary"] = o
			return s
		}, false},
		{"a cold volume where the backend sets none", func(s Snapshot) Snapshot {
			o := s["traces.span_attributes_keys"]
			o.TTL = "toDateTime(timestamp) + toIntervalSecond(2592000), toDateTime(timestamp) + toIntervalSecond(86400) TO VOLUME 'cold'"
			s["traces.span_attributes_keys"] = o
			return s
		}, false},
		{"a TTL that deletes nothing but recompresses", func(s Snapshot) Snapshot {
			o := s["metrics.samples_v4"]
			o.TTL = "toDateTime(toUInt32(unix_milli / 1000), 'UTC') + toIntervalSecond(7776000) RECOMPRESS CODEC(ZSTD(9))"
			s["metrics.samples_v4"] = o
			return s
		}, false},
		{"cold storage's policy on the logs tables", withSettings([]string{tiered}, "logs.logs_v2", "logs.logs_v2_resource"), true},
		{"cold storage's policy on the traces tables", withSettings([]string{tiered}, "traces.signoz_index_v3", "traces.trace_summary", "traces.span_attributes_keys"), true},
		{"cold storage's policy on the metrics tables", withSettings([]string{tiered}, "metrics.samples_v4"), true},
		{"another storage policy", withSettings([]string{"storage_policy = 'cold'"}, "logs.logs_v2"), false},
		{"the policy on a table no retention path reaches", withSettings([]string{tiered}, "logs.tag_attributes_v2"), false},
		{"the policy on a distributed table", withSettings([]string{tiered}, "logs.distributed_logs_v2"), false},
		{"the policy and another setting", withSettings([]string{tiered, "merge_with_ttl_timeout = 3600"}, "logs.logs_v2"), false},
		{"another setting after the policy", withSettings([]string{"merge_with_ttl_timeout = 3600", tiered}, "logs.logs_v2"), false},
		{"the policy twice", withSettings([]string{tiered, tiered}, "logs.logs_v2"), false},
		{"the policy before the table's own settings", func(s Snapshot) Snapshot {
			o := s["logs.logs_v2"]
			o.Settings = append([]string{tiered}, logsSettings...)
			s["logs.logs_v2"] = o
			return s
		}, false},
		{"the policy with one of the table's settings changed", func(s Snapshot) Snapshot {
			o := s["logs.logs_v2"]
			o.Settings = []string{"index_granularity = 1024", "ttl_only_drop_parts = 1", tiered}
			s["logs.logs_v2"] = o
			return s
		}, false},
		{"one of the table's settings dropped for the policy", func(s Snapshot) Snapshot {
			o := s["logs.logs_v2"]
			o.Settings = []string{"index_granularity = 8192", tiered}
			s["logs.logs_v2"] = o
			return s
		}, false},
		{"a TTL changed on a table the retention settings do not target", func(s Snapshot) Snapshot {
			o := s["logs.tag_attributes_v2"]
			o.TTL = "toDateTime(unix_milli / 1000) + toIntervalSecond(86400)"
			s["logs.tag_attributes_v2"] = o
			return s
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := base()
			live := map[string]Snapshot{"r1": clone(want), "r2": tt.live(clone(want))}
			diffs, err := Compare(context.Background(), want, live, &fakeCanon{})
			if err != nil {
				t.Fatal(err)
			}
			if tt.allowed != (len(diffs) == 0) {
				t.Fatalf("allowed = %v, diffs %v", len(diffs) == 0, diffs)
			}
			for _, d := range diffs {
				if d.Replica != "r2" {
					t.Errorf("a difference reported on the unchanged replica: %v", d)
				}
			}
		})
	}
}

// The tables the four retention paths call setColdStorage on: setTTLLogs and SetTTLV2 (the four
// logs tables), setTTLTraces (seven) and setTTLMetrics (eight), in reader.go about lines 942,
// 1059, 1273 and 1734.
func TestColdStorageTables(t *testing.T) {
	want := []string{
		"logs.logs_attribute_keys", "logs.logs_resource_keys", "logs.logs_v2", "logs.logs_v2_resource",
		"metrics.exp_hist", "metrics.samples_v4", "metrics.samples_v4_agg_30m", "metrics.samples_v4_agg_5m",
		"metrics.time_series_v4", "metrics.time_series_v4_1day", "metrics.time_series_v4_1week", "metrics.time_series_v4_6hrs",
		"traces.dependency_graph_minutes_v2", "traces.signoz_error_index_v2", "traces.signoz_index_v3", "traces.span_attributes_keys",
		"traces.trace_summary", "traces.traces_v3_resource", "traces.usage_explorer",
	}
	if got := slices.Sorted(slices.Values(coldStorageTables)); !slices.Equal(got, want) {
		t.Fatalf("coldStorageTables = %q\nwant %q", got, want)
	}
}
