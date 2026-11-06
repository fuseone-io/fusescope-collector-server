// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"slices"
	"testing"
)

// The engine_full values are as ClickHouse 25.12 shows the observed schema
// (docs/spec/observed/0.145.0).
func TestParseEngine(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want Engine
	}{
		{
			"replicated, with clauses and settings",
			"ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') PARTITION BY (toDate(timestamp / 1000000000), _retention_days, _retention_days_cold) ORDER BY (ts_bucket_start, resource_fingerprint, severity_text, timestamp, id) TTL toDateTime(timestamp / 1000000000) + toIntervalDay(_retention_days) SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, object_serialization_version = 'v3'",
			Engine{
				Name: "ReplicatedMergeTree", Args: []string{"'/clickhouse/tables/{uuid}/{shard}'", "'{replica}'"},
				TTL:      "toDateTime(timestamp / 1000000000) + toIntervalDay(_retention_days)",
				Settings: []string{"index_granularity = 8192", "ttl_only_drop_parts = 1", "object_serialization_version = 'v3'"},
			},
		},
		{
			"distributed",
			"Distributed('cluster', 'signoz_logs', 'logs_v2', cityHash64(id))",
			Engine{Name: "Distributed", Args: []string{"'cluster'", "'signoz_logs'", "'logs_v2'", "cityHash64(id)"}},
		},
		{
			"a quoted comma, a nested call and an escaped quote stay in one argument",
			`ReplicatedReplacingMergeTree('/p/{uuid}/{shard}', '{replica}', ver) ORDER BY (a, b) SETTINGS x = 'a, b\' TTL c'`,
			Engine{Name: "ReplicatedReplacingMergeTree", Args: []string{"'/p/{uuid}/{shard}'", "'{replica}'", "ver"}, Settings: []string{`x = 'a, b\' TTL c'`}},
		},
		{
			"a TTL with several rules",
			"ReplicatedMergeTree('/p', '{replica}') ORDER BY a TTL toDateTime(t) + toIntervalDay(15), toDateTime(t) + toIntervalDay(7) TO VOLUME 'cold' SETTINGS index_granularity = 8192",
			Engine{Name: "ReplicatedMergeTree", Args: []string{"'/p'", "'{replica}'"}, TTL: "toDateTime(t) + toIntervalDay(15), toDateTime(t) + toIntervalDay(7) TO VOLUME 'cold'", Settings: []string{"index_granularity = 8192"}},
		},
		{"no arguments", "Memory", Engine{Name: "Memory"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseEngine(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			if got.Name != tt.want.Name || !slices.Equal(got.Args, tt.want.Args) || got.TTL != tt.want.TTL || !slices.Equal(got.Settings, tt.want.Settings) {
				t.Fatalf("ParseEngine = %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

func TestParseEngineRefusesWhatItCannotSplit(t *testing.T) {
	for _, in := range []string{"", "MergeTree('a'", "MergeTree('a) ORDER BY x", "MergeTree() ORDER BY (a"} {
		if _, err := ParseEngine(in); err == nil {
			t.Errorf("ParseEngine(%q) accepted", in)
		}
	}
}

func TestReplaceDatabases(t *testing.T) {
	dbs := map[string]string{"signoz_meter": "meter", "my-logs": "logs"}
	tests := []struct{ in, want string }{
		{"SELECT a FROM signoz_meter.samples GROUP BY a", "SELECT a FROM {db:meter}.samples GROUP BY a"},
		{"SELECT a FROM `my-logs`.logs_v2", "SELECT a FROM {db:logs}.logs_v2"},
		{"SELECT 'signoz_meter.samples', x.signoz_meter.y, signoz_meter", "SELECT 'signoz_meter.samples', x.signoz_meter.y, signoz_meter"},
		{"SELECT signoz_meterx.a, `signoz_meter`.b", "SELECT signoz_meterx.a, {db:meter}.b"},
		{`SELECT 'it\'s signoz_meter.a' AS s FROM signoz_meter.t`, `SELECT 'it\'s signoz_meter.a' AS s FROM {db:meter}.t`},
	}
	for _, tt := range tests {
		if got := ReplaceDatabases(tt.in, dbs); got != tt.want {
			t.Errorf("ReplaceDatabases(%q)\n = %q\nwant %q", tt.in, got, tt.want)
		}
	}
}
