// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package chstore

import (
	"context"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

// observed is the schema the fork 0.145.0 creates, as SHOW CREATE printed it
// (docs/spec/observed/0.145.0).
var observed = filepath.Join("..", "..", "..", "..", "..", "docs", "spec", "observed", "0.145.0")

// applyObserved drops the six databases and creates the observed schema on
// the cluster: local tables, then distributed tables, then materialized views.
func applyObserved(t *testing.T, c *Conn) {
	t.Helper()
	ctx := context.Background()
	list, err := os.ReadFile(filepath.Join(observed, "tables.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	var objects [][]string
	for _, line := range strings.Split(strings.TrimSpace(string(list)), "\n") {
		objects = append(objects, strings.Split(line, "\t"))
	}
	rank := func(engine string) int {
		switch engine {
		case "Distributed":
			return 1
		case "MaterializedView":
			return 2
		}
		return 0
	}
	slices.SortStableFunc(objects, func(a, b []string) int { return rank(a[2]) - rank(b[2]) })
	for _, db := range schema.DefaultDatabases {
		for _, q := range []string{"DROP DATABASE IF EXISTS " + db + " ON CLUSTER cluster SYNC", "CREATE DATABASE " + db + " ON CLUSTER cluster"} {
			if err := c.Exec(ctx, q, ddl...); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, o := range objects {
		b, err := os.ReadFile(filepath.Join(observed, "ddl", o[0]+"."+o[1]+".sql"))
		if err != nil {
			t.Fatal(err)
		}
		head := regexp.MustCompile(`^(CREATE (?:TABLE|MATERIALIZED VIEW) ` + regexp.QuoteMeta(o[0]+"."+o[1]) + `)`)
		q := head.ReplaceAllString(string(b), "$1 ON CLUSTER cluster")
		if err := c.Exec(ctx, q, ddl...); err != nil {
			t.Fatalf("%s.%s: %v", o[0], o[1], err)
		}
	}
	t.Cleanup(func() {
		for _, db := range schema.DefaultDatabases {
			_ = c.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" ON CLUSTER cluster SYNC", ddl...)
		}
	})
}

func observedCluster(t *testing.T) (*Store, []string, schema.Snapshot) {
	t.Helper()
	addrs := clusterAddrs(t)
	c := dial(t, addrs[0])
	applyObserved(t, c)
	s := NewStore(c, "cluster")
	live, err := s.ReplicaSnapshots(context.Background(), schema.DefaultDatabases)
	if err != nil {
		t.Fatal(err)
	}
	return s, addrs, live[slices.Sorted(maps.Keys(live))[0]]
}

func exec(t *testing.T, c *Conn, queries ...string) {
	t.Helper()
	for _, q := range queries {
		if err := c.Exec(context.Background(), q, ddl...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

func compare(t *testing.T, s *Store, want schema.Snapshot) []schema.Diff {
	t.Helper()
	live, err := s.ReplicaSnapshots(context.Background(), schema.DefaultDatabases)
	if err != nil {
		t.Fatal(err)
	}
	diffs, err := schema.Compare(context.Background(), want, live, s)
	if err != nil {
		t.Fatal(err)
	}
	return diffs
}

// The observed schema reads as 135 objects, alike on every replica, with the
// database and cluster names replaced by their placeholders.
func TestIntegrationObservedSchemaReadsAlikeOnEveryReplica(t *testing.T) {
	s, _, want := observedCluster(t)
	if len(want) != 135 {
		t.Fatalf("%d objects, want 135", len(want))
	}
	if diffs := compare(t, s, want); len(diffs) > 0 {
		t.Fatalf("replicas differ: %v", diffs)
	}
	d := want["logs.distributed_logs_v2"]
	if d.Kind != schema.KindDistributed || !slices.Equal(d.EngineArgs, []string{"'{cluster}'", "'{db:logs}'", "'logs_v2'", "cityHash64(id)"}) {
		t.Errorf("distributed_logs_v2 = %+v", d)
	}
	mv := want["meter.samples_agg_1d_mv"]
	if mv.Kind != schema.KindMaterializedView || mv.Target != "{db:meter}.samples_agg_1d" || !strings.Contains(mv.Query, "FROM {db:meter}.samples GROUP BY") {
		t.Errorf("samples_agg_1d_mv = %+v", mv)
	}
	if p := want["traces.signoz_index_v2"].Projections; len(p) != 1 || p[0].Name != "timestampSort" {
		t.Errorf("signoz_index_v2 projections = %+v", p)
	}
	if r := want["metrics.samples_v4_reduced_sum_30m_mv"].Refresh; r != "REFRESH EVERY 30 MINUTE RANDOMIZE FOR 2 MINUTE APPEND" {
		t.Errorf("samples_v4_reduced_sum_30m_mv refresh = %q", r)
	}
}

// The backend's runtime changes, in the statements it writes, are allowed.
func TestIntegrationBackendChangesAreAllowed(t *testing.T) {
	s, _, want := observedCluster(t)
	c := s.conn
	for _, table := range []string{"logs_v2", "distributed_logs_v2"} {
		exec(t, c,
			"ALTER TABLE signoz_logs."+table+" ON CLUSTER cluster ADD COLUMN IF NOT EXISTS `attribute_string_http$$route` String DEFAULT attributes_string['http.route'] CODEC(ZSTD(1))",
			"ALTER TABLE signoz_logs."+table+" ON CLUSTER cluster ADD COLUMN IF NOT EXISTS `attribute_string_http$$route_exists` bool DEFAULT if(mapContains(attributes_string, 'http.route') != 0, true, false) CODEC(ZSTD(1))",
			"ALTER TABLE signoz_logs."+table+" ON CLUSTER cluster MODIFY COLUMN _retention_days UInt16 DEFAULT multiIf(resources_string['service.name'] IN ['api'], 30, 15)",
			"ALTER TABLE signoz_logs."+table+" ON CLUSTER cluster MODIFY COLUMN _retention_days_cold UInt16 DEFAULT 7",
		)
	}
	for _, table := range []string{"signoz_index_v3", "distributed_signoz_index_v3"} {
		exec(t, c,
			"ALTER TABLE signoz_traces."+table+" ON CLUSTER cluster ADD COLUMN IF NOT EXISTS `attribute_number_http$$status` Float64 DEFAULT attributes_number['http.status'] CODEC(ZSTD(1))",
			"ALTER TABLE `signoz_traces`.`"+table+"` ON CLUSTER `cluster` ADD COLUMN IF NOT EXISTS `resource_string_fusescope$$rum$$platform` String DEFAULT resources_string['fusescope.rum.platform'] CODEC(ZSTD(1))",
			"ALTER TABLE `signoz_traces`.`"+table+"` ON CLUSTER `cluster` ADD COLUMN IF NOT EXISTS `resource_string_fusescope$$rum$$platform_exists` Bool DEFAULT if(mapContains(resources_string, 'fusescope.rum.platform') != 0, true, false) CODEC(ZSTD(1))",
		)
	}
	exec(t, c,
		"ALTER TABLE signoz_logs.logs_v2 ON CLUSTER cluster ADD INDEX IF NOT EXISTS `attribute_string_http$$route_idx` (`attribute_string_http$$route`) TYPE bloom_filter(0.01)  GRANULARITY 64",
		"ALTER TABLE signoz_traces.signoz_index_v3 ON CLUSTER cluster ADD INDEX IF NOT EXISTS `attribute_number_http$$status_idx` (`attribute_number_http$$status`) TYPE bloom_filter(0.01)  GRANULARITY 64",
		"ALTER TABLE signoz_traces.signoz_index_v3 ON CLUSTER cluster ADD INDEX IF NOT EXISTS `attribute_number_http$$status_minmax_idx` (`attribute_number_http$$status`) TYPE minmax  GRANULARITY 1",
		"ALTER TABLE `signoz_traces`.`signoz_index_v3` ON CLUSTER `cluster` ADD INDEX IF NOT EXISTS `resource_string_fusescope$$rum$$platform_idx` (`resource_string_fusescope$$rum$$platform`) TYPE bloom_filter(0.01) GRANULARITY 64",
		"ALTER TABLE signoz_logs.logs_v2 ON CLUSTER cluster ADD INDEX IF NOT EXISTS `body_v2.user.e-mail_string_ngrambf_v1` lower(assumeNotNull(dynamicElement(body_v2.user.`e-mail`, 'String'))) TYPE ngrambf_v1(4, 15000, 3, 0) GRANULARITY 1",
		"ALTER TABLE signoz_logs.logs_v2 ON CLUSTER cluster ADD INDEX IF NOT EXISTS `body_promoted.http.status_int64_minmax` assumeNotNull(dynamicElement(body_promoted.http.status, 'Int64')) TYPE minmax GRANULARITY 1",
		"ALTER TABLE signoz_traces.signoz_index_v3 ON CLUSTER cluster ADD INDEX IF NOT EXISTS `attributes.http.route_string_bloom_filter` attributes.`http.route`::String TYPE bloom_filter(0.01) GRANULARITY 4",
		"ALTER TABLE signoz_traces.signoz_index_v3 ON CLUSTER cluster ADD INDEX IF NOT EXISTS `attributes_promoted.http.status_float64_minmax` toFloat64OrNull(attributes_promoted.`http.status`::String) TYPE minmax GRANULARITY 1",
		"ALTER TABLE signoz_logs.logs_v2_resource ON CLUSTER cluster MODIFY TTL toDateTime(seen_at_ts_bucket_start) + toIntervalSecond(1800) + INTERVAL 604800 SECOND DELETE SETTINGS materialize_ttl_after_modify=0",
		"ALTER TABLE signoz_logs.logs_attribute_keys ON CLUSTER cluster MODIFY TTL timestamp + toIntervalDay(30) DELETE SETTINGS materialize_ttl_after_modify=0",
		"ALTER TABLE signoz_traces.trace_summary ON CLUSTER cluster MODIFY TTL toDateTime(end) + INTERVAL 2592000 SECOND DELETE SETTINGS materialize_ttl_after_modify=0",
		"ALTER TABLE signoz_metrics.samples_v4 ON CLUSTER cluster MODIFY TTL toDateTime(toUInt32(unix_milli / 1000), 'UTC') + INTERVAL 7776000 SECOND DELETE SETTINGS materialize_ttl_after_modify=0",
	)
	if diffs := compare(t, s, want); len(diffs) > 0 {
		t.Fatalf("the backend's changes counted as drift: %v", diffs)
	}
}

// A change one statement away from a pattern is drift, on every replica.
func TestIntegrationNearlyAllowedChangesAreDrift(t *testing.T) {
	s, _, want := observedCluster(t)
	exec(t, s.conn,
		"ALTER TABLE signoz_logs.logs_v2 ON CLUSTER cluster ADD COLUMN `attribute_string_http$$route` String DEFAULT attributes_string['http.route'] CODEC(ZSTD(3))",
		"ALTER TABLE signoz_logs.logs_v2 ON CLUSTER cluster ADD INDEX `body_v2.user.name_string_tokenbf_v1` assumeNotNull(dynamicElement(body_v2.user.name, 'String')) TYPE tokenbf_v1(10000, 2, 0) GRANULARITY 1",
		"ALTER TABLE signoz_traces.trace_summary ON CLUSTER cluster MODIFY TTL toDateTime(start) + INTERVAL 2592000 SECOND DELETE SETTINGS materialize_ttl_after_modify=0",
	)
	diffs := compare(t, s, want)
	parts := map[string]int{}
	for _, d := range diffs {
		parts[d.Object+" "+d.Part]++
	}
	for _, p := range []string{"logs.logs_v2 column attribute_string_http$$route", "logs.logs_v2 index body_v2.user.name_string_tokenbf_v1", "traces.trace_summary ttl"} {
		if parts[p] != 4 {
			t.Errorf("%s: drift on %d replicas, want 4 (diffs %v)", p, parts[p], diffs)
		}
	}
}

// A distributed table changed on one replica only differs there.
func TestIntegrationDriftOnOneReplicaCounts(t *testing.T) {
	s, addrs, want := observedCluster(t)
	exec(t, dial(t, addrs[2]), "ALTER TABLE signoz_logs.distributed_logs_v2 ADD COLUMN extra String")
	diffs := compare(t, s, want)
	if len(diffs) != 1 || diffs[0].Part != "column extra" || !strings.HasPrefix(diffs[0].Replica, "ch-2-1/") {
		t.Fatalf("diffs %v, want one extra column on ch-2-1", diffs)
	}
}
