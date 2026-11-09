// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package chstore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/coord"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/migrate"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

// coldStorageTables are the local tables the backend's retention paths set the storage policy
// on, under the default database names (reader.go about lines 942, 1059, 1273 and 1734).
var coldStorageTables = []string{
	"signoz_logs.logs_v2", "signoz_logs.logs_v2_resource", "signoz_logs.logs_attribute_keys", "signoz_logs.logs_resource_keys",
	"signoz_traces.signoz_index_v3", "signoz_traces.traces_v3_resource", "signoz_traces.signoz_error_index_v2",
	"signoz_traces.usage_explorer", "signoz_traces.dependency_graph_minutes_v2", "signoz_traces.trace_summary",
	"signoz_traces.span_attributes_keys",
	"signoz_metrics.samples_v4", "signoz_metrics.samples_v4_agg_5m", "signoz_metrics.samples_v4_agg_30m", "signoz_metrics.exp_hist",
	"signoz_metrics.time_series_v4", "signoz_metrics.time_series_v4_6hrs", "signoz_metrics.time_series_v4_1day",
	"signoz_metrics.time_series_v4_1week",
}

// turnOnColdStorage runs the statements a retention with a cold volume runs: setColdStorage's
// policy on every table (reader.go about line 1929), and a TTL that moves to the volume
// (ttlclause.go's Statement).
func turnOnColdStorage(t *testing.T, c *Conn, policy string) {
	t.Helper()
	for _, table := range coldStorageTables {
		exec(t, c, "ALTER TABLE "+table+" ON CLUSTER cluster MODIFY SETTING storage_policy='"+policy+"'")
	}
	exec(t, c,
		"ALTER TABLE signoz_traces.signoz_index_v3 ON CLUSTER cluster MODIFY TTL toDateTime(timestamp) + INTERVAL 2592000 SECOND DELETE, toDateTime(timestamp) + INTERVAL 86400 SECOND TO VOLUME 'cold' SETTINGS materialize_ttl_after_modify=0",
		"ALTER TABLE signoz_metrics.samples_v4 ON CLUSTER cluster MODIFY TTL toDateTime(toUInt32(unix_milli / 1000), 'UTC') + INTERVAL 7776000 SECOND DELETE, toDateTime(toUInt32(unix_milli / 1000), 'UTC') + INTERVAL 604800 SECOND TO VOLUME 'cold' SETTINGS materialize_ttl_after_modify=0",
	)
}

// A fork install whose retention turned cold storage on is adopted, check accepts it, and a
// second migrate is a no-op (fuseone-io/fusescope-collector-server#84).
func TestIntegrationColdStorageIsAdopted(t *testing.T) {
	addrs := clusterAddrs(t)
	c := dial(t, addrs[0])
	applyObserved(t, c)
	applyObservedRows(t, c)
	emptyCluster(t, c, map[string]string{}, "cluster")
	turnOnColdStorage(t, c, "tiered")

	r := newRun(t, addrs[1], schema.DefaultDatabases, "cluster", coord.Config{})
	res, err := r.runner.Migrate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Point != migrate.PointFork || len(res.Applied) != 1 || res.Applied[0] != 2 {
		t.Fatalf("result %+v", res)
	}
	if w := telemetryWrites(r.rec.all()); len(w) > 0 {
		t.Fatalf("the adoption wrote to the telemetry databases: %v", w)
	}
	if err := r.coord.Release(context.Background()); err != nil {
		t.Fatal(err)
	}

	again := newRun(t, addrs[3], schema.DefaultDatabases, "cluster", coord.Config{})
	if st, err := again.runner.Inspect(context.Background(), nil, 0); err != nil || st.Verdict != migrate.UpToDate {
		t.Fatalf("check: %+v, %v; want up to date", st, err)
	}
	res, err = again.runner.Migrate(context.Background())
	if err != nil || res.Point != migrate.PointManaged || len(res.Applied) != 0 {
		t.Fatalf("second run: %+v, %v", res, err)
	}
	if w := telemetryWrites(again.rec.all()); len(w) > 0 {
		t.Fatalf("the second run wrote: %v", w)
	}
}

// Cold storage turned on after fusescope-schema installed the schema: check accepts it.
func TestIntegrationColdStorageAfterInstall(t *testing.T) {
	addrs := clusterAddrs(t)
	c := dial(t, addrs[0])
	emptyCluster(t, c, schema.DefaultDatabases, "cluster")
	r := newRun(t, addrs[0], schema.DefaultDatabases, "cluster", coord.Config{})
	if _, err := r.runner.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.coord.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	turnOnColdStorage(t, c, "tiered")

	again := newRun(t, addrs[2], schema.DefaultDatabases, "cluster", coord.Config{})
	if st, err := again.runner.Inspect(context.Background(), nil, 0); err != nil || st.Verdict != migrate.UpToDate {
		t.Fatalf("check: %+v, %v; want up to date", st, err)
	}
}

// A storage policy the backend does not set is drift, refused without writing.
func TestIntegrationAnotherStoragePolicyIsDrift(t *testing.T) {
	addrs := clusterAddrs(t)
	c := dial(t, addrs[0])
	applyObserved(t, c)
	emptyCluster(t, c, map[string]string{}, "cluster")
	exec(t, c, "ALTER TABLE signoz_logs.logs_v2 ON CLUSTER cluster MODIFY SETTING storage_policy='other'")

	r := newRun(t, addrs[1], schema.DefaultDatabases, "cluster", coord.Config{})
	res, err := r.runner.Migrate(context.Background())
	var d *migrate.DriftError
	if !errors.As(err, &d) || res.Started {
		t.Fatalf("Migrate = %+v, %v; want a DriftError", res, err)
	}
	if !strings.Contains(d.Error(), "logs.logs_v2 settings") || !strings.Contains(d.Error(), "storage_policy = 'other'") {
		t.Fatalf("the refusal does not name the setting: %v", d)
	}
	if w := telemetryWrites(r.rec.all()); len(w) > 0 {
		t.Fatalf("the refusal wrote to the telemetry databases: %v", w)
	}
}
