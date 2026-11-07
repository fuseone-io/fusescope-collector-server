// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package chstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/coord"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/migrate"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

// releasedBase is 0001's checksum as 0.4.0 released it (§5): binding the
// views to the connected user changes how 0001 renders, not 0001.
const releasedBase = "5c63a2a2c3f87a9c834f6a54a028de8f63778b5363e08ec1db509d06c421d788"

// boundViews are the seven views [obs] binds to default (§5.4).
var boundViews = func() []string {
	var out []string
	for _, w := range []string{"last_30m", "last_5m", "last_60s", "sum_30m", "sum_5m", "sum_60s_cumulative", "sum_60s_delta"} {
		out = append(out, "samples_v4_reduced_"+w+"_mv")
	}
	return out
}()

// requireDefiners requires each of the seven views on each of the four
// replicas to be SQL SECURITY DEFINER, bound to user.
func requireDefiners(t *testing.T, c *Conn, user string) {
	t.Helper()
	db := schema.DefaultDatabases["metrics"]
	rows, err := c.Rows(context.Background(), fmt.Sprintf("SELECT %s, name, definer, create_table_query FROM clusterAllReplicas('cluster', system.tables) WHERE database = %s AND name IN (%s) ORDER BY ALL",
		replicaKey, quote(db), "'"+strings.Join(boundViews, "', '")+"'"), 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4*len(boundViews) {
		t.Fatalf("%d views on the replicas, want %d on each of 4: %v", len(rows), len(boundViews), rows)
	}
	for _, r := range rows {
		if r[2] != user || !strings.Contains(r[3], " DEFINER = "+user+" SQL SECURITY DEFINER AS ") {
			t.Fatalf("%s on %s is bound to %q: %.300s", r[1], r[0], r[2], r[3])
		}
	}
}

// requireRefreshes refreshes the seven views on each of the four servers and
// requires every refresh to succeed, run as the views' definer.
func requireRefreshes(t *testing.T, addrs []string) {
	t.Helper()
	ctx := context.Background()
	db := schema.DefaultDatabases["metrics"]
	for _, a := range addrs {
		c := dial(t, a)
		for _, v := range boundViews {
			for _, q := range []string{"SYSTEM REFRESH VIEW " + db + "." + v, "SYSTEM WAIT VIEW " + db + "." + v} {
				if err := c.Exec(ctx, q); err != nil {
					t.Fatalf("%s on %s: %v", q, a, err)
				}
			}
		}
		rows, err := c.Rows(ctx, "SELECT view, status, exception, ifNull(toString(last_success_time), '') FROM system.view_refreshes WHERE database = "+quote(db)+" ORDER BY view", 4)
		if err != nil {
			t.Fatal(err)
		}
		var refreshed []string
		for _, r := range rows {
			if slices.Contains(boundViews, r[0]) {
				if r[2] != "" || r[3] == "" || strings.HasPrefix(r[3], "1970") {
					t.Fatalf("%s on %s: status %s, last success %s, exception %q", r[0], a, r[1], r[3], r[2])
				}
				refreshed = append(refreshed, r[0])
			}
		}
		if len(refreshed) != len(boundViews) {
			t.Fatalf("%s refreshed %v, want the seven views", a, refreshed)
		}
	}
}

// A fresh install by a user that is not default: the seven views on every
// replica are bound to it and refresh, 0001's checksum is the released one,
// and a later run accepts them. A view bound to a third user, or with
// another SQL security, is drift on its replica.
func TestIntegrationViewsAreBoundToTheMigrator(t *testing.T) {
	addrs := clusterAddrs(t)
	emptyCluster(t, dial(t, addrs[0]), schema.DefaultDatabases, "cluster")
	r := newRunAs(t, addrs[0], "migrator", schema.DefaultDatabases, "cluster", coord.Config{})
	res, err := r.runner.Migrate(context.Background())
	if err != nil || res.Point != migrate.PointEmpty || len(res.Applied) != 2 {
		t.Fatalf("migrate as migrator: %+v, %v", res, err)
	}
	h, err := r.store.History(context.Background())
	if err != nil || len(h) != 2 || h[0].Checksum != releasedBase {
		t.Fatalf("history %+v, %v; want 0001 with checksum %s", h, err, releasedBase)
	}
	c := dial(t, addrs[1])
	requireDefiners(t, c, "migrator")
	requireRefreshes(t, addrs)
	if err := r.coord.Release(context.Background()); err != nil {
		t.Fatal(err)
	}

	again := newRunAs(t, addrs[2], "migrator", schema.DefaultDatabases, "cluster", coord.Config{})
	if res, err := again.runner.Migrate(context.Background()); err != nil || len(res.Applied) != 0 {
		t.Fatalf("second run: %+v, %v", res, err)
	}
	if err := again.coord.Release(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Each view is local to its replica: the change is on ch-2-2 alone.
	db := schema.DefaultDatabases["metrics"]
	last := dial(t, addrs[3])
	for _, q := range []string{
		"ALTER TABLE " + db + ".samples_v4_reduced_last_5m_mv MODIFY SQL SECURITY DEFINER DEFINER = intruder",
		"ALTER TABLE " + db + ".samples_v4_reduced_sum_5m_mv MODIFY SQL SECURITY NONE",
	} {
		if err := last.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	drifted := newRunAs(t, addrs[0], "migrator", schema.DefaultDatabases, "cluster", coord.Config{})
	_, err = drifted.runner.Migrate(context.Background())
	var drift *migrate.DriftError
	if !errors.As(err, &drift) {
		t.Fatalf("migrate with another definer = %v, want drift", err)
	}
	var got []string
	for _, d := range drift.Diffs {
		host, _, _ := strings.Cut(d.Replica, "/")
		got = append(got, host+" "+d.Object+" "+d.Part+" "+d.Got)
	}
	want := []string{
		"ch-2-2 metrics.samples_v4_reduced_last_5m_mv definer intruder",
		"ch-2-2 metrics.samples_v4_reduced_sum_5m_mv definer ",
		"ch-2-2 metrics.samples_v4_reduced_sum_5m_mv sql security NONE",
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("drift %q\nwant %q", got, want)
	}
	if w := telemetryWrites(drifted.rec.all()); len(w) > 0 {
		t.Fatalf("the refused run wrote: %v", w)
	}
}

// The fork's views bound to default, adopted by a migrator that is not
// default: accepted as they are, nothing re-created.
func TestIntegrationForkViewsBoundToDefaultAreAdopted(t *testing.T) {
	addrs := clusterAddrs(t)
	c := dial(t, addrs[0])
	applyObserved(t, c)
	emptyCluster(t, c, map[string]string{}, "cluster")
	requireDefiners(t, c, "default")
	r := newRunAs(t, addrs[1], "migrator", schema.DefaultDatabases, "cluster", coord.Config{})
	res, err := r.runner.Migrate(context.Background())
	if err != nil || res.Point != migrate.PointFork || len(res.Applied) != 1 {
		t.Fatalf("adoption as migrator: %+v, %v", res, err)
	}
	for _, q := range r.rec.all() {
		if strings.Contains(q, "MATERIALIZED VIEW") || strings.Contains(q, "MODIFY SQL SECURITY") {
			t.Fatalf("the adoption touched a view: %.160s", q)
		}
	}
	requireDefiners(t, c, "default")
	requireRefreshes(t, addrs)
}
