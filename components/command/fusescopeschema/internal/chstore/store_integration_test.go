// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package chstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/coord"
)

// These tests need a real cluster: test/schema/cluster.sh up [keeper|zookeeper]
// prints FUSESCOPE_SCHEMA_TEST_ADDRS, one native address per replica. With
// FUSESCOPE_SCHEMA_TEST_REQUIRED=1, as make schema-integration sets, a missing
// cluster fails the tests instead of skipping them.
func clusterAddrs(t *testing.T) []string {
	t.Helper()
	v := os.Getenv("FUSESCOPE_SCHEMA_TEST_ADDRS")
	if v == "" {
		if os.Getenv("FUSESCOPE_SCHEMA_TEST_REQUIRED") == "1" {
			t.Fatal("FUSESCOPE_SCHEMA_TEST_REQUIRED=1 but FUSESCOPE_SCHEMA_TEST_ADDRS is not set")
		}
		t.Skip("FUSESCOPE_SCHEMA_TEST_ADDRS not set: run test/schema/cluster.sh up")
	}
	addrs := strings.Split(v, ",")
	if len(addrs) != 4 {
		t.Fatalf("FUSESCOPE_SCHEMA_TEST_ADDRS has %d addresses, want the 2x2 cluster's 4", len(addrs))
	}
	return addrs
}

// coordinator connects to addr; a short LeaseFor scales the other timings so
// the configuration stays valid.
func coordinator(t *testing.T, addr string, cfg coord.Config) *coord.Coordinator {
	t.Helper()
	if l := cfg.LeaseFor; l != 0 {
		cfg.LostAfter, cfg.RenewEvery, cfg.RereadFor, cfg.ClockSkew = l/4, l/8, l/4, l/4
	}
	c, err := coord.New(NewStore(dial(t, addr), "cluster"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func dial(t *testing.T, addr string) *Conn {
	t.Helper()
	c, err := Dial(context.Background(), Config{Endpoint: addr, Username: "default", Cluster: "cluster", TLS: TLSConfig{Insecure: true}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// fresh drops fusescope_schema, so each test starts from an empty cluster.
func fresh(t *testing.T, c *Conn) {
	t.Helper()
	if err := c.Exec(context.Background(), "DROP DATABASE IF EXISTS fusescope_schema ON CLUSTER cluster SYNC", ddl...); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationConcurrentBootstrapConverges(t *testing.T) {
	addrs := clusterAddrs(t)
	first := dial(t, addrs[0])
	fresh(t, first)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := range 8 {
		wg.Go(func() {
			errs <- coord.Bootstrap(context.Background(), NewStore(dial(t, addrs[i%len(addrs)]), "cluster"))
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("bootstrap: %v", err)
		}
	}
	ids, err := NewStore(first, "cluster").ReplicaIdentities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, id := range ids {
		seen[id] = true
	}
	if len(ids) != len(addrs) || len(seen) != 1 {
		t.Fatalf("identities %v, want one value on %d replicas", ids, len(addrs))
	}
}

func bootstrapped(t *testing.T) []string {
	t.Helper()
	addrs := clusterAddrs(t)
	c := dial(t, addrs[0])
	fresh(t, c)
	if err := coord.Bootstrap(context.Background(), NewStore(c, "cluster")); err != nil {
		t.Fatal(err)
	}
	return addrs
}

func TestIntegrationLeaseLifecycle(t *testing.T) {
	addrs := bootstrapped(t)
	ctx := context.Background()
	a := coordinator(t, addrs[0], coord.Config{Owner: "A", LeaseFor: 3 * time.Second, AcquireTimeout: time.Second})
	held, err := a.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if held.Owner != "A" || held.Token != 1 {
		t.Fatalf("held = %+v", held)
	}
	b := coordinator(t, addrs[3], coord.Config{Owner: "B", LeaseFor: 3 * time.Second, AcquireTimeout: time.Second, RetryEvery: 200 * time.Millisecond})
	if _, err := b.Acquire(ctx); !errors.Is(err, coord.ErrLeaseTimeout) {
		t.Fatalf("B acquired a held lease: %v", err)
	}
	if err := a.Renew(ctx); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if err := a.Valid(ctx); err != nil {
		t.Fatalf("valid: %v", err)
	}
	time.Sleep(3500 * time.Millisecond)
	if err := a.Renew(ctx); !errors.Is(err, coord.ErrLeaseLost) {
		t.Fatalf("renew after expiry = %v, want ErrLeaseLost", err)
	}
	b2 := coordinator(t, addrs[3], coord.Config{Owner: "B", AcquireTimeout: time.Second})
	held, err = b2.Acquire(ctx)
	if err != nil || held.Owner != "B" || held.Token != 2 {
		t.Fatalf("B on the expired lease: %+v, %v", held, err)
	}
	_ = a.Release(ctx)
	if l, _, _ := NewStore(dial(t, addrs[1]), "cluster").ReadLease(ctx); l.Owner != "B" {
		t.Fatalf("A's stale release changed the lease: %+v", l)
	}
	if err := b2.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationConcurrentAcquiresHaveOneWinner(t *testing.T) {
	addrs := bootstrapped(t)
	for round := range 5 {
		var wg sync.WaitGroup
		results := make(chan error, 8)
		holders := make(chan *coord.Coordinator, 8)
		for i := range 8 {
			wg.Go(func() {
				c := coordinator(t, addrs[i%len(addrs)], coord.Config{Owner: fmt.Sprintf("P%d-%d", round, i), AcquireTimeout: 2 * time.Second, RetryEvery: 100 * time.Millisecond})
				_, err := c.Acquire(context.Background())
				results <- err
				if err == nil {
					holders <- c
				}
			})
		}
		wg.Wait()
		close(results)
		close(holders)
		won, timedOut := 0, 0
		for err := range results {
			switch {
			case err == nil:
				won++
			case errors.Is(err, coord.ErrLeaseTimeout):
				timedOut++
			default:
				t.Fatalf("round %d: %v", round, err)
			}
		}
		if won != 1 || timedOut != 7 {
			t.Fatalf("round %d: %d acquired, %d timed out; want 1 and 7", round, won, timedOut)
		}
		for c := range holders {
			if err := c.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestIntegrationInterruptedMigrationIsRefused(t *testing.T) {
	addrs := bootstrapped(t)
	ctx := context.Background()
	c := dial(t, addrs[0])
	a := coordinator(t, addrs[0], coord.Config{Owner: "A", LeaseFor: 2 * time.Second})
	held, err := a.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	q := fmt.Sprintf("INSERT INTO fusescope_schema.migrations (id, name, checksum, state, adopted, token, started_at, finished_at, tool_version, host) "+
		"SELECT 1, 'base', repeat('0', 64), 'started', false, %d, now64(3), NULL, 'test', 'test'", held.Token)
	if err := c.Exec(ctx, q, strict); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2500 * time.Millisecond)
	b := coordinator(t, addrs[2], coord.Config{Owner: "B"})
	if _, err := b.Acquire(ctx); !errors.Is(err, coord.ErrInterrupted) {
		t.Fatalf("Acquire = %v, want ErrInterrupted", err)
	}
}

func TestIntegrationBootstrapRefusesAnUnknownTable(t *testing.T) {
	addrs := bootstrapped(t)
	c := dial(t, addrs[0])
	if err := c.Exec(context.Background(), "CREATE TABLE fusescope_schema.extra ON CLUSTER cluster (k String) ENGINE = KeeperMap('/fusescope_schema/extra') PRIMARY KEY k", ddl...); err != nil {
		t.Fatal(err)
	}
	if err := coord.Bootstrap(context.Background(), NewStore(c, "cluster")); !errors.Is(err, coord.ErrUnknownLayout) {
		t.Fatalf("Bootstrap = %v, want ErrUnknownLayout", err)
	}
}

// A table dropped on one replica only is created there again.
func TestIntegrationBootstrapCompletesAReplica(t *testing.T) {
	addrs := bootstrapped(t)
	if err := dial(t, addrs[3]).Exec(context.Background(), "DROP TABLE fusescope_schema.migrations SYNC"); err != nil {
		t.Fatal(err)
	}
	s := NewStore(dial(t, addrs[0]), "cluster")
	if err := coord.Bootstrap(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	layouts, err := s.ReplicaLayouts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for r, tables := range layouts {
		if _, ok := tables["migrations"]; !ok || len(tables) != 3 {
			t.Fatalf("replica %s has %v", r, tables)
		}
	}
}

// One replica's lease table is not a KeeperMap: refused, from any replica.
func TestIntegrationBootstrapRefusesAnotherEngineOnOneReplica(t *testing.T) {
	addrs := bootstrapped(t)
	other := dial(t, addrs[2])
	for _, q := range []string{
		"DROP TABLE fusescope_schema.lease SYNC",
		"CREATE TABLE fusescope_schema.lease (name String, owner String, token UInt64, expires_at DateTime64(3)) ENGINE = Memory",
	} {
		if err := other.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	if err := coord.Bootstrap(context.Background(), NewStore(dial(t, addrs[0]), "cluster")); !errors.Is(err, coord.ErrUnknownLayout) {
		t.Fatalf("Bootstrap = %v, want ErrUnknownLayout", err)
	}
}
