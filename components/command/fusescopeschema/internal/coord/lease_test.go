// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package coord

import (
	"context"
	"errors"
	"testing"
	"time"
)

var replicas = []string{"ch-1-1", "ch-1-2", "ch-2-1", "ch-2-2"}

// cluster is a fake 2x2 cluster with a bootstrapped layout and a free lease.
func cluster(t *testing.T) *fakeStore {
	t.Helper()
	s := newFakeStore(replicas...)
	m := s.maps["/shared"]
	for name := range layoutColumns {
		m.tables[name] = true
	}
	m.meta["coordination"] = "1"
	m.meta["cluster_id"] = "id-1"
	m.lease = &Lease{}
	return s
}

// newCoordinator connects to replica, with a fake clock that the store and the
// coordinator share, so sleeping advances the servers' time.
func newCoordinator(s *fakeStore, replica, owner string) *Coordinator {
	c := New(s.as(replica), Config{Owner: owner})
	c.sleep = func(ctx context.Context, d time.Duration) error {
		s.advance(d.Milliseconds())
		return ctx.Err()
	}
	c.now = func() time.Time {
		s.mu.Lock()
		defer s.mu.Unlock()
		return time.UnixMilli(s.nowMs)
	}
	return c
}

func lease(s *fakeStore) Lease {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.maps["/shared"].lease
}

func TestAcquireFreeLease(t *testing.T) {
	s := cluster(t)
	c := newCoordinator(s, "ch-1-1", "A")
	held, err := c.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := Lease{Owner: "A", Token: 1, ExpiresMs: 1_000_000 + (5 * time.Minute).Milliseconds()}
	if held != want || lease(s) != want {
		t.Fatalf("held %+v, stored %+v, want %+v", held, lease(s), want)
	}
}

func TestAcquireWaitsForAHeldLeaseThenTimesOut(t *testing.T) {
	s := cluster(t)
	if _, err := newCoordinator(s, "ch-1-1", "A").Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	b := newCoordinator(s, "ch-2-1", "B")
	b.cfg.AcquireTimeout = time.Minute
	if _, err := b.Acquire(context.Background()); !errors.Is(err, ErrLeaseTimeout) {
		t.Fatalf("err = %v, want ErrLeaseTimeout", err)
	}
	if got := lease(s); got.Owner != "A" || got.Token != 1 {
		t.Fatalf("lease = %+v, want A's", got)
	}
}

// The waiter read the lease, then the holder renewed: the waiter's conditional
// update changes nothing, its read-back shows the holder, and it keeps waiting.
func TestAcquireRacedByARenewalTakesNothing(t *testing.T) {
	s := cluster(t)
	a := newCoordinator(s, "ch-1-1", "A")
	if _, err := a.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.advance((5*time.Minute + time.Second).Milliseconds()) // A's lease has expired
	b := newCoordinator(s, "ch-2-1", "B")
	b.cfg.AcquireTimeout = 2 * time.Second
	s.onAcquire = func() { // A renews between B's read and B's update
		s.mu.Lock()
		s.maps["/shared"].lease.ExpiresMs = s.nowMs + (5 * time.Minute).Milliseconds()
		s.mu.Unlock()
	}
	if _, err := b.Acquire(context.Background()); !errors.Is(err, ErrLeaseTimeout) {
		t.Fatalf("err = %v, want ErrLeaseTimeout", err)
	}
	if got := lease(s); got.Owner != "A" || got.Token != 1 {
		t.Fatalf("lease = %+v, want A's", got)
	}
}

func TestAcquireAnExpiredLease(t *testing.T) {
	s := cluster(t)
	if _, err := newCoordinator(s, "ch-1-1", "A").Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.advance((6 * time.Minute).Milliseconds())
	held, err := newCoordinator(s, "ch-2-1", "B").Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if held.Owner != "B" || held.Token != 2 {
		t.Fatalf("held = %+v, want B with token 2", held)
	}
}

// An expired lease over a started migration means that migration was
// interrupted: refused, never taken over (spec §6, §7.3).
func TestAcquireRefusesAnExpiredLeaseOverAStartedMigration(t *testing.T) {
	s := cluster(t)
	if _, err := newCoordinator(s, "ch-1-1", "A").Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.maps["/shared"].started[1] = true
	s.advance((6 * time.Minute).Milliseconds())
	if _, err := newCoordinator(s, "ch-2-1", "B").Acquire(context.Background()); !errors.Is(err, ErrInterrupted) {
		t.Fatalf("err = %v, want ErrInterrupted", err)
	}
	if got := lease(s); got.Owner != "A" {
		t.Fatalf("lease = %+v, want unchanged", got)
	}
}

func TestRenewExtendsTheLease(t *testing.T) {
	s := cluster(t)
	c := newCoordinator(s, "ch-1-1", "A")
	if _, err := c.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.advance(time.Minute.Milliseconds())
	if err := c.Renew(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, want := lease(s).ExpiresMs, s.nowMs+(5*time.Minute).Milliseconds(); got != want {
		t.Fatalf("expires = %d, want %d", got, want)
	}
	if err := c.Valid(context.Background()); err != nil {
		t.Fatalf("Valid = %v", err)
	}
}

func TestRenewAfterExpiryLosesTheLease(t *testing.T) {
	s := cluster(t)
	c := newCoordinator(s, "ch-1-1", "A")
	if _, err := c.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := lease(s)
	s.advance((5*time.Minute + time.Second).Milliseconds())
	if err := c.Renew(context.Background()); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Renew = %v, want ErrLeaseLost", err)
	}
	if lease(s) != before {
		t.Fatalf("an expired lease was revived: %+v", lease(s))
	}
	if err := c.Valid(context.Background()); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Valid after a lost renewal = %v, want ErrLeaseLost", err)
	}
}

// Without a confirmed renewal for LostAfter, the holder stops on its own,
// before the lease could expire.
func TestValidStopsAfterLostAfterWithoutARenewal(t *testing.T) {
	s := cluster(t)
	c := newCoordinator(s, "ch-1-1", "A")
	if _, err := c.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.advance((4*time.Minute + time.Second).Milliseconds())
	if err := c.Valid(context.Background()); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Valid = %v, want ErrLeaseLost", err)
	}
}

func TestReleaseWithAStaleTokenChangesNothing(t *testing.T) {
	s := cluster(t)
	a := newCoordinator(s, "ch-1-1", "A")
	if _, err := a.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.advance((6 * time.Minute).Milliseconds())
	if _, err := newCoordinator(s, "ch-2-1", "B").Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = a.Release(context.Background())
	if got := lease(s); got.Owner != "B" {
		t.Fatalf("lease = %+v, want B's", got)
	}
}

func TestIdentityChecks(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*fakeStore)
		want  error
	}{
		{"one identity everywhere", func(*fakeStore) {}, nil},
		{"a replica bootstrapped on its own", func(s *fakeStore) {
			s.maps["/other"] = &fakeMap{meta: map[string]string{"cluster_id": "id-2"}, tables: map[string]bool{}, started: map[uint64]bool{}, lease: &Lease{}}
			s.replicas["ch-2-2"] = "/other"
		}, ErrDivergent},
		{"a replica without an identity", func(s *fakeStore) {
			s.maps["/other"] = &fakeMap{meta: map[string]string{}, tables: map[string]bool{}, started: map[uint64]bool{}, lease: &Lease{}}
			s.replicas["ch-2-2"] = "/other"
		}, ErrDivergent},
		{"a replica that does not answer", func(s *fakeStore) { s.down["ch-2-2"] = true }, ErrDivergent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := cluster(t)
			tt.setup(s)
			_, err := newCoordinator(s, "ch-1-1", "A").Acquire(context.Background())
			if !errors.Is(err, tt.want) {
				t.Fatalf("Acquire = %v, want %v", err, tt.want)
			}
			if tt.want != nil && lease(s).Owner != "" {
				t.Fatalf("lease taken despite a refusal: %+v", lease(s))
			}
		})
	}
}

// Every replica must show exactly the holder's lease: a copied store with the
// same identity is refused; a replica briefly behind is waited for (spec §3.2).
func TestLeaseComparisonAcrossReplicas(t *testing.T) {
	copied := func(s *fakeStore) {
		s.maps["/copy"] = &fakeMap{meta: map[string]string{"cluster_id": "id-1"}, tables: map[string]bool{}, started: map[uint64]bool{}, lease: &Lease{}}
		s.replicas["ch-2-2"] = "/copy"
	}
	behind := func(reads int, back time.Duration) func(*fakeStore) {
		return func(s *fakeStore) {
			s.lag["ch-2-2"] = &fakeLag{reads: reads, lease: Lease{Owner: "A", Token: 1, ExpiresMs: s.nowMs + (5*time.Minute - back).Milliseconds()}}
		}
	}
	tests := []struct {
		name  string
		setup func(*fakeStore)
		want  error
	}{
		{"every replica shows the lease", func(*fakeStore) {}, nil},
		{"a copied store with the same identity", copied, ErrDivergent},
		{"one renewal behind, catching up within the re-reads", behind(5, time.Minute), nil},
		{"one renewal behind for good", behind(1000, time.Minute), ErrDivergent},
		{"two renewals behind for good", behind(1000, 2*time.Minute), ErrDivergent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := cluster(t)
			tt.setup(s)
			_, err := newCoordinator(s, "ch-1-1", "A").Acquire(context.Background())
			if !errors.Is(err, tt.want) {
				t.Fatalf("Acquire = %v, want %v", err, tt.want)
			}
		})
	}
}

// After a renewal the comparison runs again, and Valid fails once a replica
// stops showing the lease.
func TestValidComparesEveryReplica(t *testing.T) {
	s := cluster(t)
	c := newCoordinator(s, "ch-1-1", "A")
	if _, err := c.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.lag["ch-1-2"] = &fakeLag{reads: 1000, lease: Lease{Owner: "Z", Token: 9}}
	if err := c.Valid(context.Background()); !errors.Is(err, ErrDivergent) {
		t.Fatalf("Valid = %v, want ErrDivergent", err)
	}
	if err := c.Renew(context.Background()); !errors.Is(err, ErrDivergent) {
		t.Fatalf("Renew = %v, want ErrDivergent", err)
	}
}
