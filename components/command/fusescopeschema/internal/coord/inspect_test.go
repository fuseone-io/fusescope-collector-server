// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package coord

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInspectLayout(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*fakeStore)
		complete bool
		want     error
	}{
		{"no coordination anywhere", func(*fakeStore) {}, false, nil},
		{"complete", complete, true, nil},
		{"a table missing on one replica", func(s *fakeStore) {
			complete(s)
			s.absent["ch-2-2"] = map[string]bool{"migrations": true}
		}, false, nil},
		{"meta on one replica only", func(s *fakeStore) {
			s.maps["/shared"].tables["meta"] = true
			for _, r := range []string{"ch-1-1", "ch-1-2", "ch-2-1"} {
				s.absent[r] = map[string]bool{"meta": true}
			}
		}, false, nil},
		{"the tables but not yet the rows", func(s *fakeStore) {
			for t := range layout {
				s.maps["/shared"].tables[t] = true
			}
		}, false, nil},
		{"an unknown table on one replica", func(s *fakeStore) {
			complete(s)
			s.differs["ch-1-2"] = map[string]TableLayout{"probe": {EngineFull: "Memory"}}
		}, false, ErrUnknownLayout},
		{"another engine on one replica", func(s *fakeStore) {
			complete(s)
			l := Layout("lease")
			l.EngineFull = "KeeperMap('/elsewhere/lease') PRIMARY KEY name"
			s.differs["ch-2-1"] = map[string]TableLayout{"lease": l}
		}, false, ErrUnknownLayout},
		{"a newer coordination version", func(s *fakeStore) {
			complete(s)
			s.maps["/shared"].meta["coordination"] = "2"
		}, false, ErrUnknownLayout},
		{"a replica in another store with another identity", func(s *fakeStore) {
			complete(s)
			s.maps["/other"] = otherStore("1")
			s.replicas["ch-2-2"] = "/other"
		}, false, ErrDivergent},
		{"a replica that does not answer", func(s *fakeStore) {
			complete(s)
			s.down["ch-2-2"] = true
		}, false, ErrUnavailable},
		{"partial, with a newer coordination version", func(s *fakeStore) {
			complete(s)
			s.maps["/shared"].meta["coordination"] = "2"
			s.absent["ch-1-1"] = map[string]bool{"migrations": true}
		}, false, ErrUnknownLayout},
		{"partial, with another identity on one replica", func(s *fakeStore) {
			complete(s)
			s.maps["/other"] = otherStore("1")
			s.replicas["ch-2-2"] = "/other"
			s.absent["ch-1-1"] = map[string]bool{"lease": true}
		}, false, ErrDivergent},
		{"complete but without the lease row", func(s *fakeStore) {
			complete(s)
			s.maps["/shared"].lease = nil
		}, false, nil},
		{"a copied store showing another lease", func(s *fakeStore) {
			complete(s)
			copied := otherStore("1")
			copied.meta["cluster_id"] = "id-1"
			copied.lease = &Lease{Owner: "B", Token: 4, ExpiresMs: 9}
			s.maps["/copy"] = copied
			s.replicas["ch-2-2"] = "/copy"
		}, false, ErrDivergent},
		{"a copied store showing another history", func(s *fakeStore) {
			complete(s)
			s.maps["/shared"].history = []string{"1,base,done"}
			copied := otherStore("1")
			copied.meta["cluster_id"] = "id-1"
			s.maps["/copy"] = copied
			s.replicas["ch-2-2"] = "/copy"
		}, false, ErrDivergent},
		{"a lease row with an owner and token 0, on every replica", func(s *fakeStore) {
			complete(s)
			s.maps["/shared"].lease = &Lease{Owner: "A", Token: 0}
		}, false, ErrUnknownLayout},
		{"a lease row with a token and no expiry, on every replica", func(s *fakeStore) {
			complete(s)
			s.maps["/shared"].lease = &Lease{Owner: "A", Token: 3}
		}, false, ErrUnknownLayout},
		{"the lease row unreadable", func(s *fakeStore) {
			complete(s)
			s.readLeaseErr = errors.New("connection reset")
		}, false, ErrUnavailable},
		{"a replica briefly behind on the lease", func(s *fakeStore) {
			complete(s)
			s.maps["/shared"].lease = &Lease{Owner: "A", Token: 1, ExpiresMs: 5}
			s.rowsLag["ch-2-2"] = 2
		}, true, nil},
	}
	every, window := inspectRereadEvery, inspectRereadFor
	inspectRereadEvery, inspectRereadFor = time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() { inspectRereadEvery, inspectRereadFor = every, window })
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newFakeStore(replicas...)
			tt.setup(s)
			got, err := InspectLayout(context.Background(), s.as("ch-1-1"))
			if !errors.Is(err, tt.want) || got != tt.complete {
				t.Fatalf("InspectLayout = %v, %v; want %v, %v", got, err, tt.complete, tt.want)
			}
			if len(s.creates)+len(s.inserts) > 0 {
				t.Fatalf("InspectLayout wrote: creates %v, inserts %v", s.creates, s.inserts)
			}
		})
	}
}

// The context ending while rows still differ answers nothing: a retryable
// error, not a divergence, which only the whole re-read window establishes.
func TestInspectLayoutCutShortIsUnavailable(t *testing.T) {
	every, window := inspectRereadEvery, inspectRereadFor
	inspectRereadEvery, inspectRereadFor = 5*time.Millisecond, time.Hour
	t.Cleanup(func() { inspectRereadEvery, inspectRereadFor = every, window })
	s := newFakeStore(replicas...)
	complete(s)
	s.maps["/shared"].lease = &Lease{Owner: "A", Token: 1, ExpiresMs: 5}
	s.rowsLag["ch-2-2"] = 1 << 30
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := InspectLayout(ctx, s.as("ch-1-1"))
	if !errors.Is(err, ErrUnavailable) || errors.Is(err, ErrDivergent) {
		t.Fatalf("InspectLayout = %v, want ErrUnavailable and not ErrDivergent", err)
	}
}
