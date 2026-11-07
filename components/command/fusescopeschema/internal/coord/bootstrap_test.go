// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package coord

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
)

func complete(s *fakeStore) {
	m := s.maps["/shared"]
	for t := range layout {
		m.tables[t] = true
	}
	m.meta["coordination"], m.meta["cluster_id"] = "1", "id-1"
	m.lease = &Lease{}
}

// otherStore is a complete layout in an independent coordination store.
func otherStore(version string) *fakeMap {
	m := &fakeMap{meta: map[string]string{"coordination": version, "cluster_id": "id-2"}, tables: map[string]bool{}, started: map[uint64]bool{}, lease: &Lease{}}
	for t := range layout {
		m.tables[t] = true
	}
	return m
}

func TestBootstrap(t *testing.T) {
	changed := func(table string, f func(*TableLayout)) TableLayout {
		l := Layout(table)
		f(&l)
		return l
	}
	tests := []struct {
		name    string
		setup   func(*fakeStore)
		want    error
		creates []string
		// refused before any write
		noWrites bool
	}{
		{name: "fresh: the whole layout and its rows", setup: func(*fakeStore) {}, creates: []string{"lease", "meta", "migrations"}},
		{name: "already complete: nothing to create, the rows kept", setup: complete},
		{name: "partial, this layout: completed", setup: func(s *fakeStore) {
			s.maps["/shared"].tables["meta"] = true
		}, creates: []string{"lease", "migrations"}},
		{name: "a table missing on one replica: created there", setup: func(s *fakeStore) {
			complete(s)
			s.absent["ch-2-2"] = map[string]bool{"migrations": true}
		}, creates: []string{"migrations"}},
		{name: "a newer coordination layout", setup: func(s *fakeStore) {
			m := s.maps["/shared"]
			m.tables["meta"] = true
			m.meta["coordination"] = "2"
		}, want: ErrUnknownLayout, noWrites: true},
		{name: "an unknown table", setup: func(s *fakeStore) {
			s.maps["/shared"].tables["probe_123"] = true
		}, want: ErrUnknownLayout, noWrites: true},
		{name: "a changed column on one replica", setup: func(s *fakeStore) {
			complete(s)
			s.differs["ch-1-2"] = map[string]TableLayout{"lease": changed("lease", func(l *TableLayout) { l.Columns[2] = "token UInt32" })}
		}, want: ErrUnknownLayout, noWrites: true},
		{name: "another engine on one replica", setup: func(s *fakeStore) {
			complete(s)
			s.differs["ch-2-1"] = map[string]TableLayout{"meta": changed("meta", func(l *TableLayout) { l.EngineFull = "Memory" })}
		}, want: ErrUnknownLayout, noWrites: true},
		{name: "another KeeperMap path on one replica", setup: func(s *fakeStore) {
			complete(s)
			s.differs["ch-2-1"] = map[string]TableLayout{"lease": changed("lease", func(l *TableLayout) {
				l.EngineFull = "KeeperMap('/elsewhere/lease') PRIMARY KEY name"
			})}
		}, want: ErrUnknownLayout, noWrites: true},
		{name: "another primary key on one replica", setup: func(s *fakeStore) {
			complete(s)
			s.differs["ch-1-1"] = map[string]TableLayout{"lease": changed("lease", func(l *TableLayout) { l.PrimaryKey = "owner" })}
		}, want: ErrUnknownLayout, noWrites: true},
		{name: "a replica that does not answer", setup: func(s *fakeStore) { s.down["ch-2-2"] = true }, want: ErrUnavailable, noWrites: true},
		{name: "a replica the create did not reach", setup: func(s *fakeStore) { s.createSkips["ch-2-2"] = true }, want: ErrUnknownLayout},
		{name: "the lease row's insert lost", setup: func(s *fakeStore) { s.dropLeaseInsert = true }, want: ErrUnknownLayout},
		{name: "the lease row unreadable: retryable", setup: func(s *fakeStore) { s.readLeaseErr = errors.New("connection reset") }, want: ErrUnavailable},
		{name: "the lease table holding other rows", setup: func(s *fakeStore) {
			s.readLeaseErr = fmt.Errorf("%w: the lease table holds 2 rows", ErrUnknownLayout)
		}, want: ErrUnknownLayout},
		{name: "a lease row the protocol cannot write", setup: func(s *fakeStore) {
			complete(s)
			s.maps["/shared"].lease = &Lease{Owner: "A", Token: 0}
		}, want: ErrUnknownLayout},
		{name: "an acquired lease row without an expiry", setup: func(s *fakeStore) {
			complete(s)
			s.maps["/shared"].lease = &Lease{Owner: "A", Token: 1}
		}, want: ErrUnknownLayout},
		{name: "a released lease row", setup: func(s *fakeStore) {
			complete(s)
			s.maps["/shared"].lease = &Lease{Token: 3, ExpiresMs: 5}
		}},
		{name: "meta absent on the connected replica: completed", setup: func(s *fakeStore) {
			complete(s)
			s.absent["ch-1-1"] = map[string]bool{"meta": true}
		}, creates: []string{"meta"}},
		{name: "v1 here, v2 on a replica in another store", setup: func(s *fakeStore) {
			complete(s)
			s.maps["/other"] = otherStore("2")
			s.replicas["ch-2-2"] = "/other"
		}, want: ErrUnknownLayout, noWrites: true},
		{name: "meta partial, v2 on a replica in another store: only meta created", setup: func(s *fakeStore) {
			complete(s)
			s.absent["ch-1-1"] = map[string]bool{"meta": true}
			s.maps["/other"] = otherStore("2")
			s.replicas["ch-2-2"] = "/other"
		}, want: ErrUnknownLayout, creates: []string{"meta"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newFakeStore(replicas...)
			tt.setup(s)
			err := Bootstrap(context.Background(), s.as("ch-1-1"))
			if !errors.Is(err, tt.want) {
				t.Fatalf("Bootstrap = %v, want %v", err, tt.want)
			}
			if tt.noWrites && (len(s.creates) > 0 || len(s.inserts) > 0) {
				t.Fatalf("wrote before refusing: creates %v, inserts %v", s.creates, s.inserts)
			}
			if tt.want != nil && tt.creates != nil {
				if !slices.Equal(s.creates, tt.creates) || len(s.inserts) > 0 {
					t.Fatalf("before refusing: creates %v, inserts %v; want creates %v only", s.creates, s.inserts, tt.creates)
				}
			}
			if tt.want != nil {
				return
			}
			slices.Sort(s.creates)
			if !slices.Equal(s.creates, tt.creates) {
				t.Errorf("created %v, want %v", s.creates, tt.creates)
			}
			m := s.maps["/shared"]
			if m.meta["coordination"] != "1" || m.meta["cluster_id"] == "" || m.lease == nil {
				t.Errorf("after bootstrap: meta %v, lease %v", m.meta, m.lease)
			}
		})
	}
}

// Concurrent bootstraps converge on the first identity written.
func TestBootstrapKeepsTheFirstIdentity(t *testing.T) {
	s := newFakeStore(replicas...)
	if err := Bootstrap(context.Background(), s.as("ch-1-1")); err != nil {
		t.Fatal(err)
	}
	first := s.maps["/shared"].meta["cluster_id"]
	if err := Bootstrap(context.Background(), s.as("ch-2-2")); err != nil {
		t.Fatal(err)
	}
	if got := s.maps["/shared"].meta["cluster_id"]; got != first {
		t.Fatalf("cluster_id changed from %q to %q", first, got)
	}
}
