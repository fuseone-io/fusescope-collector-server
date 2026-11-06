// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package coord

import (
	"context"
	"errors"
	"slices"
	"testing"
)

func TestBootstrap(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*fakeStore)
		want    error
		creates []string
		written bool
	}{
		{name: "fresh: the whole layout and its rows", setup: func(*fakeStore) {}, creates: []string{"lease", "meta", "migrations"}, written: true},
		{name: "already complete: nothing to create, the rows kept", setup: func(s *fakeStore) {
			m := s.maps["/shared"]
			for t := range layoutColumns {
				m.tables[t] = true
			}
			m.meta["coordination"], m.meta["cluster_id"] = "1", "id-1"
			m.lease = &Lease{}
		}, written: true},
		{name: "partial, this layout: completed", setup: func(s *fakeStore) {
			s.maps["/shared"].tables["meta"] = true
		}, creates: []string{"lease", "migrations"}, written: true},
		{name: "a newer coordination layout: refused before any write", setup: func(s *fakeStore) {
			m := s.maps["/shared"]
			m.tables["meta"] = true
			m.meta["coordination"] = "2"
		}, want: ErrUnknownLayout},
		{name: "an unknown table: refused before any write", setup: func(s *fakeStore) {
			s.maps["/shared"].tables["probe_123"] = true
		}, want: ErrUnknownLayout},
		{name: "a changed column: refused before any write", setup: func(s *fakeStore) {
			m := s.maps["/shared"]
			m.tables["lease"] = true
			m.columns = map[string][]string{"lease": {"name String", "owner String", "token UInt32", "expires_at DateTime64(3)"}}
		}, want: ErrUnknownLayout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newFakeStore(replicas...)
			tt.setup(s)
			err := Bootstrap(context.Background(), s.as("ch-1-1"))
			if !errors.Is(err, tt.want) {
				t.Fatalf("Bootstrap = %v, want %v", err, tt.want)
			}
			if tt.want != nil {
				if len(s.creates) > 0 || len(s.inserts) > 0 {
					t.Fatalf("wrote before refusing: creates %v, inserts %v", s.creates, s.inserts)
				}
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
