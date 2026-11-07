// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package coord

import (
	"context"
	"errors"
	"testing"
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
	}
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
