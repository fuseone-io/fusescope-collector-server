// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package chstore

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/migrations"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

var basePost = filepath.Join("..", "migrations", "post", "0001_base.json")

// 0001's post-conditions are the observed schema itself: the observed DDL,
// applied as it is and not through 0001's statements, reads exactly as the
// artifact on every replica. FUSESCOPE_SCHEMA_UPDATE_POST=1 rewrites the
// artifact from that read.
func TestIntegrationBasePostIsTheObservedSchema(t *testing.T) {
	s, _, _ := observedCluster(t)
	live, err := s.ReplicaSnapshots(context.Background(), schema.DefaultDatabases)
	if err != nil {
		t.Fatal(err)
	}
	for r, snap := range live {
		got, err := migrations.MarshalPost(snap)
		if err != nil {
			t.Fatal(err)
		}
		if os.Getenv("FUSESCOPE_SCHEMA_UPDATE_POST") == "1" {
			if err := os.WriteFile(basePost, got, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		want, err := os.ReadFile(basePost)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("replica %s reads the observed schema unlike %s", r, basePost)
		}
	}
}
