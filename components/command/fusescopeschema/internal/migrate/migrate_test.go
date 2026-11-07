// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package migrate

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/coord"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/migrations"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

var replicas = []string{"r1", "r2", "r3", "r4"}

// fakeStore records every write. Its live schema is post once every statement
// of the migration ran, and empty before; drift, when set, changes it.
type fakeStore struct {
	post     schema.Snapshot
	live     schema.Snapshot // what every replica shows
	history  []Row
	executed []string
	writes   []string // history writes
	drift    func(map[string]schema.Snapshot)
	failExec int // the statement that fails, 1-based; 0 never
	total    int
}

func (f *fakeStore) Expressions(_ context.Context, in []string) ([]string, error) { return in, nil }
func (f *fakeStore) TTLs(_ context.Context, in []string) ([]string, error)        { return in, nil }

func (f *fakeStore) ReplicaSnapshots(context.Context, map[string]string) (map[string]schema.Snapshot, error) {
	out := map[string]schema.Snapshot{}
	for _, r := range replicas {
		s := schema.Snapshot{}
		for k, o := range f.live {
			s[k] = o
		}
		out[r] = s
	}
	if f.drift != nil {
		f.drift(out)
	}
	return out, nil
}

func (f *fakeStore) History(context.Context) ([]Row, error) {
	return append([]Row(nil), f.history...), nil
}

func (f *fakeStore) InsertStarted(_ context.Context, r Row, _, _ string) error {
	f.writes = append(f.writes, "started")
	f.history = append(f.history, r)
	return nil
}

func (f *fakeStore) InsertAdopted(_ context.Context, r Row, _, _ string) error {
	f.writes = append(f.writes, "adopted")
	r.Done, r.Adopted = true, true
	f.history = append(f.history, r)
	return nil
}

func (f *fakeStore) MarkDone(_ context.Context, id uint32, token uint64) error {
	f.writes = append(f.writes, "done")
	for i := range f.history {
		if f.history[i].ID == id && f.history[i].Token == token {
			f.history[i].Done = true
		}
	}
	return nil
}

func (f *fakeStore) ExecDDL(_ context.Context, stmt string) error {
	if f.failExec > 0 && len(f.executed)+1 == f.failExec {
		return errors.New("connection reset")
	}
	f.executed = append(f.executed, stmt)
	if len(f.executed) == f.total {
		f.live = f.post
	} else if f.live == nil {
		f.live = schema.Snapshot{}
	}
	return nil
}

type fakeLease struct {
	token  uint64
	loseAt int // Valid fails from this call on, 1-based; 0 never
	calls  int
}

func (l *fakeLease) Valid(context.Context) error {
	l.calls++
	if l.loseAt > 0 && l.calls >= l.loseAt {
		return coord.ErrLeaseLost
	}
	return nil
}

func (l *fakeLease) Held() coord.Lease { return coord.Lease{Owner: "me", Token: l.token} }

func setup(t *testing.T) (*fakeStore, *fakeLease, *Runner, migrations.Migration) {
	t.Helper()
	all, err := migrations.All()
	if err != nil {
		t.Fatal(err)
	}
	m := all[0]
	s := &fakeStore{post: m.Post, total: len(m.Statements)}
	l := &fakeLease{token: 7}
	r := &Runner{Store: s, Lease: l, Migrations: all, Databases: schema.DefaultDatabases, Cluster: "cluster", Version: "test", Host: "h"}
	return s, l, r, m
}

func forkSchema(s *fakeStore) { s.live = s.post }

func TestEmptyRunsBaseFromScratch(t *testing.T) {
	s, _, r, m := setup(t)
	res, err := r.Migrate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Point != PointEmpty || len(res.Applied) != 1 || res.Applied[0] != 1 {
		t.Fatalf("result %+v", res)
	}
	if len(s.executed) != len(m.Statements) || strings.Join(s.writes, ",") != "started,done" {
		t.Fatalf("executed %d, history writes %v", len(s.executed), s.writes)
	}
	if h := s.history[0]; !h.Done || h.Adopted || h.Token != 7 || h.Checksum != m.Checksum() {
		t.Fatalf("history %+v", h)
	}
	if strings.Contains(s.executed[0], "{db:") || !strings.Contains(s.executed[0], "`signoz_analytics`") {
		t.Fatalf("statement not rendered: %s", s.executed[0])
	}
}

func TestForkSchemaIsAdoptedWithoutDDL(t *testing.T) {
	s, _, r, m := setup(t)
	forkSchema(s)
	res, err := r.Migrate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Point != PointFork || len(res.Applied) != 0 || len(s.executed) != 0 {
		t.Fatalf("result %+v, executed %d", res, len(s.executed))
	}
	if h := s.history; len(h) != 1 || !h[0].Done || !h[0].Adopted || h[0].ID != 1 || h[0].Checksum != m.Checksum() || h[0].Token != 7 {
		t.Fatalf("history %+v", h)
	}
}

// Drift is refused before any write: nothing created, nothing recorded.
func TestDriftIsRefusedWithoutWriting(t *testing.T) {
	cases := map[string]func(map[string]schema.Snapshot){
		"an object missing on one replica": func(l map[string]schema.Snapshot) { delete(l["r3"], "logs.logs_v2") },
		"an extra object":                  func(l map[string]schema.Snapshot) { l["r1"]["logs.extra"] = schema.Object{Kind: schema.KindTable} },
		"a changed column on every replica": func(l map[string]schema.Snapshot) {
			for _, s := range l {
				o := s["logs.logs_v2"]
				o.Columns = append([]schema.Column(nil), o.Columns...)
				o.Columns[0].Type = "String"
				s["logs.logs_v2"] = o
			}
		},
	}
	for name, drift := range cases {
		t.Run(name, func(t *testing.T) {
			s, _, r, _ := setup(t)
			forkSchema(s)
			s.drift = drift
			res, err := r.Migrate(context.Background())
			var d *DriftError
			if !errors.As(err, &d) || len(d.Diffs) == 0 {
				t.Fatalf("Migrate = %v, want a DriftError", err)
			}
			if res.Started || len(s.executed) > 0 || len(s.writes) > 0 {
				t.Fatalf("wrote despite the drift: executed %d, writes %v", len(s.executed), s.writes)
			}
		})
	}
}

// Part of the schema, as left by a crash of another tool, is neither empty
// nor the fork's schema: refused.
func TestAPartialSchemaIsRefused(t *testing.T) {
	s, _, r, _ := setup(t)
	s.live = schema.Snapshot{"logs.logs_v2": s.post["logs.logs_v2"]}
	if _, err := r.Migrate(context.Background()); !errors.As(err, new(*DriftError)) || len(s.executed)+len(s.writes) > 0 {
		t.Fatalf("Migrate = %v, executed %d, writes %v", err, len(s.executed), s.writes)
	}
}

func TestASecondRunIsANoOp(t *testing.T) {
	s, _, r, _ := setup(t)
	if _, err := r.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	executed, writes := len(s.executed), len(s.writes)
	res, err := r.Migrate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Point != PointManaged || len(res.Applied) != 0 || len(s.executed) != executed || len(s.writes) != writes {
		t.Fatalf("second run: %+v, executed %d more, %d more writes", res, len(s.executed)-executed, len(s.writes)-writes)
	}
}

func TestManagedSchemaWithDriftIsRefused(t *testing.T) {
	s, _, r, _ := setup(t)
	if _, err := r.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.drift = func(l map[string]schema.Snapshot) { delete(l["r2"], "traces.signoz_index_v3") }
	writes := len(s.writes)
	if _, err := r.Migrate(context.Background()); !errors.As(err, new(*DriftError)) || len(s.writes) != writes {
		t.Fatalf("Migrate = %v", err)
	}
}

// A statement that fails, or a lease lost before one, stops the run with the
// migration left started; the next run refuses it as interrupted.
func TestAFailureMidwayLeavesTheMigrationStarted(t *testing.T) {
	cases := map[string]func(*fakeStore, *fakeLease){
		"a statement fails":        func(s *fakeStore, _ *fakeLease) { s.failExec = 40 },
		"the lease is lost":        func(_ *fakeStore, l *fakeLease) { l.loseAt = 41 },
		"the last statement fails": func(s *fakeStore, _ *fakeLease) { s.failExec = s.total },
	}
	for name, setupFail := range cases {
		t.Run(name, func(t *testing.T) {
			s, l, r, _ := setup(t)
			setupFail(s, l)
			res, err := r.Migrate(context.Background())
			if err == nil || !res.Started {
				t.Fatalf("Migrate = %+v, %v; want a failure after started", res, err)
			}
			if name == "the lease is lost" {
				// Valid's calls: Migrate, run, then one before each statement:
				// the 41st is before statement 39, so 38 ran and no more.
				if !errors.Is(err, coord.ErrLeaseLost) || len(s.executed) != 38 {
					t.Fatalf("err = %v after %d statements, want ErrLeaseLost after 38", err, len(s.executed))
				}
			}
			if len(s.history) != 1 || s.history[0].Done {
				t.Fatalf("history %+v, want 0001 started", s.history)
			}
			executed := len(s.executed)
			s.failExec, l.loseAt = 0, 0
			res, err = r.Migrate(context.Background())
			if !errors.Is(err, ErrInterrupted) || res.Started || len(s.executed) != executed {
				t.Fatalf("rerun = %+v, %v, executed %d more", res, err, len(s.executed)-executed)
			}
		})
	}
}

func TestHistoryThisBinaryDoesNotKnowIsRefused(t *testing.T) {
	cases := map[string]Row{
		"an unknown id":    {ID: 2, Name: "future", Done: true},
		"another checksum": {ID: 1, Name: "base", Checksum: strings.Repeat("0", 64), Done: true},
	}
	for name, row := range cases {
		t.Run(name, func(t *testing.T) {
			s, _, r, m := setup(t)
			forkSchema(s)
			if row.ID == 2 {
				s.history = []Row{{ID: 1, Name: "base", Checksum: m.Checksum(), Done: true}}
			}
			s.history = append(s.history, row)
			want := ErrUnknownMigration
			if row.ID == 1 {
				want = ErrChecksum
			}
			if _, err := r.Migrate(context.Background()); !errors.Is(err, want) || len(s.executed)+len(s.writes) > 0 {
				t.Fatalf("Migrate = %v, want %v without writing", err, want)
			}
		})
	}
}

func TestMigrateNeedsAValidLease(t *testing.T) {
	s, l, r, _ := setup(t)
	l.loseAt = 1
	if _, err := r.Migrate(context.Background()); !errors.Is(err, coord.ErrLeaseLost) || len(s.writes) > 0 {
		t.Fatalf("Migrate = %v", err)
	}
}

func TestPostconditionsAreChecked(t *testing.T) {
	s, _, r, _ := setup(t)
	s.drift = func(l map[string]schema.Snapshot) {
		if len(l["r4"]) > 0 {
			delete(l["r4"], "meter.samples")
		}
	}
	res, err := r.Migrate(context.Background())
	if !errors.As(err, new(*DriftError)) || !res.Started || s.history[0].Done {
		t.Fatalf("Migrate = %+v, %v; want a post-condition failure after started", res, err)
	}
}

// Custom names reach every statement.
func TestStatementsAreRenderedWithTheConfiguredNames(t *testing.T) {
	s, _, r, _ := setup(t)
	r.Databases = map[string]string{}
	for _, role := range schema.Roles {
		r.Databases[role] = "fs-" + role
	}
	r.Cluster = "fusescope-custom"
	if _, err := r.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range s.executed {
		if !strings.Contains(stmt, "ON CLUSTER `fusescope-custom`") {
			t.Fatalf("statement %.120s", stmt)
		}
		for _, db := range schema.DefaultDatabases {
			if strings.Contains(stmt, db+".") || strings.Contains(stmt, "'"+db+"'") || strings.Contains(stmt, "`"+db+"`") {
				t.Fatalf("statement names %s: %.120s", db, stmt)
			}
		}
	}
}
