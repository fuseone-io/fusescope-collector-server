// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package migrate

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/coord"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/migrations"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

func (f *fakeStore) RepairDone(_ context.Context, id uint32, oldToken, newToken uint64) error {
	f.writes = append(f.writes, "repair done")
	for i := range f.history {
		if f.history[i].ID == id && f.history[i].Token == oldToken && !f.history[i].Done {
			f.history[i].Done, f.history[i].Token = true, newToken
		}
	}
	return nil
}

func (f *fakeStore) DeleteRow(_ context.Context, id uint32, token uint64) error {
	f.writes = append(f.writes, "delete")
	f.history = slices.DeleteFunc(f.history, func(r Row) bool { return r.ID == id && r.Token == token })
	return nil
}

// withReversible adds a synthetic reversible 0002 that creates logs.extra
// and adds a column to meter.samples.
func withReversible(t *testing.T, r *Runner) migrations.Migration {
	t.Helper()
	base := r.Migrations[0]
	post := schema.Snapshot{}
	for k, o := range base.Post {
		post[k] = o
	}
	post["logs.extra"] = schema.Object{Kind: schema.KindTable, Engine: "ReplicatedMergeTree"}
	samples := post["meter.samples"]
	samples.Columns = append(append([]schema.Column(nil), samples.Columns...), schema.Column{Name: "x", Type: "String"})
	post["meter.samples"] = samples
	m := migrations.Migration{ID: 2, Name: "extra", Statements: []string{"CREATE TABLE extra", "ALTER TABLE samples"}, Reversible: true, Pre: base.Post, Post: post}
	r.Migrations = append(r.Migrations, m)
	return m
}

func baseDone(r *Runner) Row {
	b := r.Migrations[0]
	return Row{ID: b.ID, Name: b.Name, Checksum: b.Checksum(), Done: true, Token: 3}
}

func readOnly(r *Runner) *Runner {
	r.Lease = nil // check, plan and status hold no lease
	return r
}

func TestInspect(t *testing.T) {
	cases := []struct {
		name    string
		setup   func(*fakeStore, *Runner)
		lease   *coord.Lease
		verdict Verdict
		point   Point
		pending []uint32
		err     error
	}{
		{name: "empty: 0001 pending", setup: func(*fakeStore, *Runner) {}, verdict: Pending, point: PointEmpty, pending: []uint32{1}},
		{name: "the fork's schema: the adoption pending", setup: func(s *fakeStore, _ *Runner) { forkSchema(s) }, verdict: Pending, point: PointFork},
		{name: "up to date", setup: func(s *fakeStore, r *Runner) { forkSchema(s); s.history = []Row{baseDone(r)} }, verdict: UpToDate, point: PointManaged},
		{name: "a later migration pending", setup: func(s *fakeStore, r *Runner) {
			forkSchema(s)
			s.history = []Row{baseDone(r)}
			withReversible(t, r)
		}, verdict: Pending, point: PointManaged, pending: []uint32{2}},
		{name: "in progress under a valid lease", setup: func(s *fakeStore, r *Runner) {
			b := baseDone(r)
			b.Done, b.Token = false, 9
			s.history = []Row{b}
		}, lease: &coord.Lease{Owner: "other", Token: 9, ExpiresMs: 2_000}, verdict: InProgress},
		{name: "started under an expired lease: interrupted", setup: func(s *fakeStore, r *Runner) {
			b := baseDone(r)
			b.Done, b.Token = false, 9
			s.history = []Row{b}
		}, lease: &coord.Lease{Owner: "other", Token: 9, ExpiresMs: 500}, err: ErrInterrupted},
		{name: "started under another lease generation: interrupted", setup: func(s *fakeStore, r *Runner) {
			b := baseDone(r)
			b.Done, b.Token = false, 9
			s.history = []Row{b}
		}, lease: &coord.Lease{Owner: "other", Token: 10, ExpiresMs: 2_000}, err: ErrInterrupted},
		{name: "started with the lease released: interrupted", setup: func(s *fakeStore, r *Runner) {
			b := baseDone(r)
			b.Done, b.Token = false, 9
			s.history = []Row{b}
		}, lease: &coord.Lease{Token: 9, ExpiresMs: 2_000}, err: ErrInterrupted},
		{name: "drift", setup: func(s *fakeStore, _ *Runner) {
			forkSchema(s)
			s.drift = func(l map[string]schema.Snapshot) { delete(l["r1"], "logs.logs_v2") }
		}, err: &DriftError{}},
		{name: "an unknown migration", setup: func(s *fakeStore, r *Runner) {
			s.history = []Row{baseDone(r), {ID: 2, Name: "future", Done: true}}
		}, err: ErrUnknownMigration},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			s, _, r, _ := setup(t)
			tt.setup(s, r)
			st, err := readOnly(r).Inspect(context.Background(), tt.lease, 1_000)
			switch want := tt.err.(type) {
			case nil:
				if err != nil {
					t.Fatal(err)
				}
			case *DriftError:
				if !errors.As(err, new(*DriftError)) || len(st.Diffs) == 0 {
					t.Fatalf("Inspect = %v, diffs %v; want a DriftError with the diffs", err, st.Diffs)
				}
			default:
				if !errors.Is(err, want) {
					t.Fatalf("Inspect = %v, want %v", err, want)
				}
			}
			if tt.err == nil && (st.Verdict != tt.verdict || st.Point != tt.point || !slices.Equal(st.Pending, tt.pending)) {
				t.Fatalf("state %+v, want %v %v %v", st, tt.verdict, tt.point, tt.pending)
			}
			if len(s.writes)+len(s.executed) > 0 {
				t.Fatalf("Inspect wrote: %v, executed %d", s.writes, len(s.executed))
			}
		})
	}
}

func TestRepair(t *testing.T) {
	started := func(r *Runner, id uint32) Row {
		m := r.Migrations[id-1]
		return Row{ID: m.ID, Name: m.Name, Checksum: m.Checksum(), Token: 4}
	}
	cases := []struct {
		name   string
		setup  func(*fakeStore, *Runner)
		id     uint32
		mark   Mark
		err    error
		writes string
		after  func(*testing.T, *fakeStore)
	}{
		{name: "done: the post-conditions hold", id: 1, mark: MarkDone, setup: func(s *fakeStore, r *Runner) {
			forkSchema(s)
			s.history = []Row{started(r, 1)}
		}, writes: "repair done", after: func(t *testing.T, s *fakeStore) {
			if !s.history[0].Done || s.history[0].Token != 7 {
				t.Fatalf("history %+v, want done with the current token", s.history)
			}
		}},
		{name: "done: a post-condition fails on one replica", id: 1, mark: MarkDone, setup: func(s *fakeStore, r *Runner) {
			forkSchema(s)
			s.drift = func(l map[string]schema.Snapshot) { delete(l["r4"], "meter.samples") }
			s.history = []Row{started(r, 1)}
		}, err: ErrRepairRefused},
		{name: "done: already done and holding is nothing to change", id: 1, mark: MarkDone, setup: func(s *fakeStore, r *Runner) {
			forkSchema(s)
			s.history = []Row{baseDone(r)}
		}},
		{name: "a migration not recorded", id: 1, mark: MarkDone, setup: func(s *fakeStore, _ *Runner) { forkSchema(s) }, err: ErrRepairRefused},
		{name: "a migration this binary does not know", id: 5, mark: MarkDone, setup: func(s *fakeStore, r *Runner) {
			s.history = []Row{baseDone(r)}
		}, err: ErrRepairRefused},
		{name: "not the last recorded migration", id: 1, mark: MarkDone, setup: func(s *fakeStore, r *Runner) {
			withReversible(t, r)
			b := started(r, 1)
			s.history = []Row{b, started(r, 2)}
		}, err: ErrRepairRefused},
		{name: "a recorded checksum differing", id: 1, mark: MarkDone, setup: func(s *fakeStore, r *Runner) {
			forkSchema(s)
			b := started(r, 1)
			b.Checksum = "x"
			s.history = []Row{b}
		}, err: ErrChecksum},
		{name: "absent: an irreversible migration", id: 1, mark: MarkAbsent, setup: func(s *fakeStore, r *Runner) {
			s.history = []Row{started(r, 1)}
		}, err: ErrRepairRefused},
		{name: "absent: every touched object as before", id: 2, mark: MarkAbsent, setup: func(s *fakeStore, r *Runner) {
			withReversible(t, r)
			forkSchema(s)
			s.history = []Row{baseDone(r), started(r, 2)}
		}, writes: "delete", after: func(t *testing.T, s *fakeStore) {
			if len(s.history) != 1 || s.history[0].ID != 1 {
				t.Fatalf("history %+v, want 0002 deleted", s.history)
			}
		}},
		{name: "absent: a created object still there", id: 2, mark: MarkAbsent, setup: func(s *fakeStore, r *Runner) {
			m := withReversible(t, r)
			forkSchema(s)
			s.live = schema.Snapshot{}
			for k, o := range s.post {
				s.live[k] = o
			}
			s.live["logs.extra"] = m.Post["logs.extra"]
			s.history = []Row{baseDone(r), started(r, 2)}
		}, err: ErrRepairRefused},
		{name: "absent: an altered object in a third state", id: 2, mark: MarkAbsent, setup: func(s *fakeStore, r *Runner) {
			withReversible(t, r)
			forkSchema(s)
			s.drift = func(l map[string]schema.Snapshot) {
				o := l["r2"]["meter.samples"]
				o.Columns = append(append([]schema.Column(nil), o.Columns...), schema.Column{Name: "y", Type: "String"})
				l["r2"]["meter.samples"] = o
			}
			s.history = []Row{baseDone(r), started(r, 2)}
		}, err: ErrRepairRefused},
		{name: "absent: an allowed backend change is not the precondition", id: 2, mark: MarkAbsent, setup: func(s *fakeStore, r *Runner) {
			withReversible(t, r)
			forkSchema(s)
			s.drift = func(l map[string]schema.Snapshot) {
				o := l["r1"]["meter.samples"]
				o.TTL = "changed"
				l["r1"]["meter.samples"] = o
			}
			s.history = []Row{baseDone(r), started(r, 2)}
		}, err: ErrRepairRefused},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			s, _, r, _ := setup(t)
			tt.setup(s, r)
			err := r.Repair(context.Background(), tt.id, tt.mark)
			if tt.err == nil && err != nil || tt.err != nil && !errors.Is(err, tt.err) {
				t.Fatalf("Repair = %v, want %v", err, tt.err)
			}
			if got := joinWrites(s.writes); got != tt.writes {
				t.Fatalf("writes %q, want %q", got, tt.writes)
			}
			if tt.after != nil {
				tt.after(t, s)
			}
		})
	}
}

func TestRepairNeedsTheLease(t *testing.T) {
	s, l, r, _ := setup(t)
	forkSchema(s)
	s.history = []Row{{ID: 1, Name: "base", Checksum: r.Migrations[0].Checksum(), Token: 4}}
	l.loseAt = 1
	if err := r.Repair(context.Background(), 1, MarkDone); !errors.Is(err, coord.ErrLeaseLost) || len(s.writes) > 0 {
		t.Fatalf("Repair = %v, writes %v", err, s.writes)
	}
}

func joinWrites(w []string) string {
	out := ""
	for i, x := range w {
		if i > 0 {
			out += ","
		}
		out += x
	}
	return out
}
