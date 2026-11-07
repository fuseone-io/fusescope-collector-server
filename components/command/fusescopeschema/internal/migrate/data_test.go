// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package migrate

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/migrations"
	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/schema"
)

// setupAll runs every migration; done records the first n as done.
func setupAll(t *testing.T, done int) (*fakeStore, *Runner, []migrations.Migration) {
	t.Helper()
	s, _, r, _ := setup(t)
	all, err := migrations.All()
	if err != nil {
		t.Fatal(err)
	}
	r.Migrations = all
	if done > 0 {
		forkSchema(s)
	}
	for _, m := range all[:done] {
		s.history = append(s.history, Row{ID: m.ID, Name: m.Name, Checksum: m.Checksum(), Done: true, Token: 3})
	}
	return s, r, all
}

func baseKeys(t *testing.T) [][]string {
	t.Helper()
	all, err := migrations.All()
	if err != nil {
		t.Fatal(err)
	}
	var out [][]string
	for _, r := range all[1].Data[0].Rows {
		out = append(out, r.Key)
	}
	return out
}

// rowsText lists every row, shard and replica included, sorted.
func rowsText(f *fakeStore) []string {
	var out []string
	for g, rs := range f.rows {
		for r, rows := range rs {
			for _, row := range rows {
				out = append(out, g+"/"+r+" "+strings.Join(row.Key, "|")+" "+row.Values["release_time"])
			}
		}
	}
	slices.Sort(out)
	return out
}

func keysOn(f *fakeStore, replica string) []string {
	var out []string
	for _, rs := range f.rows {
		for _, row := range rs[replica] {
			out = append(out, strings.Join(row.Key, "|"))
		}
	}
	slices.Sort(out)
	return out
}

func TestFreshInstallInsertsTheBaseRows(t *testing.T) {
	s, r, all := setupAll(t, 0)
	res, err := r.Migrate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Point != PointEmpty || !slices.Equal(res.Applied, []uint32{1, 2}) || s.dataExecs != 1 {
		t.Fatalf("result %+v, data statements %d", res, s.dataExecs)
	}
	if got := keysOn(s, "r2"); len(got) != 5 {
		t.Fatalf("rows on r2: %v", got)
	}
	if h := s.history; len(h) != 2 || !h[1].Done || h[1].Checksum != all[1].Checksum() {
		t.Fatalf("history %+v", h)
	}
	if last := s.executed[len(s.executed)-1]; last != "data" {
		t.Fatalf("the data statement ran as %q", last)
	}
}

// A second run checks 0002's rows and writes nothing.
func TestASecondRunKeepsTheRows(t *testing.T) {
	s, r, _ := setupAll(t, 0)
	if _, err := r.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	before, writes, execs := rowsText(s), len(s.writes), s.dataExecs
	res, err := r.Migrate(context.Background())
	if err != nil || len(res.Applied) != 0 || len(s.writes) != writes || s.dataExecs != execs || !slices.Equal(rowsText(s), before) {
		t.Fatalf("second run: %+v, %v; writes %d → %d", res, err, writes, len(s.writes))
	}
}

// Adoption, the chart 0.162.0 cluster and every partial state receive only
// the missing rows; rows already there keep their bytes and release_time.
func TestOnlyTheMissingRowsAreInserted(t *testing.T) {
	keys := baseKeys(t)
	cases := map[string]struct {
		done    int   // migrations already done
		present []int // base rows present, by index, with release_time 111 (v1)
	}{
		"a fork install":                      {present: []int{0, 1, 2, 3, 4}},
		"chart 0.162.0: 0001 done, no rows":   {done: 1},
		"the v0 rows only":                    {done: 1, present: []int{1, 4}},
		"one v1 row only":                     {done: 1, present: []int{2}},
		"four of five":                        {done: 1, present: []int{0, 1, 2, 4}},
		"a fork install missing a v1 row":     {present: []int{0, 1, 3, 4}},
		"rows on both shards, a fork install": {present: []int{0, 1, 2, 3, 4}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			s, r, _ := setupAll(t, c.done)
			if c.done == 0 {
				forkSchema(s)
			}
			for i, k := range c.present {
				shard := "s1"
				if strings.HasPrefix(name, "rows on both") && i%2 == 1 {
					shard = "s2"
				}
				s.addRow(shard, baseRow(keys[k], "111"))
			}
			before := rowsText(s)
			if _, err := r.Migrate(context.Background()); err != nil {
				t.Fatal(err)
			}
			after := rowsText(s)
			for _, b := range before {
				if !slices.Contains(after, b) {
					t.Errorf("row %s changed or went", b)
				}
			}
			if added := len(after) - len(before); added != 2*(5-len(c.present)) {
				t.Errorf("%d row copies added, want %d", added, 2*(5-len(c.present)))
			}
			if got := s.history[len(s.history)-1]; got.ID != 2 || !got.Done {
				t.Errorf("history %+v", s.history)
			}
			if wantExec := len(c.present) < 5; (s.dataExecs == 1) != wantExec {
				t.Errorf("%d data statements run, want one only when a row is missing", s.dataExecs)
			}
		})
	}
}

// Promoted-path rows are outside 0002's scope: allowed, untouched.
func TestPromotedPathRowsAreLeftAlone(t *testing.T) {
	s, r, _ := setupAll(t, 1)
	promoted := schema.LiveRow{Key: []string{"logs", "body_v2", "JSON()", "body", "user.id", "1"}, Values: map[string]string{"release_time": "5"}}
	s.addRow("s2", promoted)
	if _, err := r.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(rowsText(s), "s2/r3 logs|body_v2|JSON()|body|user.id|1 5") {
		t.Fatal("the promoted-path row changed")
	}
}

// A contradiction is refused before started, and nothing is written.
func TestContradictoryRowsAreRefusedBeforeStarted(t *testing.T) {
	keys := baseKeys(t)
	other := slices.Clone(keys[2])
	other[2] = "Map(String, String)"
	cases := map[string]func(*fakeStore){
		"another base row": func(s *fakeStore) { s.addRow("s1", baseRow(other, "5")) },
		"a v0 row not at 0": func(s *fakeStore) {
			s.addRow("s1", schema.LiveRow{Key: keys[1], Values: map[string]string{"release_time": "7"}})
		},
		"a v1 row at 0":              func(s *fakeStore) { s.addRow("s1", baseRow(keys[0], "0")) },
		"the same key on two shards": func(s *fakeStore) { s.addRow("s1", baseRow(keys[0], "5")); s.addRow("s2", baseRow(keys[0], "5")) },
	}
	for name, add := range cases {
		t.Run(name, func(t *testing.T) {
			s, r, _ := setupAll(t, 1)
			add(s)
			before, history := rowsText(s), len(s.history)
			res, err := r.Migrate(context.Background())
			var contra *DataError
			if !errors.As(err, &contra) || len(contra.Diffs) == 0 {
				t.Fatalf("Migrate = %v, want a DataError", err)
			}
			if res.Started || len(s.writes) > 0 || s.dataExecs > 0 || len(s.history) != history || !slices.Equal(rowsText(s), before) {
				t.Fatalf("wrote despite the contradiction: %+v, writes %v", res, s.writes)
			}
			st, err := r.Inspect(context.Background(), nil, 0)
			if !errors.As(err, &contra) || len(st.Diffs) == 0 {
				t.Fatalf("Inspect = %+v, %v; want the contradiction named", st, err)
			}
		})
	}
}

// After 0002 is done, its set must hold: a base row deleted is drift.
func TestADeletedBaseRowIsDrift(t *testing.T) {
	s, r, _ := setupAll(t, 0)
	if _, err := r.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, rp := range fakeShards["s1"] {
		s.rows["s1"][rp] = s.rows["s1"][rp][1:]
	}
	writes := len(s.writes)
	var drift *DriftError
	if _, err := r.Migrate(context.Background()); !errors.As(err, &drift) || !strings.Contains(err.Error(), "is missing") || len(s.writes) != writes {
		t.Fatalf("Migrate = %v", err)
	}
	if _, err := r.Inspect(context.Background(), nil, 0); !errors.As(err, &drift) {
		t.Fatalf("Inspect = %v", err)
	}
}

// The insert's reply lost: exit 4 (started); status reads whether 0002's
// post-conditions hold, and repair follows them.
func TestALostInsertReplyIsRepairedByThePostConditions(t *testing.T) {
	for _, applied := range []bool{true, false} {
		t.Run(map[bool]string{true: "the insert applied", false: "the insert lost"}[applied], func(t *testing.T) {
			s, r, all := setupAll(t, 1)
			s.insert = func(f *fakeStore) {
				if applied {
					f.insert = nil
					_ = f.ExecData(context.Background(), "")
					f.dataExecs--
				}
			}
			r.AfterStatement = func(id uint32, _ int) error {
				if id == 2 {
					return errors.New("connection reset by peer")
				}
				return nil
			}
			res, err := r.Migrate(context.Background())
			if err == nil || !res.Started {
				t.Fatalf("Migrate = %+v, %v; want started", res, err)
			}
			r.AfterStatement, s.insert = nil, nil
			st, err := r.Inspect(context.Background(), nil, 0)
			if !errors.Is(err, ErrInterrupted) || st.Unfinished == nil || st.Unfinished.ID != 2 {
				t.Fatalf("Inspect = %+v, %v", st, err)
			}
			if holds := len(st.Unfinished.Diffs) == 0; holds != applied {
				t.Fatalf("Unfinished %+v, want holds = %t", st.Unfinished, applied)
			}
			if err := r.Repair(context.Background(), 2, MarkAbsent); !errors.Is(err, ErrRepairRefused) {
				t.Fatalf("Repair absent = %v, want refused: 0002 is irreversible", err)
			}
			err = r.Repair(context.Background(), 2, MarkDone)
			if applied != (err == nil) || (!applied && !errors.Is(err, ErrRepairRefused)) {
				t.Fatalf("Repair done = %v with the rows applied = %t", err, applied)
			}
			if applied && !s.history[1].Done {
				t.Fatalf("history %+v", s.history)
			}
			if applied && s.history[1].Checksum != all[1].Checksum() {
				t.Fatal("checksum")
			}
		})
	}
}

// A replica behind its shard is re-read; one that never catches up is a
// divergence. A read that fails, or a run cancelled during the re-reads, is
// retryable, never a divergence: exit 2 before started, 4 after.
func TestReadingTheRows(t *testing.T) {
	cases := map[string]struct {
		before  bool // during the admission, else after the insert
		lagging int
		readErr error
		cancel  bool
		want    func(error) bool
	}{
		"a replica catching up":                  {lagging: 3, want: func(err error) bool { return err == nil }},
		"a replica that never catches up":        {lagging: 1000, want: isDrift},
		"unreachable before started":             {before: true, readErr: errors.New("connection refused"), want: retryable},
		"unreachable after started":              {readErr: errors.New("connection refused"), want: retryable},
		"cancelled while re-reading, before":     {before: true, lagging: 1000, cancel: true, want: retryable},
		"cancelled while re-reading, after":      {lagging: 1000, cancel: true, want: retryable},
		"lagging past the window before started": {before: true, lagging: 1000, want: isDrift},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			s, r, _ := setupAll(t, 1)
			keys := baseKeys(t)
			s.addRow("s1", baseRow(keys[0], "5"))
			sleeps := 0
			r.Sleep = func(ctx context.Context, _ time.Duration) error {
				sleeps++
				if c.cancel && sleeps == 4 {
					return context.Canceled
				}
				return nil
			}
			arm := func(f *fakeStore) { f.lagging, f.dataErr = c.lagging, c.readErr }
			if c.before {
				arm(s)
			} else {
				s.insert = func(f *fakeStore) {
					f.insert = nil
					_ = f.ExecData(context.Background(), "")
					arm(f)
				}
			}
			res, err := r.Migrate(context.Background())
			if !c.want(err) {
				t.Fatalf("Migrate = %v", err)
			}
			if err != nil && res.Started == c.before {
				t.Fatalf("started = %t, want %t", res.Started, !c.before)
			}
			if c.cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("Migrate = %v, want the cancellation", err)
			}
			if c.lagging == 1000 && !c.cancel && sleeps != 20 {
				t.Fatalf("%d re-reads, want 20 (10 s)", sleeps)
			}
		})
	}
}

func isDrift(err error) bool {
	var d *DriftError
	return errors.As(err, &d) && strings.Contains(err.Error(), "the rows of the other replicas of its shard")
}

func retryable(err error) bool {
	var d *DriftError
	var c *DataError
	return err != nil && !errors.As(err, &d) && !errors.As(err, &c)
}

// status, with 0001 done: a partial state lists what 0002 inserts; a
// contradiction names the row.
func TestInspectReportsTheNextMigrationsAdmission(t *testing.T) {
	keys := baseKeys(t)
	s, r, _ := setupAll(t, 1)
	s.addRow("s1", baseRow(keys[1], "0"))
	s.addRow("s2", baseRow(keys[4], "0"))
	st, err := r.Inspect(context.Background(), nil, 0)
	if err != nil || st.Verdict != Pending || !slices.Equal(st.Pending, []uint32{2}) || len(st.Inserts) != 3 {
		t.Fatalf("Inspect = %+v, %v", st, err)
	}
	for _, d := range st.Inserts {
		if d.Kind != schema.Missing || !strings.HasSuffix(d.Object, ", __all__, 1)") {
			t.Errorf("insert %s is not a v1 row", d)
		}
	}
	extra := slices.Clone(keys[0])
	extra[5] = "2"
	s.addRow("s1", baseRow(extra, "5"))
	st, err = r.Inspect(context.Background(), nil, 0)
	var contra *DataError
	if !errors.As(err, &contra) || len(st.Diffs) != 1 || !strings.Contains(st.Diffs[0].String(), "(logs, resource, JSON(), resource, __all__, 2) is extra") {
		t.Fatalf("Inspect = %+v, %v", st, err)
	}
}

// A later migration may widen 0002's rows: before it, 0002's set must hold
// and its own admit; after it, only its set is checked.
func TestALaterMigrationWidensTheRows(t *testing.T) {
	s, r, all := setupAll(t, 2)
	keys := baseKeys(t)
	for _, k := range keys {
		s.addRow("s1", baseRow(k, "5"))
	}
	sixth := []string{"logs", "attributes", "JSON()", "attribute", "__all__", "1"}
	next := all[1]
	next.ID, next.Name, next.Statements = 3, "widen", []string{migrations.DataMarker + "INSERT …"}
	c := next.Data[0]
	c.Rows = append(slices.Clone(c.Rows), schema.DataRow{Key: sixth, Values: c.Rows[0].Values})
	next.Data, next.Pre = []schema.DataCondition{c}, all[1].Post
	if err := migrations.Check(append(slices.Clone(all), next)); err != nil {
		t.Fatal(err)
	}
	r.Migrations = append(slices.Clone(all), next)
	s.insert = func(f *fakeStore) { f.addRow("s2", baseRow(sixth, "9")) }
	res, err := r.Migrate(context.Background())
	if err != nil || !slices.Equal(res.Applied, []uint32{3}) {
		t.Fatalf("Migrate = %+v, %v", res, err)
	}
	if res, err = r.Migrate(context.Background()); err != nil || len(res.Applied) != 0 {
		t.Fatalf("the rerun after 0003 = %+v, %v; the sixth row must not be refused", res, err)
	}
	// A binary that stops at 0002 checks 0002's set, which the sixth row breaks.
	r.Migrations = all
	s.history = s.history[:2]
	var drift *DriftError
	if _, err := r.Migrate(context.Background()); !errors.As(err, &drift) || !strings.Contains(err.Error(), "is extra") {
		t.Fatalf("0002's set with the sixth row = %v", err)
	}
	if !reflect.DeepEqual(r.Migrations[1].Data, all[1].Data) {
		t.Fatal("the test changed 0002")
	}
}

// A data statement that wrote nothing leaves 0002 started, never done.
func TestAnInsertThatWritesNothingIsNotDone(t *testing.T) {
	s, r, _ := setupAll(t, 1)
	s.insert = func(*fakeStore) {}
	res, err := r.Migrate(context.Background())
	var drift *DriftError
	if !errors.As(err, &drift) || !res.Started || s.history[1].Done || !strings.Contains(err.Error(), "post-conditions") {
		t.Fatalf("Migrate = %+v, %v; history %+v", res, err, s.history)
	}
}
