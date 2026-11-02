// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func push(rev string) *Push {
	return &Push{Hash: []byte("log_pipelines:" + rev), Revision: rev, Config: []byte("receivers: {}\n")}
}

func open(t *testing.T, dir string, crashAt string) *Store {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if crashAt != "" {
		s.crash = func(step string) bool { return step == crashAt }
	}
	return s
}

func TestInstanceUIDIsStable(t *testing.T) {
	dir := t.TempDir()
	a, err := open(t, dir, "").InstanceUID()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := open(t, dir, "").InstanceUID()
	if a != b || a == ([16]byte{}) {
		t.Errorf("UIDs %x and %x, want one stable non-zero UID", a, b)
	}
}

// Each transition, interrupted at each write, leaves a state the next start
// reads as the lifecycle requires.
func TestCrashAtEachTransition(t *testing.T) {
	tests := []struct {
		name        string
		run         func(s *Store) error
		crashAt     string
		wantApplied string // revision, "" for none
		wantFailed  string
	}{
		{"begin interrupted before rename", func(s *Store) error { return s.Begin(push("r1")) }, "before rename pending.json", "", ""},
		{"begin done, reload never confirmed", func(s *Store) error { return s.Begin(push("r1")) }, "", "", "r1"},
		{"promotion interrupted before rename", func(s *Store) error {
			if err := s.Begin(push("r1")); err != nil {
				return err
			}
			_, err := s.Confirm("r1")
			return err
		}, "before rename applied.json", "", "r1"},
		{"promoted, pending not yet cleared", func(s *Store) error {
			if err := s.Begin(push("r1")); err != nil {
				return err
			}
			_, err := s.Confirm("r1")
			return err
		}, "promoted", "r1", ""},
		{"confirmed", func(s *Store) error {
			if err := s.Begin(push("r1")); err != nil {
				return err
			}
			_, err := s.Confirm("r1")
			return err
		}, "", "r1", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := tt.run(open(t, dir, tt.crashAt)); err != nil && !errors.Is(err, errCrash) {
				t.Fatal(err)
			}
			st, err := open(t, dir, "").Start()
			if err != nil {
				t.Fatal(err)
			}
			if got := revision(st.Applied); got != tt.wantApplied {
				t.Errorf("applied %q, want %q", got, tt.wantApplied)
			}
			if got := revision(st.Failed); got != tt.wantFailed {
				t.Errorf("failed %q, want %q", got, tt.wantFailed)
			}
		})
	}
}

// A failed push never replaces the applied one.
func TestFailedPushKeepsTheAppliedOne(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, "")
	_ = s.Begin(push("r1"))
	if _, err := s.Confirm("r1"); err != nil {
		t.Fatal(err)
	}
	_ = s.Begin(push("r2")) // the process exits while applying r2
	st, _ := open(t, dir, "").Start()
	if revision(st.Applied) != "r1" || revision(st.Failed) != "r2" {
		t.Errorf("applied %q, failed %q; want r1 and r2", revision(st.Applied), revision(st.Failed))
	}
}

// A start from the cache that never reaches Ready drops the cache, so the
// next start runs the base: no crash loop.
func TestCacheThatNeverStartsIsDropped(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, "")
	_ = s.Begin(push("r1"))
	_, _ = s.Confirm("r1")
	if st, _ := open(t, dir, "").Start(); revision(st.Applied) != "r1" {
		t.Fatal("first start should use the cache")
	}
	// No Ready: the process died starting from the cache.
	st, _ := open(t, dir, "").Start()
	if st.Applied != nil || !st.DroppedCache {
		t.Errorf("applied %q, dropped %v; want the cache dropped", revision(st.Applied), st.DroppedCache)
	}
	if st, _ := open(t, dir, "").Start(); st.Applied != nil || st.DroppedCache {
		t.Error("the dropped cache came back")
	}
}

// A start from the cache that reaches Ready keeps it for later starts.
func TestCacheConfirmedOnStartIsKept(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, "")
	_ = s.Begin(push("r1"))
	_, _ = s.Confirm("r1")
	s2 := open(t, dir, "")
	if _, err := s2.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Confirm("r1"); err != nil {
		t.Fatal(err)
	}
	if st, _ := open(t, dir, "").Start(); revision(st.Applied) != "r1" {
		t.Error("a confirmed start lost the cache")
	}
}

// Confirm promotes only the pending revision.
func TestConfirmOtherRevision(t *testing.T) {
	s := open(t, t.TempDir(), "")
	_ = s.Begin(push("r2"))
	p, err := s.Confirm("r1")
	if err != nil || p != nil {
		t.Errorf("Confirm(r1) = %v, %v; want nothing promoted", p, err)
	}
}

func revision(p *Push) string {
	if p == nil {
		return ""
	}
	return p.Revision
}

// A retry after the promotion was written, but reported as failed (the
// directory sync after removing the marker, say), returns the applied push.
func TestConfirmRetryAfterPromotion(t *testing.T) {
	for _, step := range []string{"promoted", "pending removed"} {
		t.Run(step, func(t *testing.T) {
			dir := t.TempDir()
			s := open(t, dir, step)
			_ = s.Begin(push("r1"))
			if _, err := s.Confirm("r1"); !errors.Is(err, errCrash) {
				t.Fatalf("Confirm = %v, want the simulated failure", err)
			}
			s.crash = nil
			p, err := s.Confirm("r1")
			if err != nil || revision(p) != "r1" {
				t.Errorf("retry = %v, %v; want the applied r1", revision(p), err)
			}
		})
	}
}

// Peek, as configuration validation uses it, changes nothing on disk.
func TestPeekWritesNothing(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, "")
	_ = s.Begin(push("r1"))
	_, _ = s.Confirm("r1")
	_ = s.Begin(push("r2")) // a pending push the collector never confirmed
	for range 3 {
		st, err := open(t, dir, "").Peek()
		if err != nil || revision(st.Applied) != "r1" || revision(st.Failed) != "r2" {
			t.Fatalf("Peek = %+v, %v", st, err)
		}
	}
	st, _ := open(t, dir, "").Start()
	if revision(st.Applied) != "r1" || revision(st.Failed) != "r2" {
		t.Errorf("Start after Peek = applied %q, failed %q; want r1 and r2", revision(st.Applied), revision(st.Failed))
	}
}

// Peek neither creates the directory nor removes a temporary file, which may
// be a running collector's write in progress; Start drops leftovers.
func TestPeekLeavesDirectory(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := open(t, missing, "").Peek(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(missing); err == nil {
		t.Error("Peek created the state directory")
	}
	dir := t.TempDir()
	tmp := filepath.Join(dir, pendingFile+tmpSuffix)
	if err := os.WriteFile(tmp, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := open(t, dir, "").Peek(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); err != nil {
		t.Error("Peek removed a temporary file")
	}
	if _, err := open(t, dir, "").Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); err == nil {
		t.Error("Start kept a temporary file a crash left")
	}
}
