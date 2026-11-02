// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"

	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/state"
)

type pushRecord = state.Push

const base = `
receivers: {nop: {}}
exporters: {nop: {endpoint: local}}
extensions: {fusescope_opamp: {}}
service:
  extensions: [fusescope_opamp]
  pipelines:
    logs: {receivers: [nop], processors: [], exporters: [nop]}
`

const push = `
receivers: {nop: {}}
exporters: {nop: {endpoint: local}}
extensions: {fusescope_opamp: {}}
processors: {signozlogspipeline/pipeline_a: {}}
service:
  extensions: [fusescope_opamp]
  pipelines:
    logs: {receivers: [nop], processors: [signozlogspipeline/pipeline_a], exporters: [nop]}
`

func mustParse(t *testing.T, s string) map[string]any {
	t.Helper()
	m, err := parseYAML([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// A cached push is merged again with the current base at start; one the
// base no longer accepts is dropped and the base runs alone.
func TestCacheRevalidatedAgainstNewBase(t *testing.T) {
	dir := t.TempDir()
	a, err := New(zap.NewNop(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Start(mustParse(t, base)); err != nil {
		t.Fatal(err)
	}
	// Record an applied push without a build of its processor.
	if err := a.store.Begin(&pushRecord{Hash: []byte("h1"), Revision: "r1", Config: []byte(push)}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.store.Confirm("r1"); err != nil {
		t.Fatal(err)
	}

	same, _ := New(zap.NewNop(), dir, nil)
	if _, err := same.Start(mustParse(t, base)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fmtConfig(same.Effective()), "pipeline_a") {
		t.Error("the cached push was not merged on the unchanged base")
	}
	same.Confirm("r1") // that start reached Ready

	upgraded := strings.Replace(base, "endpoint: local", "endpoint: upgraded", 1)
	next, _ := New(zap.NewNop(), dir, nil)
	if _, err := next.Start(mustParse(t, upgraded)); err != nil {
		t.Fatal(err)
	}
	eff := fmtConfig(next.Effective())
	if strings.Contains(eff, "pipeline_a") || !strings.Contains(eff, "upgraded") {
		t.Errorf("a push the new base rejects must not revert it; effective:\n%s", eff)
	}
}

func fmtConfig(m map[string]any) string {
	b, _ := MarshalYAML(m)
	return string(b)
}

// --- lifecycle races and ordering ---

type recorder struct {
	mu       sync.Mutex
	statuses []Status
}

func (r *recorder) Report(st Status)        { r.mu.Lock(); r.statuses = append(r.statuses, st); r.mu.Unlock() }
func (r *recorder) EffectiveConfigChanged() {}
func (r *recorder) find(hash string) *Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.statuses {
		if string(r.statuses[i].Hash) == hash {
			return &r.statuses[i]
		}
	}
	return nil
}

var logsType = component.MustNewType("signozlogspipeline")

func okFactory() processor.Factory {
	return processor.NewFactory(logsType, func() component.Config { return &struct{}{} },
		processor.WithLogs(func(ctx context.Context, set processor.Settings, c component.Config, next consumer.Logs) (processor.Logs, error) {
			return processorhelper.NewLogs(ctx, set, c, next, func(_ context.Context, ld plog.Logs) (plog.Logs, error) { return ld, nil })
		}, component.StabilityLevelAlpha))
}

func started(t *testing.T, base string) (*Agent, *recorder, *atomic.Int32) {
	t.Helper()
	a, err := New(zap.NewNop(), t.TempDir(), map[component.Type]processor.Factory{logsType: okFactory()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Start(mustParse(t, base)); err != nil {
		t.Fatal(err)
	}
	r := &recorder{}
	reloads := &atomic.Int32{}
	a.SetReloader(func() { reloads.Add(1) }, r)
	return a, r, reloads
}

// revisionOf returns the revision the provider would inject now.
func revisionOf(a *Agent) string {
	cfg := a.Config(a.base)
	return cfg["extensions"].(map[string]any)[ExtensionName].(map[string]any)["revision"].(string)
}

// A push matching what runs is APPLIED only once the startup graph is Ready.
func TestNoopAppliedWaitsForStartupReady(t *testing.T) {
	a, r, _ := started(t, base)
	startRev := revisionOf(a)
	if st := a.Push(context.Background(), []byte("h0"), []byte(base)); st != nil {
		t.Fatalf("APPLIED %+v before the startup graph was confirmed", st)
	}
	a.Confirm(startRev)
	if st := r.find("h0"); st == nil || !st.Applied {
		t.Fatalf("deferred APPLIED not reported after Ready: %+v", st)
	}
	if st := a.Push(context.Background(), []byte("h0"), []byte(base)); st == nil || !st.Applied {
		t.Errorf("after Ready a matching push is APPLIED at once, got %+v", st)
	}
}

// A push that arrives while another is pending waits; it is applied after
// the first one's confirmation, and never clears its marker.
func TestPushDuringPendingIsQueued(t *testing.T) {
	a, r, reloads := started(t, base)
	a.Confirm(revisionOf(a))
	if st := a.Push(context.Background(), []byte("h1"), []byte(push)); st != nil {
		t.Fatalf("push 1: %+v", st)
	}
	rev1 := revisionOf(a)
	second := strings.Replace(push, "pipeline_a", "pipeline_b", 2)
	if st := a.Push(context.Background(), []byte("h2"), []byte(second)); st != nil {
		t.Fatalf("push 2 while pending: %+v", st)
	}
	if reloads.Load() != 1 {
		t.Fatalf("%d reloads, want 1 while the first push is pending", reloads.Load())
	}
	a.Confirm(rev1)
	waitFor(t, func() bool { return reloads.Load() == 2 })
	rev2 := revisionOf(a)
	a.Confirm(rev2)
	if st := r.find("h1"); st == nil || !st.Applied {
		t.Errorf("h1 = %+v, want APPLIED", st)
	}
	waitFor(t, func() bool { st := r.find("h2"); return st != nil && st.Applied })
	if !strings.Contains(fmtConfig(a.Effective()), "pipeline_b") {
		t.Error("the queued push is not what runs")
	}
}

// Concurrent pushes and confirmations keep exactly one pending marker.
func TestConcurrentPushAndConfirm(t *testing.T) {
	a, _, _ := started(t, base)
	a.Confirm(revisionOf(a))
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			body := strings.Replace(push, "pipeline_a", fmt.Sprint("pipeline_", i), 2)
			_ = a.Push(context.Background(), []byte(fmt.Sprint("h", i)), []byte(body))
		})
		wg.Go(func() { a.Confirm(revisionOf(a)) })
	}
	wg.Wait()
}

// A pending push merged against an old base is merged again when the base
// changes, and dropped when it no longer fits.
func TestPendingRemergedOnBaseChange(t *testing.T) {
	a, r, _ := started(t, base)
	a.Confirm(revisionOf(a))
	if st := a.Push(context.Background(), []byte("h1"), []byte(push)); st != nil {
		t.Fatalf("push: %+v", st)
	}
	newBase := mustParse(t, strings.Replace(base, "endpoint: local", "endpoint: upgraded", 1))
	cfg := a.Config(newBase)
	if eff, _ := MarshalYAML(cfg); strings.Contains(string(eff), "endpoint: local") {
		t.Errorf("the pending push brought back the old endpoint:\n%s", eff)
	}
	if st := r.find("h1"); st == nil || st.Applied {
		t.Errorf("h1 = %+v, want FAILED: it no longer fits the new base", st)
	}
	if _, err := os.Stat(filepath.Join(a.storeDir(), "pending.json")); err == nil {
		t.Error("the dropped push left its pending marker")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A confirmation that cannot be recorded stays pending, is retried, and is
// reported APPLIED only once it is on disk.
func TestConfirmRetriedUntilPersisted(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	a, r, _ := started(t, base)
	a.Confirm(revisionOf(a))
	if st := a.Push(context.Background(), []byte("h1"), []byte(push)); st != nil {
		t.Fatalf("push: %+v", st)
	}
	rev := revisionOf(a)
	dir := a.storeDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	a.Confirm(rev)
	time.Sleep(100 * time.Millisecond)
	if st := r.find("h1"); st != nil {
		t.Fatalf("reported %+v before the confirmation was recorded", st)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { st := r.find("h1"); return st != nil && st.Applied })
	if _, err := os.Stat(filepath.Join(dir, "applied.json")); err != nil {
		t.Error("APPLIED without the applied push on disk")
	}
}

// A startup confirmation that cannot be recorded is retried too.
func TestStartupConfirmRetried(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	first, err := New(zap.NewNop(), dir, map[component.Type]processor.Factory{logsType: okFactory()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Start(mustParse(t, base)); err != nil {
		t.Fatal(err)
	}
	first.SetReloader(func() {}, &recorder{})
	first.Confirm(revisionOf(first))
	_ = first.Push(context.Background(), []byte("h1"), []byte(push))
	first.Confirm(revisionOf(first))

	a, err := New(zap.NewNop(), dir, map[component.Type]processor.Factory{logsType: okFactory()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Start(mustParse(t, base)); err != nil { // starts from the cache
		t.Fatal(err)
	}
	r := &recorder{}
	a.SetReloader(func() {}, r)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	a.Confirm(revisionOf(a))
	_ = os.Chmod(dir, 0o700)
	waitFor(t, func() bool { a.mu.Lock(); defer a.mu.Unlock(); return a.ready })
	if _, err := os.Stat(filepath.Join(dir, "starting.json")); err == nil {
		t.Error("the startup marker survived its confirmation, so the next start would drop a valid cache")
	}
}

// However queued work interleaves, the latest push is what runs in the end.
func TestLatestPushWins(t *testing.T) {
	a, _, _ := started(t, base)
	a.Confirm(revisionOf(a))
	body := func(n int) []byte { return []byte(strings.Replace(push, "pipeline_a", fmt.Sprint("pipeline_", n), 2)) }
	_ = a.Push(context.Background(), []byte("h1"), body(1)) // pending
	_ = a.Push(context.Background(), []byte("h2"), body(2)) // queued
	a.Confirm(revisionOf(a))                                // h1 applied; h2 processed asynchronously
	_ = a.Push(context.Background(), []byte("h3"), body(3)) // newest
	settle := time.Now().Add(5 * time.Second)
	for time.Now().Before(settle) {
		a.Confirm(revisionOf(a))
		time.Sleep(20 * time.Millisecond)
		a.mu.Lock()
		idle := a.pending == nil && a.queued == nil
		a.mu.Unlock()
		if idle && strings.Contains(fmtConfig(a.Effective()), "pipeline_3") {
			return
		}
	}
	t.Errorf("an older push replaced the newest; effective:\n%s", fmtConfig(a.Effective()))
}
