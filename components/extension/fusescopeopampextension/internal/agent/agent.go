// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package agent is the OpAMP agent shared by the fusescopeopamp provider and
// the fusescope_opamp extension (ADR 0008): it merges and preflights pushes,
// persists their lifecycle, triggers reloads and reports status.
package agent // import "github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/agent"

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/merge"
	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/preflight"
	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/state"
)

const (
	// ExtensionName is the extension whose Ready confirms a revision.
	ExtensionName = "fusescope_opamp"
	// MaxPushBytes and MaxManagedProcessors bound what a push may hold.
	MaxPushBytes         = 4 << 20
	MaxManagedProcessors = 256
	// PreflightTimeout bounds the start of each preflighted processor.
	PreflightTimeout = 10 * time.Second
	// confirmRetryStart and confirmRetryMax pace retries of a confirmation
	// that could not be recorded.
	confirmRetryStart = 100 * time.Millisecond
	confirmRetryMax   = 30 * time.Second
)

// Status is reported to the backend for one push.
type Status struct {
	Hash    []byte
	Applied bool
	Message string // on failure: what failed, never configuration values
}

// Reporter sends status and the effective configuration to the backend. Its
// calls must not block on the network.
type Reporter interface {
	Report(Status)
	EffectiveConfigChanged()
}

// Agent holds what runs, what is pending, and the persisted lifecycle.
type Agent struct {
	logger    *zap.Logger
	store     *state.Store
	factories map[component.Type]processor.Factory

	// lifecycle serializes the transitions of pending and applied pushes, on
	// disk and in memory; mu guards the fields below and is taken inside it.
	lifecycle sync.Mutex
	mu        sync.Mutex
	base      merge.Config
	running   merge.Config // merged configuration the collector runs or will
	revision  string       // injected into the extension; confirms running
	applied   []byte       // body of the last applied push, merged again on a new base
	pending   *state.Push
	pendingM  merge.Config
	reload    func()
	reporter  Reporter
	failed    *state.Push // from the last start, reported once connected
	// ready is set once the startup graph reports Ready; until then a push
	// matching what runs has its APPLIED deferred to deferred.
	ready    bool
	deferred []byte
	// queued is the latest push received while another was pending; seq
	// numbers pushes in arrival order, so an older one never replaces a newer.
	queued *queuedPush
	seq    uint64
	// retrying is set while a confirmation that could not be recorded is
	// retried; Close closes done and waits for the retry loop in retries.
	retrying bool
	done     chan struct{}
	closed   bool
	retries  sync.WaitGroup
	// reportReady is set when the local configuration changed: once its
	// revision is Ready, the new effective configuration is reported.
	reportReady bool
	// activate runs once the collector really starts; activated is set then.
	activate  func(context.Context) error
	activated bool
	// beforeBegin, set in tests, runs while a push is being recorded.
	beforeBegin func()
}

type queuedPush struct {
	hash, body []byte
	seq        uint64
}

// New opens the agent's state.
func New(logger *zap.Logger, dir string, factories map[component.Type]processor.Factory) (*Agent, error) {
	store, err := state.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("open OpAMP state %s: %w", dir, err)
	}
	return &Agent{logger: logger, store: store, factories: factories, done: make(chan struct{})}, nil
}

// InstanceUID is the agent's persistent identity.
func (a *Agent) InstanceUID() ([16]byte, error) { return a.store.InstanceUID() }

func (a *Agent) storeDir() string { return a.store.Dir() }

// Load reads the persisted lifecycle and decides what the collector runs
// first: the base merged again with the last applied push, if it still
// fits the base, else the base alone. It returns a push that failed while
// being applied before the last exit, to report FAILED. It writes nothing,
// since validating a configuration loads it too; Activate records the start.
func (a *Agent) Load(base merge.Config) (*Status, error) {
	st, err := a.store.Peek()
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.base, a.running, a.revision = base, base, newRevision()
	if st.DroppedCache {
		a.logger.Warn("the last start from the cached OpAMP configuration never completed; running the local configuration alone")
	}
	if st.Applied != nil {
		if m, err := a.mergePush(base, st.Applied.Config); err != nil {
			a.logger.Warn("the cached OpAMP configuration no longer fits the local one; running the local configuration alone", zap.Error(err))
		} else {
			a.running, a.revision, a.applied = m, st.Applied.Revision, st.Applied.Config
		}
	}
	if st.Failed != nil {
		a.failed = st.Failed
		return &Status{Hash: st.Failed.Hash, Message: "the collector exited while applying this configuration"}, nil
	}
	return nil, nil
}

// OnActivate sets what Activate runs once the start is recorded.
func (a *Agent) OnActivate(fn func(context.Context) error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.activate = fn
}

// Activate is called by the extension when the collector starts its graph;
// only the first call does anything. It records the start on disk, so a
// start from the cache that never gets Ready is not repeated, then runs the
// OnActivate function.
func (a *Agent) Activate(ctx context.Context) error {
	a.lifecycle.Lock()
	defer a.lifecycle.Unlock()
	a.mu.Lock()
	if a.activated {
		a.mu.Unlock()
		return nil
	}
	a.activated = true
	fn := a.activate
	a.mu.Unlock()
	if _, err := a.store.Start(); err != nil {
		return fmt.Errorf("record the OpAMP start: %w", err)
	}
	if fn != nil {
		return fn(ctx)
	}
	return nil
}

// SetReloader sets how to ask the collector to reload, and the reporter.
func (a *Agent) SetReloader(reload func(), reporter Reporter) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reload, a.reporter = reload, reporter
}

// Config is what the provider returns: the merged configuration with the
// revision to confirm injected into the extension's config.
func (a *Agent) Config(base merge.Config) merge.Config {
	a.lifecycle.Lock()
	defer a.lifecycle.Unlock()
	a.mu.Lock()
	var rejected *Status
	if !equalConfig(a.base, base) {
		// The local file changed: merge the applied and the pending push again
		// on the new base, so neither brings back what the base changed. The
		// new graph gets its own revision, so its Ready reports it effective.
		a.base, a.running = base, base
		a.revision, a.ready, a.reportReady = newRevision(), false, true
		if a.applied != nil {
			if m, err := a.mergePush(base, a.applied); err != nil {
				a.logger.Warn("the applied OpAMP configuration no longer fits the changed local one; running the local configuration alone", zap.Error(err))
			} else {
				a.running = m
			}
		}
		if a.pending != nil {
			if m, err := a.mergePush(base, a.pending.Config); err != nil {
				rejected = &Status{Hash: a.pending.Hash, Message: err.Error()}
				if aerr := a.store.Abort(a.pending.Revision); aerr != nil {
					a.logger.Error("could not drop the pending OpAMP configuration", zap.Error(aerr))
				}
				a.pending, a.pendingM = nil, nil
			} else {
				a.pendingM = m
			}
		}
	}
	cfg, rev := a.running, a.revision
	if a.pendingM != nil {
		cfg, rev = a.pendingM, a.pending.Revision
	}
	reporter := a.reporter
	a.mu.Unlock()
	if rejected != nil && reporter != nil {
		reporter.Report(*rejected)
	}
	return withRevision(cfg, rev)
}

// Effective is the configuration reported as effective: what runs, without
// the injected revision.
func (a *Agent) Effective() merge.Config {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.running
}

// Push handles one pushed configuration and returns the status to report
// now: a failure, an immediate APPLIED, or nil while it is pending or queued.
func (a *Agent) Push(ctx context.Context, hash, body []byte) *Status {
	a.mu.Lock()
	a.seq++
	seq := a.seq
	a.mu.Unlock()
	return a.push(ctx, seq, hash, body)
}

// push handles the push that arrived as seq-th; one superseded by a later
// arrival is dropped wherever it is caught.
func (a *Agent) push(ctx context.Context, seq uint64, hash, body []byte) *Status {
	fail := func(msg string) *Status { return &Status{Hash: hash, Message: msg} }
	if len(body) > MaxPushBytes {
		return fail(fmt.Sprintf("the pushed configuration exceeds %d bytes", MaxPushBytes))
	}
	a.mu.Lock()
	if seq < a.seq {
		a.mu.Unlock()
		return nil // superseded
	}
	base, busy := a.base, a.pending != nil
	if busy {
		// One pending revision at a time: the latest push waits for its confirmation.
		a.queue(seq, hash, body)
	}
	a.mu.Unlock()
	if busy {
		return nil
	}
	merged, err := a.mergePush(base, body)
	if err != nil {
		return fail(err.Error())
	}
	procs, pipelines := managed(merged)
	if len(procs) > MaxManagedProcessors {
		return fail(fmt.Sprintf("the pushed configuration has more than %d managed processors", MaxManagedProcessors))
	}
	// Under the lifecycle lock, so a push being recorded is either published
	// as pending or not started: a matching push never overtakes it.
	a.lifecycle.Lock()
	a.mu.Lock()
	switch {
	case seq < a.seq:
		a.mu.Unlock()
		a.lifecycle.Unlock()
		return nil // superseded
	case a.pending != nil:
		a.queue(seq, hash, body)
		a.mu.Unlock()
		a.lifecycle.Unlock()
		return nil
	case equalConfig(merged, a.running):
		// Already running, as after a reconnect: no reload needed, but not
		// APPLIED before the startup graph is confirmed.
		ready := a.ready
		if !ready {
			a.deferred = hash
		}
		a.mu.Unlock()
		a.lifecycle.Unlock()
		if ready {
			return &Status{Hash: hash, Applied: true}
		}
		return nil
	}
	a.mu.Unlock()
	a.lifecycle.Unlock()
	if !ExtensionEnabled(merged) {
		return fail("the local configuration does not enable the " + ExtensionName + " extension, so the collector cannot confirm a push")
	}
	if err := preflight.Check(ctx, a.logger, a.factories, procs, pipelines, PreflightTimeout); err != nil {
		return fail("preflight: " + err.Error())
	}

	a.lifecycle.Lock()
	a.mu.Lock()
	if seq < a.seq {
		a.mu.Unlock()
		a.lifecycle.Unlock()
		return nil // superseded during preflight
	}
	if a.pending != nil {
		// Another push got in during preflight: try again after it.
		a.queue(seq, hash, body)
		a.mu.Unlock()
		a.lifecycle.Unlock()
		return nil
	}
	if !equalConfig(base, a.base) {
		// The local configuration changed during preflight: merge again now.
		a.mu.Unlock()
		a.lifecycle.Unlock()
		return a.push(ctx, seq, hash, body)
	}
	a.mu.Unlock()
	if a.beforeBegin != nil {
		a.beforeBegin()
	}
	p := &state.Push{Hash: hash, Revision: newRevision(), Config: body}
	if err := a.store.Begin(p); err != nil {
		a.lifecycle.Unlock()
		return fail("could not record the pending configuration")
	}
	a.mu.Lock()
	a.pending, a.pendingM = p, merged
	reload := a.reload
	a.mu.Unlock()
	a.lifecycle.Unlock()
	if reload != nil {
		reload()
	}
	return nil
}

// queue keeps seq's push for after the pending one, unless a later one is
// queued already; a.mu must be held.
func (a *Agent) queue(seq uint64, hash, body []byte) {
	if a.queued == nil || a.queued.seq < seq {
		a.queued = &queuedPush{hash: hash, body: body, seq: seq}
	}
}

// Confirm is called by the extension when revision runs. A pending push of
// that revision is promoted on disk first, then reported APPLIED; a push
// queued meanwhile is handled next.
func (a *Agent) Confirm(revision string) {
	if err := a.confirm(revision); err != nil {
		a.logger.Error("could not record the applied OpAMP configuration; retrying", zap.Error(err))
		a.retryConfirm(revision)
	}
}

// retryConfirm records a confirmation that failed (a full disk, say) until it
// succeeds; the push stays pending, and unreported, until then.
func (a *Agent) retryConfirm(revision string) {
	a.mu.Lock()
	if a.retrying || a.closed {
		a.mu.Unlock()
		return
	}
	a.retrying = true
	a.retries.Add(1)
	a.mu.Unlock()
	go func() {
		defer a.retries.Done()
		defer func() { a.mu.Lock(); a.retrying = false; a.mu.Unlock() }()
		delay := confirmRetryStart
		for {
			select {
			case <-time.After(delay):
			case <-a.done:
				return
			}
			a.mu.Lock()
			pending := a.pending != nil && a.pending.Revision == revision
			startup := revision == a.revision && !a.ready
			stale := !pending && !startup
			a.mu.Unlock()
			if stale {
				return
			}
			if err := a.confirm(revision); err == nil {
				return
			}
			delay = min(2*delay, confirmRetryMax)
		}
	}()
}

// Close stops retrying confirmations and waits for the retry loop to end.
func (a *Agent) Close() {
	a.mu.Lock()
	if !a.closed {
		a.closed = true
		close(a.done)
	}
	a.mu.Unlock()
	a.retries.Wait()
}

func (a *Agent) confirm(revision string) error {
	a.lifecycle.Lock()
	promoted, err := a.store.Confirm(revision)
	if err != nil {
		a.lifecycle.Unlock()
		return err
	}
	a.mu.Lock()
	var statuses []Status
	changed := false
	if revision == a.revision && !a.ready {
		a.ready = true
		changed, a.reportReady = a.reportReady, false
		if a.deferred != nil {
			statuses = append(statuses, Status{Hash: a.deferred, Applied: true})
			a.deferred = nil
		}
	}
	if promoted != nil && a.pending != nil && a.pending.Revision == revision {
		a.running, a.revision, a.applied = a.pendingM, revision, a.pending.Config
		a.pending, a.pendingM = nil, nil
		a.ready, a.reportReady = true, false
		statuses = append(statuses, Status{Hash: promoted.Hash, Applied: true})
		changed = true
	}
	reporter, next := a.reporter, a.queued
	if a.pending == nil {
		a.queued = nil
	} else {
		next = nil
	}
	a.mu.Unlock()
	a.lifecycle.Unlock()
	if reporter != nil {
		for _, st := range statuses {
			reporter.Report(st)
		}
		if changed {
			reporter.EffectiveConfigChanged()
		}
	}
	if next != nil {
		go func() {
			if st := a.push(context.Background(), next.seq, next.hash, next.body); st != nil && reporter != nil {
				reporter.Report(*st)
			}
		}()
	}
	return nil
}

func (a *Agent) mergePush(base merge.Config, body []byte) (merge.Config, error) {
	push, err := parseYAML(body)
	if err != nil {
		return nil, fmt.Errorf("the pushed configuration is not valid YAML")
	}
	return merge.Merge(base, withoutRevision(push))
}

// managed returns the allow-listed processors and every pipeline's processors.
func managed(cfg merge.Config) (map[string]any, map[string][]string) {
	procs := map[string]any{}
	for name, v := range asMap(cfg["processors"]) {
		if merge.AllowListed(name) {
			procs[name] = v
		}
	}
	pipelines := map[string][]string{}
	for name, p := range asMap(asMap(cfg["service"])["pipelines"]) {
		raw, _ := asMap(p)["processors"].([]any)
		for _, v := range raw {
			if s, ok := v.(string); ok {
				pipelines[name] = append(pipelines[name], s)
			}
		}
	}
	return procs, pipelines
}

// ExtensionEnabled reports whether cfg declares and enables the extension.
func ExtensionEnabled(cfg merge.Config) bool {
	if _, ok := asMap(cfg["extensions"])[ExtensionName]; !ok {
		return false
	}
	list, _ := asMap(cfg["service"])["extensions"].([]any)
	return slices.Contains(list, any(ExtensionName))
}

// withRevision returns cfg with extensions.fusescope_opamp.revision set.
func withRevision(cfg merge.Config, rev string) merge.Config {
	exts := asMap(cfg["extensions"])
	ext, ok := exts[ExtensionName]
	if !ok {
		return cfg
	}
	out := shallowCopy(cfg)
	e := shallowCopy(exts)
	x := shallowCopy(asMap(ext))
	x["revision"] = rev
	e[ExtensionName] = x
	out["extensions"] = e
	return out
}

// withoutRevision drops the injected revision a backend may echo back.
func withoutRevision(cfg merge.Config) merge.Config {
	ext, ok := asMap(cfg["extensions"])[ExtensionName].(map[string]any)
	if !ok {
		return cfg
	}
	delete(ext, "revision")
	return cfg
}

func newRevision() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func shallowCopy(m map[string]any) map[string]any {
	out := make(map[string]any, len(m)+1)
	for k, v := range m {
		out[k] = v
	}
	return out
}

var current atomic.Pointer[Agent]

// SetCurrent makes a the agent the extension confirms revisions to.
func SetCurrent(a *Agent) { current.Store(a) }

// Current is the process's agent, or nil without the provider.
func Current() *Agent { return current.Load() }

// ErrNoSettings is returned when the local file has no fusescope_opamp section.
var ErrNoSettings = errors.New("the local configuration has no fusescope_opamp section")
