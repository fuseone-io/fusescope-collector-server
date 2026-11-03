// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package report // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// The backend's bounds for report sequences in memory (§10.4).
const (
	AssemblyTTL = 120 * time.Second
	// MaxAssemblies bounds the sequences held with their pages: being
	// assembled, or complete and awaiting commit.
	MaxAssemblies = 1000
	// MaxAssemblyBytes is one budget for every page held, assembling or
	// awaiting commit.
	MaxAssemblyBytes = 128 << 20
	// MaxPendingInvalidations: no new assembly starts while this many
	// invalidations await persistence; with those of assemblies already
	// held, at most MaxPendingInvalidations + MaxAssemblies markers of an
	// agent ID and a sequence each.
	MaxPendingInvalidations = 1000
	// RetryBatch is the most store calls one retry pass makes; a page also
	// reads its agent's record once.
	RetryBatch = 8
	// StoreCallTimeout is the deadline of every store call; a call past it
	// counts as a failure and is retried with backoff. The backend's
	// database statement timeout must not exceed it.
	StoreCallTimeout = 5 * time.Second
	// CommitDeadline is how long after completion a failing commit is
	// retried before its sequence is invalidated instead; it is checked
	// on every pass, whether or not a call is in flight.
	CommitDeadline = AssemblyTTL
	// MaxCommitDecodeBytes bounds the page bytes of commit attempts in
	// flight, Commit and Retry together; each attempt decodes its pages
	// again, transiently, so the decoded copies are bounded by this times
	// the decode overhead.
	MaxCommitDecodeBytes = 32 << 20
	// MaxCommitsInFlight bounds commit attempts in flight, Commit and Retry
	// together.
	MaxCommitsInFlight = 4
	retryBase          = 100 * time.Millisecond
	retryMax           = 30 * time.Second
	commitGrace        = time.Second
)

// Errors of Assembler.Add and Commit. A page refused with any of them
// changes nothing, unless it says otherwise.
var (
	ErrStaleSequence   = errors.New("page of a sequence older than one held, applying or invalidated for this agent")
	ErrInvalidSequence = errors.New("page of an invalidated sequence")
	// ErrAlreadyApplied is a page of a sequence not above the last applied:
	// nothing changes, and the backend acknowledges it again.
	ErrAlreadyApplied = errors.New("page of a sequence already applied")
	// ErrApplying is a page for an agent whose completed sequence awaits its
	// commit: no acknowledgement yet; the retrier commits it.
	ErrApplying = errors.New("a sequence of this agent is being applied")
	// ErrBusy refuses a page when the bounds are full of what may not be
	// shed without losing protection, or a Commit when the commit budget is
	// full; the retrier commits it later.
	ErrBusy = errors.New("assembly bounds reached")
	// errUnknownResult is a store answer outside the three outcomes: the
	// commit fails closed, nothing is acknowledged.
	errUnknownResult = errors.New("unknown compare-and-apply result")
)

// AgentRecord is the backend's persisted per-agent record (§10.4): the last
// sequence applied and the highest invalidated.
type AgentRecord struct {
	Applied     uint64
	Invalidated uint64
}

// ApplyResult is the one atomic outcome of a compare-and-apply.
type ApplyResult uint8

const (
	// Applied: the report was applied and its sequence recorded.
	Applied ApplyResult = iota + 1
	// AlreadyApplied: a sequence at least as high was applied; nothing changed.
	AlreadyApplied
	// Invalidated: the sequence is not above the invalidated one; nothing changed.
	Invalidated
)

// AgentStore is the backend's persistence of AgentRecord, its database.
// Every call gets a context with a StoreCallTimeout deadline.
type AgentStore interface {
	Record(ctx context.Context, agent string) (AgentRecord, error)
	// ApplyIfGreater applies the report and records its sequence as applied,
	// in one transaction, only if the sequence is above both the applied
	// and the invalidated one.
	ApplyIfGreater(ctx context.Context, agent string, r *Report) (ApplyResult, error)
	// Invalidate raises the invalidated sequence to seq, if higher.
	Invalidate(ctx context.Context, agent string, seq uint64) error
}

// Clock tells the time; a commit is claimed at a fresh reading of it,
// immediately before it starts.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Committed is a sequence the retrier committed: the backend acknowledges it.
type Committed struct {
	Agent string
	Seq   uint64
}

// Assembler is the reference model of the backend's page assembly (§10.4).
// Memory holds, per agent, at most one entry: an assembly (its validator
// and pages), a complete sequence awaiting commit (exact copies of its page
// bytes and nothing decoded), or an invalidation awaiting persistence (an
// agent ID and a sequence). Pages count in one byte budget and one entry
// count; markers in their own bound. Store calls are made outside the lock,
// each with a deadline: a page makes one record read and one retry pass of
// at most RetryBatch store calls.
type Assembler struct {
	store AgentStore
	clock Clock

	mu       sync.Mutex
	agents   map[string]*entry
	bytes    int
	held     int // entries with pages
	pending  int // invalidations awaiting persistence
	decoding int // page bytes of commit attempts in flight
	commits  int // commit attempts in flight

	// Bounds, overridable in tests.
	maxBytes, maxHeld, maxPending, batch int
	maxDecoding, maxCommits              int
	callTimeout                          time.Duration
}

type entryState uint8

const (
	assembling   entryState = iota
	applying                // complete: only raw, until the commit
	invalidating            // invalidated, until the store records it
)

type entry struct {
	state   entryState
	seq     uint64
	started time.Time
	// pages validates an assembly; nil once complete.
	pages *merger
	// raw holds exact copies of a complete sequence's pages, decoded again
	// by Merge at each commit attempt.
	raw  [][]byte
	size int
	// Retry state of a commit or an invalidation. An entry in flight keeps
	// its pages counted until its attempt ends; only an attempt started
	// before the deadline may finish after it.
	attempts int
	next     time.Time
	deadline time.Time
	inFlight bool
}

func NewAssembler(store AgentStore) *Assembler {
	return &Assembler{
		store: store, clock: systemClock{}, agents: map[string]*entry{},
		maxBytes: MaxAssemblyBytes, maxHeld: MaxAssemblies, maxPending: MaxPendingInvalidations,
		batch: RetryBatch, maxDecoding: MaxCommitDecodeBytes, maxCommits: MaxCommitsInFlight,
		callTimeout: StoreCallTimeout,
	}
}

// since returns the time now for a call that began at now, as read by the
// clock: now plus the clock's progress since start. A pass or a commit
// reads it again immediately before each claim, so a store call that took
// long cannot let a later claim pass a deadline it has passed.
func (a *Assembler) since(now, start time.Time) time.Time {
	return now.Add(a.clock.Now().Sub(start))
}

func (a *Assembler) callCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), a.callTimeout)
}

// Add takes one page from agent. When the page completes a valid sequence
// it returns the merged report, the caller's own copy; the sequence keeps
// only its page bytes until Commit. Nothing is acknowledged before a
// commit. Each call also runs one bounded retry pass, whose commits are
// returned for acknowledgement.
func (a *Assembler) Add(agent string, page []byte, now time.Time) (*Report, []Committed, error) {
	r, perr := a.add(agent, page, now)
	return r, a.Retry(now), perr
}

func (a *Assembler) add(agent string, page []byte, now time.Time) (*Report, error) {
	a.mu.Lock()
	a.expire(now)
	a.mu.Unlock()
	if len(page) > MaxPageBytes {
		return nil, a.invalidateHeld(agent, now, fmt.Errorf("a page of %d bytes", len(page)))
	}
	r, err := decodePage(page)
	if err != nil {
		return nil, a.invalidateHeld(agent, now, err)
	}
	seq := r.ReportSequence
	ctx, cancel := a.callCtx()
	rec, err := a.store.Record(ctx, agent) // outside the lock
	cancel()
	if err != nil {
		return nil, fmt.Errorf("agent record: %w", err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	switch {
	case seq <= rec.Applied:
		return nil, ErrAlreadyApplied
	case seq < rec.Invalidated:
		return nil, ErrStaleSequence
	case seq == rec.Invalidated:
		return nil, ErrInvalidSequence
	}
	cur := a.agents[agent]
	if cur != nil {
		switch {
		case seq < cur.seq:
			return nil, ErrStaleSequence
		case cur.state == invalidating && seq == cur.seq:
			return nil, ErrInvalidSequence
		case cur.state == invalidating:
			return nil, ErrBusy // a higher sequence waits until the invalidation is durable
		case cur.state == applying:
			return nil, ErrApplying
		case seq > cur.seq:
			a.drop(agent, cur)
			cur = nil
		}
	}
	if cur == nil {
		// New entries wait while invalidations are not persisted.
		if a.pending >= a.maxPending || !a.makeRoom(agent, len(page), true, now) {
			return nil, ErrBusy
		}
		cur = &entry{state: assembling, seq: seq, started: now, pages: &merger{}}
		a.agents[agent] = cur
		a.held++
	} else if !a.makeRoom(agent, len(page), false, now) {
		return nil, ErrBusy
	}
	repeat, err := cur.pages.add(page)
	if err != nil {
		a.markInvalid(agent, cur, now)
		return nil, err
	}
	if repeat {
		return nil, nil
	}
	a.bytes += cur.pages.size - cur.size
	cur.size = cur.pages.size
	if !cur.pages.complete() {
		return nil, nil
	}
	merged, err := cur.pages.result()
	if err != nil {
		a.markInvalid(agent, cur, now)
		return nil, err
	}
	// Keep only the page bytes, already exact copies, moved and not copied
	// again; the validator, its maps and every decoded string go.
	raw := make([][]byte, 0, len(cur.pages.byPage))
	for i := 1; i <= len(cur.pages.byPage); i++ {
		raw = append(raw, cur.pages.byPage[i])
	}
	cur.pages, cur.raw = nil, raw
	cur.state, cur.deadline, cur.next = applying, now.Add(CommitDeadline), now.Add(commitGrace)
	return &merged, nil
}

// exactCopy copies b into a slice of exactly its length, so nothing larger
// is retained.
func exactCopy(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// Commit commits the completed sequence seq of agent through the store's
// compare-and-apply. Only a nil error lets the backend acknowledge. On a
// failure the sequence stays awaiting commit and the retrier tries again
// with backoff, until CommitDeadline.
func (a *Assembler) Commit(agent string, seq uint64, now time.Time) error {
	start := a.clock.Now()
	a.mu.Lock()
	now = a.since(now, start) // fresh, immediately before the claim
	cur := a.agents[agent]
	if cur == nil || cur.state != applying || cur.seq != seq || cur.inFlight {
		a.mu.Unlock()
		return fmt.Errorf("sequence %d of %s is not awaiting commit", seq, agent)
	}
	if err := a.claimCommit(agent, cur, now); err != nil {
		a.mu.Unlock()
		return err
	}
	a.mu.Unlock()
	return a.commit(agent, cur, now)
}

// claimCommit starts a commit attempt of cur, under the lock, immediately
// before it runs: never past the commit deadline, and only within the
// commit budget. The entry keeps its pages counted until the attempt ends.
func (a *Assembler) claimCommit(agent string, cur *entry, now time.Time) error {
	if now.After(cur.deadline) {
		a.markInvalid(agent, cur, now)
		return ErrInvalidSequence
	}
	if a.commits >= a.maxCommits || a.decoding+cur.size > a.maxDecoding {
		return ErrBusy
	}
	cur.inFlight = true
	a.commits++
	a.decoding += cur.size
	return nil
}

// commit runs one claimed attempt, outside the lock. Only Applied and
// AlreadyApplied let the backend acknowledge; any other answer, or an
// error, fails closed and keeps the entry for a retry, until the deadline.
func (a *Assembler) commit(agent string, cur *entry, now time.Time) error {
	var result ApplyResult
	report, err := mergePages(cur.raw) // decoded again for this attempt only
	if err == nil {
		ctx, cancel := a.callCtx()
		result, err = a.store.ApplyIfGreater(ctx, agent, &report)
		cancel()
	}
	if err == nil && result != Applied && result != AlreadyApplied && result != Invalidated {
		err = fmt.Errorf("%w %d", errUnknownResult, result)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	cur.inFlight = false
	a.commits--
	a.decoding -= cur.size
	if err != nil {
		a.backoff(cur, now) // past the deadline, the next pass invalidates it
		return fmt.Errorf("commit: %w", err)
	}
	a.drop(agent, cur)
	if result == Invalidated {
		return ErrInvalidSequence
	}
	return nil // applied now, or already: acknowledge
}

// Retry runs one pass: it invalidates sequences past their commit deadline
// and assemblies past their TTL (no store call), then retries due commits
// and invalidations, at most the batch, each one store call outside the
// lock. The backend also calls it from a ticker. It returns the sequences
// it committed, which the backend acknowledges.
func (a *Assembler) Retry(start time.Time) []Committed {
	var done []Committed
	began := a.clock.Now()
	// One job at a time: each is chosen, revalidated and claimed under the
	// lock immediately before it runs, at a fresh reading of the clock, so a
	// job never outlives a deadline or an invalidation that came while an
	// earlier one was running.
	for range a.batch {
		a.mu.Lock()
		now := a.since(start, began)
		a.expire(now)
		agent, cur := a.nextJob(now)
		a.mu.Unlock()
		if cur == nil {
			break
		}
		if cur.state == applying {
			if a.commit(agent, cur, now) == nil {
				done = append(done, Committed{agent, cur.seq})
			}
			continue
		}
		ctx, cancel := a.callCtx()
		err := a.store.Invalidate(ctx, agent, cur.seq)
		cancel()
		a.mu.Lock()
		cur.inFlight = false
		if err == nil {
			a.drop(agent, cur)
		} else {
			a.backoff(cur, now)
		}
		a.mu.Unlock()
	}
	return done
}

// nextJob claims the next due commit or invalidation, under the lock: a
// commit only within the commit budget and its deadline.
func (a *Assembler) nextJob(now time.Time) (string, *entry) {
	for agent, cur := range a.agents {
		if cur.inFlight || now.Before(cur.next) {
			continue
		}
		switch cur.state {
		case applying:
			if a.claimCommit(agent, cur, now) == nil {
				return agent, cur
			}
		case invalidating:
			cur.inFlight = true
			return agent, cur
		}
	}
	return "", nil
}

// backoff schedules the next attempt: exponential, capped, with jitter.
func (a *Assembler) backoff(cur *entry, now time.Time) {
	cur.attempts++
	d := retryBase << min(cur.attempts-1, 20)
	if d > retryMax || d <= 0 {
		d = retryMax
	}
	jitter := time.Duration(rand.Int64N(int64(d))) - d/2
	cur.next = now.Add(d + jitter)
}

// makeRoom makes space for size more bytes (and one more entry when
// newEntry) by shedding the oldest assemblies of other agents; it never
// sheds a sequence awaiting commit. false when the bounds stay full.
func (a *Assembler) makeRoom(agent string, size int, newEntry bool, now time.Time) bool {
	for a.bytes+size > a.maxBytes || (newEntry && a.held >= a.maxHeld) {
		if !a.shedOldest(agent, now) {
			return false
		}
	}
	return true
}

func (a *Assembler) shedOldest(keep string, now time.Time) bool {
	var oldestAgent string
	var oldest *entry
	for agent, cur := range a.agents {
		if cur.state == assembling && agent != keep && (oldest == nil || cur.started.Before(oldest.started)) {
			oldestAgent, oldest = agent, cur
		}
	}
	if oldest == nil {
		return false
	}
	a.markInvalid(oldestAgent, oldest, now)
	return true
}

// expire marks assemblies past their TTL and sequences past their commit
// deadline invalid; no store call, so a stuck call cannot delay it. A
// sequence whose commit attempt started before the deadline is left to
// that attempt, bounded by StoreCallTimeout, which keeps its pages counted;
// if the attempt fails, the next pass invalidates it.
func (a *Assembler) expire(now time.Time) {
	for agent, cur := range a.agents {
		switch {
		case cur.state == assembling && now.Sub(cur.started) > AssemblyTTL:
			a.markInvalid(agent, cur, now)
		case cur.state == applying && !cur.inFlight && now.After(cur.deadline):
			a.markInvalid(agent, cur, now)
		}
	}
}

// invalidateHeld invalidates the agent's assembly, if it has one.
func (a *Assembler) invalidateHeld(agent string, now time.Time, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if cur := a.agents[agent]; cur != nil && cur.state == assembling {
		a.markInvalid(agent, cur, now)
	}
	return err
}

// markInvalid frees the entry's pages and leaves an invalidation marker,
// due at once; a retry pass persists it.
func (a *Assembler) markInvalid(agent string, cur *entry, now time.Time) {
	a.free(cur)
	if cur.state != invalidating {
		a.pending++
	}
	cur.state, cur.attempts, cur.next = invalidating, 0, now
	a.agents[agent] = cur
}

func (a *Assembler) free(cur *entry) {
	if cur.pages != nil || cur.raw != nil {
		a.bytes -= cur.size
		a.held--
	}
	cur.pages, cur.raw, cur.size = nil, nil, 0
}

func (a *Assembler) drop(agent string, cur *entry) {
	if a.agents[agent] != cur {
		return
	}
	a.free(cur)
	if cur.state == invalidating {
		a.pending--
	}
	delete(a.agents, agent)
}

// Buffered is the bytes of pages held, the entries with pages and the
// invalidations awaiting persistence: all the memory the assembler keeps,
// pages of commit attempts in flight included.
func (a *Assembler) Buffered() (bytes, held, pending int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.bytes, a.held, a.pending
}

// MemoryAgentStore is an AgentStore in memory with a real compare-and-apply,
// safe for concurrent use, for tests; the backend's is its database.
type MemoryAgentStore struct {
	mu      sync.Mutex
	records map[string]AgentRecord
	applied map[string][]uint64
	fail    map[string]bool
	block   map[string]bool
	calls   map[string]int
	delay   time.Duration
}

func NewMemoryAgentStore() *MemoryAgentStore {
	return &MemoryAgentStore{
		records: map[string]AgentRecord{}, applied: map[string][]uint64{},
		fail: map[string]bool{}, block: map[string]bool{}, calls: map[string]int{},
	}
}

var errStoreDown = errors.New("store unavailable")

func (m *MemoryAgentStore) op(ctx context.Context, name string) error {
	m.mu.Lock()
	m.calls[name]++
	fail, block, delay := m.fail[name], m.block[name], m.delay
	m.mu.Unlock()
	if block {
		<-ctx.Done() // a call that never returns on its own
		return ctx.Err()
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if fail {
		return errStoreDown
	}
	return nil
}

func (m *MemoryAgentStore) Record(ctx context.Context, agent string) (AgentRecord, error) {
	if err := m.op(ctx, "record"); err != nil {
		return AgentRecord{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.records[agent], nil
}

func (m *MemoryAgentStore) ApplyIfGreater(ctx context.Context, agent string, r *Report) (ApplyResult, error) {
	if err := m.op(ctx, "apply"); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rec := m.records[agent]
	switch {
	case r.ReportSequence <= rec.Applied:
		return AlreadyApplied, nil
	case r.ReportSequence <= rec.Invalidated:
		return Invalidated, nil
	}
	rec.Applied = r.ReportSequence
	m.records[agent] = rec
	m.applied[agent] = append(m.applied[agent], r.ReportSequence)
	return Applied, nil
}

func (m *MemoryAgentStore) Invalidate(ctx context.Context, agent string, seq uint64) error {
	if err := m.op(ctx, "invalidate"); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rec := m.records[agent]
	if seq > rec.Invalidated {
		rec.Invalidated = seq
		m.records[agent] = rec
	}
	return nil
}

// SetFail makes an operation ("record", "apply", "invalidate") fail or not.
func (m *MemoryAgentStore) SetFail(op string, fail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fail[op] = fail
}

// SetBlock makes an operation block until its context ends.
func (m *MemoryAgentStore) SetBlock(op string, block bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.block[op] = block
}

// SetDelay makes every operation take d.
func (m *MemoryAgentStore) SetDelay(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delay = d
}

// SetRecord writes a record directly, as another replica would.
func (m *MemoryAgentStore) SetRecord(agent string, r AgentRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[agent] = r
}

// Calls returns and resets the count of calls of an operation.
func (m *MemoryAgentStore) Calls(op string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := m.calls[op]
	m.calls[op] = 0
	return n
}

// RecordOf reads a record for tests.
func (m *MemoryAgentStore) RecordOf(agent string) AgentRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.records[agent]
}

// AppliedOf lists the sequences applied for agent, in order.
func (m *MemoryAgentStore) AppliedOf(agent string) []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]uint64(nil), m.applied[agent]...)
}
