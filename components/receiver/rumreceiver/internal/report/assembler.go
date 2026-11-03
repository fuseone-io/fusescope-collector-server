// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package report // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"

import (
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
	// RetryBatch is the most store calls one retry pass makes.
	RetryBatch = 8
	// CommitDeadline is how long a failing commit is retried before its
	// sequence is invalidated instead.
	CommitDeadline = AssemblyTTL
	retryBase      = 100 * time.Millisecond
	commitGrace    = time.Second
	retryMax       = 30 * time.Second
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
	// shed without losing protection.
	ErrBusy = errors.New("assembly bounds reached")
)

// AgentRecord is the backend's persisted per-agent record (§10.4): the last
// sequence applied and the highest invalidated.
type AgentRecord struct {
	Applied     uint64
	Invalidated uint64
}

// AgentStore is the backend's persistence of AgentRecord, its database.
type AgentStore interface {
	Record(agent string) (AgentRecord, error)
	// ApplyIfGreater applies the report and records its sequence as applied,
	// atomically, only if the sequence is above both the applied and the
	// invalidated one; applied is false otherwise, with nothing changed.
	ApplyIfGreater(agent string, r *Report) (applied bool, err error)
	// Invalidate raises the invalidated sequence to seq, if higher.
	Invalidate(agent string, seq uint64) error
}

// Committed is a sequence the retrier committed: the backend acknowledges it.
type Committed struct {
	Agent string
	Seq   uint64
}

// Assembler is the reference model of the backend's page assembly (§10.4).
// Memory holds, per agent, at most one entry: an assembly, or a complete
// sequence awaiting commit (both with their pages, counted in one byte
// budget and one count), or an invalidation awaiting persistence (counted
// in its own bound). Store calls are made outside the lock, at most one
// record read per page and RetryBatch retries per pass, each failure backed
// off exponentially with jitter.
type Assembler struct {
	store AgentStore

	mu      sync.Mutex
	agents  map[string]*entry
	bytes   int
	held    int // entries with pages
	pending int // invalidations awaiting persistence

	// Bounds, overridable in tests.
	maxBytes, maxHeld, maxPending, batch int
}

type entryState uint8

const (
	assembling   entryState = iota
	applying                // complete, its pages held until the commit
	invalidating            // invalidated, until the store records it
)

type entry struct {
	state   entryState
	seq     uint64
	started time.Time
	pages   *merger
	// Retry state of a commit or an invalidation.
	attempts int
	next     time.Time
	deadline time.Time
	inFlight bool
}

func NewAssembler(store AgentStore) *Assembler {
	return &Assembler{
		store: store, agents: map[string]*entry{},
		maxBytes: MaxAssemblyBytes, maxHeld: MaxAssemblies, maxPending: MaxPendingInvalidations, batch: RetryBatch,
	}
}

// Add takes one page from agent. When the page completes a valid sequence
// it returns the merged report, decoded for the caller only; the sequence
// stays applying, with its pages, until Commit. Nothing is acknowledged
// before a commit. Each call also runs one bounded retry pass, whose
// commits are returned for acknowledgement.
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
	rec, err := a.store.Record(agent) // outside the lock
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
			// A higher sequence waits until the invalidation is durable.
			return nil, ErrBusy
		case cur.state == applying:
			return nil, ErrApplying
		case seq > cur.seq:
			a.drop(agent, cur)
			cur = nil
		}
	}
	if cur == nil {
		// New entries wait while invalidations are not persisted: each could
		// need one more. Markers only ever come from entries with pages, so
		// pending stays within maxPending + maxHeld.
		if a.pending >= a.maxPending || !a.makeRoom(agent, len(page), true, now) {
			return nil, ErrBusy
		}
		cur = &entry{state: assembling, seq: seq, started: now, pages: &merger{}}
		a.agents[agent] = cur
		a.held++
	} else if !a.makeRoom(agent, len(page), false, now) {
		return nil, ErrBusy
	}
	before := cur.pages.size
	repeat, err := cur.pages.add(page)
	if err != nil {
		a.markInvalid(agent, cur, now)
		return nil, err
	}
	if repeat {
		return nil, nil
	}
	a.bytes += cur.pages.size - before
	if !cur.pages.complete() {
		return nil, nil
	}
	merged, err := cur.pages.result()
	if err != nil {
		a.markInvalid(agent, cur, now)
		return nil, err
	}
	// The pages stay, counted, until the commit; the decoded report is the
	// caller's and is not kept.
	// The caller commits at once; the retrier steps in after commitGrace.
	cur.state, cur.deadline, cur.next = applying, now.Add(CommitDeadline), now.Add(commitGrace)
	return &merged, nil
}

// Commit commits the completed sequence seq of agent through the store's
// compare-and-apply. Only a nil error lets the backend acknowledge. On a
// store error the sequence stays applying, with its pages, and the
// retrier tries again with backoff until CommitDeadline.
func (a *Assembler) Commit(agent string, seq uint64, now time.Time) error {
	a.mu.Lock()
	cur := a.agents[agent]
	if cur == nil || cur.state != applying || cur.seq != seq || cur.inFlight {
		a.mu.Unlock()
		return fmt.Errorf("sequence %d of %s is not awaiting commit", seq, agent)
	}
	cur.inFlight = true
	a.mu.Unlock()
	return a.commit(agent, cur, now)
}

// commit runs one attempt, outside the lock, for an entry marked in flight.
func (a *Assembler) commit(agent string, cur *entry, now time.Time) error {
	report, err := cur.pages.result() // decoded for this attempt only
	var applied bool
	if err == nil {
		applied, err = a.store.ApplyIfGreater(agent, &report)
	}
	var rec AgentRecord
	if err == nil && !applied {
		rec, err = a.store.Record(agent)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	cur.inFlight = false
	if err != nil {
		a.backoff(cur, now)
		if now.After(cur.deadline) {
			// Give up committing; protect the sequence by invalidating it.
			a.markInvalid(agent, cur, now)
		}
		return fmt.Errorf("commit: %w", err)
	}
	a.drop(agent, cur)
	if !applied && cur.seq > rec.Applied {
		return ErrInvalidSequence // invalidated meanwhile
	}
	return nil // applied now, or a higher sequence was already: acknowledge
}

// Retry runs one pass over due commits and invalidations: at most the
// batch, each outside the lock. The backend also calls it from a ticker,
// so a failed commit is retried without waiting for the agent's next page.
// It returns the sequences it committed, which the backend acknowledges.
func (a *Assembler) Retry(now time.Time) []Committed {
	type job struct {
		agent string
		e     *entry
	}
	a.mu.Lock()
	var jobs []job
	for agent, cur := range a.agents {
		if len(jobs) == a.batch {
			break
		}
		if cur.inFlight || now.Before(cur.next) || cur.state == assembling {
			continue
		}
		cur.inFlight = true
		jobs = append(jobs, job{agent, cur})
	}
	a.mu.Unlock()

	var done []Committed
	for _, j := range jobs {
		switch j.e.state {
		case applying:
			if a.commit(j.agent, j.e, now) == nil {
				done = append(done, Committed{j.agent, j.e.seq})
			}
		case invalidating:
			err := a.store.Invalidate(j.agent, j.e.seq)
			a.mu.Lock()
			j.e.inFlight = false
			if err == nil {
				a.drop(j.agent, j.e)
			} else {
				a.backoff(j.e, now)
			}
			a.mu.Unlock()
		}
	}
	return done
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

// expire marks assemblies older than the TTL invalid; no store call.
func (a *Assembler) expire(now time.Time) {
	for agent, cur := range a.agents {
		if cur.state == assembling && now.Sub(cur.started) > AssemblyTTL {
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
// due at once; the retry pass persists it.
func (a *Assembler) markInvalid(agent string, cur *entry, now time.Time) {
	if cur.pages != nil {
		a.bytes -= cur.pages.size
		a.held--
		cur.pages = nil
	}
	if cur.state != invalidating {
		a.pending++
	}
	cur.state, cur.attempts, cur.next = invalidating, 0, now
	a.agents[agent] = cur
}

func (a *Assembler) drop(agent string, cur *entry) {
	if a.agents[agent] != cur {
		return
	}
	if cur.pages != nil {
		a.bytes -= cur.pages.size
		a.held--
	}
	if cur.state == invalidating {
		a.pending--
	}
	delete(a.agents, agent)
}

// Buffered is the bytes of pages held, the entries with pages and the
// invalidations awaiting persistence: all the memory the assembler keeps.
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
	calls   map[string]int
	delay   time.Duration
}

func NewMemoryAgentStore() *MemoryAgentStore {
	return &MemoryAgentStore{records: map[string]AgentRecord{}, applied: map[string][]uint64{}, fail: map[string]bool{}, calls: map[string]int{}}
}

var errStoreDown = errors.New("store unavailable")

func (m *MemoryAgentStore) op(name string) error {
	m.mu.Lock()
	m.calls[name]++
	fail, delay := m.fail[name], m.delay
	m.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	if fail {
		return errStoreDown
	}
	return nil
}

func (m *MemoryAgentStore) Record(agent string) (AgentRecord, error) {
	if err := m.op("record"); err != nil {
		return AgentRecord{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.records[agent], nil
}

func (m *MemoryAgentStore) ApplyIfGreater(agent string, r *Report) (bool, error) {
	if err := m.op("apply"); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rec := m.records[agent]
	if r.ReportSequence <= rec.Applied || r.ReportSequence <= rec.Invalidated {
		return false, nil
	}
	rec.Applied = r.ReportSequence
	m.records[agent] = rec
	m.applied[agent] = append(m.applied[agent], r.ReportSequence)
	return true, nil
}

func (m *MemoryAgentStore) Invalidate(agent string, seq uint64) error {
	if err := m.op("invalidate"); err != nil {
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

// SetDelay makes every operation take d.
func (m *MemoryAgentStore) SetDelay(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delay = d
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
