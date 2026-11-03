// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package report // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// The backend's bounds for report sequences in memory (§10.4).
const (
	AssemblyTTL      = 120 * time.Second
	MaxAssemblies    = 1000
	MaxAssemblyBytes = 128 << 20
)

// Errors of Assembler.Add and Commit. A page refused with any of them
// changes nothing, unless it says otherwise.
var (
	ErrStaleSequence   = errors.New("page of a sequence older than one held, applying or invalidated for this agent")
	ErrInvalidSequence = errors.New("page of an invalidated sequence")
	// ErrAlreadyApplied is a page of a sequence not above the last applied:
	// nothing changes, and the backend acknowledges it again.
	ErrAlreadyApplied = errors.New("page of a sequence already applied")
	// ErrApplying is a page for an agent whose completed sequence is still
	// being committed: no acknowledgement yet, the collector sends again.
	ErrApplying = errors.New("a sequence of this agent is being applied")
	// ErrBusy refuses a new assembly when the bound is full of entries that
	// may not be shed without losing protection.
	ErrBusy = errors.New("assembly bound reached")
)

// AgentRecord is the backend's persisted per-agent record (§10.4): the last
// sequence applied and the highest invalidated.
type AgentRecord struct {
	Applied     uint64
	Invalidated uint64
}

// AgentStore is the backend's persistence of AgentRecord, its database. A
// failing call fails closed: nothing is acknowledged, and what memory knows
// is kept until a later call succeeds.
type AgentStore interface {
	Record(agent string) (AgentRecord, error)
	// ApplyIfGreater applies the report and records its sequence as applied,
	// atomically, only if the sequence is above both the applied and the
	// invalidated one; applied is false otherwise, with nothing changed.
	ApplyIfGreater(agent string, r *Report) (applied bool, err error)
	// Invalidate raises the invalidated sequence to seq, if higher.
	Invalidate(agent string, seq uint64) error
}

// Assembler is the reference model of the backend's page assembly (§10.4).
// Per agent it holds at most one entry: an assembly with pages, a completed
// sequence being applied, or an invalidation not yet persisted. Entries
// count toward MaxAssemblies; only assemblies with pages are shed. The
// persisted record holds the rest, so memory does not grow with agents.
// Calls are serialised; the store's compare-and-apply orders applications
// across backend replicas.
type Assembler struct {
	mu     sync.Mutex
	store  AgentStore
	agents map[string]*entry
	bytes  int
}

type entryState uint8

const (
	assembling   entryState = iota
	applying                // complete, until Commit persists it
	invalidating            // invalidated, until the store records it
)

type entry struct {
	state   entryState
	seq     uint64
	started time.Time
	pages   *merger
	report  *Report
}

func NewAssembler(store AgentStore) *Assembler {
	return &Assembler{store: store, agents: map[string]*entry{}}
}

// Add takes one page from agent. When the page completes a valid sequence
// it returns the merged report, and the sequence stays applying until
// Commit; nothing is acknowledged before that.
func (a *Assembler) Add(agent string, page []byte, now time.Time) (*Report, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.expire(now)
	cur := a.agents[agent]
	if cur != nil && cur.state == invalidating {
		a.persistInvalidation(agent, cur)
		cur = a.agents[agent]
	}
	if len(page) > MaxPageBytes {
		return nil, a.invalidateHeld(agent, cur, now, fmt.Errorf("a page of %d bytes", len(page)))
	}
	r, err := decodePage(page)
	if err != nil {
		return nil, a.invalidateHeld(agent, cur, now, err)
	}
	seq := r.ReportSequence
	rec, err := a.store.Record(agent)
	if err != nil {
		return nil, fmt.Errorf("agent record: %w", err)
	}
	switch {
	case seq <= rec.Applied:
		return nil, ErrAlreadyApplied
	case seq < rec.Invalidated:
		return nil, ErrStaleSequence
	case seq == rec.Invalidated:
		return nil, ErrInvalidSequence
	}
	if cur != nil {
		switch cur.state {
		case invalidating:
			if seq < cur.seq {
				return nil, ErrStaleSequence
			}
			if seq == cur.seq {
				return nil, ErrInvalidSequence
			}
			// A higher sequence: keep the protection until it is persisted.
			return nil, ErrBusy
		case applying:
			if seq < cur.seq {
				return nil, ErrStaleSequence
			}
			return nil, ErrApplying
		case assembling:
			if seq < cur.seq {
				return nil, ErrStaleSequence
			}
			if seq > cur.seq {
				a.free(agent, cur)
				cur = nil
			}
		}
	}
	if cur == nil {
		if !a.room() {
			return nil, ErrBusy
		}
		cur = &entry{state: assembling, seq: seq, started: now, pages: &merger{}}
		a.agents[agent] = cur
	}
	before := cur.pages.size
	repeat, err := cur.pages.add(page)
	if err != nil {
		return nil, a.invalidate(agent, cur, err)
	}
	if repeat {
		return nil, nil
	}
	a.bytes += cur.pages.size - before
	if !cur.pages.complete() {
		a.shed(agent)
		return nil, nil
	}
	merged, err := cur.pages.result()
	if err != nil {
		return nil, a.invalidate(agent, cur, err)
	}
	a.bytes -= cur.pages.size
	cur.state, cur.pages, cur.report = applying, nil, &merged
	return &merged, nil
}

// Commit applies the completed sequence seq of agent through the store's
// compare-and-apply. Only a nil error lets the backend acknowledge. On a
// store error the sequence stays applying, still blocking older ones, and
// Commit may be called again.
func (a *Assembler) Commit(agent string, seq uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	cur := a.agents[agent]
	if cur == nil || cur.state != applying || cur.seq != seq {
		return fmt.Errorf("sequence %d of %s is not awaiting commit", seq, agent)
	}
	applied, err := a.store.ApplyIfGreater(agent, cur.report)
	if err != nil {
		return fmt.Errorf("apply: %w", err)
	}
	delete(a.agents, agent)
	if !applied {
		// Another replica applied a sequence at least as high, or it was
		// invalidated: the backend's state is already newer.
		rec, err := a.store.Record(agent)
		if err != nil {
			return fmt.Errorf("agent record: %w", err)
		}
		if seq <= rec.Applied {
			return nil // idempotent: acknowledged again
		}
		return ErrInvalidSequence
	}
	return nil
}

// room makes a slot for a new entry, shedding the oldest assembly with
// pages if needed; false when only protected entries fill the bound.
func (a *Assembler) room() bool {
	for len(a.agents) >= MaxAssemblies {
		if !a.shedOldest("") {
			return false
		}
	}
	return true
}

// shed sheds the oldest assemblies, not keep, while over the byte bound.
func (a *Assembler) shed(keep string) {
	for a.bytes > MaxAssemblyBytes {
		if !a.shedOldest(keep) {
			return
		}
	}
}

func (a *Assembler) shedOldest(keep string) bool {
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
	_ = a.invalidate(oldestAgent, oldest, nil)
	return true
}

// expire invalidates assemblies older than the TTL and retries pending
// invalidations.
func (a *Assembler) expire(now time.Time) {
	for agent, cur := range a.agents {
		switch {
		case cur.state == assembling && now.Sub(cur.started) > AssemblyTTL:
			_ = a.invalidate(agent, cur, nil)
		case cur.state == invalidating:
			a.persistInvalidation(agent, cur)
		}
	}
}

// invalidateHeld invalidates the agent's assembly, if it has one.
func (a *Assembler) invalidateHeld(agent string, cur *entry, _ time.Time, err error) error {
	if cur != nil && cur.state == assembling {
		return a.invalidate(agent, cur, err)
	}
	return err
}

// invalidate frees the pages and persists the invalidation; until the store
// records it, the entry stays as an invalidating marker.
func (a *Assembler) invalidate(agent string, cur *entry, err error) error {
	if cur.pages != nil {
		a.bytes -= cur.pages.size
		cur.pages = nil
	}
	cur.state = invalidating
	a.persistInvalidation(agent, cur)
	return err
}

func (a *Assembler) persistInvalidation(agent string, cur *entry) {
	if a.store.Invalidate(agent, cur.seq) == nil {
		delete(a.agents, agent)
	}
}

func (a *Assembler) free(agent string, cur *entry) {
	if cur.pages != nil {
		a.bytes -= cur.pages.size
	}
	delete(a.agents, agent)
}

// Buffered is the bytes of pages and the entries held: all the memory the
// assembler keeps.
func (a *Assembler) Buffered() (int, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.bytes, len(a.agents)
}

// MemoryAgentStore is an AgentStore in memory, for tests; the backend's is
// its database. Fail, when set, makes the named operation fail.
type MemoryAgentStore struct {
	mu      sync.Mutex
	records map[string]AgentRecord
	// Applied lists the sequences applied, per agent, in order.
	Applied map[string][]uint64
	Fail    map[string]bool
}

func NewMemoryAgentStore() *MemoryAgentStore {
	return &MemoryAgentStore{records: map[string]AgentRecord{}, Applied: map[string][]uint64{}, Fail: map[string]bool{}}
}

var errStoreDown = errors.New("store unavailable")

func (m *MemoryAgentStore) Record(agent string) (AgentRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Fail["record"] {
		return AgentRecord{}, errStoreDown
	}
	return m.records[agent], nil
}

func (m *MemoryAgentStore) ApplyIfGreater(agent string, r *Report) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Fail["apply"] {
		return false, errStoreDown
	}
	rec := m.records[agent]
	if r.ReportSequence <= rec.Applied || r.ReportSequence <= rec.Invalidated {
		return false, nil
	}
	rec.Applied = r.ReportSequence
	m.records[agent] = rec
	m.Applied[agent] = append(m.Applied[agent], r.ReportSequence)
	return true, nil
}

func (m *MemoryAgentStore) Invalidate(agent string, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Fail["invalidate"] {
		return errStoreDown
	}
	rec := m.records[agent]
	if seq > rec.Invalidated {
		rec.Invalidated = seq
		m.records[agent] = rec
	}
	return nil
}

// SetFail sets or clears a failure, safely while the store is in use.
func (m *MemoryAgentStore) SetFail(op string, fail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Fail[op] = fail
}

// RecordOf reads a record for tests.
func (m *MemoryAgentStore) RecordOf(agent string) AgentRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.records[agent]
}
