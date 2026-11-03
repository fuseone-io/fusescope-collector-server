// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package report // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"

import (
	"errors"
	"fmt"
	"time"
)

// The backend's bounds for incomplete report sequences (§10.4).
const (
	AssemblyTTL      = 120 * time.Second
	MaxAssemblies    = 1000
	MaxAssemblyBytes = 128 << 20
)

// Errors of Assembler.Add. A page refused with any of them changes nothing.
var (
	ErrStaleSequence   = errors.New("page of a sequence older than one already applied or invalidated for this agent")
	ErrInvalidSequence = errors.New("page of an invalidated sequence")
	// ErrAlreadyApplied is a page of the sequence last applied: nothing
	// changes, and the backend acknowledges it again.
	ErrAlreadyApplied = errors.New("page of the sequence already applied")
)

// AgentRecord is the backend's persisted per-agent record (§10.4): the last
// sequence applied and the highest sequence invalidated. It lives with the
// agent's other persisted state, bounded by the agents the backend knows,
// never in the assembler's memory.
type AgentRecord struct {
	Applied     uint64
	Invalidated uint64
}

// AgentStore is where the backend persists AgentRecord, its database.
type AgentStore interface {
	Record(agent string) AgentRecord
	Put(agent string, r AgentRecord)
}

// Assembler is the reference model of the backend's page assembly (§10.4),
// for tests and as the contract the backend implements: at most one
// incomplete sequence per agent, a newer sequence discarding it, an older
// one never replacing it, a TTL and global bounds on what is buffered, and
// an invalidated sequence refused until a higher one arrives. Its memory
// holds only assemblies with pages, at most MaxAssemblies; the applied and
// invalidated markers are in the AgentStore.
type Assembler struct {
	store  AgentStore
	agents map[string]*assembly
	bytes  int
}

type assembly struct {
	seq     uint64
	started time.Time
	pages   *merger
}

func NewAssembler(store AgentStore) *Assembler {
	return &Assembler{store: store, agents: map[string]*assembly{}}
}

// Add takes one page from agent. It returns the merged report once every
// page of the sequence is held and valid; the caller applies it, then calls
// Applied, and only then acknowledges. A non-nil error refuses the page;
// any but the three sentinel errors also invalidates the sequence.
func (a *Assembler) Add(agent string, page []byte, now time.Time) (*Report, error) {
	a.expire(now)
	if len(page) > MaxPageBytes {
		return nil, a.invalidateCurrent(agent, fmt.Errorf("a page of %d bytes", len(page)))
	}
	r, err := decodePage(page)
	if err != nil {
		return nil, a.invalidateCurrent(agent, err)
	}
	seq := r.ReportSequence
	rec := a.store.Record(agent)
	switch {
	case seq == rec.Applied && seq != 0:
		return nil, ErrAlreadyApplied
	case seq < rec.Applied, seq < rec.Invalidated:
		return nil, ErrStaleSequence
	case seq == rec.Invalidated && seq != 0:
		return nil, ErrInvalidSequence
	}
	cur := a.agents[agent]
	switch {
	case cur != nil && seq < cur.seq:
		return nil, ErrStaleSequence
	case cur == nil || seq > cur.seq:
		if cur != nil {
			a.release(agent, cur)
		}
		cur = &assembly{seq: seq, started: now, pages: &merger{}}
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
		a.shed()
		return nil, nil
	}
	merged, err := cur.pages.result()
	if err != nil {
		return nil, a.invalidate(agent, cur, err)
	}
	a.release(agent, cur)
	return &merged, nil
}

// Applied records that the report of seq was applied and persisted; the
// backend then acknowledges it.
func (a *Assembler) Applied(agent string, seq uint64) {
	rec := a.store.Record(agent)
	if seq > rec.Applied {
		rec.Applied = seq
		a.store.Put(agent, rec)
	}
}

func (a *Assembler) invalidateCurrent(agent string, err error) error {
	if cur := a.agents[agent]; cur != nil {
		return a.invalidate(agent, cur, err)
	}
	return err
}

// invalidate frees the assembly and records its sequence as invalidated.
func (a *Assembler) invalidate(agent string, cur *assembly, err error) error {
	a.release(agent, cur)
	rec := a.store.Record(agent)
	if cur.seq > rec.Invalidated {
		rec.Invalidated = cur.seq
		a.store.Put(agent, rec)
	}
	return err
}

func (a *Assembler) release(agent string, cur *assembly) {
	if a.agents[agent] == cur {
		delete(a.agents, agent)
		a.bytes -= cur.pages.size
	}
}

// expire invalidates incomplete assemblies older than the TTL.
func (a *Assembler) expire(now time.Time) {
	for agent, cur := range a.agents {
		if now.Sub(cur.started) > AssemblyTTL {
			_ = a.invalidate(agent, cur, nil)
		}
	}
}

// shed invalidates the oldest incomplete assemblies while over a bound.
func (a *Assembler) shed() {
	for a.bytes > MaxAssemblyBytes || len(a.agents) > MaxAssemblies {
		var oldestAgent string
		var oldest *assembly
		for agent, cur := range a.agents {
			if oldest == nil || cur.started.Before(oldest.started) {
				oldestAgent, oldest = agent, cur
			}
		}
		if oldest == nil {
			return
		}
		_ = a.invalidate(oldestAgent, oldest, nil)
	}
}

// Buffered is the bytes and incomplete assemblies held: all the memory the
// assembler keeps.
func (a *Assembler) Buffered() (int, int) { return a.bytes, len(a.agents) }

// MemoryAgentStore is an AgentStore in memory, for tests; the backend's is
// its database.
type MemoryAgentStore map[string]AgentRecord

func (m MemoryAgentStore) Record(agent string) AgentRecord { return m[agent] }
func (m MemoryAgentStore) Put(agent string, r AgentRecord) { m[agent] = r }
