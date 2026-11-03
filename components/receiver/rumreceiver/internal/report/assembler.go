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
	ErrStaleSequence   = errors.New("page of a sequence older than the one held for this agent")
	ErrInvalidSequence = errors.New("page of an invalidated sequence")
)

// Assembler is the reference model of the backend's page assembly (§10.4),
// for tests and as the contract the backend implements: at most one
// incomplete sequence per agent, a newer sequence discarding it, an older
// one never replacing it, a TTL and global bounds on what is buffered, and
// an invalidated sequence refused until a higher one arrives.
type Assembler struct {
	agents map[string]*assembly
	bytes  int
	open   int
}

type assembly struct {
	seq     uint64
	started time.Time
	pages   *merger
	// invalid: refused until a higher sequence; done: assembled, and its
	// repeats are ignored.
	invalid, done bool
}

func NewAssembler() *Assembler { return &Assembler{agents: map[string]*assembly{}} }

// Add takes one page from agent. It returns the merged report once every
// page of the sequence is held and valid; the caller then applies and
// persists it, and only then acknowledges. A non-nil error refuses the
// page; any but ErrStaleSequence and ErrInvalidSequence also invalidates
// the agent's current sequence.
func (a *Assembler) Add(agent string, page []byte, now time.Time) (*Report, error) {
	a.expire(now)
	if len(page) > MaxPageBytes {
		return nil, a.invalidate(a.agents[agent], fmt.Errorf("a page of %d bytes", len(page)))
	}
	r, err := decodePage(page)
	if err != nil {
		return nil, a.invalidate(a.agents[agent], err)
	}
	cur := a.agents[agent]
	switch {
	case cur != nil && r.ReportSequence < cur.seq:
		return nil, ErrStaleSequence
	case cur != nil && r.ReportSequence == cur.seq && cur.invalid:
		return nil, ErrInvalidSequence
	case cur != nil && r.ReportSequence == cur.seq && cur.done:
		return nil, nil // a repeat of a sequence already assembled
	case cur == nil || r.ReportSequence > cur.seq:
		if cur != nil {
			a.release(cur)
		}
		cur = &assembly{seq: r.ReportSequence, started: now, pages: &merger{}}
		a.agents[agent] = cur
		a.open++
	}
	before := cur.pages.size
	repeat, err := cur.pages.add(page)
	if err != nil {
		return nil, a.invalidate(cur, err)
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
	a.release(cur)
	if err != nil {
		cur.invalid = true
		return nil, err
	}
	cur.done = true
	return &merged, nil
}

// invalidate marks an incomplete sequence invalid and frees its pages.
func (a *Assembler) invalidate(cur *assembly, err error) error {
	if cur != nil && !cur.done {
		a.release(cur)
		cur.invalid = true
	}
	return err
}

func (a *Assembler) release(cur *assembly) {
	if cur.pages == nil {
		return
	}
	a.bytes -= cur.pages.size
	a.open--
	cur.pages = nil
}

// expire invalidates incomplete assemblies older than the TTL.
func (a *Assembler) expire(now time.Time) {
	for _, cur := range a.agents {
		if cur.pages != nil && now.Sub(cur.started) > AssemblyTTL {
			a.release(cur)
			cur.invalid = true
		}
	}
}

// shed invalidates the oldest incomplete assemblies while over a bound.
func (a *Assembler) shed() {
	for a.bytes > MaxAssemblyBytes || a.open > MaxAssemblies {
		var oldest *assembly
		for _, cur := range a.agents {
			if cur.pages != nil && (oldest == nil || cur.started.Before(oldest.started)) {
				oldest = cur
			}
		}
		if oldest == nil {
			return
		}
		a.release(oldest)
		oldest.invalid = true
	}
}

// Buffered is the bytes and incomplete assemblies held.
func (a *Assembler) Buffered() (int, int) { return a.bytes, a.open }
