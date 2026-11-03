// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package report // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"

import (
	"iter"
	"time"
)

// pendingKeys is the last use of each key not yet acknowledged, most recent
// first, holding at most capacity keys (§7.1).
type pendingKeys struct {
	capacity   int
	byID       map[string]*keyUse
	head, tail *keyUse
}

type keyUse struct {
	id         string
	used       time.Time
	prev, next *keyUse
}

func newPendingKeys(capacity int) pendingKeys {
	return pendingKeys{capacity: capacity, byID: map[string]*keyUse{}}
}

// use records a use of id at now and reports whether the oldest use was
// evicted to make room.
func (p *pendingKeys) use(id string, now time.Time) bool {
	// Requests can finish out of order: the newest use wins, and the list
	// stays ordered by use time, not by arrival.
	if k, ok := p.byID[id]; ok {
		if now.After(k.used) {
			k.used = now
			p.unlink(k)
			p.insert(k)
		}
		return false
	}
	evicted := false
	if len(p.byID) >= p.capacity && p.tail != nil {
		if !now.After(p.tail.used) {
			// The new use is the oldest of all: it is the one evicted.
			return true
		}
		old := p.tail
		p.unlink(old)
		delete(p.byID, old.id)
		evicted = true
	}
	k := &keyUse{id: id, used: now}
	p.byID[id] = k
	p.insert(k)
	return evicted
}

// insert places k by its use time, newest first; usually at the head.
func (p *pendingKeys) insert(k *keyUse) {
	next := p.head
	for next != nil && next.used.After(k.used) {
		next = next.next
	}
	if next == nil {
		k.prev, k.next = p.tail, nil
		if p.tail != nil {
			p.tail.next = k
		} else {
			p.head = k
		}
		p.tail = k
		return
	}
	k.prev, k.next = next.prev, next
	if next.prev != nil {
		next.prev.next = k
	} else {
		p.head = k
	}
	next.prev = k
}

func (p *pendingKeys) len() int { return len(p.byID) }

func (p *pendingKeys) get(id string) time.Time {
	if k, ok := p.byID[id]; ok {
		return k.used
	}
	return time.Time{}
}

// clearIf drops id when its last use is still used.
func (p *pendingKeys) clearIf(id string, used time.Time) {
	if k, ok := p.byID[id]; ok && k.used.Equal(used) {
		p.unlink(k)
		delete(p.byID, id)
	}
}

func (p *pendingKeys) newestFirst() iter.Seq2[string, time.Time] {
	return func(yield func(string, time.Time) bool) {
		for k := p.head; k != nil; k = k.next {
			if !yield(k.id, k.used) {
				return
			}
		}
	}
}

func (p *pendingKeys) unlink(k *keyUse) {
	if k.prev != nil {
		k.prev.next = k.next
	} else {
		p.head = k.next
	}
	if k.next != nil {
		k.next.prev = k.prev
	} else {
		p.tail = k.prev
	}
	k.prev, k.next = nil, nil
}
