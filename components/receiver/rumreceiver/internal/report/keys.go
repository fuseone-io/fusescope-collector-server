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
	if k, ok := p.byID[id]; ok {
		k.used = now
		p.unlink(k)
		p.pushFront(k)
		return false
	}
	evicted := false
	if len(p.byID) >= p.capacity && p.tail != nil {
		old := p.tail
		p.unlink(old)
		delete(p.byID, old.id)
		evicted = true
	}
	k := &keyUse{id: id, used: now}
	p.byID[id] = k
	p.pushFront(k)
	return evicted
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

func (p *pendingKeys) pushFront(k *keyUse) {
	k.prev, k.next = nil, p.head
	if p.head != nil {
		p.head.prev = k
	}
	p.head = k
	if p.tail == nil {
		p.tail = k
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
