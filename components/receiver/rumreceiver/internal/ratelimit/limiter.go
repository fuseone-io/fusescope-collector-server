// Copyright FuseOne
// SPDX-License-Identifier: MIT
//
// Moved from fuseone-io/fusescope pkg/ratelimit/limiter.go at 6c8991257
// (MIT, see LICENSE-MIT in the module root). Changed (docs/spec/rum.md
// §7.1, G6): identities are typed, the limit is given per call (a key's
// share comes from the policy), the map is capped with least-recently-used
// eviction, and idle buckets are dropped from the LRU tail on each call
// instead of by a periodic full sweep.

// Package ratelimit holds the RUM token buckets and the client address rules.
package ratelimit // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/ratelimit"

import (
	"sync"
	"time"
)

// IdleTTL is how long an unused bucket is kept.
const IdleTTL = 10 * time.Minute

// Limit is a token bucket: Rate tokens per second, up to Burst.
type Limit struct {
	Rate  float64
	Burst int64
}

// Limiter is an in-memory token bucket per identity, holding at most
// capacity identities.
type Limiter[K comparable] struct {
	mu       sync.Mutex
	capacity int
	buckets  map[K]*bucket[K]
	// head is the most recently used bucket, tail the least.
	head, tail *bucket[K]
}

type bucket[K comparable] struct {
	identity   K
	tokens     float64
	last       time.Time
	prev, next *bucket[K]
}

func NewLimiter[K comparable](capacity int) *Limiter[K] {
	return &Limiter[K]{capacity: capacity, buckets: make(map[K]*bucket[K])}
}

// Allow takes one token from identity's bucket. A new identity starts with
// a full bucket.
func (l *Limiter[K]) Allow(identity K, limit Limit, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.dropIdle(now)
	burst := float64(limit.Burst)
	b, ok := l.buckets[identity]
	if !ok {
		b = l.newBucket()
		b.identity, b.tokens, b.last = identity, burst, now
		l.buckets[identity] = b
		l.pushFront(b)
	} else {
		l.moveToFront(b)
	}

	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = min(burst, b.tokens+elapsed*limit.Rate)
		b.last = now
	}
	// A smaller burst from a new policy applies at once.
	b.tokens = min(b.tokens, burst)
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Len is the number of identities held.
func (l *Limiter[K]) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// newBucket reuses the least recently used bucket when full, so a flood of
// new identities costs no allocation and no growth.
func (l *Limiter[K]) newBucket() *bucket[K] {
	if len(l.buckets) < l.capacity || l.tail == nil {
		return &bucket[K]{}
	}
	b := l.tail
	l.unlink(b)
	delete(l.buckets, b.identity)
	*b = bucket[K]{}
	return b
}

func (l *Limiter[K]) dropIdle(now time.Time) {
	for b := l.tail; b != nil && now.Sub(b.last) > IdleTTL; b = l.tail {
		l.unlink(b)
		delete(l.buckets, b.identity)
	}
}

func (l *Limiter[K]) pushFront(b *bucket[K]) {
	b.prev, b.next = nil, l.head
	if l.head != nil {
		l.head.prev = b
	}
	l.head = b
	if l.tail == nil {
		l.tail = b
	}
}

func (l *Limiter[K]) moveToFront(b *bucket[K]) {
	if l.head == b {
		return
	}
	l.unlink(b)
	l.pushFront(b)
}

func (l *Limiter[K]) unlink(b *bucket[K]) {
	if b.prev != nil {
		b.prev.next = b.next
	} else {
		l.head = b.next
	}
	if b.next != nil {
		b.next.prev = b.prev
	} else {
		l.tail = b.prev
	}
	b.prev, b.next = nil, nil
}
