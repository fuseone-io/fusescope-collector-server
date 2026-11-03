// Copyright FuseOne
// SPDX-License-Identifier: MIT
//
// TestLimiterAllow, TestLimiterIsolatesIdentities and
// TestLimiterSweepsIdleBuckets are moved from fuseone-io/fusescope
// pkg/ratelimit/limiter_test.go at 6c8991257 (MIT, see LICENSE-MIT in the
// module root), adapted to the per-call limit; the capacity, eviction and
// policy-change cases are new (docs/spec/rum.md §7.1).

package ratelimit

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLimiterAllow(t *testing.T) {
	start := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

	testCases := []struct {
		name     string
		limit    Limit
		requests []time.Duration
		expected []bool
	}{
		{name: "BurstThenRejected", limit: Limit{Rate: 1, Burst: 2}, requests: []time.Duration{0, 0, 0}, expected: []bool{true, true, false}},
		{name: "RefillsAtRate", limit: Limit{Rate: 2, Burst: 1}, requests: []time.Duration{0, 100 * time.Millisecond, 500 * time.Millisecond, 600 * time.Millisecond}, expected: []bool{true, false, true, false}},
		{name: "NeverExceedsBurst", limit: Limit{Rate: 100, Burst: 2}, requests: []time.Duration{0, time.Minute, time.Minute, time.Minute}, expected: []bool{true, true, true, false}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			limiter := NewLimiter[string](10)
			for i, offset := range tc.requests {
				assert.Equal(t, tc.expected[i], limiter.Allow("key", tc.limit, start.Add(offset)), "request %d", i)
			}
		})
	}
}

func TestLimiterIsolatesIdentities(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	limiter := NewLimiter[string](10)
	one := Limit{Rate: 1, Burst: 1}

	assert.True(t, limiter.Allow("key-a|10.0.0.1", one, now))
	assert.False(t, limiter.Allow("key-a|10.0.0.1", one, now))
	assert.True(t, limiter.Allow("key-a|10.0.0.2", one, now))
	assert.True(t, limiter.Allow("key-b|10.0.0.1", one, now))
}

func TestLimiterSweepsIdleBuckets(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	limiter := NewLimiter[string](10)
	one := Limit{Rate: 1, Burst: 1}

	limiter.Allow("idle", one, now)
	limiter.Allow("recent", one, now.Add(IdleTTL))
	assert.Equal(t, 2, limiter.Len(), "idle for exactly the TTL is kept")
	limiter.Allow("other", one, now.Add(2*IdleTTL))

	assert.Equal(t, 2, limiter.Len())
	assert.True(t, limiter.Allow("idle", one, now.Add(2*IdleTTL)), "a dropped identity starts with a full bucket")
}

func TestLimiterCapacityEvictsLeastRecentlyUsed(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	limiter := NewLimiter[string](3)
	one := Limit{Rate: 0.001, Burst: 1}

	for i := range 3 {
		assert.True(t, limiter.Allow("c"+strconv.Itoa(i), one, now))
	}
	assert.False(t, limiter.Allow("c0", one, now), "c0 is now the most recently used")
	assert.True(t, limiter.Allow("c3", one, now), "a new identity evicts c1, the least recently used")
	assert.Equal(t, 3, limiter.Len())
	assert.False(t, limiter.Allow("c0", one, now), "c0 kept its empty bucket")
	assert.True(t, limiter.Allow("c1", one, now), "c1 was evicted and starts full")

	for i := range 10_000 {
		limiter.Allow("flood"+strconv.Itoa(i), one, now)
	}
	assert.Equal(t, 3, limiter.Len(), "the map never grows past its capacity")
}

func TestLimiterPolicyLimitChanges(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	limiter := NewLimiter[string](10)

	assert.True(t, limiter.Allow("key", Limit{Rate: 1, Burst: 100}, now))
	// A new policy lowers the key's share: the bucket holds at most the new burst.
	assert.True(t, limiter.Allow("key", Limit{Rate: 1, Burst: 1}, now))
	assert.False(t, limiter.Allow("key", Limit{Rate: 1, Burst: 1}, now))
}

func BenchmarkLimiterAllow(b *testing.B) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	limiter := NewLimiter[string](100_000)
	limit := Limit{Rate: 1e9, Burst: 1e9}
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		limiter.Allow("key", limit, now.Add(time.Duration(i)))
	}
}
