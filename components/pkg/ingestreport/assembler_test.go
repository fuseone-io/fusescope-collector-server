// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package ingestreport

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clockOf is the assembler's fake clock, installed on first use.
func clockOf(a *Assembler) *fakeClock {
	c, ok := a.clock.(*fakeClock)
	if !ok {
		c = &fakeClock{}
		a.clock = c
	}
	return c
}

// addAt, commitAt and retryAt set the assembler's clock to when, then call.
func addAt(a *Assembler, when time.Time, agent string, page []byte) (*Report, []Committed, error) {
	clockOf(a).Set(when)
	return a.Add(agent, page)
}

func commitAt(a *Assembler, when time.Time, agent string, seq uint64) error {
	clockOf(a).Set(when)
	return a.Commit(agent, seq)
}

func retryAt(a *Assembler, when time.Time) []Committed {
	clockOf(a).Set(when)
	return a.Retry()
}

const (
	appA   = "0191c5e2-7f0a-7c3e-9a51-3c2b1d4e5f60"
	keyOne = "0191c5e2-8a1b-7d2e-b0c4-000000000001"
)

var t0 = time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)

func keyID(i int) string { return fmt.Sprintf("0191c5e2-8a1b-7d2e-b0c4-%012d", i) }

// build encodes report seq with one application and keys, as the
// collector's monitor does.
func build(t *testing.T, seq uint64, keys []KeyReport) [][]byte {
	t.Helper()
	header := Report{
		SchemaVersion: 1, ReportSequence: seq, GeneratedAt: "2026-11-03T12:00:00Z", WindowSeconds: 300,
		Policy: PolicyReport{Version: 1, Status: "ok"},
	}
	apps := []ApplicationReport{{ID: appA, FirstSeenAt: "2026-11-03T12:00:00Z", Counts: Counts{}, Rejections: []RejectionReport{}}}
	pages, err := Paginate(header, apps, keys)
	require.NoError(t, err)
	return pages
}

// sequences builds reports 1..n, each of several pages.
func sequences(t *testing.T, n int) [][][]byte {
	t.Helper()
	keys := make([]KeyReport, 6000)
	for i := range keys {
		keys[i] = KeyReport{ID: keyID(i), LastUsedAt: "2026-11-03T12:00:00Z"}
	}
	out := make([][][]byte, n+1)
	for seq := 1; seq <= n; seq++ {
		pages := build(t, uint64(seq), keys)
		require.GreaterOrEqual(t, len(pages), 2)
		out[seq] = pages
	}
	return out
}

// complete sends every page of a sequence and returns the last result.
func complete(t *testing.T, a *Assembler, agent string, pages [][]byte, now time.Time) *Report {
	t.Helper()
	var r *Report
	for _, p := range pages {
		got, _, err := addAt(a, now, agent, p)
		require.NoError(t, err)
		if got != nil {
			r = got
		}
	}
	require.NotNil(t, r)
	return r
}

// Maintainer review of #43: the backend's assembly of pages, as a model.
func TestAssembler(t *testing.T) {
	seqs := sequences(t, 3)
	now := t0

	t.Run("CompleteInAnyOrderThenCommit", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		pages := seqs[1]
		for i := len(pages) - 1; i > 0; i-- {
			r, _, err := addAt(a, now, "agent", pages[i])
			require.NoError(t, err)
			assert.Nil(t, r)
		}
		r, _, err := addAt(a, now, "agent", pages[0])
		require.NoError(t, err)
		require.NotNil(t, r)
		assert.Len(t, r.Keys, 6000)
		require.NoError(t, commitAt(a, now, "agent", 1), "commit, then acknowledge")
		assert.Equal(t, []uint64{1}, store.AppliedOf("agent"))
		b, open, _ := a.Buffered()
		assert.Zero(t, b)
		assert.Zero(t, open)
		_, _, err = addAt(a, now, "agent", pages[0])
		assert.ErrorIs(t, err, ErrAlreadyApplied, "acknowledged again, nothing applied")
	})
	t.Run("IdenticalRepeatIgnored", func(t *testing.T) {
		a := NewAssembler(NewMemoryAgentStore())
		_, _, err := addAt(a, now, "agent", seqs[1][0])
		require.NoError(t, err)
		_, _, err = addAt(a, now, "agent", seqs[1][0])
		assert.NoError(t, err)
		complete(t, a, "agent", seqs[1][1:], now)
	})
	t.Run("LowerSequenceNeverReplacesNewer", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		_, _, err := addAt(a, now, "agent", seqs[2][0])
		require.NoError(t, err)
		_, _, err = addAt(a, now, "agent", seqs[1][0])
		assert.ErrorIs(t, err, ErrStaleSequence)
		r := complete(t, a, "agent", seqs[2][1:], now)
		assert.Equal(t, uint64(2), r.ReportSequence)
		require.NoError(t, commitAt(a, now, "agent", 2))
		_, _, err = addAt(a, now, "agent", seqs[1][1])
		assert.ErrorIs(t, err, ErrAlreadyApplied, "a sequence below the applied one is acknowledged again")
	})
	// The maintainer's reproduction: sequence 2 complete, not yet committed.
	t.Run("CompleteNotYetPersistedBlocksOlder", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		complete(t, a, "agent", seqs[2], now)
		_, _, err := addAt(a, now, "agent", seqs[1][0])
		assert.ErrorIs(t, err, ErrStaleSequence, "sequence 1 is refused while 2 is applying")
		_, _, err = addAt(a, now, "agent", seqs[3][0])
		assert.ErrorIs(t, err, ErrApplying, "a newer one waits for the commit")
		_, open, _ := a.Buffered()
		assert.Equal(t, 1, open, "the applying marker counts toward the bound")
		require.NoError(t, commitAt(a, now, "agent", 2))
		_, _, err = addAt(a, now, "agent", seqs[1][0])
		assert.ErrorIs(t, err, ErrAlreadyApplied)
		assert.Equal(t, []uint64{2}, store.AppliedOf("agent"))
	})
	t.Run("ApplyFailureNoAckStillProtected", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		complete(t, a, "agent", seqs[2], now)
		store.SetFail("apply", true)
		require.Error(t, commitAt(a, now, "agent", 2), "no acknowledgement")
		assert.Empty(t, store.AppliedOf("agent"))
		_, _, err := addAt(a, now, "agent", seqs[1][0])
		assert.ErrorIs(t, err, ErrStaleSequence, "the marker stays in memory: fail closed")
		store.SetFail("apply", false)
		require.NoError(t, commitAt(a, now, "agent", 2), "the commit can be retried")
		assert.Equal(t, []uint64{2}, store.AppliedOf("agent"))
		_, open, _ := a.Buffered()
		assert.Zero(t, open, "released only after the commit")
	})
	t.Run("RecordFailureRefuses", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		store.SetFail("record", true)
		_, _, err := addAt(a, now, "agent", seqs[1][0])
		require.Error(t, err)
		_, open, _ := a.Buffered()
		assert.Zero(t, open, "nothing assembled without the agent's record")
		store.SetFail("record", false)
		complete(t, a, "agent", seqs[1], now)
	})
	t.Run("InvalidationNotPersistedStaysInMemory", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		_, _, err := addAt(a, now, "agent", seqs[1][0])
		require.NoError(t, err)
		store.SetFail("invalidate", true)
		_, _, err = addAt(a, now, "agent", []byte(strings.Replace(string(seqs[1][1]), `"page":`, `"x":1,"page":`, 1)))
		require.Error(t, err)
		assert.Zero(t, store.RecordOf("agent").Invalidated, "the store did not record it")
		b, held, pending := a.Buffered()
		assert.Zero(t, b, "the pages are freed")
		assert.Zero(t, held)
		assert.Equal(t, 1, pending, "the marker stays, bounded")
		for _, p := range seqs[1] {
			_, _, err = addAt(a, now, "agent", p)
			assert.ErrorIs(t, err, ErrInvalidSequence, "still refused from memory")
		}
		_, _, err = addAt(a, now, "agent", seqs[2][0])
		assert.ErrorIs(t, err, ErrBusy, "a newer sequence waits until the invalidation is durable")
		store.SetFail("invalidate", false)
		later := now.Add(time.Minute) // past the backoff
		_, _, err = addAt(a, later, "agent", seqs[1][0])
		assert.ErrorIs(t, err, ErrInvalidSequence)
		assert.Equal(t, uint64(1), store.RecordOf("agent").Invalidated, "the retry pass persisted it")
		_, _, pending = a.Buffered()
		assert.Zero(t, pending, "released once persisted")
		complete(t, a, "agent", seqs[2], later)
	})
	t.Run("NewerSequenceDiscardsIncomplete", func(t *testing.T) {
		a := NewAssembler(NewMemoryAgentStore())
		_, _, err := addAt(a, now, "agent", seqs[1][0])
		require.NoError(t, err)
		_, _, err = addAt(a, now, "agent", seqs[2][0])
		require.NoError(t, err)
		b, open, _ := a.Buffered()
		assert.Equal(t, len(seqs[2][0]), b, "only sequence 2 is held")
		assert.Equal(t, 1, open)
		_, _, err = addAt(a, now, "agent", seqs[1][1])
		assert.ErrorIs(t, err, ErrStaleSequence)
	})
	t.Run("ConflictInvalidatesUntilHigherSequence", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		_, _, err := addAt(a, now, "agent", seqs[1][0])
		require.NoError(t, err)
		other := []byte(strings.Replace(string(seqs[1][0]), `"keys":[`, `"keys":[{"id":"0191c5e2-8a1b-7d2e-b0c4-ffffffffffff","lastUsedAt":"2026-11-03T12:00:00Z"},`, 1))
		_, _, err = addAt(a, now, "agent", other)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrInvalidSequence)
		for _, p := range seqs[1] {
			_, _, err = addAt(a, now, "agent", p)
			assert.ErrorIs(t, err, ErrInvalidSequence, "a replay never restarts it")
		}
		b, open, _ := a.Buffered()
		assert.Zero(t, b)
		assert.Zero(t, open)
		complete(t, a, "agent", seqs[2], now)
	})
	t.Run("InvalidPageInvalidates", func(t *testing.T) {
		for name, bad := range map[string][]byte{
			"OverSize":   append(append([]byte{}, seqs[1][1][:len(seqs[1][1])-1]...), []byte(strings.Repeat(" ", MaxPageBytes)+"}")...),
			"NotStrict":  []byte(strings.Replace(string(seqs[1][1]), `"page":`, `"extra":1,"page":`, 1)),
			"HeaderDiff": []byte(strings.Replace(string(seqs[1][1]), `"generatedAt":"`, `"generatedAt":"1`, 1)),
			"KeyTwice":   []byte(strings.Replace(string(seqs[1][1]), `"keys":[`, `"keys":[{"id":"`+firstKeyOf(t, seqs[1][0])+`","lastUsedAt":"2026-11-03T12:00:00Z"},`, 1)),
		} {
			t.Run(name, func(t *testing.T) {
				a := NewAssembler(NewMemoryAgentStore())
				_, _, err := addAt(a, now, "agent", seqs[1][0])
				require.NoError(t, err)
				_, _, err = addAt(a, now, "agent", bad)
				require.Error(t, err)
				_, _, err = addAt(a, now, "agent", seqs[1][1])
				assert.ErrorIs(t, err, ErrInvalidSequence)
			})
		}
	})
	t.Run("ExpiryInvalidates", func(t *testing.T) {
		a := NewAssembler(NewMemoryAgentStore())
		_, _, err := addAt(a, now, "agent", seqs[1][0])
		require.NoError(t, err)
		_, _, err = addAt(a, now.Add(AssemblyTTL+time.Second), "agent", seqs[1][1])
		assert.ErrorIs(t, err, ErrInvalidSequence)
		b, open, _ := a.Buffered()
		assert.Zero(t, b)
		assert.Zero(t, open)
	})
	t.Run("AssemblyCountBoundShedsOldest", func(t *testing.T) {
		small := smallSequence(t, 1)
		a := NewAssembler(NewMemoryAgentStore())
		for i := range MaxAssemblies + 50 {
			_, _, err := addAt(a, now.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("agent-%d", i), small[0])
			require.NoError(t, err)
			_, open, _ := a.Buffered()
			require.LessOrEqual(t, open, MaxAssemblies)
		}
		_, _, err := addAt(a, now.Add(time.Second), "agent-0", small[1])
		assert.ErrorIs(t, err, ErrInvalidSequence, "the oldest assembly was discarded and stays invalid")
		_, _, err = addAt(a, now.Add(time.Second), fmt.Sprintf("agent-%d", MaxAssemblies+49), small[1])
		assert.NoError(t, err, "the newest is kept")
	})
	t.Run("BoundFullOfProtectedEntriesRefusesNew", func(t *testing.T) {
		small := smallSequence(t, 1)
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		for i := range MaxAssemblies {
			complete(t, a, fmt.Sprintf("applying-%d", i), small, now)
		}
		_, _, err := addAt(a, now, "newcomer", small[0])
		assert.ErrorIs(t, err, ErrBusy, "applying markers are never shed")
		require.NoError(t, commitAt(a, now, "applying-0", 1))
		_, _, err = addAt(a, now, "newcomer", small[0])
		assert.NoError(t, err)
	})
	t.Run("ByteBoundShedsOldest", func(t *testing.T) {
		small := smallSequence(t, 1)
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		a.maxBytes = 10 * len(small[0])
		for i := range 30 {
			_, _, err := addAt(a, now.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("agent-%d", i), small[0])
			require.NoError(t, err)
			b, _, _ := a.Buffered()
			require.LessOrEqual(t, b, a.maxBytes)
		}
		assert.Equal(t, uint64(1), store.RecordOf("agent-0").Invalidated, "the oldest went first")
		assert.NotContains(t, a.agents, "agent-0")
		assert.Contains(t, a.agents, "agent-29")
	})
	// Maintainer review of #43: memory holds only entries in progress; the
	// applied and invalidated markers are in the persisted record.
	t.Run("ManyFinishedAgentsLeaveNoMemory", func(t *testing.T) {
		small := smallSequence(t, 1)
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		for i := range MaxAssemblies + 1 {
			agent := fmt.Sprintf("done-%d", i)
			complete(t, a, agent, small, now)
			require.NoError(t, commitAt(a, now, agent, 1))
		}
		for i := range MaxAssemblies + 1 {
			agent := fmt.Sprintf("invalid-%d", i)
			_, _, err := addAt(a, now, agent, small[0])
			require.NoError(t, err)
			_, _, err = addAt(a, now, agent, []byte(strings.Replace(string(small[0]), `"page":1`, `"page":1,"x":1`, 1)))
			require.Error(t, err)
		}
		for i := range MaxAssemblies - 1 {
			_, _, err := addAt(a, now, fmt.Sprintf("expired-%d", i), small[0])
			require.NoError(t, err)
		}
		_, _, err := addAt(a, now.Add(AssemblyTTL+time.Second), "trigger", small[0])
		require.NoError(t, err)
		for range MaxAssemblies {
			if _, _, p := a.Buffered(); p == 0 {
				break
			}
			retryAt(a, now.Add(AssemblyTTL+time.Second))
		}
		b, held, pending := a.Buffered()
		assert.Equal(t, 1, held, "only the open assembly is held")
		assert.Zero(t, pending)
		assert.Equal(t, len(small[0]), b)
		assert.Equal(t, AgentRecord{Applied: 1}, store.RecordOf("done-0"))
		assert.Equal(t, AgentRecord{Invalidated: 1}, store.RecordOf("invalid-0"))
		assert.Equal(t, AgentRecord{Invalidated: 1}, store.RecordOf("expired-0"), "expiry is recorded in the store")
		_, _, err = addAt(a, now.Add(AssemblyTTL+time.Second), "done-0", small[0])
		assert.ErrorIs(t, err, ErrAlreadyApplied)
		_, _, err = addAt(a, now.Add(AssemblyTTL+time.Second), "invalid-0", small[1])
		assert.ErrorIs(t, err, ErrInvalidSequence)
	})
}

// Maintainer review of #43: two backend replicas, two Assemblers sharing
// one store with a real compare-and-apply, each completing one sequence of
// the same agent and racing their commits, never apply out of order.
// Run with -race.
func TestAssemblerReplicasRaceCommits(t *testing.T) {
	one, two := smallSequence(t, 1), smallSequence(t, 2)
	for range 500 {
		store := NewMemoryAgentStore()
		r1, r2 := NewAssembler(store), NewAssembler(store)
		complete(t, r1, "agent", one, t0)
		complete(t, r2, "agent", two, t0)
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i, c := range []struct {
			a   *Assembler
			seq uint64
		}{{r1, 1}, {r2, 2}} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = commitAt(c.a, t0, "agent", c.seq)
			}()
		}
		wg.Wait()
		applied := store.AppliedOf("agent")
		require.NotEmpty(t, applied)
		for i := 1; i < len(applied); i++ {
			require.Greater(t, applied[i], applied[i-1], "applied in order: %v", applied)
		}
		require.Equal(t, uint64(2), applied[len(applied)-1], "the higher sequence is applied")
		require.NoError(t, errs[1])
		// Sequence 1 committed after 2 is a re-ack with no effect, never an
		// application.
		require.NoError(t, errs[0])
		if len(applied) == 1 {
			require.Equal(t, []uint64{2}, applied)
		}
	}
}

// Maintainer review of #43: one budget for every page held, assembling or
// awaiting commit; while commits fail, the next assembly is refused rather
// than letting a payload escape the count.
func TestAssemblerApplyingWithinBudget(t *testing.T) {
	store := NewMemoryAgentStore()
	store.SetFail("apply", true)
	a := NewAssembler(store)
	page := smallSequence(t, 1)
	a.maxBytes = 3*(len(page[0])+len(page[1])) + 10
	for i := range 3 {
		agent := fmt.Sprintf("agent-%d", i)
		r := complete(t, a, agent, page, t0)
		require.Error(t, commitAt(a, t0, agent, r.ReportSequence), "the store is down")
	}
	b, held, _ := a.Buffered()
	assert.Equal(t, 3*(len(page[0])+len(page[1])), b, "the pages awaiting commit are counted")
	assert.Equal(t, 3, held)
	_, _, err := addAt(a, t0, "newcomer", page[0])
	assert.ErrorIs(t, err, ErrBusy, "no room: a protected sequence is never shed")
	b, _, _ = a.Buffered()
	assert.LessOrEqual(t, b, a.maxBytes)
	for _, e := range a.agents {
		assertOnlyBytes(t, e)
	}
}

// assertOnlyBytes checks what an entry awaiting commit holds: exact copies
// of its pages, and no validator, maps or decoded strings.
func assertOnlyBytes(t *testing.T, e *entry) {
	t.Helper()
	require.Equal(t, applying, e.state)
	assert.Nil(t, e.pages, "no validator, maps or decoded report")
	require.NotEmpty(t, e.raw)
	size := 0
	for _, p := range e.raw {
		assert.Equal(t, len(p), cap(p), "an exact copy, no larger backing array")
		size += len(p)
	}
	assert.Equal(t, size, e.size, "what is held is what is counted")
}

// Maintainer review of #43: a sequence awaiting commit retains its page
// bytes and nothing else; measured on the heap.
func TestAssemblerApplyingRetainedHeap(t *testing.T) {
	seqs := sequences(t, 1)
	pages := seqs[1]
	pageBytes := 0
	for _, p := range pages {
		pageBytes += len(p)
	}
	const n = 16
	store := NewMemoryAgentStore()
	a := NewAssembler(store)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := range n {
		agent := fmt.Sprintf("agent-%d", i)
		for _, p := range pages {
			_, _, err := addAt(a, t0, agent, p)
			require.NoError(t, err)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	growth := int64(after.HeapInuse) - int64(before.HeapInuse)
	t.Logf("%d sequences of %d page bytes awaiting commit: heap in use grew %.1f MiB, pages %.1f MiB",
		n, pageBytes, float64(growth)/(1<<20), float64(n*pageBytes)/(1<<20))
	assert.Less(t, growth, int64(n*pageBytes+(4<<20)), "the pages and a small overhead, nothing decoded")
	b, held, _ := a.Buffered()
	assert.Equal(t, n*pageBytes, b)
	assert.Equal(t, n, held)
	for _, e := range a.agents {
		assertOnlyBytes(t, e)
	}
	// A commit decodes again, from the bytes alone.
	require.NoError(t, commitAt(a, t0, "agent-0", 1))
	assert.Equal(t, []uint64{1}, store.AppliedOf("agent-0"))
	runtime.KeepAlive(a)
}

// Maintainer review of #43: a store call that never returns on its own is
// cut at StoreCallTimeout and counted as a failure; the commit deadline is
// enforced without waiting for it, and other agents keep progressing.
func TestAssemblerStuckStoreCall(t *testing.T) {
	small := smallSequence(t, 1)
	store := NewMemoryAgentStore()
	a := NewAssembler(store)
	a.callTimeout = 100 * time.Millisecond
	complete(t, a, "stuck", small, t0)
	store.SetBlock("apply", true)

	start := time.Now()
	var wg sync.WaitGroup
	var commitErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		commitErr = commitAt(a, t0, "stuck", 1)
	}()
	time.Sleep(20 * time.Millisecond) // the commit is in its store call
	other := time.Now()
	complete(t, a, "other", small, t0)
	assert.Less(t, time.Since(other), a.callTimeout, "another agent is not held up by the stuck call")
	wg.Wait()
	require.Error(t, commitErr, "the stuck call ends at its deadline, as a failure")
	assert.Less(t, time.Since(start), 2*time.Second)

	// The retrier is not stuck either: each attempt ends at the deadline.
	start = time.Now()
	retryAt(a, t0.Add(time.Minute))
	assert.Less(t, time.Since(start), time.Duration(RetryBatch+1)*a.callTimeout+time.Second)

	// Past the commit deadline (injected clock) the sequence is invalidated
	// whatever the store does with commits.
	late := t0.Add(CommitDeadline + time.Second)
	retryAt(a, late)
	retryAt(a, late.Add(time.Minute))
	assert.Equal(t, uint64(1), store.RecordOf("stuck").Invalidated)
	assert.Empty(t, store.AppliedOf("stuck"))
	_, _, err := addAt(a, late.Add(2*time.Minute), "stuck", small[0])
	assert.ErrorIs(t, err, ErrInvalidSequence)
}

// Maintainer review of #43: one pass makes at most RetryBatch store calls,
// negative commit results included: the compare-and-apply returns its
// outcome atomically, with no second read.
func TestAssemblerRetryBatchCountsStoreCalls(t *testing.T) {
	store := NewMemoryAgentStore()
	a := NewAssembler(store)
	for i := range 2 * RetryBatch {
		agent := fmt.Sprintf("agent-%d", i)
		complete(t, a, agent, smallSequence(t, 1), t0)
		store.SetFail("apply", true)
		require.Error(t, commitAt(a, t0, agent, 1))
		store.SetFail("apply", false)
		if i%2 == 0 {
			store.SetRecord(agent, AgentRecord{Applied: 1}) // another replica applied it
		} else {
			store.SetRecord(agent, AgentRecord{Invalidated: 1})
		}
	}
	store.Calls("apply")
	store.Calls("record")
	done := retryAt(a, t0.Add(time.Minute)) // past the backoff, within the commit deadline
	assert.Equal(t, RetryBatch, store.Calls("apply"), "one call per job")
	assert.Zero(t, store.Calls("record"), "no second read")
	assert.Zero(t, store.Calls("invalidate"))
	for _, c := range done {
		assert.Equal(t, AgentRecord{Applied: 1}, store.RecordOf(c.Agent), "already applied: acknowledged again")
	}
	_, held, _ := a.Buffered()
	assert.Equal(t, RetryBatch, held, "the rest waits for the next pass")
}

// Maintainer review of #43: with the store down and 1 000 invalidations
// pending, a page costs at most one record read and one retry batch, other
// agents' pages are not held up behind it, and everything is persisted
// once the store returns.
func TestAssemblerRetriesAreBounded(t *testing.T) {
	small := smallSequence(t, 1)
	store := NewMemoryAgentStore()
	a := NewAssembler(store)
	store.SetFail("invalidate", true)
	for i := range MaxPendingInvalidations {
		agent := fmt.Sprintf("invalid-%d", i)
		_, _, err := addAt(a, t0, agent, small[0])
		require.NoError(t, err)
		_, _, err = addAt(a, t0, agent, []byte(strings.Replace(string(small[0]), `"page":1`, `"page":1,"x":1`, 1)))
		require.Error(t, err)
	}
	_, _, pending := a.Buffered()
	require.Equal(t, MaxPendingInvalidations, pending)
	_, _, err := addAt(a, t0, "newcomer", small[0])
	assert.ErrorIs(t, err, ErrBusy, "no new assembly while invalidations wait")

	later := t0.Add(time.Hour) // every backoff has passed
	store.Calls("invalidate")
	store.Calls("record")
	_, _, _ = addAt(a, later, "other", small[0])
	assert.LessOrEqual(t, store.Calls("invalidate"), RetryBatch, "one bounded batch per page")
	assert.LessOrEqual(t, store.Calls("record"), 1)

	// Store calls run outside the lock: a slow store holds a page up by at
	// most its own calls, not by another agent's.
	store.SetDelay(40 * time.Millisecond)
	var wg sync.WaitGroup
	durations := make([]time.Duration, 2)
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			_, _, _ = addAt(a, later.Add(time.Hour), fmt.Sprintf("slow-%d", i), small[0])
			durations[i] = time.Since(start)
		}()
	}
	wg.Wait()
	for _, d := range durations {
		assert.Less(t, d, time.Duration(RetryBatch+1)*40*time.Millisecond+400*time.Millisecond, "bounded by its own calls")
	}
	store.SetDelay(0)

	store.SetFail("invalidate", false)
	when := later.Add(2 * time.Hour)
	passes := 0
	for ; passes < 10*MaxPendingInvalidations/RetryBatch; passes++ {
		if _, _, p := a.Buffered(); p == 0 {
			break
		}
		retryAt(a, when)
		when = when.Add(time.Minute)
	}
	_, _, pending = a.Buffered()
	assert.Zero(t, pending, "recovered once the store returned")
	assert.LessOrEqual(t, passes, 2*MaxPendingInvalidations/RetryBatch+2)
	assert.Equal(t, uint64(1), store.RecordOf("invalid-0").Invalidated)
}

// Maintainer review of #43: who retries a failed commit. The retrier, with
// backoff and jitter, run from each page's pass and from the backend's
// ticker; a resent page answers ErrApplying and attempts nothing itself.
// Past CommitDeadline the sequence is invalidated instead.
func TestAssemblerCommitRetry(t *testing.T) {
	small := smallSequence(t, 1)
	t.Run("RetrierCommitsAfterStoreReturns", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		complete(t, a, "agent", small, t0)
		store.SetFail("apply", true)
		require.Error(t, commitAt(a, t0, "agent", 1))
		store.Calls("apply")
		_, _, err := addAt(a, t0, "agent", small[0])
		assert.ErrorIs(t, err, ErrApplying)
		assert.Zero(t, store.Calls("apply"), "a resent page attempts nothing while backing off")
		store.SetFail("apply", false)
		done := retryAt(a, t0.Add(time.Minute))
		assert.Equal(t, []Committed{{"agent", 1}}, done, "the retrier commits; the backend acknowledges")
		assert.Equal(t, []uint64{1}, store.AppliedOf("agent"))
		_, held, _ := a.Buffered()
		assert.Zero(t, held)
	})
	t.Run("CallerNeverCommits", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		complete(t, a, "agent", small, t0)
		assert.Empty(t, retryAt(a, t0), "the caller's own commit comes first")
		assert.Equal(t, []Committed{{"agent", 1}}, retryAt(a, t0.Add(2*time.Second)))
	})
	t.Run("DeadlineInvalidates", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		complete(t, a, "agent", small, t0)
		store.SetFail("apply", true)
		when := t0
		for when.Before(t0.Add(CommitDeadline + time.Minute)) {
			when = when.Add(time.Minute)
			retryAt(a, when)
		}
		retryAt(a, when.Add(time.Minute)) // persists the invalidation
		assert.Equal(t, uint64(1), store.RecordOf("agent").Invalidated, "given up and protected")
		assert.Empty(t, store.AppliedOf("agent"))
		_, _, err := addAt(a, when.Add(2*time.Minute), "agent", small[0])
		assert.ErrorIs(t, err, ErrInvalidSequence)
		b, held, pending := a.Buffered()
		assert.Zero(t, b+held+pending)
	})
}

// smallSequence is a two-page report of small pages with sequence seq.
func smallSequence(t *testing.T, seq int) [][]byte {
	t.Helper()
	pages := build(t, uint64(seq), []KeyReport{{ID: keyOne, LastUsedAt: "2026-11-03T12:00:00Z"}})
	require.Len(t, pages, 1)
	// Two pages: the same content, renumbered, with the key moved.
	one := strings.Replace(string(pages[0]), `"pages":1`, `"pages":2`, 1)
	two := strings.Replace(strings.Replace(one, `"page":1`, `"page":2`, 1), `"keys":[{"id":"`+keyOne+`","lastUsedAt":"2026-11-03T12:00:00Z"}]`, `"keys":[]`, 1)
	two = replaceApplications(two, `"applications":[]`)
	_, err := Merge([][]byte{[]byte(one), []byte(two)})
	require.NoError(t, err, "a valid two-page sequence")
	return [][]byte{[]byte(one), []byte(two)}
}

func replaceApplications(page, with string) string {
	i := strings.Index(page, `"applications":[`)
	j := strings.Index(page, `],"keys"`)
	return page[:i] + with + page[j+1:]
}

func firstKeyOf(t *testing.T, page []byte) string {
	t.Helper()
	r, err := decodePage(page)
	require.NoError(t, err)
	require.NotEmpty(t, r.Keys)
	return r.Keys[0].ID
}

// gatedStore holds every ApplyIfGreater until release is closed, and can
// answer with a fixed result instead of applying.
type gatedStore struct {
	*MemoryAgentStore
	gate    chan struct{}
	started chan string
	answer  ApplyResult // non-zero: returned as is, nothing applied
}

func newGatedStore() *gatedStore {
	return &gatedStore{MemoryAgentStore: NewMemoryAgentStore(), gate: make(chan struct{}), started: make(chan string, 64)}
}

func (g *gatedStore) ApplyIfGreater(ctx context.Context, agent string, r *Report) (ApplyResult, error) {
	g.started <- agent
	select {
	case <-g.gate:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	if g.answer != 0 {
		return g.answer, nil
	}
	return g.MemoryAgentStore.ApplyIfGreater(ctx, agent, r)
}

// Maintainer review of #43 (reproduced 20 times): a pass claimed eight
// commits at once and ran them one after another, so commits queued behind
// a blocked one were applied after the deadline had invalidated them. Each
// job is now claimed immediately before it runs; only the attempt already
// in flight may finish after the deadline, its pages still counted.
func TestAssemblerQueuedCommitsNeverOutliveDeadline(t *testing.T) {
	for range 20 {
		store := newGatedStore()
		a := NewAssembler(store)
		a.callTimeout = 10 * time.Second
		agents := make([]string, RetryBatch)
		for i := range agents {
			agents[i] = fmt.Sprintf("agent-%d", i)
			complete(t, a, agents[i], smallSequence(t, 1), t0)
		}
		pass := make(chan []Committed)
		go func() { pass <- retryAt(a, t0.Add(time.Minute)) }()
		first := <-store.started // one commit is in its store call, blocked
		bytes, held, _ := a.Buffered()

		late := t0.Add(CommitDeadline + time.Second)
		retryAt(a, late) // invalidates every sequence not in flight, persists the invalidations
		b, h, _ := a.Buffered()
		assert.Equal(t, held-(RetryBatch-1), h, "only the attempt in flight keeps its entry")
		assert.Less(t, b, bytes)
		assert.Positive(t, b, "the attempt in flight keeps its pages counted")

		close(store.gate)
		done := <-pass
		for _, agent := range agents {
			applied := store.AppliedOf(agent)
			if agent == first {
				assert.LessOrEqual(t, len(applied), 1, "started before the deadline: the store's answer decides")
				continue
			}
			assert.Empty(t, applied, "queued behind the blocked commit, invalidated: never applied")
			assert.Equal(t, uint64(1), store.RecordOf(agent).Invalidated)
		}
		assert.LessOrEqual(t, len(done), 1)
		select {
		case agent := <-store.started:
			t.Fatalf("%s started a commit after the deadline", agent)
		default:
		}
	}
}

// Maintainer review of #43: commit attempts in flight, Commit and Retry
// together, are bounded in number and in page bytes; past the bound a
// Commit is refused with ErrBusy and the retrier commits it later.
func TestAssemblerCommitBudget(t *testing.T) {
	small := smallSequence(t, 1)
	t.Run("Concurrency", func(t *testing.T) {
		store := newGatedStore()
		a := NewAssembler(store)
		a.maxCommits = 2
		for i := range 3 {
			complete(t, a, fmt.Sprintf("agent-%d", i), small, t0)
		}
		errs := make(chan error, 2)
		for i := range 2 {
			go func() { errs <- commitAt(a, t0, fmt.Sprintf("agent-%d", i), 1) }()
		}
		<-store.started
		<-store.started
		assert.ErrorIs(t, commitAt(a, t0, "agent-2", 1), ErrBusy, "two attempts in flight: the third waits")
		assert.Empty(t, retryAt(a, t0.Add(time.Minute)), "the retrier respects the same budget")
		close(store.gate)
		require.NoError(t, <-errs)
		require.NoError(t, <-errs)
		assert.Equal(t, []Committed{{"agent-2", 1}}, retryAt(a, t0.Add(time.Minute)))
	})
	t.Run("DecodeBytes", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		complete(t, a, "agent", small, t0)
		b, _, _ := a.Buffered()
		a.maxDecoding = b - 1
		assert.ErrorIs(t, commitAt(a, t0, "agent", 1), ErrBusy, "its pages exceed the decode budget")
		a.maxDecoding = b
		require.NoError(t, commitAt(a, t0, "agent", 1))
	})
	t.Run("InvalidationsStillRun", func(t *testing.T) {
		store := newGatedStore()
		a := NewAssembler(store)
		a.maxCommits = 1
		complete(t, a, "applying", small, t0)
		go func() { _ = commitAt(a, t0, "applying", 1) }()
		<-store.started
		_, _, err := addAt(a, t0, "other", []byte(`{}`)) // a malformed page: nothing held, no marker
		require.Error(t, err)
		complete(t, a, "doomed", small, t0)
		retryAt(a, t0.Add(CommitDeadline+time.Second)) // invalidated and persisted despite the full commit budget
		assert.Equal(t, uint64(1), store.RecordOf("doomed").Invalidated)
		close(store.gate)
	})
}

// Maintainer review of #43: only Applied and AlreadyApplied acknowledge; a
// store answer outside the three outcomes, zero included, fails closed.
func TestAssemblerUnknownApplyResultFailsClosed(t *testing.T) {
	for _, answer := range []ApplyResult{ApplyResult(0xff), ApplyResult(42)} {
		store := newGatedStore()
		close(store.gate)
		store.answer = answer
		a := NewAssembler(store)
		complete(t, a, "agent", smallSequence(t, 1), t0)
		require.ErrorIs(t, commitAt(a, t0, "agent", 1), errUnknownResult)
		assert.Empty(t, retryAt(a, t0.Add(time.Minute)), "nothing acknowledged on a retry either")
		_, held, _ := a.Buffered()
		assert.Equal(t, 1, held, "the sequence stays awaiting commit")
		_, _, err := addAt(a, t0.Add(time.Minute), "agent", smallSequence(t, 2)[0])
		assert.ErrorIs(t, err, ErrApplying, "a higher sequence still waits behind it")
	}
	// A zero answer from a real call is also unknown.
	store := newGatedStore()
	close(store.gate)
	store.answer = 0
	zero := &zeroStore{store.MemoryAgentStore}
	a := NewAssembler(zero)
	complete(t, a, "agent", smallSequence(t, 1), t0)
	require.ErrorIs(t, commitAt(a, t0, "agent", 1), errUnknownResult)
}

type zeroStore struct{ *MemoryAgentStore }

func (zeroStore) ApplyIfGreater(context.Context, string, *Report) (ApplyResult, error) {
	return 0, nil
}

// Maintainer review of #43: completing a sequence moves the pages it
// already holds as exact copies, without copying them again.
func TestAssemblerCompletionMovesPages(t *testing.T) {
	small := smallSequence(t, 1)
	a := NewAssembler(NewMemoryAgentStore())
	_, _, err := addAt(a, t0, "agent", small[0])
	require.NoError(t, err)
	held := a.agents["agent"].pages.byPage[1]
	_, _, err = addAt(a, t0, "agent", small[1])
	require.NoError(t, err)
	e := a.agents["agent"]
	assertOnlyBytes(t, e)
	assert.Same(t, &held[0], &e.raw[0][0], "the same bytes, not a second copy")
}

// fakeClock is a Clock tests advance by hand.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// slowStore advances the clock during each compare-and-apply, as a call
// that takes that long.
type slowStore struct {
	*MemoryAgentStore
	clock *fakeClock
	took  time.Duration
}

func (s *slowStore) ApplyIfGreater(ctx context.Context, agent string, r *Report) (ApplyResult, error) {
	s.clock.Advance(s.took)
	return s.MemoryAgentStore.ApplyIfGreater(ctx, agent, r)
}

// Maintainer review of #43 (reproduced 20 times): a pass used the time it
// was given for every claim, so after a slow first store call a second
// commit was claimed past its real deadline and applied. Each claim reads
// the clock again: only the attempt that started before the deadline is
// applied; the next is invalidated.
func TestAssemblerClaimsAtFreshClock(t *testing.T) {
	for range 20 {
		clock := &fakeClock{now: t0}
		store := &slowStore{MemoryAgentStore: NewMemoryAgentStore(), clock: clock, took: 5 * time.Second}
		a := NewAssembler(store)
		a.clock = clock
		complete(t, a, "first", smallSequence(t, 1), t0)
		complete(t, a, "second", smallSequence(t, 1), t0)

		// One second before the deadline; the first call takes five.
		done := retryAt(a, t0.Add(CommitDeadline-time.Second))
		require.Len(t, done, 1, "only the attempt started before the deadline")
		started := done[0].Agent
		other := map[string]string{"first": "second", "second": "first"}[started]
		assert.Equal(t, []uint64{1}, store.AppliedOf(started))
		assert.Empty(t, store.AppliedOf(other), "claimed after the real deadline: never applied")
		assert.Equal(t, uint64(1), store.RecordOf(other).Invalidated, "invalidated, and persisted in the same pass")
	}
	// The caller's own Commit, then the retrier: the time a call is given is
	// when it started, and the clock's progress counts from there, whatever
	// a store call took in between.
	clock := &fakeClock{now: t0}
	store := &slowStore{MemoryAgentStore: NewMemoryAgentStore(), clock: clock, took: 5 * time.Second}
	store.SetFail("apply", true)
	a := NewAssembler(store)
	a.clock = clock
	complete(t, a, "agent", smallSequence(t, 1), t0)
	require.Error(t, commitAt(a, t0.Add(CommitDeadline-time.Second), "agent", 1), "started before the deadline, failed")
	store.SetFail("apply", false)
	assert.Empty(t, retryAt(a, t0.Add(CommitDeadline+time.Second)), "past the deadline: invalidated, not committed")
	assert.Empty(t, store.AppliedOf("agent"))
}

// slowRecordStore advances the clock during each record read.
type slowRecordStore struct {
	*MemoryAgentStore
	clock *fakeClock
	took  time.Duration
}

func (s *slowRecordStore) Record(ctx context.Context, agent string) (AgentRecord, error) {
	s.clock.Advance(s.took)
	return s.MemoryAgentStore.Record(ctx, agent)
}

// Maintainer review of #43 (reproduced 20 times): Add took the time once,
// so a record read that took long did not count, and its retry pass
// applied a sequence past its deadline. The clock is the only source of
// time: Add, Commit and Retry take none, and each decision reads it.
func TestAssemblerAddRecordReadCounts(t *testing.T) {
	for range 20 {
		clock := &fakeClock{now: t0}
		store := &slowRecordStore{MemoryAgentStore: NewMemoryAgentStore(), clock: clock}
		a := NewAssemblerWithClock(store, clock)
		complete(t, a, "waiting", smallSequence(t, 1), t0) // the caller never commits it

		clock.Set(t0.Add(CommitDeadline - time.Second)) // a new page arrives one second before the deadline
		store.took = 5 * time.Second                    // and its record read takes five
		_, done, err := a.Add("other", smallSequence(t, 1)[0])
		require.NoError(t, err)
		assert.Empty(t, done, "past its deadline when the pass runs: not committed")
		assert.Empty(t, store.AppliedOf("waiting"))
		assert.Equal(t, uint64(1), store.RecordOf("waiting").Invalidated, "invalidated and persisted")
	}
}
