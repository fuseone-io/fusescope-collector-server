// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package report

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
)

// sequences builds reports 1..n of one monitor, each of several pages.
func sequences(t *testing.T, n int) [][][]byte {
	t.Helper()
	m := NewMonitor(Config{SessionsPerBucket: 10, PendingKeys: 10_000})
	snap := snapshot(t, 1)
	m.SetPolicy(snap)
	for i := range 6000 {
		m.Ingest(snap.Application(appA), keyID(i), &Batch{}, t0)
	}
	out := make([][][]byte, n+1)
	for seq := 1; seq <= n; seq++ {
		pages, err := m.Build(uint64(seq), t0, ingestpolicy.Status{Version: 1})
		require.NoError(t, err)
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
		got, _, err := a.Add(agent, p, now)
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
			r, _, err := a.Add("agent", pages[i], now)
			require.NoError(t, err)
			assert.Nil(t, r)
		}
		r, _, err := a.Add("agent", pages[0], now)
		require.NoError(t, err)
		require.NotNil(t, r)
		assert.Len(t, r.Keys, 6000)
		require.NoError(t, a.Commit("agent", 1, now), "commit, then acknowledge")
		assert.Equal(t, []uint64{1}, store.AppliedOf("agent"))
		b, open, _ := a.Buffered()
		assert.Zero(t, b)
		assert.Zero(t, open)
		_, _, err = a.Add("agent", pages[0], now)
		assert.ErrorIs(t, err, ErrAlreadyApplied, "acknowledged again, nothing applied")
	})
	t.Run("IdenticalRepeatIgnored", func(t *testing.T) {
		a := NewAssembler(NewMemoryAgentStore())
		_, _, err := a.Add("agent", seqs[1][0], now)
		require.NoError(t, err)
		_, _, err = a.Add("agent", seqs[1][0], now)
		assert.NoError(t, err)
		complete(t, a, "agent", seqs[1][1:], now)
	})
	t.Run("LowerSequenceNeverReplacesNewer", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		_, _, err := a.Add("agent", seqs[2][0], now)
		require.NoError(t, err)
		_, _, err = a.Add("agent", seqs[1][0], now)
		assert.ErrorIs(t, err, ErrStaleSequence)
		r := complete(t, a, "agent", seqs[2][1:], now)
		assert.Equal(t, uint64(2), r.ReportSequence)
		require.NoError(t, a.Commit("agent", 2, now))
		_, _, err = a.Add("agent", seqs[1][1], now)
		assert.ErrorIs(t, err, ErrAlreadyApplied, "a sequence below the applied one is acknowledged again")
	})
	// The maintainer's reproduction: sequence 2 complete, not yet committed.
	t.Run("CompleteNotYetPersistedBlocksOlder", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		complete(t, a, "agent", seqs[2], now)
		_, _, err := a.Add("agent", seqs[1][0], now)
		assert.ErrorIs(t, err, ErrStaleSequence, "sequence 1 is refused while 2 is applying")
		_, _, err = a.Add("agent", seqs[3][0], now)
		assert.ErrorIs(t, err, ErrApplying, "a newer one waits for the commit")
		_, open, _ := a.Buffered()
		assert.Equal(t, 1, open, "the applying marker counts toward the bound")
		require.NoError(t, a.Commit("agent", 2, now))
		_, _, err = a.Add("agent", seqs[1][0], now)
		assert.ErrorIs(t, err, ErrAlreadyApplied)
		assert.Equal(t, []uint64{2}, store.AppliedOf("agent"))
	})
	t.Run("ApplyFailureNoAckStillProtected", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		complete(t, a, "agent", seqs[2], now)
		store.SetFail("apply", true)
		require.Error(t, a.Commit("agent", 2, now), "no acknowledgement")
		assert.Empty(t, store.AppliedOf("agent"))
		_, _, err := a.Add("agent", seqs[1][0], now)
		assert.ErrorIs(t, err, ErrStaleSequence, "the marker stays in memory: fail closed")
		store.SetFail("apply", false)
		require.NoError(t, a.Commit("agent", 2, now), "the commit can be retried")
		assert.Equal(t, []uint64{2}, store.AppliedOf("agent"))
		_, open, _ := a.Buffered()
		assert.Zero(t, open, "released only after the commit")
	})
	t.Run("RecordFailureRefuses", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		store.SetFail("record", true)
		_, _, err := a.Add("agent", seqs[1][0], now)
		require.Error(t, err)
		_, open, _ := a.Buffered()
		assert.Zero(t, open, "nothing assembled without the agent's record")
		store.SetFail("record", false)
		complete(t, a, "agent", seqs[1], now)
	})
	t.Run("InvalidationNotPersistedStaysInMemory", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		_, _, err := a.Add("agent", seqs[1][0], now)
		require.NoError(t, err)
		store.SetFail("invalidate", true)
		_, _, err = a.Add("agent", []byte(strings.Replace(string(seqs[1][1]), `"page":`, `"x":1,"page":`, 1)), now)
		require.Error(t, err)
		assert.Zero(t, store.RecordOf("agent").Invalidated, "the store did not record it")
		b, held, pending := a.Buffered()
		assert.Zero(t, b, "the pages are freed")
		assert.Zero(t, held)
		assert.Equal(t, 1, pending, "the marker stays, bounded")
		for _, p := range seqs[1] {
			_, _, err = a.Add("agent", p, now)
			assert.ErrorIs(t, err, ErrInvalidSequence, "still refused from memory")
		}
		_, _, err = a.Add("agent", seqs[2][0], now)
		assert.ErrorIs(t, err, ErrBusy, "a newer sequence waits until the invalidation is durable")
		store.SetFail("invalidate", false)
		later := now.Add(time.Minute) // past the backoff
		_, _, err = a.Add("agent", seqs[1][0], later)
		assert.ErrorIs(t, err, ErrInvalidSequence)
		assert.Equal(t, uint64(1), store.RecordOf("agent").Invalidated, "the retry pass persisted it")
		_, _, pending = a.Buffered()
		assert.Zero(t, pending, "released once persisted")
		complete(t, a, "agent", seqs[2], later)
	})
	t.Run("NewerSequenceDiscardsIncomplete", func(t *testing.T) {
		a := NewAssembler(NewMemoryAgentStore())
		_, _, err := a.Add("agent", seqs[1][0], now)
		require.NoError(t, err)
		_, _, err = a.Add("agent", seqs[2][0], now)
		require.NoError(t, err)
		b, open, _ := a.Buffered()
		assert.Equal(t, len(seqs[2][0]), b, "only sequence 2 is held")
		assert.Equal(t, 1, open)
		_, _, err = a.Add("agent", seqs[1][1], now)
		assert.ErrorIs(t, err, ErrStaleSequence)
	})
	t.Run("ConflictInvalidatesUntilHigherSequence", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		_, _, err := a.Add("agent", seqs[1][0], now)
		require.NoError(t, err)
		other := []byte(strings.Replace(string(seqs[1][0]), `"keys":[`, `"keys":[{"id":"0191c5e2-8a1b-7d2e-b0c4-ffffffffffff","lastUsedAt":"2026-11-03T12:00:00Z"},`, 1))
		_, _, err = a.Add("agent", other, now)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrInvalidSequence)
		for _, p := range seqs[1] {
			_, _, err = a.Add("agent", p, now)
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
				_, _, err := a.Add("agent", seqs[1][0], now)
				require.NoError(t, err)
				_, _, err = a.Add("agent", bad, now)
				require.Error(t, err)
				_, _, err = a.Add("agent", seqs[1][1], now)
				assert.ErrorIs(t, err, ErrInvalidSequence)
			})
		}
	})
	t.Run("ExpiryInvalidates", func(t *testing.T) {
		a := NewAssembler(NewMemoryAgentStore())
		_, _, err := a.Add("agent", seqs[1][0], now)
		require.NoError(t, err)
		_, _, err = a.Add("agent", seqs[1][1], now.Add(AssemblyTTL+time.Second))
		assert.ErrorIs(t, err, ErrInvalidSequence)
		b, open, _ := a.Buffered()
		assert.Zero(t, b)
		assert.Zero(t, open)
	})
	t.Run("AssemblyCountBoundShedsOldest", func(t *testing.T) {
		small := smallSequence(t, 1)
		a := NewAssembler(NewMemoryAgentStore())
		for i := range MaxAssemblies + 50 {
			_, _, err := a.Add(fmt.Sprintf("agent-%d", i), small[0], now.Add(time.Duration(i)*time.Millisecond))
			require.NoError(t, err)
			_, open, _ := a.Buffered()
			require.LessOrEqual(t, open, MaxAssemblies)
		}
		_, _, err := a.Add("agent-0", small[1], now.Add(time.Second))
		assert.ErrorIs(t, err, ErrInvalidSequence, "the oldest assembly was discarded and stays invalid")
		_, _, err = a.Add(fmt.Sprintf("agent-%d", MaxAssemblies+49), small[1], now.Add(time.Second))
		assert.NoError(t, err, "the newest is kept")
	})
	t.Run("BoundFullOfProtectedEntriesRefusesNew", func(t *testing.T) {
		small := smallSequence(t, 1)
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		for i := range MaxAssemblies {
			complete(t, a, fmt.Sprintf("applying-%d", i), small, now)
		}
		_, _, err := a.Add("newcomer", small[0], now)
		assert.ErrorIs(t, err, ErrBusy, "applying markers are never shed")
		require.NoError(t, a.Commit("applying-0", 1, now))
		_, _, err = a.Add("newcomer", small[0], now)
		assert.NoError(t, err)
	})
	t.Run("ByteBoundShedsOldest", func(t *testing.T) {
		small := smallSequence(t, 1)
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		a.maxBytes = 10 * len(small[0])
		for i := range 30 {
			_, _, err := a.Add(fmt.Sprintf("agent-%d", i), small[0], now.Add(time.Duration(i)*time.Millisecond))
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
			require.NoError(t, a.Commit(agent, 1, now))
		}
		for i := range MaxAssemblies + 1 {
			agent := fmt.Sprintf("invalid-%d", i)
			_, _, err := a.Add(agent, small[0], now)
			require.NoError(t, err)
			_, _, err = a.Add(agent, []byte(strings.Replace(string(small[0]), `"page":1`, `"page":1,"x":1`, 1)), now)
			require.Error(t, err)
		}
		for i := range MaxAssemblies - 1 {
			_, _, err := a.Add(fmt.Sprintf("expired-%d", i), small[0], now)
			require.NoError(t, err)
		}
		_, _, err := a.Add("trigger", small[0], now.Add(AssemblyTTL+time.Second))
		require.NoError(t, err)
		for range MaxAssemblies {
			if _, _, p := a.Buffered(); p == 0 {
				break
			}
			a.Retry(now.Add(AssemblyTTL + time.Second))
		}
		b, held, pending := a.Buffered()
		assert.Equal(t, 1, held, "only the open assembly is held")
		assert.Zero(t, pending)
		assert.Equal(t, len(small[0]), b)
		assert.Equal(t, AgentRecord{Applied: 1}, store.RecordOf("done-0"))
		assert.Equal(t, AgentRecord{Invalidated: 1}, store.RecordOf("invalid-0"))
		assert.Equal(t, AgentRecord{Invalidated: 1}, store.RecordOf("expired-0"), "expiry is recorded in the store")
		_, _, err = a.Add("done-0", small[0], now.Add(AssemblyTTL+time.Second))
		assert.ErrorIs(t, err, ErrAlreadyApplied)
		_, _, err = a.Add("invalid-0", small[1], now.Add(AssemblyTTL+time.Second))
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
				errs[i] = c.a.Commit("agent", c.seq, t0)
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
		require.Error(t, a.Commit(agent, r.ReportSequence, t0), "the store is down")
	}
	b, held, _ := a.Buffered()
	assert.Equal(t, 3*(len(page[0])+len(page[1])), b, "the pages awaiting commit are counted")
	assert.Equal(t, 3, held)
	_, _, err := a.Add("newcomer", page[0], t0)
	assert.ErrorIs(t, err, ErrBusy, "no room: a protected sequence is never shed")
	b, _, _ = a.Buffered()
	assert.LessOrEqual(t, b, a.maxBytes)
	for _, e := range a.agents {
		assert.NotNil(t, e.pages, "applying entries hold their counted pages, not a decoded report")
	}
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
		_, _, err := a.Add(agent, small[0], t0)
		require.NoError(t, err)
		_, _, err = a.Add(agent, []byte(strings.Replace(string(small[0]), `"page":1`, `"page":1,"x":1`, 1)), t0)
		require.Error(t, err)
	}
	_, _, pending := a.Buffered()
	require.Equal(t, MaxPendingInvalidations, pending)
	_, _, err := a.Add("newcomer", small[0], t0)
	assert.ErrorIs(t, err, ErrBusy, "no new assembly while invalidations wait")

	later := t0.Add(time.Hour) // every backoff has passed
	store.Calls("invalidate")
	store.Calls("record")
	_, _, _ = a.Add("other", small[0], later)
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
			_, _, _ = a.Add(fmt.Sprintf("slow-%d", i), small[0], later.Add(time.Hour))
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
		a.Retry(when)
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
		require.Error(t, a.Commit("agent", 1, t0))
		store.Calls("apply")
		_, _, err := a.Add("agent", small[0], t0)
		assert.ErrorIs(t, err, ErrApplying)
		assert.Zero(t, store.Calls("apply"), "a resent page attempts nothing while backing off")
		store.SetFail("apply", false)
		done := a.Retry(t0.Add(time.Minute))
		assert.Equal(t, []Committed{{"agent", 1}}, done, "the retrier commits; the backend acknowledges")
		assert.Equal(t, []uint64{1}, store.AppliedOf("agent"))
		_, held, _ := a.Buffered()
		assert.Zero(t, held)
	})
	t.Run("CallerNeverCommits", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		complete(t, a, "agent", small, t0)
		assert.Empty(t, a.Retry(t0), "the caller's own commit comes first")
		assert.Equal(t, []Committed{{"agent", 1}}, a.Retry(t0.Add(2*time.Second)))
	})
	t.Run("DeadlineInvalidates", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		complete(t, a, "agent", small, t0)
		store.SetFail("apply", true)
		when := t0
		for when.Before(t0.Add(CommitDeadline + time.Minute)) {
			when = when.Add(time.Minute)
			a.Retry(when)
		}
		a.Retry(when.Add(time.Minute)) // persists the invalidation
		assert.Equal(t, uint64(1), store.RecordOf("agent").Invalidated, "given up and protected")
		assert.Empty(t, store.AppliedOf("agent"))
		_, _, err := a.Add("agent", small[0], when.Add(2*time.Minute))
		assert.ErrorIs(t, err, ErrInvalidSequence)
		b, held, pending := a.Buffered()
		assert.Zero(t, b+held+pending)
	})
}

// smallSequence is a two-page report of small pages with sequence seq.
func smallSequence(t *testing.T, seq int) [][]byte {
	t.Helper()
	m, snap := newTestMonitor(t, 1)
	m.Ingest(snap.Application(appA), keyOne, &Batch{}, t0)
	pages, err := m.Build(uint64(seq), t0, ingestpolicy.Status{Version: 1})
	require.NoError(t, err)
	require.Len(t, pages, 1)
	// Two pages: the same content, renumbered, with the key moved.
	one := strings.Replace(string(pages[0]), `"pages":1`, `"pages":2`, 1)
	two := strings.Replace(strings.Replace(one, `"page":1`, `"page":2`, 1), `"keys":[{"id":"`+keyOne+`","lastUsedAt":"2026-11-03T12:00:00Z"}]`, `"keys":[]`, 1)
	two = replaceApplications(two, `"applications":[]`)
	_, err = Merge([][]byte{[]byte(one), []byte(two)})
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
