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
		got, err := a.Add(agent, p, now)
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
			r, err := a.Add("agent", pages[i], now)
			require.NoError(t, err)
			assert.Nil(t, r)
		}
		r, err := a.Add("agent", pages[0], now)
		require.NoError(t, err)
		require.NotNil(t, r)
		assert.Len(t, r.Keys, 6000)
		require.NoError(t, a.Commit("agent", 1), "commit, then acknowledge")
		assert.Equal(t, []uint64{1}, store.Applied["agent"])
		b, open := a.Buffered()
		assert.Zero(t, b)
		assert.Zero(t, open)
		_, err = a.Add("agent", pages[0], now)
		assert.ErrorIs(t, err, ErrAlreadyApplied, "acknowledged again, nothing applied")
	})
	t.Run("IdenticalRepeatIgnored", func(t *testing.T) {
		a := NewAssembler(NewMemoryAgentStore())
		_, err := a.Add("agent", seqs[1][0], now)
		require.NoError(t, err)
		_, err = a.Add("agent", seqs[1][0], now)
		assert.NoError(t, err)
		complete(t, a, "agent", seqs[1][1:], now)
	})
	t.Run("LowerSequenceNeverReplacesNewer", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		_, err := a.Add("agent", seqs[2][0], now)
		require.NoError(t, err)
		_, err = a.Add("agent", seqs[1][0], now)
		assert.ErrorIs(t, err, ErrStaleSequence)
		r := complete(t, a, "agent", seqs[2][1:], now)
		assert.Equal(t, uint64(2), r.ReportSequence)
		require.NoError(t, a.Commit("agent", 2))
		_, err = a.Add("agent", seqs[1][1], now)
		assert.ErrorIs(t, err, ErrAlreadyApplied, "a sequence below the applied one is acknowledged again")
	})
	// The maintainer's reproduction: sequence 2 complete, not yet committed.
	t.Run("CompleteNotYetPersistedBlocksOlder", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		complete(t, a, "agent", seqs[2], now)
		_, err := a.Add("agent", seqs[1][0], now)
		assert.ErrorIs(t, err, ErrStaleSequence, "sequence 1 is refused while 2 is applying")
		_, err = a.Add("agent", seqs[3][0], now)
		assert.ErrorIs(t, err, ErrApplying, "a newer one waits for the commit")
		_, open := a.Buffered()
		assert.Equal(t, 1, open, "the applying marker counts toward the bound")
		require.NoError(t, a.Commit("agent", 2))
		_, err = a.Add("agent", seqs[1][0], now)
		assert.ErrorIs(t, err, ErrAlreadyApplied)
		assert.Equal(t, []uint64{2}, store.Applied["agent"])
	})
	t.Run("ApplyFailureNoAckStillProtected", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		complete(t, a, "agent", seqs[2], now)
		store.SetFail("apply", true)
		require.Error(t, a.Commit("agent", 2), "no acknowledgement")
		assert.Empty(t, store.Applied["agent"])
		_, err := a.Add("agent", seqs[1][0], now)
		assert.ErrorIs(t, err, ErrStaleSequence, "the marker stays in memory: fail closed")
		store.SetFail("apply", false)
		require.NoError(t, a.Commit("agent", 2), "the commit can be retried")
		assert.Equal(t, []uint64{2}, store.Applied["agent"])
		_, open := a.Buffered()
		assert.Zero(t, open, "released only after the commit")
	})
	t.Run("RecordFailureRefuses", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		store.SetFail("record", true)
		_, err := a.Add("agent", seqs[1][0], now)
		require.Error(t, err)
		_, open := a.Buffered()
		assert.Zero(t, open, "nothing assembled without the agent's record")
		store.SetFail("record", false)
		complete(t, a, "agent", seqs[1], now)
	})
	t.Run("InvalidationNotPersistedStaysInMemory", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		_, err := a.Add("agent", seqs[1][0], now)
		require.NoError(t, err)
		store.SetFail("invalidate", true)
		_, err = a.Add("agent", []byte(strings.Replace(string(seqs[1][1]), `"page":`, `"x":1,"page":`, 1)), now)
		require.Error(t, err)
		assert.Zero(t, store.RecordOf("agent").Invalidated, "the store did not record it")
		b, open := a.Buffered()
		assert.Zero(t, b, "the pages are freed")
		assert.Equal(t, 1, open, "the marker stays, bounded")
		for _, p := range seqs[1] {
			_, err = a.Add("agent", p, now)
			assert.ErrorIs(t, err, ErrInvalidSequence, "still refused from memory")
		}
		_, err = a.Add("agent", seqs[2][0], now)
		assert.ErrorIs(t, err, ErrBusy, "a newer sequence waits until the invalidation is durable")
		store.SetFail("invalidate", false)
		_, err = a.Add("agent", seqs[1][0], now)
		assert.ErrorIs(t, err, ErrInvalidSequence, "now refused from the store")
		assert.Equal(t, uint64(1), store.RecordOf("agent").Invalidated)
		_, open = a.Buffered()
		assert.Zero(t, open, "released once persisted")
		complete(t, a, "agent", seqs[2], now)
	})
	t.Run("NewerSequenceDiscardsIncomplete", func(t *testing.T) {
		a := NewAssembler(NewMemoryAgentStore())
		_, err := a.Add("agent", seqs[1][0], now)
		require.NoError(t, err)
		_, err = a.Add("agent", seqs[2][0], now)
		require.NoError(t, err)
		b, open := a.Buffered()
		assert.Equal(t, len(seqs[2][0]), b, "only sequence 2 is held")
		assert.Equal(t, 1, open)
		_, err = a.Add("agent", seqs[1][1], now)
		assert.ErrorIs(t, err, ErrStaleSequence)
	})
	t.Run("ConflictInvalidatesUntilHigherSequence", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		_, err := a.Add("agent", seqs[1][0], now)
		require.NoError(t, err)
		other := []byte(strings.Replace(string(seqs[1][0]), `"keys":[`, `"keys":[{"id":"0191c5e2-8a1b-7d2e-b0c4-ffffffffffff","lastUsedAt":"2026-11-03T12:00:00Z"},`, 1))
		_, err = a.Add("agent", other, now)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrInvalidSequence)
		for _, p := range seqs[1] {
			_, err = a.Add("agent", p, now)
			assert.ErrorIs(t, err, ErrInvalidSequence, "a replay never restarts it")
		}
		b, open := a.Buffered()
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
				_, err := a.Add("agent", seqs[1][0], now)
				require.NoError(t, err)
				_, err = a.Add("agent", bad, now)
				require.Error(t, err)
				_, err = a.Add("agent", seqs[1][1], now)
				assert.ErrorIs(t, err, ErrInvalidSequence)
			})
		}
	})
	t.Run("ExpiryInvalidates", func(t *testing.T) {
		a := NewAssembler(NewMemoryAgentStore())
		_, err := a.Add("agent", seqs[1][0], now)
		require.NoError(t, err)
		_, err = a.Add("agent", seqs[1][1], now.Add(AssemblyTTL+time.Second))
		assert.ErrorIs(t, err, ErrInvalidSequence)
		b, open := a.Buffered()
		assert.Zero(t, b)
		assert.Zero(t, open)
	})
	t.Run("AssemblyCountBoundShedsOldest", func(t *testing.T) {
		small := smallSequence(t, 1)
		a := NewAssembler(NewMemoryAgentStore())
		for i := range MaxAssemblies + 50 {
			_, err := a.Add(fmt.Sprintf("agent-%d", i), small[0], now.Add(time.Duration(i)*time.Millisecond))
			require.NoError(t, err)
			_, open := a.Buffered()
			require.LessOrEqual(t, open, MaxAssemblies)
		}
		_, err := a.Add("agent-0", small[1], now.Add(time.Second))
		assert.ErrorIs(t, err, ErrInvalidSequence, "the oldest assembly was discarded and stays invalid")
		_, err = a.Add(fmt.Sprintf("agent-%d", MaxAssemblies+49), small[1], now.Add(time.Second))
		assert.NoError(t, err, "the newest is kept")
	})
	t.Run("BoundFullOfProtectedEntriesRefusesNew", func(t *testing.T) {
		small := smallSequence(t, 1)
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		for i := range MaxAssemblies {
			complete(t, a, fmt.Sprintf("applying-%d", i), small, now)
		}
		_, err := a.Add("newcomer", small[0], now)
		assert.ErrorIs(t, err, ErrBusy, "applying markers are never shed")
		require.NoError(t, a.Commit("applying-0", 1))
		_, err = a.Add("newcomer", small[0], now)
		assert.NoError(t, err)
	})
	t.Run("ByteBoundShedsOldest", func(t *testing.T) {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		// Pages stand in for buffered sizes; shed only reads the sizes.
		for i := range 600 {
			a.agents[fmt.Sprintf("agent-%d", i)] = &entry{seq: 1, started: now.Add(time.Duration(i) * time.Millisecond), pages: &merger{size: MaxPageBytes}}
			a.bytes += MaxPageBytes
			a.shed("")
			b, _ := a.Buffered()
			require.LessOrEqual(t, b, MaxAssemblyBytes)
		}
		assert.Equal(t, uint64(1), store.RecordOf("agent-0").Invalidated, "the oldest went first")
		assert.NotContains(t, a.agents, "agent-0")
		assert.Contains(t, a.agents, "agent-599")
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
			require.NoError(t, a.Commit(agent, 1))
		}
		for i := range MaxAssemblies + 1 {
			agent := fmt.Sprintf("invalid-%d", i)
			_, err := a.Add(agent, small[0], now)
			require.NoError(t, err)
			_, err = a.Add(agent, []byte(strings.Replace(string(small[0]), `"page":1`, `"page":1,"x":1`, 1)), now)
			require.Error(t, err)
		}
		for i := range MaxAssemblies - 1 {
			_, err := a.Add(fmt.Sprintf("expired-%d", i), small[0], now)
			require.NoError(t, err)
		}
		_, err := a.Add("trigger", small[0], now.Add(AssemblyTTL+time.Second))
		require.NoError(t, err)
		b, open := a.Buffered()
		assert.Equal(t, 1, open, "only the open assembly is held")
		assert.Equal(t, len(small[0]), b)
		assert.Equal(t, AgentRecord{Applied: 1}, store.RecordOf("done-0"))
		assert.Equal(t, AgentRecord{Invalidated: 1}, store.RecordOf("invalid-0"))
		assert.Equal(t, AgentRecord{Invalidated: 1}, store.RecordOf("expired-0"), "expiry is recorded in the store")
		_, err = a.Add("done-0", small[0], now.Add(AssemblyTTL+time.Second))
		assert.ErrorIs(t, err, ErrAlreadyApplied)
		_, err = a.Add("invalid-0", small[1], now.Add(AssemblyTTL+time.Second))
		assert.ErrorIs(t, err, ErrInvalidSequence)
	})
}

// Maintainer review of #43: goroutines racing two sequences of one agent,
// completing and committing in any order, never apply them out of order.
// Run with -race.
func TestAssemblerConcurrentSequences(t *testing.T) {
	one, two := smallSequence(t, 1), smallSequence(t, 2)
	for range 200 {
		store := NewMemoryAgentStore()
		a := NewAssembler(store)
		var wg sync.WaitGroup
		for _, pages := range [][][]byte{one, two} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var done *Report
				for _, p := range pages {
					if r, err := a.Add("agent", p, t0); err == nil && r != nil {
						done = r
					}
				}
				if done != nil {
					_ = a.Commit("agent", done.ReportSequence)
				}
			}()
		}
		wg.Wait()
		applied := store.Applied["agent"]
		for i := 1; i < len(applied); i++ {
			require.Greater(t, applied[i], applied[i-1], "applied in order: %v", applied)
		}
		if len(applied) > 0 && applied[len(applied)-1] == 2 {
			_, err := a.Add("agent", one[0], t0)
			require.ErrorIs(t, err, ErrAlreadyApplied, "1 is never applied after 2")
		}
	}
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
