// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package report

import (
	"fmt"
	"strings"
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

// Maintainer review of #43: the backend's assembly of pages, as a model.
func TestAssembler(t *testing.T) {
	seqs := sequences(t, 3)
	now := t0

	t.Run("CompleteInAnyOrder", func(t *testing.T) {
		a := NewAssembler(MemoryAgentStore{})
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
		b, open := a.Buffered()
		assert.Zero(t, b)
		assert.Zero(t, open)
		a.Applied("agent", 1)
		r, err = a.Add("agent", pages[0], now)
		assert.ErrorIs(t, err, ErrAlreadyApplied, "a repeat after it was applied is acknowledged again")
		assert.Nil(t, r)
	})
	t.Run("IdenticalRepeatIgnored", func(t *testing.T) {
		a := NewAssembler(MemoryAgentStore{})
		_, err := a.Add("agent", seqs[1][0], now)
		require.NoError(t, err)
		_, err = a.Add("agent", seqs[1][0], now)
		assert.NoError(t, err)
		var r *Report
		for _, p := range seqs[1][1:] {
			r, err = a.Add("agent", p, now)
			require.NoError(t, err)
		}
		assert.NotNil(t, r)
	})
	t.Run("LowerSequenceNeverReplacesNewer", func(t *testing.T) {
		a := NewAssembler(MemoryAgentStore{})
		_, err := a.Add("agent", seqs[2][0], now)
		require.NoError(t, err)
		_, err = a.Add("agent", seqs[1][0], now)
		assert.ErrorIs(t, err, ErrStaleSequence)
		var r *Report
		for _, p := range seqs[2][1:] {
			r, err = a.Add("agent", p, now)
			require.NoError(t, err)
		}
		require.NotNil(t, r, "sequence 2 still assembles")
		assert.Equal(t, uint64(2), r.ReportSequence)
		a.Applied("agent", 2)
		_, err = a.Add("agent", seqs[1][1], now)
		assert.ErrorIs(t, err, ErrStaleSequence, "nor after it completed")
	})
	t.Run("NewerSequenceDiscardsIncomplete", func(t *testing.T) {
		a := NewAssembler(MemoryAgentStore{})
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
		a := NewAssembler(MemoryAgentStore{})
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
		var r *Report
		for _, p := range seqs[2] {
			r, err = a.Add("agent", p, now)
			require.NoError(t, err)
		}
		assert.NotNil(t, r, "a higher sequence assembles")
	})
	t.Run("InvalidPageInvalidates", func(t *testing.T) {
		for name, bad := range map[string][]byte{
			"OverSize":   append(append([]byte{}, seqs[1][1][:len(seqs[1][1])-1]...), []byte(strings.Repeat(" ", MaxPageBytes)+"}")...),
			"NotStrict":  []byte(strings.Replace(string(seqs[1][1]), `"page":`, `"extra":1,"page":`, 1)),
			"HeaderDiff": []byte(strings.Replace(string(seqs[1][1]), `"generatedAt":"`, `"generatedAt":"1`, 1)),
			"KeyTwice":   []byte(strings.Replace(string(seqs[1][1]), `"keys":[`, `"keys":[{"id":"`+firstKeyOf(t, seqs[1][0])+`","lastUsedAt":"2026-11-03T12:00:00Z"},`, 1)),
		} {
			t.Run(name, func(t *testing.T) {
				a := NewAssembler(MemoryAgentStore{})
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
		a := NewAssembler(MemoryAgentStore{})
		_, err := a.Add("agent", seqs[1][0], now)
		require.NoError(t, err)
		_, err = a.Add("agent", seqs[1][1], now.Add(AssemblyTTL+time.Second))
		assert.ErrorIs(t, err, ErrInvalidSequence)
		b, open := a.Buffered()
		assert.Zero(t, b)
		assert.Zero(t, open)
	})
	t.Run("AssemblyCountBoundShedsOldest", func(t *testing.T) {
		small := smallSequence(t)
		a := NewAssembler(MemoryAgentStore{})
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
	t.Run("ByteBoundShedsOldest", func(t *testing.T) {
		store := MemoryAgentStore{}
		a := NewAssembler(store)
		// Pages stand in for buffered sizes; shed only reads the sizes.
		for i := range 600 {
			a.agents[fmt.Sprintf("agent-%d", i)] = &assembly{seq: 1, started: now.Add(time.Duration(i) * time.Millisecond), pages: &merger{size: MaxPageBytes}}
			a.bytes += MaxPageBytes
			a.shed()
			b, _ := a.Buffered()
			require.LessOrEqual(t, b, MaxAssemblyBytes)
		}
		assert.Equal(t, uint64(1), store["agent-0"].Invalidated, "the oldest went first")
		assert.NotContains(t, a.agents, "agent-0")
		assert.Contains(t, a.agents, "agent-599")
	})
	// Maintainer review of #43: the assembler's memory holds only
	// assemblies with pages; applied and invalidated markers are in the
	// backend's persisted per-agent record.
	t.Run("ManyFinishedAgentsLeaveNoMemory", func(t *testing.T) {
		small := smallSequence(t)
		store := MemoryAgentStore{}
		a := NewAssembler(store)
		for i := range MaxAssemblies + 1 {
			agent := fmt.Sprintf("done-%d", i)
			_, err := a.Add(agent, small[0], now)
			require.NoError(t, err)
			r, err := a.Add(agent, small[1], now)
			require.NoError(t, err)
			require.NotNil(t, r)
			a.Applied(agent, 1)
		}
		for i := range MaxAssemblies + 1 {
			agent := fmt.Sprintf("invalid-%d", i)
			_, err := a.Add(agent, small[0], now)
			require.NoError(t, err)
			_, err = a.Add(agent, []byte(strings.Replace(string(small[0]), `"page":1`, `"page":1,"x":1`, 1)), now)
			require.Error(t, err)
		}
		for i := range MaxAssemblies + 1 {
			_, err := a.Add(fmt.Sprintf("expired-%d", i), small[0], now)
			require.NoError(t, err)
		}
		_, err := a.Add("trigger", small[0], now.Add(AssemblyTTL+time.Second))
		require.NoError(t, err)
		b, open := a.Buffered()
		assert.Equal(t, 1, open, "only the open assembly is held")
		assert.Equal(t, len(small[0]), b)
		assert.Len(t, a.agents, 1)
		assert.Equal(t, AgentRecord{Applied: 1}, store["done-0"])
		assert.Equal(t, AgentRecord{Invalidated: 1}, store["invalid-0"])
		assert.Equal(t, AgentRecord{Invalidated: 1}, store["expired-0"], "expiry is recorded in the store")
		_, err = a.Add("done-0", small[0], now.Add(AssemblyTTL+time.Second))
		assert.ErrorIs(t, err, ErrAlreadyApplied)
		_, err = a.Add("invalid-0", small[1], now.Add(AssemblyTTL+time.Second))
		assert.ErrorIs(t, err, ErrInvalidSequence)
	})
}

// smallSequence is a two-page report of small pages.
func smallSequence(t *testing.T) [][]byte {
	t.Helper()
	m, snap := newTestMonitor(t, 1)
	m.Ingest(snap.Application(appA), keyOne, &Batch{}, t0)
	pages, err := m.Build(1, t0, ingestpolicy.Status{Version: 1})
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
