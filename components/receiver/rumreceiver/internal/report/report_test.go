// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package report

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
)

var t0 = time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)

func TestReportShapeIsStrict(t *testing.T) {
	m, snap := newTestMonitor(t, 2)
	b := batch("s1")
	b.Views, b.Errors, b.Vitals = 1, 2, 3
	b.SetSDK("@fusescope/browser", "0.2.0-beta.1")
	m.Ingest(snap.Application(appA), keyOne, b, t0)
	m.Reject(appA, OriginNotAllowed, "https://staging.example.com", t0)
	m.Reject(appA, PackageNotAllowed, "com.example.debug", t0)
	m.UnknownKey(t0)

	pages, err := m.Build(1874, t0.Add(time.Minute), ingestpolicy.Status{Version: 42})
	require.NoError(t, err)
	require.Len(t, pages, 1)
	data := pages[0]

	// Decoding into the report type with unknown fields refused proves no
	// other field exists (G5).
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var r Report
	require.NoError(t, dec.Decode(&r))
	assert.Equal(t, Report{
		SchemaVersion: 1, ReportSequence: 1874, Page: 1, Pages: 1, GeneratedAt: "2026-11-03T12:01:00Z", WindowSeconds: 300,
		Policy: PolicyReport{Version: 42, Status: "ok"},
		Applications: []ApplicationReport{{
			ID: appA, FirstSeenAt: "2026-11-03T12:00:00Z",
			Counts: Counts{Sessions: 1, Views: 1, Errors: 2, Vitals: 3},
			SDK:    SDKReport{Name: "@fusescope/browser", Version: "0.2.0-beta.1"},
			Rejections: []RejectionReport{
				{Reason: "origin_not_allowed", Count: 1, LastOrigin: "https://staging.example.com", LastSeenAt: "2026-11-03T12:00:00Z"},
				{Reason: "package_not_allowed", Count: 1, LastOrigin: "com.example.debug", LastSeenAt: "2026-11-03T12:00:00Z"},
			},
		}},
		Keys:        []KeyReport{{ID: keyOne, LastUsedAt: "2026-11-03T12:00:00Z"}},
		UnknownKeys: UnknownKeysReport{Count: 1, LastSeenAt: "2026-11-03T12:00:00Z"},
		Dropped:     DroppedReport{},
	}, r)

	var generic map[string]any
	require.NoError(t, json.Unmarshal(data, &generic))
	assert.ElementsMatch(t, []string{"schemaVersion", "reportSequence", "page", "pages", "generatedAt", "windowSeconds", "policy", "applications", "keys", "unknownKeys", "dropped"}, keys(generic))
}

func TestReportPolicyStatus(t *testing.T) {
	testCases := []struct {
		name   string
		status ingestpolicy.Status
		want   PolicyReport
	}{
		{name: "Accepted", status: ingestpolicy.Status{Version: 7}, want: PolicyReport{Version: 7, Status: "ok"}},
		{name: "RejectedWhileHolding", status: ingestpolicy.Status{Version: 7, Rejected: ingestpolicy.CodeLowerVersion}, want: PolicyReport{Version: 7, Status: "rejected", Error: "lower_version"}},
		{name: "NoneHeld", status: ingestpolicy.Status{Rejected: ingestpolicy.CodeSchemaVersion}, want: PolicyReport{Status: "rejected", Error: "schema_version"}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m, _ := newTestMonitor(t, 1)
			pages, err := m.Build(1, t0, tc.status)
			require.NoError(t, err)
			r, err := Merge(pages)
			require.NoError(t, err)
			assert.Equal(t, tc.want, r.Policy)
		})
	}
}

func TestReportSequenceMustIncrease(t *testing.T) {
	m, _ := newTestMonitor(t, 1)
	_, err := m.Build(5, t0, ingestpolicy.Status{})
	require.NoError(t, err)
	_, err = m.Build(5, t0, ingestpolicy.Status{})
	assert.ErrorIs(t, err, ErrSequence)
	_, err = m.Build(4, t0, ingestpolicy.Status{})
	assert.ErrorIs(t, err, ErrSequence)
	_, err = m.Build(6, t0, ingestpolicy.Status{})
	assert.NoError(t, err)
}

func TestReportAck(t *testing.T) {
	type step struct {
		ingest bool          // use keyOne on appA at t0+at
		at     time.Duration //
		build  uint64        // build report with this sequence
		ack    uint64        // then acknowledge this sequence
	}
	testCases := []struct {
		name          string
		steps         []step
		wantFirstSeen bool
		wantKey       string // lastUsedAt still pending, or ""
	}{
		{name: "NotAcked_Pending", steps: []step{{ingest: true}, {build: 1}}, wantFirstSeen: true, wantKey: "2026-11-03T12:00:00Z"},
		{name: "Acked_Cleared", steps: []step{{ingest: true}, {build: 1}, {ack: 1}}},
		{name: "UnknownAck_Ignored", steps: []step{{ingest: true}, {build: 1}, {ack: 9}}, wantFirstSeen: true, wantKey: "2026-11-03T12:00:00Z"},
		{name: "AckBeforeSend_Ignored", steps: []step{{ingest: true}, {ack: 1}, {build: 1}}, wantFirstSeen: true, wantKey: "2026-11-03T12:00:00Z"},
		{name: "RepeatedAck_NoEffect", steps: []step{{ingest: true}, {build: 1}, {ack: 1}, {ingest: true, at: time.Minute}, {ack: 1}}, wantKey: "2026-11-03T12:01:00Z"},
		{name: "ChangedAfterReport_StaysPending", steps: []step{{ingest: true}, {build: 1}, {ingest: true, at: time.Minute}, {ack: 1}}, wantKey: "2026-11-03T12:01:00Z"},
		{name: "OlderReportAcked_NewerValueKept", steps: []step{{ingest: true}, {build: 1}, {ingest: true, at: time.Minute}, {build: 2}, {ack: 1}}, wantKey: "2026-11-03T12:01:00Z"},
		{name: "NewerReportAcked_AllCleared", steps: []step{{ingest: true}, {build: 1}, {ingest: true, at: time.Minute}, {build: 2}, {ack: 2}, {ack: 1}}},
		{name: "OutOfOrderAcks_Cleared", steps: []step{{ingest: true}, {build: 1}, {build: 2}, {ack: 2}, {ack: 1}}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m, snap := newTestMonitor(t, 1)
			for _, s := range tc.steps {
				if s.ingest {
					m.Ingest(snap.Application(appA), keyOne, &Batch{}, t0.Add(s.at))
				}
				if s.build != 0 {
					_, err := m.Build(s.build, t0.Add(2*time.Minute), ingestpolicy.Status{})
					require.NoError(t, err)
				}
				if s.ack != 0 {
					m.Ack(s.ack)
				}
			}
			pages, err := m.Build(100, t0.Add(2*time.Minute), ingestpolicy.Status{})
			require.NoError(t, err)
			r, err := Merge(pages)
			require.NoError(t, err)
			a := appReport(r, appA)
			require.NotNil(t, a)
			assert.Equal(t, tc.wantFirstSeen, a.FirstSeenAt != "")
			if tc.wantKey == "" {
				assert.Empty(t, r.Keys)
			} else {
				assert.Equal(t, []KeyReport{{ID: keyOne, LastUsedAt: tc.wantKey}}, r.Keys)
			}
		})
	}
}

func TestReportOutstandingBounded(t *testing.T) {
	m, snap := newTestMonitor(t, 1)
	m.Ingest(snap.Application(appA), keyOne, &Batch{}, t0)
	for s := uint64(1); s <= maxOutstanding+10; s++ {
		_, err := m.Build(s, t0, ingestpolicy.Status{})
		require.NoError(t, err)
	}
	assert.Len(t, m.sent, maxOutstanding)
	m.Ack(1) // forgotten: ignored, values stay pending
	pages, err := m.Build(1000, t0, ingestpolicy.Status{})
	require.NoError(t, err)
	assert.Contains(t, string(bytes.Join(pages, nil)), keyOne)
	m.Ack(1000)
	pages, err = m.Build(1001, t0, ingestpolicy.Status{})
	require.NoError(t, err)
	assert.NotContains(t, string(bytes.Join(pages, nil)), keyOne)
}

func keyID(i int) string { return fmt.Sprintf("0191c5e2-8a1b-7d2e-b0c4-%012d", i) }

// pagesOf builds report seq and checks the page contract (§10.2-10.4).
func pagesOf(t *testing.T, m *Monitor, seq uint64, now time.Time) (Report, [][]byte) {
	t.Helper()
	pages, err := m.Build(seq, now, ingestpolicy.Status{Version: 1})
	require.NoError(t, err)
	require.NotEmpty(t, pages)
	require.LessOrEqual(t, len(pages), MaxPages)
	for i, p := range pages {
		require.LessOrEqual(t, len(p), MaxPageBytes, "page %d", i+1)
		var r Report
		require.NoError(t, json.Unmarshal(p, &r))
		assert.Equal(t, seq, r.ReportSequence)
		assert.Equal(t, i+1, r.Page)
		assert.Equal(t, len(pages), r.Pages)
	}
	r, err := Merge(pages)
	require.NoError(t, err)
	return r, pages
}

// heavy fills an application with its largest window values.
func heavy(m *Monitor, a *ingestpolicy.Application, key string, at time.Time) {
	b := batch()
	b.SetSDK(strings.Repeat("n", MaxSDKName), strings.Repeat("v", MaxSDKVersion))
	m.Ingest(a, key, b, at)
	for reason := range numReasons {
		observed := ""
		switch reason {
		case OriginNotAllowed:
			observed = "https://" + strings.Repeat("o", 244) + ".com"
		case PackageNotAllowed:
			observed = "com." + strings.Repeat("p", 252)
		}
		m.Reject(a.ID, reason, observed, at)
	}
}

// Maintainer review of #43: one logical report in pages of at most 256 KiB,
// every application and pending key in exactly one page of every report.
func TestReportPages(t *testing.T) {
	t.Run("SmallReport_OnePage", func(t *testing.T) {
		m, snap := newTestMonitor(t, 1)
		m.Ingest(snap.Application(appA), keyOne, &Batch{}, t0)
		_, pages := pagesOf(t, m, 1, t0)
		assert.Len(t, pages, 1)
	})
	t.Run("AllKeysEveryReport_NoneDropped", func(t *testing.T) {
		m := NewMonitor(Config{SessionsPerBucket: 10, PendingKeys: 10_000})
		snap := snapshot(t, 1)
		m.SetPolicy(snap)
		for i := range 10_000 {
			m.Ingest(snap.Application(appA), keyID(i), &Batch{}, t0.Add(time.Duration(i)*time.Millisecond))
		}
		r, pages := pagesOf(t, m, 1, t0.Add(time.Minute))
		assert.Greater(t, len(pages), 1)
		assert.Len(t, r.Keys, 10_000)
		assert.Equal(t, keyID(9999), r.Keys[0].ID, "newest first across pages")
		assert.Zero(t, r.Dropped.Keys, "keys in other pages are not dropped")
		unique := map[string]bool{}
		for _, k := range r.Keys {
			unique[k.ID] = true
		}
		assert.Len(t, unique, 10_000, "each key in exactly one page")
	})
	t.Run("LargeApplicationsNeverPushKeysOut", func(t *testing.T) {
		m, snap := newTestMonitor(t, 1000)
		for i, a := range snap.Applications() {
			heavy(m, a, keyID(i), t0.Add(time.Duration(i)*time.Millisecond))
		}
		r, pages := pagesOf(t, m, 1, t0.Add(time.Second))
		assert.Greater(t, len(pages), 1)
		assert.Len(t, r.Applications, 1000)
		assert.Len(t, r.Keys, 1000)
		assert.Equal(t, snap.Applications()[999].ID, r.Applications[0].ID, "most recently active first")
		assert.Zero(t, r.Dropped.Keys)
	})
	t.Run("WorstCaseWithinMaxPages", func(t *testing.T) {
		m := NewMonitor(Config{SessionsPerBucket: 10, PendingKeys: 10_000})
		snap := snapshot(t, 10_000)
		m.SetPolicy(snap)
		for i, a := range snap.Applications() {
			heavy(m, a, keyID(i), t0.Add(time.Duration(i)*time.Microsecond))
		}
		r, pages := pagesOf(t, m, 1, t0.Add(time.Second))
		t.Logf("worst case: %d pages", len(pages))
		assert.Len(t, r.Applications, 10_000)
		assert.Len(t, r.Keys, 10_000)
	})
	t.Run("OnlyEvictionCountsAsDropped", func(t *testing.T) {
		m := NewMonitor(Config{SessionsPerBucket: 10, PendingKeys: 3})
		snap := snapshot(t, 1)
		m.SetPolicy(snap)
		for i := range 5 {
			m.Ingest(snap.Application(appA), keyID(i), &Batch{}, t0.Add(time.Duration(i)*time.Second))
		}
		r, _ := pagesOf(t, m, 1, t0.Add(time.Minute))
		assert.Len(t, r.Keys, 3)
		assert.Equal(t, uint64(2), r.Dropped.Keys)
	})
	t.Run("MergeRefusesIncompleteOrMixedPages", func(t *testing.T) {
		m := NewMonitor(Config{SessionsPerBucket: 10, PendingKeys: 10_000})
		snap := snapshot(t, 1)
		m.SetPolicy(snap)
		for i := range 10_000 {
			m.Ingest(snap.Application(appA), keyID(i), &Batch{}, t0)
		}
		_, pages := pagesOf(t, m, 1, t0)
		_, err := Merge(pages[1:])
		assert.Error(t, err, "a missing page")
		_, err = Merge(append([][]byte{pages[0]}, pages...))
		assert.NoError(t, err, "a page received twice with identical bytes")
		other, err := m.Build(2, t0, ingestpolicy.Status{Version: 1})
		require.NoError(t, err)
		_, err = Merge(append([][]byte{other[0]}, pages[1:]...))
		assert.Error(t, err, "pages of two sequences")
	})
}

// Maintainer review of #43: Merge is the backend's strict contract; any
// violation invalidates the sequence.
func TestMergeContract(t *testing.T) {
	m := NewMonitor(Config{SessionsPerBucket: 10, PendingKeys: 10_000})
	snap := snapshot(t, 2)
	m.SetPolicy(snap)
	m.Ingest(snap.Application(appA), keyOne, &Batch{}, t0)
	for i := range 6000 {
		m.Ingest(snap.Application(appB), keyID(i), &Batch{}, t0)
	}
	pages, err := m.Build(1, t0, ingestpolicy.Status{Version: 1})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(pages), 2)
	p1, p2 := string(pages[0]), string(pages[1])
	rest := pages[2:]
	with := func(first ...string) [][]byte {
		out := [][]byte{}
		for _, f := range first {
			out = append(out, []byte(f))
		}
		return append(out, rest...)
	}
	sub := func(page, old, new string) string {
		require.Contains(t, page, old)
		return strings.Replace(page, old, new, 1)
	}
	var r1 Report
	require.NoError(t, json.Unmarshal(pages[0], &r1))
	require.NotEmpty(t, r1.Keys)
	firstKey := `{"id":"` + r1.Keys[0].ID + `"`

	testCases := []struct {
		name  string
		pages [][]byte
		ok    bool
	}{
		{name: "Complete", pages: pages, ok: true},
		{name: "OutOfOrder", pages: append(append([][]byte{}, pages[1:]...), pages[0]), ok: true},
		{name: "IdenticalDuplicate", pages: append([][]byte{pages[1]}, pages...), ok: true},
		{name: "NoPage", pages: [][]byte{}},
		{name: "MissingPage", pages: pages[1:]},
		{name: "DifferentDuplicate", pages: append(with(p1, p2), []byte(sub(p2, `"keys":[`, `"keys":[{"id":"0191c5e2-8a1b-7d2e-b0c4-ffffffffffff","lastUsedAt":"2026-11-03T12:00:00Z"},`))), ok: false},
		{name: "PageOverSize", pages: with(p1[:len(p1)-1]+strings.Repeat(" ", MaxPageBytes)+"}", p2)},
		{name: "UnknownField", pages: with(sub(p1, `"page":`, `"extra":1,"page":`), p2)},
		{name: "CaseVariantField", pages: with(sub(p1, `"page":1`, `"Page":1`), p2)},
		{name: "DuplicateField", pages: with(sub(p1, `"page":1`, `"page":1,"page":1`), p2)},
		{name: "TrailingData", pages: with(p1+" {}", p2)},
		{name: "SchemaVersion2", pages: with(sub(p1, `"schemaVersion":1`, `"schemaVersion":2`), p2)},
		{name: "HeaderDiffers", pages: with(p1, sub(p2, `"generatedAt":"`, `"generatedAt":"1`))},
		{name: "PagesDiffer", pages: with(p1, sub(p2, fmt.Sprintf(`"pages":%d`, len(pages)), fmt.Sprintf(`"pages":%d`, len(pages)+1)))},
		{name: "PageOutOfRange", pages: with(sub(p1, `"page":1`, `"page":0`), p2)},
		{name: "OverMaxPages", pages: with(sub(sub(p1, fmt.Sprintf(`"pages":%d`, len(pages)), `"pages":129`), `"page":1`, `"page":1`), p2)},
		{name: "KeyInTwoPages", pages: with(p1, sub(p2, `"keys":[`, `"keys":[`+firstKey+`,"lastUsedAt":"2026-11-03T12:00:00Z"},`))},
		{name: "ApplicationInTwoPages", pages: with(p1, sub(p2, `"applications":[`, `"applications":[{"id":"`+appA+`","counts":{"sessions":0,"views":0,"errors":0,"vitals":0},"sdk":{"name":"","version":""},"rejections":[]}`+map[bool]string{true: "", false: ","}[strings.Contains(p2, `"applications":[]`)]))},
		{name: "MissingRequiredField", pages: with(sub(p1, `"windowSeconds":300,`, ``), p2)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Merge(tc.pages)
			if tc.ok {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

// Maintainer review of #43: under continuous activity, with old keys still
// pending, every key is reported and acknowledged; none is starved.
func TestReportNoStarvationUnderContinuousActivity(t *testing.T) {
	m := NewMonitor(Config{SessionsPerBucket: 10, PendingKeys: 10_000})
	snap := snapshot(t, 200)
	m.SetPolicy(snap)
	now := t0
	used := map[string]bool{}
	for i := range 6000 {
		m.Ingest(snap.Applications()[i%200], keyID(i), &Batch{}, now)
		used[keyID(i)] = true
	}
	reported := map[string]bool{}
	for round := range 20 {
		now = now.Add(time.Minute)
		// Fresh keys and large applications keep arriving every minute.
		for i := range 500 {
			k := keyID(10_000 + round*500 + i)
			heavy(m, snap.Applications()[i%200], k, now)
			used[k] = true
		}
		r, _ := pagesOf(t, m, uint64(round+1), now)
		for _, k := range r.Keys {
			reported[k.ID] = true
		}
		m.Ack(uint64(round + 1))
	}
	for k := range used {
		assert.True(t, reported[k], "key %s was never reported", k)
	}
	r, _ := pagesOf(t, m, 100, now)
	assert.Empty(t, r.Keys, "every use acknowledged, none left pending")
}

// Codex review on #43: client-controlled strings and session maps of an
// application that goes quiet are released after the status window, while
// its pending first-seen time is kept (§10.3 retention, G5, G6).
func TestExpireReleasesWindowState(t *testing.T) {
	for _, viaBuild := range []bool{false, true} {
		t.Run(fmt.Sprintf("viaBuild=%v", viaBuild), func(t *testing.T) {
			m, snap := newTestMonitor(t, 1)
			b := batch("s1")
			b.SetSDK("@fusescope/browser", "0.2.0")
			m.Ingest(snap.Application(appA), keyOne, b, t0)
			m.Reject(appA, OriginNotAllowed, "https://staging.example.com", t0)
			m.UnknownKey(t0)

			later := t0.Add(Window + time.Minute)
			if viaBuild {
				_, err := m.Build(1, later, ingestpolicy.Status{})
				require.NoError(t, err)
			} else {
				m.Expire(later)
			}
			m.mu.Lock()
			defer m.mu.Unlock()
			app := m.applications[appA]
			require.NotNil(t, app)
			for i := range app.buckets {
				assert.False(t, app.buckets[i].used)
				assert.Nil(t, app.buckets[i].sessions)
				for _, r := range app.buckets[i].rejections {
					assert.Empty(t, r.observed)
				}
			}
			assert.Empty(t, app.sdkName)
			assert.Empty(t, app.sdkVersion)
			assert.Equal(t, t0, app.firstSeen, "durable values stay pending")
			for _, u := range m.unknown {
				assert.Zero(t, u.count)
			}
		})
	}
}

func TestExpireKeepsWindow(t *testing.T) {
	m, snap := newTestMonitor(t, 1)
	b := batch("s1")
	b.SetSDK("@fusescope/browser", "0.2.0")
	m.Ingest(snap.Application(appA), keyOne, b, t0)
	m.Expire(t0.Add(4 * time.Minute))
	a := appReport(build(t, m, t0.Add(4*time.Minute)), appA)
	require.NotNil(t, a)
	assert.Equal(t, int64(1), a.Counts.Sessions)
	assert.Equal(t, "@fusescope/browser", a.SDK.Name)
}

func TestParseAck(t *testing.T) {
	testCases := []struct {
		data string
		seq  uint64
		ok   bool
	}{
		{`{"schemaVersion":1,"reportSequence":1874}`, 1874, true},
		{`{"schemaVersion":2,"reportSequence":1874}`, 0, false},
		{`{"reportSequence":1874}`, 0, false},
		{`{"schemaVersion":1}`, 0, false},
		{`{"schemaVersion":1,"reportSequence":-1}`, 0, false},
		{`{"schemaVersion":1,"reportSequence":1,"extra":true}`, 0, false},
		{`{"schemaVersion":1,"reportSequence":1} {}`, 0, false},
		{`not json`, 0, false},
	}
	for _, tc := range testCases {
		t.Run(tc.data, func(t *testing.T) {
			seq, err := ParseAck([]byte(tc.data))
			assert.Equal(t, tc.ok, err == nil, "%v", err)
			assert.Equal(t, tc.seq, seq)
		})
	}
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
