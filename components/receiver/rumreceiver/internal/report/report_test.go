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

	data, err := m.Build(1874, t0.Add(time.Minute), ingestpolicy.Status{Version: 42})
	require.NoError(t, err)

	// Decoding into the report type with unknown fields refused proves no
	// other field exists (G5).
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var r Report
	require.NoError(t, dec.Decode(&r))
	assert.Equal(t, Report{
		SchemaVersion: 1, ReportSequence: 1874, GeneratedAt: "2026-11-03T12:01:00Z", WindowSeconds: 300,
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
	assert.ElementsMatch(t, []string{"schemaVersion", "reportSequence", "generatedAt", "windowSeconds", "policy", "applications", "keys", "unknownKeys", "dropped"}, keys(generic))
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
			data, err := m.Build(1, t0, tc.status)
			require.NoError(t, err)
			var r Report
			require.NoError(t, json.Unmarshal(data, &r))
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
			data, err := m.Build(100, t0.Add(2*time.Minute), ingestpolicy.Status{})
			require.NoError(t, err)
			var r Report
			require.NoError(t, json.Unmarshal(data, &r))
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
	data, err := m.Build(1000, t0, ingestpolicy.Status{})
	require.NoError(t, err)
	assert.Contains(t, string(data), keyOne)
	m.Ack(1000)
	data, err = m.Build(1001, t0, ingestpolicy.Status{})
	require.NoError(t, err)
	assert.NotContains(t, string(data), keyOne)
}

func TestReportBounds(t *testing.T) {
	t.Run("AtMost1000Applications_MostRecentFirst", func(t *testing.T) {
		m, snap := newTestMonitor(t, 1100)
		for i, a := range snap.Applications() {
			m.Reject(a.ID, TooLarge, "", t0.Add(time.Duration(i)*time.Millisecond))
		}
		r := build(t, m, t0.Add(time.Second))
		require.Len(t, r.Applications, MaxApplications)
		assert.Equal(t, snap.Applications()[1099].ID, r.Applications[0].ID)
		assert.Equal(t, uint64(100), r.Dropped.Applications)
	})
	t.Run("KeysWithinCountAndSize_NewestFirst", func(t *testing.T) {
		m := NewMonitor(Config{SessionsPerBucket: 10, PendingKeys: 20_000})
		snap := snapshot(t, 1)
		m.SetPolicy(snap)
		for i := range 10_005 {
			m.Ingest(snap.Application(appA), fmt.Sprintf("0191c5e2-8a1b-7d2e-b0c4-%012d", i), &Batch{}, t0.Add(time.Duration(i)*time.Millisecond))
		}
		data, err := m.Build(1, t0.Add(time.Minute), ingestpolicy.Status{})
		require.NoError(t, err)
		assert.LessOrEqual(t, len(data), MaxReportBytes)
		var r Report
		require.NoError(t, json.Unmarshal(data, &r))
		// 10 000 UUID keys need about 700 KiB: the 256 KiB bound binds first.
		assert.LessOrEqual(t, len(r.Keys), MaxKeys)
		assert.Greater(t, len(r.Keys), 3000)
		assert.Equal(t, "0191c5e2-8a1b-7d2e-b0c4-000000010004", r.Keys[0].ID)
		assert.Equal(t, uint64(10_005-len(r.Keys)), r.Dropped.Keys)
	})
	t.Run("Over256KiB_KeysDroppedFirstAndStayPending", func(t *testing.T) {
		m, snap := newTestMonitor(t, 1000)
		for i, a := range snap.Applications() {
			b := batch()
			b.SetSDK(strings.Repeat("n", MaxSDKName), strings.Repeat("v", MaxSDKVersion))
			m.Ingest(a, fmt.Sprintf("0191c5e2-8a1b-7d2e-b0c4-%012d", i), b, t0.Add(time.Duration(i)*time.Millisecond))
			for reason := range numReasons {
				observed := ""
				if reason == OriginNotAllowed {
					observed = "https://" + strings.Repeat("o", 240) + ".com"
				}
				m.Reject(a.ID, reason, observed, t0.Add(time.Duration(i)*time.Millisecond))
			}
		}
		data, err := m.Build(1, t0.Add(time.Second), ingestpolicy.Status{})
		require.NoError(t, err)
		assert.LessOrEqual(t, len(data), MaxReportBytes)
		var r Report
		require.NoError(t, json.Unmarshal(data, &r))
		assert.Empty(t, r.Keys, "keys go first")
		assert.Less(t, len(r.Applications), 1000, "then the least recently active applications")
		assert.Equal(t, snap.Applications()[999].ID, r.Applications[0].ID)
		assert.Equal(t, uint64(1000-len(r.Applications)), r.Dropped.Applications)
		assert.Equal(t, uint64(1000), r.Dropped.Keys)

		m.Ack(1)
		data, err = m.Build(2, t0.Add(time.Second), ingestpolicy.Status{})
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(data, &r))
		dropped := snap.Applications()[0].ID
		assert.Nil(t, appReport(r, dropped), "still left out")
		m.mu.Lock()
		assert.False(t, m.applications[dropped].firstSeen.IsZero(), "a value left out of an acknowledged report stays pending")
		m.mu.Unlock()
	})
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
