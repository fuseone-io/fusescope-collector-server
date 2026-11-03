// Copyright FuseOne
// SPDX-License-Identifier: MIT
//
// TestMonitorSnapshotWindow, TestMonitorRejections and TestMonitorUsage are
// moved from fuseone-io/fusescope
// pkg/modules/rumapplication/implrumapplication/monitor_test.go at
// 6c8991257 (MIT, see LICENSE-MIT in the module root), read through the
// report instead of Snapshot and TakeUsage; the bound and policy cases are
// new (docs/spec/rum.md §7.1, §10).

package report

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
)

const (
	org    = "0191c5e2-0000-7000-8000-00000000000a"
	appA   = "0191c5e2-7f0a-7c3e-9a51-3c2b1d4e5f60"
	appB   = "0191c5e2-7f0a-7c3e-9a51-3c2b1d4e5f61"
	keyOne = "0191c5e2-8a1b-7d2e-b0c4-000000000001"
)

func appID(i int) string {
	s := strconv.Itoa(i)
	return "0191c5e2-7f0a-7c3e-9a51-" + pad(s, 12)
}

func pad(s string, n int) string {
	for len(s) < n {
		s = "0" + s
	}
	return s
}

// snapshot is a policy with n web applications (appA, appB, then numbered).
func snapshot(t testing.TB, n int) *ingestpolicy.Snapshot {
	t.Helper()
	p := ingestpolicy.Policy{SchemaVersion: 1, Version: 1, IssuedAt: "2026-11-03T12:00:00Z", Keys: []ingestpolicy.Key{}}
	for i := range n {
		id := appID(i)
		switch i {
		case 0:
			id = appA
		case 1:
			id = appB
		}
		p.RUMApplications = append(p.RUMApplications, ingestpolicy.RUMApplication{
			ID: id, OrgID: org, Name: "app", Environment: "production", Platform: ingestpolicy.PlatformWeb,
			AllowedOrigins: []string{}, AllowedPackageNames: []string{},
			PIIPolicy: ingestpolicy.PIIPolicy{QueryAllowlist: []string{}, URLAllowlist: []string{}},
		})
	}
	data, err := json.Marshal(p)
	require.NoError(t, err)
	s := ingestpolicy.NewStore(zap.NewNop(), nil)
	require.NoError(t, s.Apply(data, time.Now()))
	return s.Current()
}

func newTestMonitor(t testing.TB, apps int) (*Monitor, *ingestpolicy.Snapshot) {
	m := NewMonitor(Config{SessionsPerBucket: 10_000, PendingKeys: 10_000})
	snap := snapshot(t, apps)
	m.SetPolicy(snap)
	return m, snap
}

func batch(sessions ...string) *Batch {
	b := &Batch{}
	for _, s := range sessions {
		b.AddSession(s)
	}
	return b
}

var seq uint64

func build(t testing.TB, m *Monitor, now time.Time) Report {
	t.Helper()
	seq++
	data, err := m.Build(seq, now, ingestpolicy.Status{Version: 1})
	require.NoError(t, err)
	var r Report
	require.NoError(t, json.Unmarshal(data, &r))
	return r
}

func appReport(r Report, id string) *ApplicationReport {
	for i := range r.Applications {
		if r.Applications[i].ID == id {
			return &r.Applications[i]
		}
	}
	return nil
}

func TestMonitorSnapshotWindow(t *testing.T) {
	start := time.Date(2026, 10, 21, 12, 0, 30, 0, time.UTC)

	testCases := []struct {
		name     string
		at       time.Duration
		sessions int64
		views    int64
		sdk      bool
	}{
		{name: "SameMinute_AllCounted", at: 0, sessions: 2, views: 3, sdk: true},
		{name: "FourMinutesLater_AllCounted", at: 4 * time.Minute, sessions: 2, views: 3, sdk: true},
		{name: "FiveMinutesLater_FirstBatchExpired", at: 5 * time.Minute, sessions: 1, views: 1, sdk: true},
		{name: "TenMinutesLater_Empty", at: 10 * time.Minute},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m, snap := newTestMonitor(t, 2)
			app := snap.Application(appA)
			first := batch("s1", "s2")
			first.Views = 2
			m.Ingest(app, keyOne, first, start)
			second := batch("s2")
			second.Views = 1
			second.SetSDK("@fusescope/browser", "0.1.0")
			m.Ingest(app, keyOne, second, start.Add(time.Minute))
			m.Ack(seq) // nothing sent yet: ignored

			r := build(t, m, start.Add(tc.at))
			a := appReport(r, appA)
			require.NotNil(t, a, "firstSeenAt keeps it in the report until acknowledged")
			assert.Equal(t, tc.sessions, a.Counts.Sessions)
			assert.Equal(t, tc.views, a.Counts.Views)
			if tc.sdk {
				assert.Equal(t, SDKReport{Name: "@fusescope/browser", Version: "0.1.0"}, a.SDK)
			} else {
				assert.Equal(t, SDKReport{}, a.SDK, "the SDK is a window value")
			}
		})
	}
}

func TestMonitorRejections(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	m, _ := newTestMonitor(t, 2)

	m.Reject(appA, OriginNotAllowed, "https://evil.test", now)
	m.Reject(appA, OriginNotAllowed, "https://other.test", now.Add(time.Minute))
	m.Reject(appA, RateLimited, "https://ignored.test", now.Add(30*time.Second))

	a := appReport(build(t, m, now.Add(2*time.Minute)), appA)
	require.NotNil(t, a)
	assert.Equal(t, []RejectionReport{
		{Reason: "origin_not_allowed", Count: 2, LastOrigin: "https://other.test", LastSeenAt: "2026-10-21T12:01:00Z"},
		{Reason: "rate_limited", Count: 1, LastSeenAt: "2026-10-21T12:00:30Z"},
	}, a.Rejections, "lastOrigin only for the two reasons the setup screen shows")
}

func TestMonitorUsage(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	m, snap := newTestMonitor(t, 2)
	app := snap.Application(appA)

	m.Ingest(app, keyOne, &Batch{}, now)
	m.Ingest(app, keyOne, &Batch{}, now.Add(time.Second))

	r := build(t, m, now.Add(time.Second))
	assert.Equal(t, "2026-10-21T12:00:00Z", appReport(r, appA).FirstSeenAt)
	assert.Equal(t, []KeyReport{{ID: keyOne, LastUsedAt: "2026-10-21T12:00:01Z"}}, r.Keys)

	m.Ack(r.ReportSequence)
	r = build(t, m, now.Add(time.Second))
	assert.Empty(t, appReport(r, appA).FirstSeenAt, "acknowledged: no longer pending")
	assert.Empty(t, r.Keys)

	m.Ingest(app, keyOne, &Batch{}, now.Add(time.Minute))
	r = build(t, m, now.Add(time.Minute))
	assert.Empty(t, appReport(r, appA).FirstSeenAt, "first seen is reported once")
	assert.Equal(t, []KeyReport{{ID: keyOne, LastUsedAt: "2026-10-21T12:01:00Z"}}, r.Keys, "a newer use is pending again")
}

func TestMonitorOnlyPolicyApplications(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	m := NewMonitor(Config{SessionsPerBucket: 10, PendingKeys: 10})
	snap := snapshot(t, 2)
	outside := &ingestpolicy.Application{ID: "0191c5e2-7f0a-7c3e-9a51-ffffffffffff"}

	m.Ingest(snap.Application(appA), keyOne, &Batch{}, now)
	assert.Empty(t, build(t, m, now).Applications, "no monitor without a policy")

	m.SetPolicy(snap)
	m.Ingest(outside, keyOne, &Batch{}, now)
	m.Reject(outside.ID, BadKey, "", now)
	m.Ingest(snap.Application(appA), keyOne, &Batch{}, now)
	m.Reject(appB, TooLarge, "", now)
	r := build(t, m, now)
	assert.Len(t, r.Applications, 2)
	assert.Nil(t, appReport(r, outside.ID))

	m.SetPolicy(snapshot(t, 1))
	r = build(t, m, now)
	assert.Len(t, r.Applications, 1, "a monitor is dropped when its application leaves the policy")
	assert.NotNil(t, appReport(r, appA))
}

func TestMonitorSessionsBounded(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	m := NewMonitor(Config{SessionsPerBucket: 3, PendingKeys: 10})
	snap := snapshot(t, 1)
	m.SetPolicy(snap)

	m.Ingest(snap.Application(appA), keyOne, batch("a", "b", "c", "d", "e"), now)
	m.Ingest(snap.Application(appA), keyOne, batch("a"), now)
	assert.Equal(t, int64(3), appReport(build(t, m, now), appA).Counts.Sessions)
	// The next minute has its own bucket.
	m.Ingest(snap.Application(appA), keyOne, batch("d", "e"), now.Add(time.Minute))
	assert.Equal(t, int64(5), appReport(build(t, m, now.Add(time.Minute)), appA).Counts.Sessions)
}

func TestMonitorPendingKeysBounded(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	m := NewMonitor(Config{SessionsPerBucket: 10, PendingKeys: 2})
	snap := snapshot(t, 1)
	m.SetPolicy(snap)
	for i, id := range []string{"k1", "k2", "k3"} {
		m.Ingest(snap.Application(appA), id, &Batch{}, now.Add(time.Duration(i)*time.Second))
	}
	r := build(t, m, now.Add(3*time.Second))
	assert.Equal(t, []KeyReport{{ID: "k3", LastUsedAt: "2026-10-21T12:00:02Z"}, {ID: "k2", LastUsedAt: "2026-10-21T12:00:01Z"}}, r.Keys, "oldest use evicted, newest first")
	assert.Equal(t, uint64(1), r.Dropped.Keys)
}

func TestMonitorUnknownKeys(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	m, _ := newTestMonitor(t, 1)
	m.UnknownKey(now)
	m.UnknownKey(now.Add(2 * time.Minute))

	r := build(t, m, now.Add(2*time.Minute))
	assert.Equal(t, UnknownKeysReport{Count: 2, LastSeenAt: "2026-10-21T12:02:00Z"}, r.UnknownKeys)
	r = build(t, m, now.Add(5*time.Minute))
	assert.Equal(t, UnknownKeysReport{Count: 1, LastSeenAt: "2026-10-21T12:02:00Z"}, r.UnknownKeys)
	r = build(t, m, now.Add(7*time.Minute))
	assert.Equal(t, UnknownKeysReport{}, r.UnknownKeys)
}

func TestObserved(t *testing.T) {
	testCases := []struct {
		name, origin, want string
	}{
		{"Origin_LowerCased", "HTTPS://Shop.Example.com:8443", "https://shop.example.com:8443"},
		{"Null_Empty", "null", ""},
		{"FileScheme_Empty", "file://x", ""},
		{"WithPath_Empty", "https://a.com/p", ""},
		{"WithQuery_Empty", "https://a.com?x", ""},
		{"WithUser_Empty", "https://u@a.com", ""},
		{"Over256_Empty", "https://" + strings.Repeat("a", 250) + ".com", ""},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { assert.Equal(t, tc.want, ObservedOrigin(tc.origin)) })
	}
	assert.Equal(t, "com.example.shop", ObservedPackage("com.example.shop"))
	assert.Empty(t, ObservedPackage("com.example.shop debug"))
	assert.Empty(t, ObservedPackage(""))
	assert.Empty(t, ObservedPackage("com."+strings.Repeat("a", 253)))
}
