// Copyright FuseOne
// SPDX-License-Identifier: MIT
//
// Moved from fuseone-io/fusescope
// pkg/modules/rumapplication/implrumapplication/monitor.go at 6c8991257
// (MIT, see LICENSE-MIT in the module root). Changed (docs/spec/rum.md §7.1,
// §10, G5, G6): monitors exist only for applications of the current policy;
// sessions are 64-bit hashes; rejections are a fixed array of six reasons
// and keep a validated value only for two of them; unknown keys are one
// global windowed counter; the SDK is windowed; pending key usage is
// capped with oldest-use eviction; pending values are cleared by
// acknowledgement (ack.go) instead of TakeUsage and Restore.

package report // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"

import (
	"sync"
	"time"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
)

const (
	// Window is the status window ([B] rumtypes/status.go:10).
	Window         = 5 * time.Minute
	monitorBuckets = int(Window / time.Minute)
)

// Config bounds the monitor (§7.1).
type Config struct {
	SessionsPerBucket int
	PendingKeys       int
}

// Monitor keeps per-application ingest statistics over the status window,
// and the key and application usage times waiting for acknowledgement.
type Monitor struct {
	cfg Config

	mu           sync.Mutex
	policy       *ingestpolicy.Snapshot
	applications map[string]*applicationMonitor
	unknown      [monitorBuckets]countBucket
	unknownLast  time.Time
	keys         pendingKeys
	droppedKeys  uint64
	lastSequence uint64
	sent         map[uint64]*carried
	sentOrder    []uint64
}

type applicationMonitor struct {
	buckets    [monitorBuckets]monitorBucket
	sdkName    string
	sdkVersion string
	sdkSeen    time.Time
	lastActive time.Time
	// firstSeen is pending until acknowledged; acked stops it being set again.
	firstSeen time.Time
	acked     bool
}

type monitorBucket struct {
	minute     int64
	used       bool
	sessions   map[uint64]struct{}
	views      int64
	errors     int64
	vitals     int64
	rejections [numReasons]rejection
}

type rejection struct {
	count    int64
	observed string
	seen     time.Time
}

type countBucket struct {
	minute int64
	count  int64
}

func NewMonitor(cfg Config) *Monitor {
	return &Monitor{
		cfg:          cfg,
		applications: map[string]*applicationMonitor{},
		keys:         newPendingKeys(cfg.PendingKeys),
		sent:         map[uint64]*carried{},
	}
}

// SetPolicy keeps monitors only for the applications of snap (none for nil).
func (m *Monitor) SetPolicy(snap *ingestpolicy.Snapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.policy = snap
	for id := range m.applications {
		if snap == nil || snap.Application(id) == nil {
			delete(m.applications, id)
		}
	}
}

// Ingest records an accepted request of a key of application.
func (m *Monitor) Ingest(application *ingestpolicy.Application, keyID string, batch *Batch, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	app := m.application(application.ID)
	if app == nil {
		return
	}
	bucket := app.bucket(now)
	for _, session := range batch.Sessions {
		if _, ok := bucket.sessions[session]; ok || len(bucket.sessions) < m.cfg.SessionsPerBucket {
			bucket.sessions[session] = struct{}{}
		}
	}
	bucket.views += batch.Views
	bucket.errors += batch.Errors
	bucket.vitals += batch.Vitals

	if batch.SDKName != "" {
		app.sdkName, app.sdkVersion, app.sdkSeen = batch.SDKName, batch.SDKVersion, now
	}
	app.lastActive = now
	if !app.acked && app.firstSeen.IsZero() {
		app.firstSeen = now
	}
	if m.keys.use(keyID, now) {
		m.droppedKeys++
	}
}

// Reject records a refused request on its application. observed is the
// already validated lastOrigin value, kept only for the two reasons the
// setup screen shows (§10.3).
func (m *Monitor) Reject(applicationID string, reason Reason, observed string, now time.Time) {
	if reason != OriginNotAllowed && reason != PackageNotAllowed {
		observed = ""
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	app := m.application(applicationID)
	if app == nil {
		return
	}
	r := &app.bucket(now).rejections[reason]
	r.count++
	r.observed = observed
	r.seen = now
	app.lastActive = now
}

// UnknownKey counts a key absent from the policy or of a wrong kind, with no
// per-key or per-origin state.
func (m *Monitor) UnknownKey(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	minute := minuteOf(now)
	b := &m.unknown[minute%int64(monitorBuckets)]
	if b.minute != minute {
		*b = countBucket{minute: minute}
	}
	b.count++
	m.unknownLast = now
}

// application returns the monitor of an application of the current policy,
// creating it; nil for any other.
func (m *Monitor) application(id string) *applicationMonitor {
	if app, ok := m.applications[id]; ok {
		return app
	}
	if m.policy == nil || m.policy.Application(id) == nil {
		return nil
	}
	app := &applicationMonitor{}
	m.applications[id] = app
	return app
}

func (app *applicationMonitor) bucket(now time.Time) *monitorBucket {
	minute := minuteOf(now)
	bucket := &app.buckets[minute%int64(monitorBuckets)]
	if !bucket.used || bucket.minute != minute {
		sessions := bucket.sessions
		if sessions == nil {
			sessions = map[uint64]struct{}{}
		}
		clear(sessions)
		*bucket = monitorBucket{minute: minute, used: true, sessions: sessions}
	}
	return bucket
}

func minuteOf(t time.Time) int64 { return t.Unix() / 60 }

// oldestMinute is the first minute of the window ending at now.
func oldestMinute(now time.Time) int64 { return minuteOf(now) - int64(monitorBuckets) + 1 }
