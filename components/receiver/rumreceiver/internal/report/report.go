// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package report // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
)

// Bounds of §10.3.
const (
	MaxApplications = 1000
	MaxKeys         = 10_000
	MaxReportBytes  = 256 << 10
	// maxOutstanding is how many unacknowledged reports remember what they
	// carried; an older one's values are carried again by every newer report.
	maxOutstanding = 64
)

// Report is the `report` message (§10.2). These are the only fields (G5).
type Report struct {
	SchemaVersion  int                 `json:"schemaVersion"`
	ReportSequence uint64              `json:"reportSequence"`
	GeneratedAt    string              `json:"generatedAt"`
	WindowSeconds  int                 `json:"windowSeconds"`
	Policy         PolicyReport        `json:"policy"`
	Applications   []ApplicationReport `json:"applications"`
	Keys           []KeyReport         `json:"keys"`
	UnknownKeys    UnknownKeysReport   `json:"unknownKeys"`
	Dropped        DroppedReport       `json:"dropped"`
}

type PolicyReport struct {
	Version uint64 `json:"version"`
	Status  string `json:"status"`
	Error   string `json:"error,omitempty"`
}

type ApplicationReport struct {
	ID          string            `json:"id"`
	FirstSeenAt string            `json:"firstSeenAt,omitempty"`
	Counts      Counts            `json:"counts"`
	SDK         SDKReport         `json:"sdk"`
	Rejections  []RejectionReport `json:"rejections"`
}

type Counts struct {
	Sessions int64 `json:"sessions"`
	Views    int64 `json:"views"`
	Errors   int64 `json:"errors"`
	Vitals   int64 `json:"vitals"`
}

type SDKReport struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type RejectionReport struct {
	Reason     string `json:"reason"`
	Count      int64  `json:"count"`
	LastOrigin string `json:"lastOrigin"`
	LastSeenAt string `json:"lastSeenAt"`
}

type KeyReport struct {
	ID         string `json:"id"`
	LastUsedAt string `json:"lastUsedAt"`
}

type UnknownKeysReport struct {
	Count      int64  `json:"count"`
	LastSeenAt string `json:"lastSeenAt,omitempty"`
}

type DroppedReport struct {
	Applications uint64 `json:"applications"`
	Keys         uint64 `json:"keys"`
}

// carried is what a sent report held of the pending durable values, by
// value, so an acknowledgement clears only what has not changed since.
type carried struct {
	firstSeen map[string]time.Time
	keys      map[string]time.Time
}

// ErrSequence is returned for a reportSequence not above the last one.
var ErrSequence = errors.New("reportSequence must increase")

// Build encodes the report of sequence seq, at most MaxReportBytes, and
// remembers what it carried for Ack. The caller persists seq before
// sending (§10.4). dropped counts keys evicted from the pending set since
// start plus keys and applications left out of this report.
func (m *Monitor) Build(seq uint64, now time.Time, policy ingestpolicy.Status) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if seq <= m.lastSequence {
		return nil, ErrSequence
	}
	m.expire(now)

	r := Report{
		SchemaVersion:  1,
		ReportSequence: seq,
		GeneratedAt:    stamp(now),
		WindowSeconds:  int(Window / time.Second),
		Policy:         PolicyReport{Version: policy.Version, Status: "ok"},
		Applications:   []ApplicationReport{},
		Keys:           []KeyReport{},
	}
	if policy.Rejected != "" {
		r.Policy.Status, r.Policy.Error = "rejected", string(policy.Rejected)
	}

	type active struct {
		id  string
		app *applicationMonitor
	}
	apps := make([]active, 0, len(m.applications))
	for id, app := range m.applications {
		if app.activeSince(oldestMinute(now)) || !app.firstSeen.IsZero() {
			apps = append(apps, active{id, app})
		}
	}
	slices.SortFunc(apps, func(a, b active) int { return b.app.lastActive.Compare(a.app.lastActive) })
	var droppedApps uint64
	if len(apps) > MaxApplications {
		droppedApps = uint64(len(apps) - MaxApplications)
		apps = apps[:MaxApplications]
	}
	for _, a := range apps {
		r.Applications = append(r.Applications, a.app.report(a.id, now))
	}

	for id, used := range m.keys.newestFirst() {
		if len(r.Keys) == MaxKeys {
			break
		}
		r.Keys = append(r.Keys, KeyReport{ID: id, LastUsedAt: stamp(used)})
	}
	droppedKeys := uint64(m.keys.len() - len(r.Keys))
	r.UnknownKeys = m.unknownReport(now)

	data, err := fit(&r, droppedApps, m.droppedKeys+droppedKeys)
	if err != nil {
		return nil, err
	}

	c := &carried{firstSeen: map[string]time.Time{}, keys: map[string]time.Time{}}
	for _, a := range r.Applications {
		if app := m.applications[a.ID]; app != nil && a.FirstSeenAt != "" {
			c.firstSeen[a.ID] = app.firstSeen
		}
	}
	for _, k := range r.Keys {
		c.keys[k.ID] = m.keys.get(k.ID)
	}
	m.remember(seq, c)
	return data, nil
}

// fit drops keys first, then the least recently active applications, until
// the encoded report fits (§10.3).
func fit(r *Report, droppedApps, droppedKeys uint64) ([]byte, error) {
	for {
		r.Dropped = DroppedReport{Applications: droppedApps, Keys: droppedKeys}
		data, err := json.Marshal(r)
		if err != nil {
			return nil, err
		}
		excess := len(data) - MaxReportBytes
		if excess <= 0 {
			return data, nil
		}
		// Entries are dropped from the end, a size estimate at a time, so a
		// large report is re-encoded a few times rather than once per entry.
		switch {
		case len(r.Keys) > 0:
			n := min(len(r.Keys), excess/encodedSize(r.Keys[len(r.Keys)-1])+1)
			r.Keys = r.Keys[:len(r.Keys)-n]
			droppedKeys += uint64(n)
		case len(r.Applications) > 0:
			n := min(len(r.Applications), excess/encodedSize(r.Applications[len(r.Applications)-1])+1)
			r.Applications = r.Applications[:len(r.Applications)-n]
			droppedApps += uint64(n)
		default:
			return nil, fmt.Errorf("an empty report is %d bytes", len(data))
		}
	}
}

// encodedSize is the size of one entry in the array, its comma included.
func encodedSize(v any) int {
	b, _ := json.Marshal(v)
	return len(b) + 1
}

func (m *Monitor) remember(seq uint64, c *carried) {
	m.lastSequence = seq
	m.sent[seq] = c
	m.sentOrder = append(m.sentOrder, seq)
	if len(m.sentOrder) > maxOutstanding {
		delete(m.sent, m.sentOrder[0])
		m.sentOrder = slices.Delete(m.sentOrder, 0, 1)
	}
}

// Ack handles a reportAck: values carried by report seq are no longer
// pending, unless they changed since. An ack for a sequence never sent, or
// already processed, changes nothing (§10.4).
func (m *Monitor) Ack(seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.sent[seq]
	if !ok {
		return
	}
	delete(m.sent, seq)
	m.sentOrder = slices.DeleteFunc(m.sentOrder, func(s uint64) bool { return s == seq })
	for id, seen := range c.firstSeen {
		if app := m.applications[id]; app != nil && app.firstSeen.Equal(seen) {
			app.firstSeen, app.acked = time.Time{}, true
		}
	}
	for id, used := range c.keys {
		m.keys.clearIf(id, used)
	}
}

// ParseAck decodes a reportAck strictly: {"schemaVersion":1,"reportSequence":N}.
func ParseAck(data []byte) (uint64, error) {
	var ack struct {
		SchemaVersion  *int    `json:"schemaVersion"`
		ReportSequence *uint64 `json:"reportSequence"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ack); err != nil {
		return 0, fmt.Errorf("reportAck: %w", err)
	}
	if dec.More() {
		return 0, errors.New("reportAck: trailing data")
	}
	if ack.SchemaVersion == nil || *ack.SchemaVersion != 1 || ack.ReportSequence == nil {
		return 0, errors.New("reportAck: schemaVersion 1 and reportSequence are required")
	}
	return *ack.ReportSequence, nil
}

func (app *applicationMonitor) activeSince(oldest int64) bool {
	for i := range app.buckets {
		if app.buckets[i].used && app.buckets[i].minute >= oldest {
			return true
		}
	}
	return false
}

func (app *applicationMonitor) report(id string, now time.Time) ApplicationReport {
	oldest := oldestMinute(now)
	out := ApplicationReport{ID: id, Rejections: []RejectionReport{}}
	if !app.firstSeen.IsZero() {
		out.FirstSeenAt = stamp(app.firstSeen)
	}
	sessions := map[uint64]struct{}{}
	var merged [numReasons]rejection
	for i := range app.buckets {
		b := &app.buckets[i]
		if !b.used || b.minute < oldest {
			continue
		}
		for s := range b.sessions {
			sessions[s] = struct{}{}
		}
		out.Counts.Views += b.views
		out.Counts.Errors += b.errors
		out.Counts.Vitals += b.vitals
		for reason := range b.rejections {
			r, into := b.rejections[reason], &merged[reason]
			into.count += r.count
			if r.count > 0 && r.seen.After(into.seen) {
				into.seen, into.observed = r.seen, r.observed
			}
		}
	}
	out.Counts.Sessions = int64(len(sessions))
	if !app.sdkSeen.IsZero() && minuteOf(app.sdkSeen) >= oldest {
		out.SDK = SDKReport{Name: app.sdkName, Version: app.sdkVersion}
	}
	reasons := make([]Reason, 0, numReasons)
	for reason := range merged {
		if merged[reason].count > 0 {
			reasons = append(reasons, Reason(reason))
		}
	}
	// Newest first ([B] impl/monitor.go:135-140).
	slices.SortStableFunc(reasons, func(a, b Reason) int { return merged[b].seen.Compare(merged[a].seen) })
	for _, reason := range reasons {
		r := merged[reason]
		out.Rejections = append(out.Rejections, RejectionReport{
			Reason: reason.String(), Count: r.count, LastOrigin: r.observed, LastSeenAt: stamp(r.seen),
		})
	}
	return out
}

func (m *Monitor) unknownReport(now time.Time) UnknownKeysReport {
	oldest := oldestMinute(now)
	var out UnknownKeysReport
	for _, b := range m.unknown {
		if b.count > 0 && b.minute >= oldest {
			out.Count += b.count
		}
	}
	if out.Count > 0 {
		out.LastSeenAt = stamp(m.unknownLast)
	}
	return out
}

func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339) }
