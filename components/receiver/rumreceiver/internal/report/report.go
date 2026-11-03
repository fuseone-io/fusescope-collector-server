// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package report // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"

import (
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestreport"
)

// The wire contract lives in ingestreport, shared with the backend.
type (
	Report            = ingestreport.Report
	PolicyReport      = ingestreport.PolicyReport
	ApplicationReport = ingestreport.ApplicationReport
	Counts            = ingestreport.Counts
	SDKReport         = ingestreport.SDKReport
	RejectionReport   = ingestreport.RejectionReport
	KeyReport         = ingestreport.KeyReport
	UnknownKeysReport = ingestreport.UnknownKeysReport
	DroppedReport     = ingestreport.DroppedReport
)

const (
	MaxPageBytes = ingestreport.MaxPageBytes
	MaxPages     = ingestreport.MaxPages
	// maxOutstanding is how many unacknowledged reports remember what they
	// carried; an older one's values are carried again by every newer report.
	maxOutstanding = 64
)

// Merge reassembles and validates the pages of one sequence
// (ingestreport.Merge).
func Merge(pages [][]byte) (Report, error) { return ingestreport.Merge(pages) }

// carried is what a sent report held of the pending durable values, by
// value, so an acknowledgement clears only what has not changed since.
type carried struct {
	firstSeen map[string]time.Time
	keys      map[string]time.Time
}

// ErrSequence is returned for a reportSequence not above the last one.
var ErrSequence = errors.New("reportSequence must increase")

// Build encodes the report of sequence seq as its pages, each at most
// MaxPageBytes, and remembers everything they carried for Ack. The caller
// persists seq before sending (§10.4) and sends every page.
func (m *Monitor) Build(seq uint64, now time.Time, policy ingestpolicy.Status) ([][]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if seq <= m.lastSequence {
		return nil, ErrSequence
	}
	m.expire(now)

	header := Report{
		SchemaVersion:  1,
		ReportSequence: seq,
		GeneratedAt:    stamp(now),
		WindowSeconds:  int(Window / time.Second),
		Policy:         PolicyReport{Version: policy.Version, Status: "ok"},
		UnknownKeys:    m.unknownReport(now),
		Dropped:        DroppedReport{Keys: m.droppedKeys},
	}
	if policy.Rejected != "" {
		header.Policy.Status, header.Policy.Error = "rejected", string(policy.Rejected)
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
	// Most recently active first, then by ID so pages are deterministic.
	slices.SortFunc(apps, func(a, b active) int {
		if c := b.app.lastActive.Compare(a.app.lastActive); c != 0 {
			return c
		}
		return strings.Compare(a.id, b.id)
	})
	applications := make([]ApplicationReport, 0, len(apps))
	for _, a := range apps {
		applications = append(applications, a.app.report(a.id, now))
	}
	keys := make([]KeyReport, 0, m.keys.len())
	for id, used := range m.keys.newestFirst() {
		keys = append(keys, KeyReport{ID: id, LastUsedAt: stamp(used)})
	}

	pages, err := ingestreport.Paginate(header, applications, keys)
	if err != nil {
		return nil, err
	}

	c := &carried{firstSeen: map[string]time.Time{}, keys: map[string]time.Time{}}
	for _, a := range applications {
		if app := m.applications[a.ID]; app != nil && a.FirstSeenAt != "" {
			c.firstSeen[a.ID] = app.firstSeen
		}
	}
	for _, k := range keys {
		c.keys[k.ID] = m.keys.get(k.ID)
	}
	m.remember(seq, c)
	return pages, nil
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

// ParseAck decodes a reportAck strictly (ingestpolicy.ParseReportAck).
func ParseAck(data []byte) (uint64, error) { return ingestpolicy.ParseReportAck(data) }

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
