// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package report // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
)

// Bounds of §10.3. A report is one logical report sent as pages of at most
// MaxPageBytes; every active application of the policy (at most 10 000)
// and every pending key (at most limits.pending_keys, 10 000 by default)
// is in exactly one page of every report, so none is starved.
const (
	MaxPageBytes = 256 << 10
	// MaxPages is what the bounds can need, with room: 10 000 applications
	// at their largest and 10 000 keys take 58 pages (TestReportPages).
	MaxPages = 128
	// maxOutstanding is how many unacknowledged reports remember what they
	// carried; an older one's values are carried again by every newer report.
	maxOutstanding = 64
)

// Report is one page of the `report` message (§10.2). These are the only
// fields (G5). Every page of a sequence carries the same header fields;
// applications and keys are split between pages, each in exactly one.
type Report struct {
	SchemaVersion  int                 `json:"schemaVersion"`
	ReportSequence uint64              `json:"reportSequence"`
	Page           int                 `json:"page"`
	Pages          int                 `json:"pages"`
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

// DroppedReport counts pending key uses evicted, since start, to keep the
// pending set within its bound; never entries sent in another page.
type DroppedReport struct {
	Keys uint64 `json:"keys"`
}

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

	pages, err := paginate(header, applications, keys)
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

// paginate packs applications, then keys, into as few pages as fit.
func paginate(header Report, applications []ApplicationReport, keys []KeyReport) ([][]byte, error) {
	empty := header
	empty.Page, empty.Pages = MaxPages, MaxPages
	empty.Applications, empty.Keys = []ApplicationReport{}, []KeyReport{}
	base, err := json.Marshal(empty)
	if err != nil {
		return nil, err
	}
	budget := MaxPageBytes - len(base)

	type page struct {
		apps []ApplicationReport
		keys []KeyReport
		used int
	}
	pages := []*page{{}}
	add := func(size int) (*page, error) {
		if size > budget {
			return nil, fmt.Errorf("a report entry of %d bytes does not fit a page", size)
		}
		cur := pages[len(pages)-1]
		if cur.used+size > budget {
			cur = &page{}
			pages = append(pages, cur)
		}
		cur.used += size
		return cur, nil
	}
	for _, a := range applications {
		pg, err := add(encodedSize(a))
		if err != nil {
			return nil, err
		}
		pg.apps = append(pg.apps, a)
	}
	for _, k := range keys {
		pg, err := add(encodedSize(k))
		if err != nil {
			return nil, err
		}
		pg.keys = append(pg.keys, k)
	}
	if len(pages) > MaxPages {
		return nil, fmt.Errorf("a report of %d pages is over the bound of %d", len(pages), MaxPages)
	}

	out := make([][]byte, 0, len(pages))
	for i, pg := range pages {
		r := header
		r.Page, r.Pages = i+1, len(pages)
		r.Applications, r.Keys = pg.apps, pg.keys
		if r.Applications == nil {
			r.Applications = []ApplicationReport{}
		}
		if r.Keys == nil {
			r.Keys = []KeyReport{}
		}
		data, err := json.Marshal(r)
		if err != nil {
			return nil, err
		}
		if len(data) > MaxPageBytes {
			return nil, fmt.Errorf("report page %d is %d bytes", i+1, len(data))
		}
		out = append(out, data)
	}
	return out, nil
}

// Merge reassembles the pages of one sequence under the backend's contract
// (§10.4): 1 to MaxPages pages, each at most MaxPageBytes of strict JSON
// (only the report's fields, each once, nothing after it), identical
// headers, every page from 1 to pages present, no application or key ID
// twice, and a page received twice only with identical bytes. Any other
// input invalidates the whole sequence.
func Merge(pages [][]byte) (Report, error) {
	if len(pages) == 0 {
		return Report{}, errors.New("no page")
	}
	var merged, header Report
	byPage := map[int][]byte{}
	apps, keys := map[string]bool{}, map[string]bool{}
	for i, data := range pages {
		if len(data) > MaxPageBytes {
			return Report{}, fmt.Errorf("page %d is %d bytes", i+1, len(data))
		}
		r, err := decodePage(data)
		if err != nil {
			return Report{}, err
		}
		if r.Pages < 1 || r.Pages > MaxPages || r.Page < 1 || r.Page > r.Pages {
			return Report{}, fmt.Errorf("page %d of %d is out of range", r.Page, r.Pages)
		}
		if prev, ok := byPage[r.Page]; ok {
			if !bytes.Equal(prev, data) {
				return Report{}, fmt.Errorf("page %d received twice with different content", r.Page)
			}
			continue
		}
		byPage[r.Page] = data
		head := r
		head.Applications, head.Keys, head.Page = nil, nil, 0
		if len(byPage) == 1 {
			header = head
		} else if !reflect.DeepEqual(head, header) {
			return Report{}, fmt.Errorf("page %d header differs", r.Page)
		}
		for _, a := range r.Applications {
			if apps[a.ID] {
				return Report{}, fmt.Errorf("application %s in two pages", a.ID)
			}
			apps[a.ID] = true
		}
		for _, k := range r.Keys {
			if keys[k.ID] {
				return Report{}, fmt.Errorf("key %s in two pages", k.ID)
			}
			keys[k.ID] = true
		}
		merged.Applications = append(merged.Applications, r.Applications...)
		merged.Keys = append(merged.Keys, r.Keys...)
	}
	if len(byPage) != header.Pages {
		return Report{}, fmt.Errorf("%d of %d pages", len(byPage), header.Pages)
	}
	apps1, keys1 := merged.Applications, merged.Keys
	merged = header
	merged.Applications, merged.Keys = apps1, keys1
	if merged.Applications == nil {
		merged.Applications = []ApplicationReport{}
	}
	if merged.Keys == nil {
		merged.Keys = []KeyReport{}
	}
	return merged, nil
}

// decodePage decodes one page strictly. encoding/json matches field names
// case-insensitively and keeps the last of duplicate keys, so the page must
// also re-encode to the same JSON value, and no object may repeat a key.
func decodePage(data []byte) (Report, error) {
	var r Report
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return Report{}, fmt.Errorf("page: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Report{}, errors.New("page: trailing data")
	}
	if r.SchemaVersion != 1 {
		return Report{}, fmt.Errorf("page: schemaVersion %d", r.SchemaVersion)
	}
	if err := noDuplicateKeys(data); err != nil {
		return Report{}, err
	}
	canonical, err := json.Marshal(r)
	if err != nil {
		return Report{}, err
	}
	var a, b any
	if json.Unmarshal(data, &a) != nil || json.Unmarshal(canonical, &b) != nil || !reflect.DeepEqual(a, b) {
		return Report{}, errors.New("page: not in the report schema (field names, types or missing fields)")
	}
	return r, nil
}

func noDuplicateKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	var walk func() error
	walk = func() error {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		d, ok := tok.(json.Delim)
		if !ok {
			return nil
		}
		switch d {
		case '{':
			seen := map[string]bool{}
			for dec.More() {
				k, err := dec.Token()
				if err != nil {
					return err
				}
				key, _ := k.(string)
				if seen[key] {
					return fmt.Errorf("page: duplicate field %q", key)
				}
				seen[key] = true
				if err := walk(); err != nil {
					return err
				}
			}
		case '[':
			for dec.More() {
				if err := walk(); err != nil {
					return err
				}
			}
		}
		_, err = dec.Token()
		return err
	}
	return walk()
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
