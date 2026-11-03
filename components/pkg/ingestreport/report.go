// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package ingestreport is the wire contract of the ingest report (the
// collector's docs/spec/rum.md §10): its pages, their strict decoding and
// merging, and the backend's bounded assembly of pages into reports.
package ingestreport // import "github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestreport"

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
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

// Paginate packs applications, then keys, into as few pages as fit, each
// a copy of header numbered page of pages.
func Paginate(header Report, applications []ApplicationReport, keys []KeyReport) ([][]byte, error) {
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
	return mergeWith(merger{}, pages)
}

// mergePages is Merge over pages the caller owns and keeps: they are
// validated in place, not copied again.
func mergePages(pages [][]byte) (Report, error) {
	return mergeWith(merger{borrow: true}, pages)
}

func mergeWith(m merger, pages [][]byte) (Report, error) {
	if len(pages) == 0 {
		return Report{}, errors.New("no page")
	}
	for _, data := range pages {
		if _, err := m.add(data); err != nil {
			return Report{}, err
		}
	}
	return m.result()
}

// merger validates the pages of one sequence as they come.
type merger struct {
	// borrow keeps the caller's page slices instead of copying them, for a
	// merger that does not outlive the pages.
	borrow bool
	header Report
	byPage map[int][]byte
	apps   map[string]bool
	keys   map[string]bool
	merged Report
	size   int
}

// add takes one page; repeat reports an identical copy of a page held.
func (m *merger) add(data []byte) (repeat bool, err error) {
	if len(data) > MaxPageBytes {
		return false, fmt.Errorf("a page is %d bytes", len(data))
	}
	r, err := decodePage(data)
	if err != nil {
		return false, err
	}
	if r.Pages < 1 || r.Pages > MaxPages || r.Page < 1 || r.Page > r.Pages {
		return false, fmt.Errorf("page %d of %d is out of range", r.Page, r.Pages)
	}
	if m.byPage == nil {
		m.byPage, m.apps, m.keys = map[int][]byte{}, map[string]bool{}, map[string]bool{}
	}
	if prev, ok := m.byPage[r.Page]; ok {
		if !bytes.Equal(prev, data) {
			return false, fmt.Errorf("page %d received twice with different content", r.Page)
		}
		return true, nil
	}
	head := r
	head.Applications, head.Keys, head.Page = nil, nil, 0
	if len(m.byPage) == 0 {
		m.header = head
	} else if !reflect.DeepEqual(head, m.header) {
		return false, fmt.Errorf("page %d header differs", r.Page)
	}
	for _, a := range r.Applications {
		if m.apps[a.ID] {
			return false, fmt.Errorf("application %s in two pages", a.ID)
		}
		m.apps[a.ID] = true
	}
	for _, k := range r.Keys {
		if m.keys[k.ID] {
			return false, fmt.Errorf("key %s in two pages", k.ID)
		}
		m.keys[k.ID] = true
	}
	if m.borrow {
		m.byPage[r.Page] = data
	} else {
		m.byPage[r.Page] = exactCopy(data) // never the caller's backing array
	}
	m.size += len(data)
	m.merged.Applications = append(m.merged.Applications, r.Applications...)
	m.merged.Keys = append(m.merged.Keys, r.Keys...)
	return false, nil
}

func (m *merger) complete() bool { return len(m.byPage) > 0 && len(m.byPage) == m.header.Pages }

func (m *merger) result() (Report, error) {
	if !m.complete() {
		return Report{}, fmt.Errorf("%d of %d pages", len(m.byPage), m.header.Pages)
	}
	out := m.header
	out.Applications, out.Keys = m.merged.Applications, m.merged.Keys
	if out.Applications == nil {
		out.Applications = []ApplicationReport{}
	}
	if out.Keys == nil {
		out.Keys = []KeyReport{}
	}
	return out, nil
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
