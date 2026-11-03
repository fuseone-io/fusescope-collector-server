// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package ingestpolicy // import "github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"

import (
	"strings"
	"time"
)

// Snapshot is an accepted policy, compiled for lookups. It is immutable:
// readers hold it without locks, and a new policy replaces it whole.
type Snapshot struct {
	Version  uint64
	SHA256   [32]byte
	IssuedAt time.Time

	keys         map[[32]byte]*SnapshotKey
	applications map[string]*Application
	ordered      []*Application
	origins      originIndex
}

// SnapshotKey is one key of a snapshot.
type SnapshotKey struct {
	ID      string
	OrgID   string
	Kind    string
	Signals Signal
	// ExpiresAt is zero for a key without expiry.
	ExpiresAt time.Time
	// Limit is nil when the policy gives none.
	Limit *Limit
	// Application is set for public keys.
	Application *Application
}

// UsableAt reports whether the key has not expired at now: a key whose
// expiresAt is not after now is refused ([B] rumtypes/key.go:151-167).
func (k *SnapshotKey) UsableAt(now time.Time) bool {
	return k.ExpiresAt.IsZero() || now.Before(k.ExpiresAt)
}

// Application is one RUM application of a snapshot.
type Application struct {
	ID                  string
	OrgID               string
	Name                string
	Environment         string
	Platform            string
	AllowedOrigins      []string
	AllowedPackageNames []string
	PIIPolicy           PIIPolicy

	origins []originEntry
}

// originIndex answers "does any application allow this origin" for a
// preflight in a few map lookups, whatever the number of applications.
type originIndex struct {
	any      bool
	exact    map[originKey]struct{}
	wildcard map[originKey]struct{}
}

func compile(p *Policy, sum [32]byte) *Snapshot {
	issuedAt, _ := time.Parse(time.RFC3339, p.IssuedAt)
	s := &Snapshot{
		Version:      p.Version,
		SHA256:       sum,
		IssuedAt:     issuedAt,
		keys:         make(map[[32]byte]*SnapshotKey, len(p.Keys)),
		applications: make(map[string]*Application, len(p.RUMApplications)),
		ordered:      make([]*Application, 0, len(p.RUMApplications)),
		origins:      originIndex{exact: map[originKey]struct{}{}, wildcard: map[originKey]struct{}{}},
	}
	for i := range p.RUMApplications {
		ra := &p.RUMApplications[i]
		a := &Application{
			ID:                  ra.ID,
			OrgID:               ra.OrgID,
			Name:                ra.Name,
			Environment:         ra.Environment,
			Platform:            ra.Platform,
			AllowedOrigins:      ra.AllowedOrigins,
			AllowedPackageNames: ra.AllowedPackageNames,
			PIIPolicy:           ra.PIIPolicy,
		}
		for _, o := range ra.AllowedOrigins {
			e, ok := compileOrigin(o)
			if !ok {
				continue
			}
			a.origins = append(a.origins, e)
			switch {
			case e.any:
				s.origins.any = true
			case e.wildcard:
				s.origins.wildcard[originKey{e.scheme, e.port, e.host}] = struct{}{}
			default:
				s.origins.exact[originKey{e.scheme, e.port, e.host}] = struct{}{}
			}
		}
		s.applications[a.ID] = a
		s.ordered = append(s.ordered, a)
	}
	for i := range p.Keys {
		pk := &p.Keys[i]
		signals, _ := signalSet(pk)
		k := &SnapshotKey{ID: pk.ID, OrgID: pk.OrgID, Kind: pk.Kind, Signals: signals, Limit: pk.Limit}
		if pk.ExpiresAt != nil {
			k.ExpiresAt, _ = time.Parse(time.RFC3339, *pk.ExpiresAt)
		}
		if pk.ApplicationID != nil {
			k.Application = s.applications[*pk.ApplicationID]
		}
		s.keys[decodeVerifier(pk.SHA256)] = k
	}
	return s
}

// Key returns the key whose SHA-256 verifier is v, or nil.
func (s *Snapshot) Key(v [32]byte) *SnapshotKey { return s.keys[v] }

// Application returns the application of that ID, or nil.
func (s *Snapshot) Application(id string) *Application { return s.applications[id] }

// Applications returns the applications in policy order; callers must not
// modify the slice.
func (s *Snapshot) Applications() []*Application { return s.ordered }

// KeyCount is the number of keys.
func (s *Snapshot) KeyCount() int { return len(s.keys) }

// AnyApplicationAllowsOrigin reports whether at least one RUM application
// allows origin, which is all a CORS preflight can be checked against (§4).
func (s *Snapshot) AnyApplicationAllowsOrigin(origin string) bool {
	c, ok := parseCandidate(origin)
	if !ok {
		return false
	}
	if s.origins.any {
		return true
	}
	if _, ok := s.origins.exact[c]; ok {
		return true
	}
	// Each proper suffix starting at a dot, as a wildcard entry stores it.
	for i := strings.IndexByte(c.host, '.'); i > 0; {
		if _, ok := s.origins.wildcard[originKey{c.scheme, c.port, c.host[i:]}]; ok {
			return true
		}
		next := strings.IndexByte(c.host[i+1:], '.')
		if next < 0 {
			break
		}
		i += 1 + next
	}
	return false
}
