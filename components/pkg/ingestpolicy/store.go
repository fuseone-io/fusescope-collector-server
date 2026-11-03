// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package ingestpolicy // import "github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"

import (
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// Source is what consumers of the policy read. Current is lock-free and nil
// while no valid policy is held (fail closed, §11.4).
type Source interface {
	Current() *Snapshot
	// LastReceived is when the policy held was last received, for staleness;
	// zero with no policy.
	LastReceived() time.Time
	// Changed is closed when Current changes; call it again for the next change.
	Changed() <-chan struct{}
	// Status is the revision held and the outcome of the last policy received.
	Status() Status
}

// Provider is implemented by the component that owns the Store (the OpAMP
// extension, step 3), so receivers find it through the host.
type Provider interface {
	IngestPolicy() Source
}

// Status is reported as policy.{version,status,error} (§10.2).
type Status struct {
	// Version is the revision held, 0 with none.
	Version uint64
	// Rejected is the code of the last policy received when it was rejected,
	// empty when it was accepted.
	Rejected Code
}

// Floor is the highest revision accepted: anti-rollback state (§11.3).
type Floor struct {
	Version uint64
	SHA256  [32]byte
}

// Cache persists accepted policies and the floor. Save writes the policy,
// then the floor. Load returns the cached policy (nil if absent), the floor
// (nil if absent) and when the cached policy was written.
type Cache interface {
	Load() (data []byte, floor *Floor, written time.Time, err error)
	Save(data []byte, floor Floor) error
}

// Store holds the policy. Apply is serialised; readers never lock.
type Store struct {
	logger *zap.Logger
	cache  Cache

	mu    sync.Mutex
	floor *Floor
	// unsaved is the accepted policy whose caching failed, retried on the
	// next identical push so a restart cannot load an older revision.
	unsaved []byte

	current  atomic.Pointer[Snapshot]
	received atomic.Int64
	status   atomic.Pointer[Status]
	changed  atomic.Pointer[chan struct{}]
}

var _ Source = (*Store)(nil)

// NewStore returns an empty store. cache may be nil (nothing persisted).
func NewStore(logger *zap.Logger, cache Cache) *Store {
	s := &Store{logger: logger, cache: cache}
	ch := make(chan struct{})
	s.changed.Store(&ch)
	s.status.Store(&Status{})
	return s
}

// Current returns the snapshot held, or nil.
func (s *Store) Current() *Snapshot { return s.current.Load() }

// LastReceived returns when the policy held was last received.
func (s *Store) LastReceived() time.Time {
	n := s.received.Load()
	if n == 0 || s.current.Load() == nil {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// Changed is closed on the next change of Current.
func (s *Store) Changed() <-chan struct{} { return *s.changed.Load() }

// Status returns the revision held and the last outcome.
func (s *Store) Status() Status { return *s.status.Load() }

// Floor returns the highest revision accepted, or nil.
func (s *Store) Floor() *Floor {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.floor == nil {
		return nil
	}
	f := *s.floor
	return &f
}

// Start loads the cache (§11.3): the state file first, then the cached
// policy, used only if its hash equals the state's and it is valid at that
// version. Any other outcome starts with no policy; a floor that was read is
// kept so an older policy cannot be replayed.
func (s *Store) Start() {
	if s.cache == nil {
		return
	}
	data, floor, written, err := s.cache.Load()
	s.mu.Lock()
	defer s.mu.Unlock()
	if floor == nil {
		if err != nil {
			s.logger.Error("Ingest policy cache unreadable; starting with no policy and no floor", zap.Error(err))
		} else {
			s.logger.Error("No ingest policy state cached; starting with no policy until the backend pushes one")
		}
		return
	}
	s.floor = floor
	if err != nil || data == nil {
		s.logger.Error("Cached ingest policy unreadable; starting with no policy", zap.Uint64("floor", floor.Version), zap.Error(err))
		return
	}
	if sha256.Sum256(data) != floor.SHA256 {
		s.logger.Error("Cached ingest policy does not match its state; starting with no policy", zap.Uint64("floor", floor.Version))
		return
	}
	p, rerr := parseAndValidate(data)
	if rerr == nil && p.Version != floor.Version {
		rerr = reject(CodeVersionConflict, "version")
	}
	if rerr != nil {
		s.logger.Error("Cached ingest policy rejected; starting with no policy", rejectionFields(rerr)...)
		return
	}
	if written.IsZero() {
		written = time.Now()
	}
	s.publish(compile(p, floor.SHA256), written)
	s.logger.Info("Ingest policy loaded from cache", zap.Uint64("version", p.Version))
}

// Apply handles one `policy` message (§11.2, §11.3). A rejection returns a
// *RejectionError and leaves the policy held, if any, in force. An accepted
// policy is published even if caching it fails: refusing it would keep
// revoked keys working; that failure is logged.
func (s *Store) Apply(data []byte, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.apply(data, now)
	var rerr *RejectionError
	switch {
	case err == nil:
		st := Status{Version: s.current.Load().Version}
		s.status.Store(&st)
	case errors.As(err, &rerr):
		st := Status{Rejected: rerr.Code}
		if cur := s.current.Load(); cur != nil {
			st.Version = cur.Version
		}
		s.status.Store(&st)
		s.logRejection("Ingest policy rejected", rerr)
	}
	return err
}

func (s *Store) apply(data []byte, now time.Time) error {
	p, err := Parse(data)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	if s.floor != nil {
		switch {
		case p.Version < s.floor.Version:
			return reject(CodeLowerVersion, "version")
		case p.Version == s.floor.Version && sum != s.floor.SHA256:
			return reject(CodeVersionConflict, "version")
		case p.Version == s.floor.Version && s.current.Load() != nil:
			// The same revision again: only its freshness changes.
			s.received.Store(now.UnixNano())
			s.save(data, *s.floor)
			s.touch(now)
			return nil
		}
	}
	if err := Validate(p); err != nil {
		return err
	}
	floor := Floor{Version: p.Version, SHA256: sum}
	s.unsaved = data
	s.save(data, floor)
	s.floor = &floor
	s.publish(compile(p, sum), now)
	s.logger.Info("Ingest policy applied", zap.Uint64("version", p.Version),
		zap.Int("keys", len(p.Keys)), zap.Int("rum_applications", len(p.RUMApplications)))
	return nil
}

// save caches data if it is the accepted policy not cached yet.
func (s *Store) save(data []byte, floor Floor) {
	if s.cache == nil || s.unsaved == nil {
		return
	}
	if err := s.cache.Save(data, floor); err != nil {
		s.logger.Error("Ingest policy applied but not cached", zap.Uint64("version", floor.Version), zap.Error(err))
		return
	}
	s.unsaved = nil
}

// Toucher is a Cache that can record a fresher receipt of the policy it
// holds without rewriting it, so freshness survives a restart (§11.4).
type Toucher interface {
	Touch(received time.Time) error
}

func (s *Store) touch(now time.Time) {
	t, ok := s.cache.(Toucher)
	if !ok || s.unsaved != nil {
		return
	}
	if err := t.Touch(now); err != nil {
		s.logger.Warn("Ingest policy freshness not cached", zap.Error(err))
	}
}

func (s *Store) publish(snap *Snapshot, received time.Time) {
	s.received.Store(received.UnixNano())
	s.current.Store(snap)
	st := Status{Version: snap.Version}
	s.status.Store(&st)
	next := make(chan struct{})
	close(*s.changed.Swap(&next))
}

func (s *Store) logRejection(msg string, err error) {
	s.logger.Warn(msg, rejectionFields(err)...)
}

// rejectionFields names the code and where, never the content (§11.2).
func rejectionFields(err error) []zap.Field {
	var rerr *RejectionError
	if errors.As(err, &rerr) {
		return []zap.Field{zap.String("code", string(rerr.Code)), zap.String("path", rerr.Path)}
	}
	return nil
}

func parseAndValidate(data []byte) (*Policy, error) {
	p, err := Parse(data)
	if err != nil {
		return nil, err
	}
	if err := Validate(p); err != nil {
		return nil, err
	}
	return p, nil
}
