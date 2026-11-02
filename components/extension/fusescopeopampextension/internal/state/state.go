// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package state persists the OpAMP agent's state across restarts (ADR 0008
// §4 and the reload lifecycle): the instance UID, the last applied push, a
// pending push and the start-from-cache marker. Every write is atomic.
package state // import "github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/state"

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const (
	uidFile      = "instance_uid"
	appliedFile  = "applied.json"
	pendingFile  = "pending.json"
	startingFile = "starting.json"
	tmpSuffix    = ".tmp"
)

// Push is one pushed configuration: the backend's hash, echoed in statuses,
// the revision injected into the extension's config, and the pushed YAML.
type Push struct {
	Hash     []byte `json:"hash"`
	Revision string `json:"revision"`
	Config   []byte `json:"config"`
}

// Store is a state directory.
type Store struct {
	dir string
	// crash, set in tests, stops a sequence of writes at a named step.
	crash func(step string) bool
}

// errCrash is returned when a test stops a sequence midway.
var errCrash = errors.New("simulated crash")

// Open opens dir without touching it: a collector that only validates its
// configuration may share dir with one that runs.
func Open(dir string) (*Store, error) {
	return &Store{dir: dir}, nil
}

// prepare creates the directory and drops temporary files a crash left; only
// the collector that really starts does it.
func (s *Store) prepare() error {
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), tmpSuffix) {
			_ = os.Remove(filepath.Join(s.dir, e.Name()))
		}
	}
	return nil
}

// Dir is the state directory.
func (s *Store) Dir() string { return s.dir }

// InstanceUID returns the stored instance UID, creating it once.
func (s *Store) InstanceUID() ([16]byte, error) {
	var uid [16]byte
	b, err := os.ReadFile(filepath.Join(s.dir, uidFile))
	if err == nil {
		raw, derr := hex.DecodeString(strings.TrimSpace(string(b)))
		if derr == nil && len(raw) == 16 {
			copy(uid[:], raw)
			return uid, nil
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return uid, err
	}
	_, _ = rand.Read(uid[:])
	uid[6], uid[8] = uid[6]&0x0f|0x40, uid[8]&0x3f|0x80 // UUIDv4, as the backend parses it
	return uid, s.write(uidFile, []byte(hex.EncodeToString(uid[:])))
}

// Startup is what a start finds.
type Startup struct {
	// Applied is the cached push to merge again with the base, or nil.
	Applied *Push
	// Failed is a push the collector exited while applying, to report FAILED.
	Failed *Push
	// DroppedCache is set when a start from the cache never reached Ready, so
	// the cache was discarded.
	DroppedCache bool
}

// Peek reports what Start would find, without writing anything, so that
// validating a configuration leaves the state as it was.
func (s *Store) Peek() (Startup, error) {
	return s.start(false)
}

// Start reads the state when the collector really starts, and records it:
// it clears a failed pending push and marks a start from the cache.
func (s *Store) Start() (Startup, error) {
	return s.start(true)
}

func (s *Store) start(write bool) (Startup, error) {
	var st Startup
	if write {
		if err := s.prepare(); err != nil {
			return st, err
		}
	}
	applied, err := s.read(appliedFile)
	if err != nil {
		return st, err
	}
	pending, err := s.read(pendingFile)
	if err != nil {
		return st, err
	}
	starting, err := s.read(startingFile)
	if err != nil {
		return st, err
	}
	if pending != nil {
		// Promoted but not yet cleared when the process stopped: it was applied.
		if applied == nil || applied.Revision != pending.Revision {
			st.Failed = pending
		}
		if write {
			if err := s.remove(pendingFile); err != nil {
				return st, err
			}
		}
	}
	if starting != nil && applied != nil && starting.Revision == applied.Revision {
		// The last start from this cache never reached Ready.
		st.DroppedCache = true
		if write {
			if err := s.remove(appliedFile); err != nil {
				return st, err
			}
			if err := s.remove(startingFile); err != nil {
				return st, err
			}
		}
		return st, nil
	}
	if applied != nil {
		if write {
			if err := s.writeJSON(startingFile, &Push{Revision: applied.Revision}); err != nil {
				return st, err
			}
		}
		st.Applied = applied
	}
	return st, nil
}

// Begin records p as pending before the reload that applies it.
func (s *Store) Begin(p *Push) error { return s.writeJSON(pendingFile, p) }

// Abort drops the pending push of revision, which will not be applied.
func (s *Store) Abort(revision string) error {
	pending, err := s.read(pendingFile)
	if err != nil || pending == nil || pending.Revision != revision {
		return err
	}
	return s.remove(pendingFile)
}

// Confirm records that revision runs. A pending push of that revision is
// promoted to applied, persisted before Confirm returns it, so the caller
// reports APPLIED only for what is on disk; nil means nothing was pending.
func (s *Store) Confirm(revision string) (*Push, error) {
	if starting, err := s.read(startingFile); err != nil {
		return nil, err
	} else if starting != nil && starting.Revision == revision {
		if err := s.remove(startingFile); err != nil {
			return nil, err
		}
	}
	pending, err := s.read(pendingFile)
	if err != nil {
		return nil, err
	}
	if pending == nil || pending.Revision != revision {
		// A retry after the promotion was written: report it again.
		applied, err := s.read(appliedFile)
		if err != nil || applied == nil || applied.Revision != revision || pending != nil {
			return nil, err
		}
		return applied, nil
	}
	if err := s.writeJSON(appliedFile, pending); err != nil {
		return nil, err
	}
	if s.crash != nil && s.crash("promoted") {
		return nil, errCrash
	}
	if err := s.remove(pendingFile); err != nil {
		return nil, err
	}
	if s.crash != nil && s.crash("pending removed") {
		return nil, errCrash
	}
	return pending, nil
}

func (s *Store) read(name string) (*Push, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var p Push
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return &p, nil
}

func (s *Store) writeJSON(name string, p *Push) error {
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return s.write(name, b)
}

// write replaces name atomically: a synced temporary file renamed over it,
// then the directory synced.
func (s *Store) write(name string, b []byte) error {
	tmp := filepath.Join(s.dir, name+tmpSuffix)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if s.crash != nil && s.crash("before rename "+name) {
		return errCrash
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, name)); err != nil {
		return err
	}
	return s.syncDir()
}

func (s *Store) remove(name string) error {
	if err := os.Remove(filepath.Join(s.dir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return s.syncDir()
}

func (s *Store) syncDir() error {
	d, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
