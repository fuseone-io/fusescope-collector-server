// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package ingestpolicy // import "github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// Cache file names in the storage directory (§11.3).
const (
	PolicyFile = "ingest-policy.json"
	StateFile  = "ingest-policy.state"
)

// FileCache is the on-disk cache of §11.3. It lives with the revision rules
// it protects, so the write order and the checks at start are tested
// together; the OpAMP extension only chooses the directory.
type FileCache struct {
	dir string
	// crash, set by tests, stops a write at a named step as a process death
	// would: nothing after it runs, temporary files are left behind.
	crash func(step string) bool
}

// errCrash is returned when a test stops a write midway.
var errCrash = errors.New("simulated crash")

var (
	_ Cache   = (*FileCache)(nil)
	_ Toucher = (*FileCache)(nil)
)

// NewFileCache caches in dir, created on the first Save.
func NewFileCache(dir string) *FileCache { return &FileCache{dir: dir} }

type stateFile struct {
	Version uint64 `json:"version"`
	SHA256  string `json:"sha256"`
}

// Load reads the state, then the policy. A missing state is (nil, nil, zero,
// nil); a missing policy with a state returns the floor and no data.
func (c *FileCache) Load() ([]byte, *Floor, time.Time, error) {
	raw, err := readCapped(filepath.Join(c.dir, StateFile), 4096)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil, time.Time{}, nil
	}
	if err != nil {
		return nil, nil, time.Time{}, err
	}
	var st stateFile
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&st); err != nil {
		return nil, nil, time.Time{}, fmt.Errorf("%s: %w", StateFile, err)
	}
	if st.Version < 1 || !validVerifier(st.SHA256) {
		return nil, nil, time.Time{}, fmt.Errorf("%s: invalid content", StateFile)
	}
	floor := &Floor{Version: st.Version, SHA256: decodeVerifier(st.SHA256)}

	path := filepath.Join(c.dir, PolicyFile)
	data, err := readCapped(path, MaxSize)
	if err != nil {
		return nil, floor, time.Time{}, err
	}
	var written time.Time
	if info, err := os.Stat(path); err == nil {
		written = info.ModTime()
	}
	return data, floor, written, nil
}

// SaveFloor writes the state file, atomically with mode 0600.
func (c *FileCache) SaveFloor(floor Floor) error {
	st, err := json.Marshal(stateFile{Version: floor.Version, SHA256: hex.EncodeToString(floor.SHA256[:])})
	if err != nil {
		return err
	}
	return c.write(StateFile, st)
}

// SavePolicy writes the policy file, atomically with mode 0600.
func (c *FileCache) SavePolicy(data []byte) error {
	return c.write(PolicyFile, data)
}

// Touch sets the cached policy's modification time, which Load returns as
// when it was last received.
func (c *FileCache) Touch(received time.Time) error {
	return os.Chtimes(filepath.Join(c.dir, PolicyFile), received, received)
}

// write replaces name atomically: a synced temporary file renamed over it,
// then the directory synced.
func (c *FileCache) write(name string, b []byte) (err error) {
	if err := os.MkdirAll(c.dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(c.dir, name+".*.tmp")
	if err != nil {
		return err
	}
	defer func() {
		if !errors.Is(err, errCrash) {
			_ = os.Remove(tmp.Name())
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if c.crash != nil && c.crash(name+" tmp") {
		return errCrash
	}
	if err := os.Rename(tmp.Name(), filepath.Join(c.dir, name)); err != nil {
		return err
	}
	d, err := os.Open(c.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return err
	}
	if c.crash != nil && c.crash(name+" renamed") {
		return errCrash
	}
	return nil
}

// readCapped reads at most limit bytes and fails on more, so a corrupt or
// planted file cannot exhaust memory.
func readCapped(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("%s is larger than %d bytes", filepath.Base(path), limit)
	}
	return b, nil
}
