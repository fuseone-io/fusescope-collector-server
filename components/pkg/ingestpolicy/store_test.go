// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package ingestpolicy

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

var t0 = time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)

func TestStoreRevisions(t *testing.T) {
	v2 := encode(t, testPolicy(2))
	v2other := testPolicy(2)
	v2other.IssuedAt = "2026-11-03T12:00:01Z"
	testCases := []struct {
		name     string
		next     []byte
		code     Code
		version  uint64
		received time.Time
	}{
		{name: "Higher_Replaces", next: encode(t, testPolicy(3)), version: 3, received: t0.Add(time.Minute)},
		{name: "Lower_LowerVersion", next: encode(t, testPolicy(1)), code: CodeLowerVersion, version: 2, received: t0},
		{name: "SameBytes_NoOpRefreshes", next: v2, version: 2, received: t0.Add(time.Minute)},
		{name: "SameVersionOtherBytes_VersionConflict", next: encode(t, v2other), code: CodeVersionConflict, version: 2, received: t0},
		{name: "HigherButInvalid_KeepsHeld", next: func() []byte { p := testPolicy(5); p.Keys[0].Kind = "x"; return encode(t, p) }(), code: CodeInvalidValue, version: 2, received: t0},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewStore(zap.NewNop(), nil)
			require.NoError(t, s.Apply(v2, t0))
			first := s.Current()
			changed := s.Changed()

			err := s.Apply(tc.next, t0.Add(time.Minute))
			assert.Equal(t, tc.code, codeOf(t, err))
			assert.Equal(t, tc.version, s.Current().Version)
			assert.True(t, tc.received.Equal(s.LastReceived()), "%v", s.LastReceived())
			assert.Equal(t, Status{Version: tc.version, Rejected: tc.code}, s.Status())
			assert.Equal(t, tc.version, s.Floor().Version)

			replaced := s.Current() != first
			assert.Equal(t, tc.version != 2, replaced)
			select {
			case <-changed:
				assert.True(t, replaced, "Changed closes only on a new snapshot")
			default:
				assert.False(t, replaced)
			}
		})
	}
}

func TestStoreRejectionWithNothingHeld(t *testing.T) {
	s := NewStore(zap.NewNop(), nil)
	err := s.Apply([]byte(`{}`), t0)
	assert.Equal(t, CodeSchemaVersion, codeOf(t, err))
	assert.Nil(t, s.Current())
	assert.True(t, s.LastReceived().IsZero())
	assert.Equal(t, Status{Rejected: CodeSchemaVersion}, s.Status())
	assert.Nil(t, s.Floor())
}

func TestStoreSnapshotLookups(t *testing.T) {
	s := NewStore(zap.NewNop(), nil)
	require.NoError(t, s.Apply(encode(t, testPolicy(1)), t0))
	snap := s.Current()

	k := snap.Key(sha256.Sum256([]byte(webKey)))
	require.NotNil(t, k)
	assert.Equal(t, webKeyID, k.ID)
	assert.Equal(t, orgA, k.OrgID)
	assert.Equal(t, SignalTraces|SignalLogs, k.Signals)
	assert.Equal(t, &Limit{Rate: 50, Burst: 100}, k.Limit)
	require.NotNil(t, k.Application)
	assert.Equal(t, "shop-web", k.Application.Name)
	assert.True(t, k.UsableAt(t0.Add(100*365*24*time.Hour)))

	android := snap.Key(sha256.Sum256([]byte("rum_pub_android")))
	require.NotNil(t, android)
	assert.True(t, android.UsableAt(time.Date(2026, 11, 3, 23, 59, 59, 0, time.UTC)))
	assert.False(t, android.UsableAt(time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)), "expiresAt not after now is refused")

	assert.Nil(t, snap.Key(sha256.Sum256([]byte("rum_pub_unknown"))))
	assert.Nil(t, snap.Key(sha256.Sum256([]byte("fs_secret"))).Application)
	assert.Equal(t, 3, snap.KeyCount())
	assert.Len(t, snap.Applications(), 2)
	assert.Equal(t, PlatformAndroid, snap.Application(droidApp).Platform)
	assert.Equal(t, time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC), snap.IssuedAt)
}

func TestStoreConcurrentReaders(t *testing.T) {
	s := NewStore(zap.NewNop(), nil)
	require.NoError(t, s.Apply(encode(t, testPolicy(1)), t0))
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				snap := s.Current()
				if k := snap.Key(sha256.Sum256([]byte(webKey))); k == nil || k.Application == nil {
					t.Error("a published snapshot is always complete")
					return
				}
			}
		}()
	}
	for v := uint64(2); v < 50; v++ {
		require.NoError(t, s.Apply(encode(t, testPolicy(v)), t0))
	}
	close(stop)
	wg.Wait()
}

func TestFileCacheStart(t *testing.T) {
	v3 := encode(t, testPolicy(3))
	testCases := []struct {
		name    string
		prepare func(t *testing.T, dir string)
		version uint64 // 0: no policy
		floor   uint64 // 0: no floor
	}{
		{name: "Empty_NoPolicyNoFloor", prepare: func(*testing.T, string) {}},
		{name: "Cached_Loaded", prepare: func(t *testing.T, dir string) { saveFile(t, dir, v3) }, version: 3, floor: 3},
		{
			name: "StateMissing_NoPolicyNoFloor",
			prepare: func(t *testing.T, dir string) {
				saveFile(t, dir, v3)
				require.NoError(t, os.Remove(filepath.Join(dir, StateFile)))
			},
		},
		{
			name:    "StateCorrupt_NoPolicyNoFloor",
			prepare: func(t *testing.T, dir string) { saveFile(t, dir, v3); writeFile(t, dir, StateFile, `{"version":3`) },
		},
		{
			name: "StateUnknownField_NoPolicyNoFloor",
			prepare: func(t *testing.T, dir string) {
				saveFile(t, dir, v3)
				writeFile(t, dir, StateFile, `{"version":3,"sha256":"`+hexSum(v3)+`","x":1}`)
			},
		},
		{
			name: "PolicyReplacedOnDisk_NoPolicyFloorKept",
			prepare: func(t *testing.T, dir string) {
				saveFile(t, dir, v3)
				writeFile(t, dir, PolicyFile, string(encode(t, testPolicy(1))))
			},
			floor: 3,
		},
		{
			name: "PolicyMissing_NoPolicyFloorKept",
			prepare: func(t *testing.T, dir string) {
				saveFile(t, dir, v3)
				require.NoError(t, os.Remove(filepath.Join(dir, PolicyFile)))
			},
			floor: 3,
		},
		{
			name: "CachedInvalid_NoPolicyFloorKept",
			prepare: func(t *testing.T, dir string) {
				p := testPolicy(3)
				p.Keys[1].OrgID = orgB
				data := encode(t, p)
				require.NoError(t, NewFileCache(dir).Save(data, Floor{Version: 3, SHA256: sha256.Sum256(data)}))
			},
			floor: 3,
		},
		{
			name: "CachedVersionNotFloor_NoPolicy",
			prepare: func(t *testing.T, dir string) {
				data := encode(t, testPolicy(2))
				require.NoError(t, NewFileCache(dir).Save(data, Floor{Version: 3, SHA256: sha256.Sum256(data)}))
			},
			floor: 3,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tc.prepare(t, dir)
			core, logs := observer.New(zap.InfoLevel)
			s := NewStore(zap.New(core), NewFileCache(dir))
			s.Start()

			if tc.version == 0 {
				assert.Nil(t, s.Current())
				assert.NotEmpty(t, logs.FilterLevelExact(zap.ErrorLevel).All(), "starting without a policy is logged as an error")
			} else {
				require.NotNil(t, s.Current())
				assert.Equal(t, tc.version, s.Current().Version)
				assert.False(t, s.LastReceived().IsZero())
			}
			if tc.floor == 0 {
				assert.Nil(t, s.Floor())
			} else {
				require.NotNil(t, s.Floor())
				assert.Equal(t, tc.floor, s.Floor().Version)
				// Anti-rollback survives the restart.
				assert.Equal(t, CodeLowerVersion, codeOf(t, s.Apply(encode(t, testPolicy(2)), t0)))
			}
			for _, e := range logs.All() {
				for _, f := range e.Context {
					assert.NotContains(t, f.String, verifier(webKey), "cache logs carry no verifier")
				}
			}
		})
	}
}

func TestFileCacheSameVersionAfterRejectedCache(t *testing.T) {
	dir := t.TempDir()
	v3 := encode(t, testPolicy(3))
	saveFile(t, dir, v3)
	writeFile(t, dir, PolicyFile, `{}`)
	s := NewStore(zap.NewNop(), NewFileCache(dir))
	s.Start()
	require.Nil(t, s.Current())

	// The backend's push of the floor revision is applied, not a no-op.
	require.NoError(t, s.Apply(v3, t0))
	require.NotNil(t, s.Current())
	assert.Equal(t, uint64(3), s.Current().Version)
	b, err := os.ReadFile(filepath.Join(dir, PolicyFile))
	require.NoError(t, err)
	assert.Equal(t, v3, b)
}

func TestFileCacheWritesAtomicallyWithMode0600(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "storage")
	s := NewStore(zap.NewNop(), NewFileCache(dir))
	v1 := encode(t, testPolicy(1))
	require.NoError(t, s.Apply(v1, t0))
	require.NoError(t, s.Apply(encode(t, testPolicy(2)), t0))

	for _, name := range []string{PolicyFile, StateFile} {
		info, err := os.Stat(filepath.Join(dir, name))
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), info.Mode().Perm(), name)
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 2, "no temporary file is left")

	state, err := os.ReadFile(filepath.Join(dir, StateFile))
	require.NoError(t, err)
	assert.JSONEq(t, `{"version":2,"sha256":"`+hexSum(encode(t, testPolicy(2)))+`"}`, string(state))

	restarted := NewStore(zap.NewNop(), NewFileCache(dir))
	restarted.Start()
	require.NotNil(t, restarted.Current())
	assert.Equal(t, uint64(2), restarted.Current().Version)
}

type failingCache struct{}

func (failingCache) Load() ([]byte, *Floor, time.Time, error) { return nil, nil, time.Time{}, nil }
func (failingCache) Save([]byte, Floor) error                 { return errors.New("disk full") }

func TestStoreAppliesWhenCacheFails(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	s := NewStore(zap.New(core), failingCache{})
	require.NoError(t, s.Apply(encode(t, testPolicy(1)), t0))
	require.NotNil(t, s.Current(), "a policy that cannot be cached still replaces the one held")
	assert.Len(t, logs.FilterMessage("Ingest policy applied but not cached").All(), 1)
}

func saveFile(t *testing.T, dir string, data []byte) {
	t.Helper()
	require.NoError(t, NewFileCache(dir).Save(data, Floor{Version: mustParse(t, data).Version, SHA256: sha256.Sum256(data)}))
}

func mustParse(t *testing.T, data []byte) *Policy {
	t.Helper()
	p, err := Parse(data)
	require.NoError(t, err)
	return p
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
}

// Codex review on #43: an explicit expiresAt at Go's zero time is an
// expired key, not a key without expiry.
func TestZeroTimeExpiryIsExpired(t *testing.T) {
	p := testPolicy(1)
	p.Keys[0].ExpiresAt = ptr("0001-01-01T00:00:00Z")
	s := NewStore(zap.NewNop(), nil)
	require.NoError(t, s.Apply(encode(t, p), t0))
	k := s.Current().Key(sha256.Sum256([]byte(webKey)))
	require.NotNil(t, k)
	assert.False(t, k.UsableAt(t0))

	p.Version, p.Keys[0].ExpiresAt = 2, nil
	require.NoError(t, s.Apply(encode(t, p), t0))
	assert.True(t, s.Current().Key(sha256.Sum256([]byte(webKey))).UsableAt(t0), "absent expiresAt never expires")
}

// Codex review on #43: a policy applied but not cached is cached again on
// the next identical push, so a restart cannot bring back an older one.
func TestStoreRetriesFailedCache(t *testing.T) {
	cache := &flakyCache{FileCache: NewFileCache(t.TempDir()), fail: true}
	s := NewStore(zap.NewNop(), cache)
	require.NoError(t, s.Apply(encode(t, testPolicy(1)), t0))
	v2 := encode(t, testPolicy(2))
	require.NoError(t, s.Apply(v2, t0))
	assert.Equal(t, uint64(2), s.Current().Version)

	cache.fail = false
	require.NoError(t, s.Apply(v2, t0.Add(time.Minute)))
	assert.Equal(t, 1, cache.saves, "the identical push saves what failed")
	require.NoError(t, s.Apply(v2, t0.Add(2*time.Minute)))
	assert.Equal(t, 1, cache.saves, "once saved, identical pushes write nothing")

	restarted := NewStore(zap.NewNop(), cache.FileCache)
	restarted.Start()
	require.NotNil(t, restarted.Current())
	assert.Equal(t, uint64(2), restarted.Current().Version)
}

type flakyCache struct {
	*FileCache
	fail  bool
	saves int
}

func (c *flakyCache) Save(data []byte, floor Floor) error {
	if c.fail {
		return errors.New("disk full")
	}
	c.saves++
	return c.FileCache.Save(data, floor)
}

func hexSum(b []byte) string {
	sum := sha256.Sum256(b)
	return verifierHex(sum)
}

func verifierHex(sum [32]byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 64)
	for i, c := range sum {
		out[2*i], out[2*i+1] = digits[c>>4], digits[c&0xf]
	}
	return string(out)
}
