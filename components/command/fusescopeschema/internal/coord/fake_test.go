// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package coord

import (
	"context"
	"errors"
	"sync"
)

// fakeStore models the KeeperMap semantics the spike observed (spec §11): one
// shared map per coordination path, conditional updates that change nothing
// when their condition fails and report no error, and per-replica views that
// can diverge when a replica is attached elsewhere.
type fakeStore struct {
	mu       sync.Mutex
	nowMs    int64
	maps     map[string]*fakeMap // by coordination path
	replicas map[string]string   // replica -> coordination path
	self     string              // the replica this store is connected to
	down     map[string]bool     // replicas that do not answer
	// lagging, when set, makes a replica's lease reads return lag[replica]
	// for the next n reads.
	lag map[string]*fakeLag
	// onAcquire runs inside TryAcquire before the condition, to race it.
	onAcquire func()
	failNext  error
	// creates and inserts record the bootstrap's writes.
	creates []string
	inserts []string
}

type fakeLag struct {
	lease Lease
	reads int
}

type fakeMap struct {
	lease    *Lease
	meta     map[string]string
	tables   map[string]bool
	started  map[uint64]bool // migrations started by a lease token
	creating bool
	columns  map[string][]string // overrides layoutColumns, to model a changed table
}

func newFakeStore(replicas ...string) *fakeStore {
	s := &fakeStore{nowMs: 1_000_000, maps: map[string]*fakeMap{"/shared": {meta: map[string]string{}, tables: map[string]bool{}, started: map[uint64]bool{}}}, replicas: map[string]string{}, down: map[string]bool{}, lag: map[string]*fakeLag{}}
	for _, r := range replicas {
		s.replicas[r] = "/shared"
	}
	s.self = replicas[0]
	return s
}

// as returns a store connected to another replica, sharing the same state.
func (s *fakeStore) as(replica string) *fakeView { return &fakeView{s: s, self: replica} }

func (s *fakeStore) advance(ms int64) {
	s.mu.Lock()
	s.nowMs += ms
	s.mu.Unlock()
}

// fakeView is one replica's connection to the fake cluster.
type fakeView struct {
	s    *fakeStore
	self string
}

func (v *fakeView) m() *fakeMap { return v.s.maps[v.s.replicas[v.self]] }

func (v *fakeView) check() error {
	if v.s.failNext != nil {
		err := v.s.failNext
		v.s.failNext = nil
		return err
	}
	if v.s.down[v.self] {
		return errors.New("replica down")
	}
	return nil
}

func (v *fakeView) ServerNowMs(context.Context) (int64, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	return v.s.nowMs, v.check()
}

func (v *fakeView) ReadLease(context.Context) (Lease, bool, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.check(); err != nil {
		return Lease{}, false, err
	}
	if l := v.m().lease; l != nil {
		return *l, true, nil
	}
	return Lease{}, false, nil
}

func (v *fakeView) TryAcquire(_ context.Context, owner string, readToken uint64, forMs int64) error {
	if hook := v.s.onAcquire; hook != nil {
		v.s.onAcquire = nil
		hook()
	}
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.check(); err != nil {
		return err
	}
	l := v.m().lease
	if l != nil && l.Token == readToken && (l.Owner == "" || l.ExpiresMs <= v.s.nowMs) {
		*l = Lease{Owner: owner, Token: l.Token + 1, ExpiresMs: v.s.nowMs + forMs}
	}
	return nil
}

func (v *fakeView) TryRenew(_ context.Context, owner string, token uint64, forMs int64) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.check(); err != nil {
		return err
	}
	l := v.m().lease
	if l != nil && l.Owner == owner && l.Token == token && l.ExpiresMs > v.s.nowMs {
		l.ExpiresMs = v.s.nowMs + forMs
	}
	return nil
}

func (v *fakeView) Release(_ context.Context, owner string, token uint64) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.check(); err != nil {
		return err
	}
	l := v.m().lease
	if l != nil && l.Owner == owner && l.Token == token {
		l.Owner = ""
	}
	return nil
}

func (v *fakeView) Replicas(context.Context) ([]string, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	var out []string
	for r := range v.s.replicas {
		out = append(out, r)
	}
	return out, v.check()
}

func (v *fakeView) ReplicaLeases(context.Context) (map[string]Lease, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.check(); err != nil {
		return nil, err
	}
	out := map[string]Lease{}
	for r, path := range v.s.replicas {
		if v.s.down[r] {
			return nil, errors.New("replica " + r + " does not answer")
		}
		if lag := v.s.lag[r]; lag != nil && lag.reads > 0 {
			lag.reads--
			out[r] = lag.lease
			continue
		}
		if l := v.s.maps[path].lease; l != nil {
			out[r] = *l
		}
	}
	return out, nil
}

func (v *fakeView) ReplicaIdentities(context.Context) (map[string]string, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.check(); err != nil {
		return nil, err
	}
	out := map[string]string{}
	for r, path := range v.s.replicas {
		if v.s.down[r] {
			return nil, errors.New("replica " + r + " does not answer")
		}
		out[r] = v.s.maps[path].meta["cluster_id"]
	}
	return out, nil
}

func (v *fakeView) StartedWithToken(_ context.Context, token uint64) (bool, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	return v.m().started[token], v.check()
}

// Layout methods, used by the bootstrap.

func (v *fakeView) LayoutTables(context.Context) (map[string][]string, bool, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.check(); err != nil {
		return nil, false, err
	}
	m := v.m()
	if len(m.tables) == 0 && !m.creating {
		return nil, false, nil
	}
	out := map[string][]string{}
	for t := range m.tables {
		if cols, ok := m.columns[t]; ok {
			out[t] = cols
			continue
		}
		out[t] = layoutColumns[t]
	}
	return out, true, nil
}

func (v *fakeView) CreateLayout(_ context.Context, missing []string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.check(); err != nil {
		return err
	}
	m := v.m()
	m.creating = true
	v.s.creates = append(v.s.creates, missing...)
	for _, t := range missing {
		m.tables[t] = true
	}
	return nil
}

func (v *fakeView) InsertMetaIfAbsent(_ context.Context, name, value string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.check(); err != nil {
		return err
	}
	v.s.inserts = append(v.s.inserts, "meta:"+name)
	if _, ok := v.m().meta[name]; !ok {
		v.m().meta[name] = value
	}
	return nil
}

func (v *fakeView) InsertLeaseIfAbsent(context.Context) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.check(); err != nil {
		return err
	}
	v.s.inserts = append(v.s.inserts, "lease")
	if v.m().lease == nil {
		v.m().lease = &Lease{}
	}
	return nil
}

func (v *fakeView) Meta(_ context.Context, name string) (string, bool, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	val, ok := v.m().meta[name]
	return val, ok, v.check()
}
