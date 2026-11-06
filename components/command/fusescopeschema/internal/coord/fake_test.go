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
	// onReplicaLeases runs before each ReplicaLeases; onRenew before each
	// TryRenew.
	onReplicaLeases func(ctx context.Context) error
	onRenew         func()
	failNext        error
	// listErr fails Replicas; listed, when set, is what Replicas returns.
	listErr error
	listed  []string
	// absent and differs override, per replica, the tables created on the
	// cluster: absent tables are not there, differing ones show another
	// layout. createSkips names replicas a CreateLayout does not reach.
	absent      map[string]map[string]bool
	differs     map[string]map[string]TableLayout
	createSkips map[string]bool
	// dropLeaseInsert loses the lease row's insert.
	dropLeaseInsert bool
	// creates and inserts record the bootstrap's writes.
	creates []string
	inserts []string
}

type fakeLag struct {
	lease Lease
	reads int
}

type fakeMap struct {
	lease   *Lease
	meta    map[string]string
	tables  map[string]bool
	started map[uint64]bool // migrations started by a lease token
}

func newFakeStore(replicas ...string) *fakeStore {
	s := &fakeStore{
		nowMs: 1_000_000, maps: map[string]*fakeMap{"/shared": {meta: map[string]string{}, tables: map[string]bool{}, started: map[uint64]bool{}}},
		replicas: map[string]string{}, down: map[string]bool{}, lag: map[string]*fakeLag{},
		absent: map[string]map[string]bool{}, differs: map[string]map[string]TableLayout{}, createSkips: map[string]bool{},
	}
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
	if hook := v.s.onRenew; hook != nil {
		hook()
	}
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
	if err := v.check(); err != nil {
		return nil, err
	}
	if v.s.listErr != nil {
		return nil, v.s.listErr
	}
	if v.s.listed != nil {
		return v.s.listed, nil
	}
	var out []string
	for r := range v.s.replicas {
		out = append(out, r)
	}
	return out, nil
}

func (v *fakeView) ReplicaLeases(ctx context.Context) (map[string]Lease, error) {
	if hook := v.s.onReplicaLeases; hook != nil {
		if err := hook(ctx); err != nil {
			return nil, err
		}
	}
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

func (v *fakeView) ReplicaLayouts(context.Context) (map[string]map[string]TableLayout, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.check(); err != nil {
		return nil, err
	}
	out := map[string]map[string]TableLayout{}
	for r, path := range v.s.replicas {
		if v.s.down[r] {
			return nil, errors.New("replica " + r + " does not answer")
		}
		tables := map[string]TableLayout{}
		for t := range v.s.maps[path].tables {
			if !v.s.absent[r][t] {
				tables[t] = Layout(t)
			}
		}
		for t, l := range v.s.differs[r] {
			tables[t] = l
		}
		out[r] = tables
	}
	return out, nil
}

func (v *fakeView) CreateLayout(_ context.Context, missing []string) error {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.check(); err != nil {
		return err
	}
	m := v.m()
	v.s.creates = append(v.s.creates, missing...)
	for _, t := range missing {
		existed := m.tables[t]
		m.tables[t] = true
		for r := range v.s.replicas {
			switch {
			case !v.s.createSkips[r]:
				delete(v.s.absent[r], t)
			case !existed:
				if v.s.absent[r] == nil {
					v.s.absent[r] = map[string]bool{}
				}
				v.s.absent[r][t] = true
			}
		}
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
	if v.m().lease == nil && !v.s.dropLeaseInsert {
		v.m().lease = &Lease{}
	}
	return nil
}

// ReplicaMeta fails, as clusterAllReplicas does, unless every replica has
// the meta table; each replica reads its own coordination path.
func (v *fakeView) ReplicaMeta(_ context.Context, name string) (map[string]string, error) {
	v.s.mu.Lock()
	defer v.s.mu.Unlock()
	if err := v.check(); err != nil {
		return nil, err
	}
	out := map[string]string{}
	for r, path := range v.s.replicas {
		m := v.s.maps[path]
		if v.s.down[r] || !m.tables["meta"] || v.s.absent[r]["meta"] {
			return nil, errors.New("replica " + r + " cannot read " + Database + ".meta")
		}
		if val, ok := m.meta[name]; ok {
			out[r] = val
		}
	}
	return out, nil
}
