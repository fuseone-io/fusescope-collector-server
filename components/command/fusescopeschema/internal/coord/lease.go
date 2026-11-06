// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package coord holds the coordination of fusescope-schema: the bootstrap of
// its own layout, the cluster identity and the migration lease
// (docs/spec/fusescope-schema.md §3, §6). It depends on a Store, whose SQL
// implementation runs the conditional KeeperMap writes the spike verified
// (§11).
package coord

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

// Lease is the single lease row.
type Lease struct {
	Owner     string
	Token     uint64
	ExpiresMs int64
}

// Store is the coordination storage as seen from the server the migrator is
// connected to. Every write is conditional and reports no outcome: callers
// learn it by reading back (§11).
type Store interface {
	ServerNowMs(ctx context.Context) (int64, error)
	ReadLease(ctx context.Context) (Lease, bool, error)
	TryAcquire(ctx context.Context, owner string, readToken uint64, forMs int64) error
	TryRenew(ctx context.Context, owner string, token uint64, forMs int64) error
	Release(ctx context.Context, owner string, token uint64) error
	// StartedWithToken reports a migration left started by a lease generation.
	StartedWithToken(ctx context.Context, token uint64) (bool, error)
	// Replicas lists the cluster's replicas; ReplicaLeases and
	// ReplicaIdentities read every one of them, failing if one does not answer.
	Replicas(ctx context.Context) ([]string, error)
	ReplicaLeases(ctx context.Context) (map[string]Lease, error)
	ReplicaIdentities(ctx context.Context) (map[string]string, error)
}

var (
	// ErrLeaseTimeout: another migrator held the lease for all of AcquireTimeout.
	ErrLeaseTimeout = errors.New("the lease stayed held by another migrator")
	// ErrInterrupted: the lease expired over a started migration (§7.3).
	ErrInterrupted = errors.New("a migration was interrupted: its lease expired while it was started")
	// ErrLeaseLost: the holder could not renew in time; it must issue no
	// further statement (§6).
	ErrLeaseLost = errors.New("the lease was lost")
	// ErrDivergent: a replica shows another identity or lease, or does not
	// answer (§3.2, §7.6).
	ErrDivergent = errors.New("a replica is out of step with the cluster's coordination")
)

// Config holds the timings of §6 and §3.2. Zero values take the defaults.
type Config struct {
	Owner          string
	LeaseFor       time.Duration // 5 min
	RenewEvery     time.Duration // 1 min
	LostAfter      time.Duration // 4 min
	AcquireTimeout time.Duration // 10 min
	RetryEvery     time.Duration // 2 s
	RereadEvery    time.Duration // 500 ms
	RereadFor      time.Duration // 10 s
}

func (c Config) withDefaults() Config {
	def := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	def(&c.LeaseFor, 5*time.Minute)
	def(&c.RenewEvery, time.Minute)
	def(&c.LostAfter, 4*time.Minute)
	def(&c.AcquireTimeout, 10*time.Minute)
	def(&c.RetryEvery, 2*time.Second)
	def(&c.RereadEvery, 500*time.Millisecond)
	def(&c.RereadFor, 10*time.Second)
	return c
}

// Coordinator holds the lease for one migrator.
type Coordinator struct {
	store Store
	cfg   Config
	sleep func(ctx context.Context, d time.Duration) error
	now   func() time.Time

	mu          sync.Mutex
	held        Lease
	confirmedAt time.Time
	lost        error
}

func New(store Store, cfg Config) *Coordinator {
	return &Coordinator{store: store, cfg: cfg.withDefaults(), sleep: sleepCtx, now: time.Now}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// CheckIdentity requires the same non-empty cluster_id on every replica (§3.2).
func (c *Coordinator) CheckIdentity(ctx context.Context) error {
	ids, err := c.store.ReplicaIdentities(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrDivergent, err)
	}
	want := ""
	for _, r := range c.replicasOr(ctx, ids) {
		id, ok := ids[r]
		switch {
		case !ok || id == "":
			return fmt.Errorf("%w: replica %s has no cluster identity", ErrDivergent, r)
		case want == "":
			want = id
		case id != want:
			return fmt.Errorf("%w: replica %s has cluster identity %q, others %q", ErrDivergent, r, id, want)
		}
	}
	return nil
}

// replicasOr lists the replicas in a stable order, from the store, falling
// back to the keys of a reading.
func (c *Coordinator) replicasOr(ctx context.Context, read map[string]string) []string {
	rs, err := c.store.Replicas(ctx)
	if err != nil || len(rs) == 0 {
		rs = nil
		for r := range read {
			rs = append(rs, r)
		}
	}
	slices.Sort(rs)
	return rs
}

// Acquire takes the lease (§6): the identity first, then a conditional update
// on a free or expired lease, confirmed by reading back, then the same lease
// on every replica.
func (c *Coordinator) Acquire(ctx context.Context) (Lease, error) {
	if err := c.CheckIdentity(ctx); err != nil {
		return Lease{}, err
	}
	start := c.now()
	for {
		l, ok, err := c.store.ReadLease(ctx)
		if err != nil {
			return Lease{}, err
		}
		if !ok {
			return Lease{}, errors.New("the lease row is missing: run the bootstrap first")
		}
		now, err := c.store.ServerNowMs(ctx)
		if err != nil {
			return Lease{}, err
		}
		if l.Owner == "" || l.ExpiresMs <= now {
			if l.Owner != "" {
				started, err := c.store.StartedWithToken(ctx, l.Token)
				if err != nil {
					return Lease{}, err
				}
				if started {
					return Lease{}, fmt.Errorf("%w (owner %s, token %d)", ErrInterrupted, l.Owner, l.Token)
				}
			}
			if err := c.store.TryAcquire(ctx, c.cfg.Owner, l.Token, c.cfg.LeaseFor.Milliseconds()); err != nil {
				return Lease{}, err
			}
			got, _, err := c.store.ReadLease(ctx)
			if err != nil {
				return Lease{}, err
			}
			if got.Owner == c.cfg.Owner && got.Token == l.Token+1 {
				c.mu.Lock()
				c.held, c.confirmedAt, c.lost = got, c.now(), nil
				c.mu.Unlock()
				if err := c.compareReplicas(ctx, got); err != nil {
					c.loseWith(err)
					_ = c.Release(ctx)
					return Lease{}, err
				}
				return got, nil
			}
		}
		if c.now().Sub(start) >= c.cfg.AcquireTimeout {
			return Lease{}, ErrLeaseTimeout
		}
		if err := c.sleep(ctx, c.cfg.RetryEvery); err != nil {
			return Lease{}, err
		}
	}
}

// Renew extends the held lease, confirms it by reading back and compares every
// replica. Any failure loses the lease for good.
func (c *Coordinator) Renew(ctx context.Context) error {
	c.mu.Lock()
	held, lost := c.held, c.lost
	c.mu.Unlock()
	if lost != nil {
		return lost
	}
	// The renewal took if the lease now expires at least LeaseFor after the
	// server's time before it: an expired lease keeps its old expiry.
	before, err := c.store.ServerNowMs(ctx)
	if err != nil {
		return err // a failed call is retried by the renewal loop until LostAfter
	}
	if err := c.store.TryRenew(ctx, held.Owner, held.Token, c.cfg.LeaseFor.Milliseconds()); err != nil {
		return err
	}
	got, _, err := c.store.ReadLease(ctx)
	if err != nil {
		return err
	}
	if got.Owner != held.Owner || got.Token != held.Token || got.ExpiresMs < before+c.cfg.LeaseFor.Milliseconds() {
		return c.loseWith(fmt.Errorf("%w: the renewal did not take (lease now %+v)", ErrLeaseLost, got))
	}
	c.mu.Lock()
	c.held, c.confirmedAt = got, c.now()
	c.mu.Unlock()
	if err := c.compareReplicas(ctx, got); err != nil {
		return c.loseWith(err)
	}
	return nil
}

// Valid is checked before every statement (§5.1): the lease is held, renewed
// within LostAfter, and shown by every replica.
func (c *Coordinator) Valid(ctx context.Context) error {
	c.mu.Lock()
	held, lost, confirmedAt := c.held, c.lost, c.confirmedAt
	c.mu.Unlock()
	if lost != nil {
		return lost
	}
	if held.Owner == "" {
		return fmt.Errorf("%w: not held", ErrLeaseLost)
	}
	if c.now().Sub(confirmedAt) >= c.cfg.LostAfter {
		return c.loseWith(fmt.Errorf("%w: no confirmed renewal for %s", ErrLeaseLost, c.cfg.LostAfter))
	}
	if err := c.compareReplicas(ctx, held); err != nil {
		return c.loseWith(err)
	}
	return nil
}

// Release frees the lease if it is still this holder's.
func (c *Coordinator) Release(ctx context.Context) error {
	c.mu.Lock()
	held := c.held
	c.held = Lease{}
	c.mu.Unlock()
	if held.Owner == "" {
		return nil
	}
	return c.store.Release(ctx, held.Owner, held.Token)
}

// Held returns the held lease, the zero Lease if none.
func (c *Coordinator) Held() Lease {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.held
}

func (c *Coordinator) loseWith(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lost == nil {
		c.lost = err
	}
	return c.lost
}

// compareReplicas requires every replica to show exactly want, re-reading a
// differing one every RereadEvery for up to RereadFor (§3.2).
func (c *Coordinator) compareReplicas(ctx context.Context, want Lease) error {
	deadline := c.now().Add(c.cfg.RereadFor)
	for {
		leases, err := c.store.ReplicaLeases(ctx)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrDivergent, err)
		}
		bad := ""
		for _, r := range c.replicasOr(ctx, keysOf(leases)) {
			if got, ok := leases[r]; !ok || got != want {
				bad = fmt.Sprintf("replica %s shows lease %+v, want %+v", r, got, want)
				break
			}
		}
		if bad == "" {
			return nil
		}
		if !c.now().Before(deadline) {
			return fmt.Errorf("%w: %s", ErrDivergent, bad)
		}
		if err := c.sleep(ctx, c.cfg.RereadEvery); err != nil {
			return err
		}
	}
}

func keysOf(m map[string]Lease) map[string]string {
	out := make(map[string]string, len(m))
	for k := range m {
		out[k] = ""
	}
	return out
}

// RunRenewals renews every RenewEvery until ctx ends or the lease is lost.
// A failed call is retried at the next tick; Valid reports the loss once
// LostAfter passes without a confirmed renewal.
func (c *Coordinator) RunRenewals(ctx context.Context) {
	for {
		if err := c.sleep(ctx, c.cfg.RenewEvery); err != nil {
			return
		}
		if err := c.Renew(ctx); errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrDivergent) {
			return
		}
	}
}
