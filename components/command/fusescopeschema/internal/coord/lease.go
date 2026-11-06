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
// learn it by reading back (§11). Replicas are keyed by
// "hostName()/serverUUID()", unique per server.
type Store interface {
	ServerNowMs(ctx context.Context) (int64, error)
	ReadLease(ctx context.Context) (Lease, bool, error)
	TryAcquire(ctx context.Context, owner string, readToken uint64, forMs int64) error
	TryRenew(ctx context.Context, owner string, token uint64, forMs int64) error
	Release(ctx context.Context, owner string, token uint64) error
	// StartedWithToken reports a migration left started by a lease generation.
	StartedWithToken(ctx context.Context, token uint64) (bool, error)
	// Replicas lists every replica of the cluster; it fails unless each one
	// answers. ReplicaLeases and ReplicaIdentities read every replica.
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
	// ErrDivergent: a replica shows another identity or lease, or the replicas
	// cannot all be read (§3.2, §7.6).
	ErrDivergent = errors.New("a replica is out of step with the cluster's coordination")
	// ErrConfig: the timings or the owner would break the fencing.
	ErrConfig = errors.New("invalid coordination configuration")
)

// Config holds the timings of §6 and §3.2. Zero durations take the defaults.
type Config struct {
	Owner          string
	LeaseFor       time.Duration // 5 min
	RenewEvery     time.Duration // 1 min
	LostAfter      time.Duration // 4 min
	AcquireTimeout time.Duration // 10 min
	RetryEvery     time.Duration // 2 s
	RereadEvery    time.Duration // 500 ms
	RereadFor      time.Duration // 10 s
	// ClockSkew is the largest difference allowed between the replicas'
	// clocks (§2.1); the lease must outlast LostAfter by it.
	ClockSkew time.Duration // 30 s
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
	def(&c.ClockSkew, 30*time.Second)
	return c
}

// Validate requires an owner, positive durations, renewals well within the
// loss deadline, and a holder that stops before its lease can expire on any
// server: a Valid that started just before LostAfter may compare replicas for
// RereadFor, and the servers' clocks may differ by ClockSkew.
func (c Config) Validate() error {
	var errs []error
	if c.Owner == "" {
		errs = append(errs, errors.New("owner must not be empty"))
	}
	for name, d := range map[string]time.Duration{
		"LeaseFor": c.LeaseFor, "RenewEvery": c.RenewEvery, "LostAfter": c.LostAfter, "AcquireTimeout": c.AcquireTimeout,
		"RetryEvery": c.RetryEvery, "RereadEvery": c.RereadEvery, "RereadFor": c.RereadFor, "ClockSkew": c.ClockSkew,
	} {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive", name))
		}
	}
	if c.RenewEvery >= c.LostAfter {
		errs = append(errs, fmt.Errorf("RenewEvery (%s) must be shorter than LostAfter (%s)", c.RenewEvery, c.LostAfter))
	}
	if c.LostAfter+c.RereadFor+c.ClockSkew > c.LeaseFor {
		errs = append(errs, fmt.Errorf("LostAfter + RereadFor + ClockSkew (%s) must not exceed LeaseFor (%s)", c.LostAfter+c.RereadFor+c.ClockSkew, c.LeaseFor))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("%w: %w", ErrConfig, err)
	}
	return nil
}

// Coordinator holds the lease for one migrator. Its operations are
// serialized: a renewal never runs during a Valid's comparison, so a
// comparison always checks the lease it read.
type Coordinator struct {
	store Store
	cfg   Config
	sleep func(ctx context.Context, d time.Duration) error
	now   func() time.Time

	op sync.Mutex // serializes Acquire, Renew, Valid and Release

	mu          sync.Mutex // guards the fields below, for Held
	held        Lease
	confirmedAt time.Time
	lost        error
}

func New(store Store, cfg Config) (*Coordinator, error) {
	cfg = cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Coordinator{store: store, cfg: cfg, sleep: sleepCtx, now: time.Now}, nil
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

// replicas lists every replica, sorted; failing to enumerate them is terminal.
func (c *Coordinator) replicas(ctx context.Context) ([]string, error) {
	return listReplicas(ctx, c.store)
}

func listReplicas(ctx context.Context, s interface {
	Replicas(context.Context) ([]string, error)
}) ([]string, error) {
	rs, err := s.Replicas(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: enumerating the replicas: %v", ErrDivergent, err)
	}
	if len(rs) == 0 {
		return nil, fmt.Errorf("%w: the cluster lists no replica", ErrDivergent)
	}
	rs = slices.Clone(rs)
	slices.Sort(rs)
	for i := 1; i < len(rs); i++ {
		if rs[i] == rs[i-1] {
			return nil, fmt.Errorf("%w: replica %s answered twice: one server stands for two replicas", ErrDivergent, rs[i])
		}
	}
	return rs, nil
}

// matchReplicas requires read to hold exactly the replicas rs, each passing ok.
func matchReplicas[V any](rs []string, read map[string]V, ok func(string, V) error) error {
	for _, r := range rs {
		v, found := read[r]
		if !found {
			return fmt.Errorf("replica %s was not read", r)
		}
		if err := ok(r, v); err != nil {
			return err
		}
	}
	for r := range read {
		if !slices.Contains(rs, r) {
			return fmt.Errorf("replica %s answered but is not listed in the cluster", r)
		}
	}
	return nil
}

// CheckIdentity requires the same non-empty cluster_id on every replica (§3.2).
func (c *Coordinator) CheckIdentity(ctx context.Context) error {
	c.op.Lock()
	defer c.op.Unlock()
	return c.checkIdentity(ctx)
}

func (c *Coordinator) checkIdentity(ctx context.Context) error {
	rs, err := c.replicas(ctx)
	if err != nil {
		return err
	}
	ids, err := c.store.ReplicaIdentities(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrDivergent, err)
	}
	want := ids[rs[0]]
	if err := matchReplicas(rs, ids, func(r, id string) error {
		if id == "" || id != want {
			return fmt.Errorf("replica %s has cluster identity %q, %s has %q", r, id, rs[0], want)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("%w: %v", ErrDivergent, err)
	}
	return nil
}

// Acquire takes the lease (§6): the identity first, then a conditional update
// on a free or expired lease, confirmed by reading back, then the same lease
// on every replica. All of it within AcquireTimeout.
func (c *Coordinator) Acquire(ctx context.Context) (Lease, error) {
	c.op.Lock()
	defer c.op.Unlock()
	ctx, cancel := context.WithTimeout(ctx, c.cfg.AcquireTimeout+c.cfg.RereadFor)
	defer cancel()
	if err := c.checkIdentity(ctx); err != nil {
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
				c.setHeld(got)
				if err := c.compareReplicas(ctx, got); err != nil {
					c.loseWith(err)
					c.release(ctx)
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

func (c *Coordinator) setHeld(l Lease) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.held, c.confirmedAt, c.lost = l, c.now(), nil
}

// Renew extends the held lease, confirms it by reading back and compares every
// replica. Any failure loses the lease for good (§6).
func (c *Coordinator) Renew(ctx context.Context) error {
	c.op.Lock()
	defer c.op.Unlock()
	held, _, lost := c.state()
	if lost != nil {
		return lost
	}
	if held.Owner == "" {
		return fmt.Errorf("%w: not held", ErrLeaseLost)
	}
	ctx, cancel := context.WithTimeout(ctx, c.cfg.RenewEvery+c.cfg.RereadFor)
	defer cancel()
	// The renewal took if the lease now expires at least LeaseFor after the
	// server's time before it: an expired lease keeps its old expiry.
	before, err := c.store.ServerNowMs(ctx)
	if err != nil {
		return c.loseWith(fmt.Errorf("%w: renewal: %v", ErrLeaseLost, err))
	}
	if err := c.store.TryRenew(ctx, held.Owner, held.Token, c.cfg.LeaseFor.Milliseconds()); err != nil {
		return c.loseWith(fmt.Errorf("%w: renewal: %v", ErrLeaseLost, err))
	}
	got, _, err := c.store.ReadLease(ctx)
	if err != nil {
		return c.loseWith(fmt.Errorf("%w: renewal read-back: %v", ErrLeaseLost, err))
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

func (c *Coordinator) state() (Lease, time.Time, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.held, c.confirmedAt, c.lost
}

// Valid is checked before every statement (§5.1): the lease is held, renewed
// within LostAfter, and shown by every replica.
func (c *Coordinator) Valid(ctx context.Context) error {
	c.op.Lock()
	defer c.op.Unlock()
	held, confirmedAt, lost := c.state()
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
	c.op.Lock()
	defer c.op.Unlock()
	return c.release(ctx)
}

func (c *Coordinator) release(ctx context.Context) error {
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
// differing one every RereadEvery for up to RereadFor (§3.2). A read counts
// only if it ends within RereadFor, and store calls are bounded by it, so a
// comparison never succeeds past the margin the timings leave (Validate).
func (c *Coordinator) compareReplicas(ctx context.Context, want Lease) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.RereadFor)
	defer cancel()
	deadline := c.now().Add(c.cfg.RereadFor)
	for {
		rs, err := c.replicas(ctx)
		if err != nil {
			return err
		}
		leases, err := c.store.ReplicaLeases(ctx)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrDivergent, err)
		}
		mismatch := matchReplicas(rs, leases, func(r string, got Lease) error {
			if got != want {
				return fmt.Errorf("replica %s shows lease %+v, want %+v", r, got, want)
			}
			return nil
		})
		left := deadline.Sub(c.now())
		switch {
		case left < 0 && mismatch == nil:
			return fmt.Errorf("%w: the replicas showed the lease only after %s", ErrDivergent, c.cfg.RereadFor)
		case mismatch == nil:
			return nil
		case left <= 0:
			return fmt.Errorf("%w: %v", ErrDivergent, mismatch)
		}
		if err := c.sleep(ctx, min(c.cfg.RereadEvery, left)); err != nil {
			return fmt.Errorf("%w: %v (%v)", ErrDivergent, mismatch, err)
		}
	}
}

// RunRenewals renews every RenewEvery until ctx ends or a renewal fails; a
// failed renewal loses the lease (§6), which Valid then reports.
func (c *Coordinator) RunRenewals(ctx context.Context) {
	for {
		if err := c.sleep(ctx, c.cfg.RenewEvery); err != nil {
			return
		}
		if err := c.Renew(ctx); err != nil {
			return
		}
	}
}
