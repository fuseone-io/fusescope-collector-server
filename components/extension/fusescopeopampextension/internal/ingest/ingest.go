// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package ingest carries the ingest policy and report over OpAMP for a
// collector of the ingress role (ADR 0006, docs/spec/rum.md §10–11): it owns
// the policy store, receives `policy` and `reportAck` custom messages, and
// sends the receiver's `report` pages with a durable reportSequence.
package ingest // import "github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/ingest"

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/open-telemetry/opamp-go/client/types"
	"github.com/open-telemetry/opamp-go/protobufs"
	"go.uber.org/zap"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
)

// MaxMessageSize bounds every OpAMP message the client reads: the largest
// policy and a margin for the envelope; nothing larger is buffered.
const MaxMessageSize = ingestpolicy.MaxSize + 1<<20

// ackBuffer bounds the acknowledgements not yet taken by the receiver; one
// dropped only leaves its values pending for the next report.
const ackBuffer = 16

// sequenceFile holds "<instance UID hex> <last reportSequence>\n": the
// sequence is valid only with the UID it was used with (§10.4).
const (
	sequenceFile    = "report-sequence"
	maxSequenceFile = 64
)

var (
	errNotStarted = errors.New("reportSequence: the ingest side has not started")
	errExhausted  = errors.New("reportSequence: exhausted")
	// ErrReporterClaimed is returned to a second receiver: two would
	// interleave the pages of their reports and share acknowledgements.
	ErrReporterClaimed = errors.New("the ingest report is already sent by another receiver; a collector runs one rumreceiver")
)

// Identity is the agent's instance UID, which scopes reportSequence.
type Identity interface {
	InstanceUID() ([16]byte, error)
	RenewInstanceUID() ([16]byte, error)
}

// Sender is the OpAMP client's custom message sending.
type Sender interface {
	SendCustomMessage(*protobufs.CustomMessage) (chan struct{}, error)
}

// Ingest is the ingress side of the OpAMP extension. It holds no key and
// logs no message content.
type Ingest struct {
	logger *zap.Logger
	dir    string
	store  *ingestpolicy.Store
	now    func() time.Time

	sender    sendSlot
	acks      chan uint64
	connected chan struct{}

	// Policies are applied by one worker, latest first: a slot holds the
	// newest unapplied message, so at most two are in memory.
	slotMu  sync.Mutex
	pending []byte
	wake    chan struct{}

	seqMu sync.Mutex
	uid   [16]byte
	// lastSeq is the last sequence persisted for uid; seqErr, when set,
	// stops every report (fail closed).
	lastSeq uint64
	seqErr  error

	claimed atomic.Bool

	startOnce, closeOnce sync.Once
	done                 chan struct{}
	wg                   sync.WaitGroup
}

type sendSlot struct {
	mu sync.Mutex
	s  Sender
}

func (s *sendSlot) get() Sender  { s.mu.Lock(); defer s.mu.Unlock(); return s.s }
func (s *sendSlot) set(v Sender) { s.mu.Lock(); defer s.mu.Unlock(); s.s = v }

// New prepares the ingress side with its state under dir; nothing is read
// or written until Start, so validating a configuration touches nothing.
func New(logger *zap.Logger, dir string) *Ingest {
	return &Ingest{
		logger:    logger,
		dir:       dir,
		store:     ingestpolicy.NewStore(logger, ingestpolicy.NewFileCache(dir)),
		now:       time.Now,
		acks:      make(chan uint64, ackBuffer),
		connected: make(chan struct{}, 1),
		wake:      make(chan struct{}, 1),
		done:      make(chan struct{}),
		seqErr:    errNotStarted,
	}
}

// Start creates the state directory, loads the cached policy (§11.3) and
// the reportSequence state (§10.4), and starts the policy worker. It runs
// when the collector really starts, before the OpAMP client, which reads
// the instance UID after a renewal here.
func (i *Ingest) Start(id Identity) error {
	var err error
	i.startOnce.Do(func() {
		// Only this collector reads the policy cache and the sequence.
		if err = os.MkdirAll(i.dir, 0o700); err == nil {
			err = os.Chmod(i.dir, 0o700)
		}
		if err != nil {
			err = fmt.Errorf("ingest state %s: %w", i.dir, err)
			return
		}
		i.store.Start()
		i.loadSequence(id)
		i.wg.Add(1)
		go i.work()
	})
	return err
}

// Close stops the worker; a policy not yet applied is dropped, and the
// backend pushes it again.
func (i *Ingest) Close() {
	i.closeOnce.Do(func() { close(i.done) })
	i.wg.Wait()
}

// Source is the policy for receivers; it fails closed while none is held.
func (i *Ingest) Source() ingestpolicy.Source { return i.store }

// SetSender sets the OpAMP client once it has started.
func (i *Ingest) SetSender(s Sender) { i.sender.set(s) }

// CustomCapabilities is what the client declares: only the ingest one.
func CustomCapabilities() *protobufs.CustomCapabilities {
	return &protobufs.CustomCapabilities{Capabilities: []string{ingestpolicy.Capability}}
}

// OnConnect signals a (re)connection, after which a report is sent at once.
func (i *Ingest) OnConnect() {
	select {
	case i.connected <- struct{}{}:
	default: // one already pending
	}
}

// OnCustomMessage handles a custom message from the backend. It never
// blocks the client: a policy goes to the worker, an ack to the receiver.
// Anything else is ignored, and no content is ever logged.
func (i *Ingest) OnCustomMessage(m *protobufs.CustomMessage) {
	if m == nil || m.GetCapability() != ingestpolicy.Capability {
		return
	}
	switch m.GetType() {
	case ingestpolicy.TypePolicy:
		i.slotMu.Lock()
		i.pending = m.GetData() // a newer push replaces one not yet applied
		i.slotMu.Unlock()
		select {
		case i.wake <- struct{}{}:
		default:
		}
	case ingestpolicy.TypeReportAck:
		seq, err := ingestpolicy.ParseReportAck(m.GetData())
		if err != nil {
			i.logger.Warn("Ingest reportAck refused", zap.Int("bytes", len(m.GetData())))
			return
		}
		select {
		case i.acks <- seq:
		default: // the receiver is behind; the values stay pending
		}
	default:
		i.logger.Debug("Ingest custom message of an unknown type ignored", zap.Int("bytes", len(m.GetData())))
	}
}

func (i *Ingest) work() {
	defer i.wg.Done()
	for {
		select {
		case <-i.done:
			return
		case <-i.wake:
		}
		i.slotMu.Lock()
		data := i.pending
		i.pending = nil
		i.slotMu.Unlock()
		if data != nil {
			_ = i.store.Apply(data, i.now()) // the store logs and reports the outcome
		}
	}
}

// Reporter is the receiver's report transport, held by one receiver at a
// time until it calls Release.
func (i *Ingest) Reporter() (ingestpolicy.Reporter, error) {
	if !i.claimed.CompareAndSwap(false, true) {
		return nil, ErrReporterClaimed
	}
	return &reporter{i: i}, nil
}

type reporter struct {
	i        *Ingest
	released atomic.Bool
}

func (r *reporter) Acks() <-chan uint64        { return r.i.acks }
func (r *reporter) Connected() <-chan struct{} { return r.i.connected }

// Release lets another receiver take the transport, after a reload.
func (r *reporter) Release() {
	if r.released.CompareAndSwap(false, true) {
		r.i.claimed.Store(false)
	}
}

// loadSequence finds the sequence domain (§10.4). A valid state for the
// current UID continues from its last sequence. A state that is absent, or
// valid for another UID, cannot tell which sequences the backend has
// applied for the current UID, so the UID is renewed: a new agent with a
// new domain, starting at 1, recorded before anything uses it. A state that
// exists but cannot be read or parsed fails closed: no report is sent
// until an operator removes it, rather than guess a sequence the backend
// may already have applied and acknowledge values it never saw.
func (i *Ingest) loadSequence(id Identity) {
	i.seqMu.Lock()
	defer i.seqMu.Unlock()
	path := filepath.Join(i.dir, sequenceFile)
	stored, last, err := readSequence(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		i.seqErr = fmt.Errorf("reportSequence state %s unusable, no report is sent until it is removed: %w", path, err)
		i.logger.Error("Ingest reports disabled: the reportSequence state is unreadable or corrupt; remove it to start a new instance UID", zap.String("path", path), zap.Error(err))
		return
	default:
		uid, err := id.InstanceUID()
		if err != nil {
			i.seqErr = fmt.Errorf("instance UID: %w", err)
			i.logger.Error("Ingest reports disabled: the instance UID is unreadable", zap.Error(err))
			return
		}
		if uid == stored {
			i.uid, i.lastSeq, i.seqErr = uid, last, nil
			return
		}
	}
	uid, err := id.RenewInstanceUID()
	if err == nil {
		err = writeSequence(path, uid, 0)
	}
	if err != nil {
		i.seqErr = fmt.Errorf("renew the instance UID: %w", err)
		i.logger.Error("Ingest reports disabled: a new instance UID could not be recorded", zap.Error(err))
		return
	}
	i.logger.Warn("No reportSequence state for this instance UID; renewed the UID, so the backend sees a new agent")
	i.uid, i.lastSeq, i.seqErr = uid, 0, nil
}

// NextSequence reserves the next reportSequence of the UID's domain and
// persists it before returning it (§10.4).
func (r *reporter) NextSequence() (uint64, error) {
	i := r.i
	i.seqMu.Lock()
	defer i.seqMu.Unlock()
	if i.seqErr != nil {
		return 0, i.seqErr
	}
	if i.lastSeq == math.MaxUint64 {
		return 0, errExhausted
	}
	seq := i.lastSeq + 1
	if err := writeSequence(filepath.Join(i.dir, sequenceFile), i.uid, seq); err != nil {
		// Unsent, seq may be reserved again: it reached no backend.
		return 0, fmt.Errorf("persist reportSequence: %w", err)
	}
	i.lastSeq = seq
	return seq, nil
}

// Send hands the pages, in order, to the client, one at a time.
func (r *reporter) Send(ctx context.Context, pages [][]byte) error {
	s := r.i.sender.get()
	if s == nil {
		return errors.New("OpAMP client not started")
	}
	for _, page := range pages {
		msg := &protobufs.CustomMessage{Capability: ingestpolicy.Capability, Type: ingestpolicy.TypeReport, Data: page}
		for {
			sent, err := s.SendCustomMessage(msg)
			if errors.Is(err, types.ErrCustomMessagePending) {
				if err := wait(ctx, sent); err != nil {
					return err
				}
				continue // the previous message went out; send this one
			}
			if err != nil {
				return fmt.Errorf("send report page: %w", err)
			}
			if err := wait(ctx, sent); err != nil {
				return err
			}
			break
		}
	}
	return nil
}

func wait(ctx context.Context, ch chan struct{}) error {
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// readSequence reads the state strictly, at most maxSequenceFile bytes.
func readSequence(path string) (uid [16]byte, seq uint64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return uid, 0, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxSequenceFile+1))
	if err != nil {
		return uid, 0, err
	}
	if len(b) > maxSequenceFile {
		return uid, 0, errors.New("too large")
	}
	hexUID, digits, ok := strings.Cut(strings.TrimSuffix(string(b), "\n"), " ")
	if !ok || hex.DecodedLen(len(hexUID)) != len(uid) {
		return uid, 0, errors.New("malformed")
	}
	if _, err := hex.Decode(uid[:], []byte(hexUID)); err != nil {
		return uid, 0, errors.New("malformed")
	}
	if seq, err = strconv.ParseUint(digits, 10, 64); err != nil {
		return uid, 0, errors.New("malformed")
	}
	return uid, seq, nil
}

func writeSequence(path string, uid [16]byte, seq uint64) error {
	return writeAtomic(path, []byte(hex.EncodeToString(uid[:])+" "+strconv.FormatUint(seq, 10)+"\n"))
}

// writeAtomic writes b to path through a synced temporary file renamed
// over it, then syncs the directory.
func writeAtomic(path string, b []byte) (err error) {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmp.Name())
		}
	}()
	if err = tmp.Chmod(0o600); err == nil {
		if _, err = tmp.Write(b); err == nil {
			err = tmp.Sync()
		}
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
