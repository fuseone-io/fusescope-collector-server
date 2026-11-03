// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package ingest carries the ingest policy and report over OpAMP for a
// collector of the ingress role (ADR 0006, docs/spec/rum.md §10–11): it owns
// the policy store, receives `policy` and `reportAck` custom messages, and
// sends the receiver's `report` pages with a durable reportSequence.
package ingest // import "github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/ingest"

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

const sequenceFile = "report-sequence"

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

	seqMu   sync.Mutex
	nextSeq uint64

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
	}
}

// Start creates the state directory, loads the cached policy (§11.3) and
// starts the policy worker. It runs when the collector really starts.
func (i *Ingest) Start() error {
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

// Reporter is the receiver's report transport.
func (i *Ingest) Reporter() ingestpolicy.Reporter { return reporter{i} }

type reporter struct{ i *Ingest }

func (r reporter) Acks() <-chan uint64        { return r.i.acks }
func (r reporter) Connected() <-chan struct{} { return r.i.connected }

// NextSequence reserves the next reportSequence and persists it before
// returning it (§10.4). A missing or unreadable file starts from the clock
// in microseconds, above any sequence a previous run used.
func (r reporter) NextSequence() (uint64, error) {
	i := r.i
	i.seqMu.Lock()
	defer i.seqMu.Unlock()
	path := filepath.Join(i.dir, sequenceFile)
	if i.nextSeq == 0 {
		last, err := readSequence(path)
		if err != nil {
			i.logger.Warn("reportSequence state unreadable; continuing from the clock", zap.Error(err))
		}
		i.nextSeq = max(last+1, uint64(i.now().UnixMicro()))
	}
	seq := i.nextSeq
	if err := writeAtomic(path, []byte(strconv.FormatUint(seq, 10)+"\n")); err != nil {
		return 0, fmt.Errorf("persist reportSequence: %w", err)
	}
	i.nextSeq = seq + 1
	return seq, nil
}

// Send hands the pages, in order, to the client, one at a time.
func (r reporter) Send(ctx context.Context, pages [][]byte) error {
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

func readSequence(path string) (uint64, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
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
