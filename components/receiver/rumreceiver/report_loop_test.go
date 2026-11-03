// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package rumreceiver

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
	"github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"
)

type sentReport struct {
	seq    uint64
	report report.Report
}

// fakeReporter records the reports handed to it.
type fakeReporter struct {
	t         *testing.T
	mu        sync.Mutex
	seq       uint64
	seqErr    error
	stall     bool
	sent      chan sentReport
	acks      chan uint64
	connected chan struct{}
}

func newFakeReporter(t *testing.T) *fakeReporter {
	return &fakeReporter{t: t, seq: 100, sent: make(chan sentReport, 16), acks: make(chan uint64, 16), connected: make(chan struct{}, 1)}
}

func (f *fakeReporter) NextSequence() (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.seqErr != nil {
		return 0, f.seqErr
	}
	f.seq++
	return f.seq, nil
}

func (f *fakeReporter) Send(ctx context.Context, pages [][]byte) error {
	f.mu.Lock()
	stall := f.stall
	f.mu.Unlock()
	if stall {
		<-ctx.Done()
		f.sent <- sentReport{}
		return ctx.Err()
	}
	r, err := report.Merge(pages)
	require.NoError(f.t, err)
	f.sent <- sentReport{seq: r.ReportSequence, report: r}
	return nil
}

func (f *fakeReporter) Acks() <-chan uint64        { return f.acks }
func (f *fakeReporter) Connected() <-chan struct{} { return f.connected }

func (f *fakeReporter) set(fn func(*fakeReporter)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *fakeReporter) next(t *testing.T) sentReport {
	t.Helper()
	select {
	case s := <-f.sent:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("no report sent")
		return sentReport{}
	}
}

func (f *fakeReporter) none(t *testing.T, within time.Duration) {
	t.Helper()
	select {
	case s := <-f.sent:
		t.Fatalf("unexpected report %d", s.seq)
	case <-time.After(within):
	}
}

type reporterExtension struct {
	providerExtension
	reporter ingestpolicy.Reporter
}

func (r reporterExtension) IngestReporter() ingestpolicy.Reporter { return r.reporter }

func withReportEvery(t *testing.T, every, timeout time.Duration) {
	oldEvery, oldTimeout := reportEvery, reportTimeout
	reportEvery, reportTimeout = every, timeout
	t.Cleanup(func() { reportEvery, reportTimeout = oldEvery, oldTimeout })
}

func hasKey(r report.Report, id string) bool {
	for _, k := range r.Keys {
		if k.ID == id {
			return true
		}
	}
	return false
}

func TestReceiverReportsOnConnectAndAppliesAcks(t *testing.T) {
	withReportEvery(t, time.Hour, time.Second)
	store := newStore(t, testPolicy(1))
	rep := newFakeReporter(t)
	host := newStatusHost(map[component.ID]component.Component{policyExtension: reporterExtension{providerExtension{source: store}, rep}})
	s := startReceiver(t, nil, host)

	require.Equal(t, http.StatusOK, s.post(t, tracesPath, testTraceJSON, jsonHeaders(webKey)).StatusCode)
	rep.connected <- struct{}{}
	first := rep.next(t)
	assert.Equal(t, uint64(101), first.seq, "the sequence comes from the reporter")
	assert.Equal(t, uint64(1), first.report.Policy.Version)
	assert.True(t, hasKey(first.report, webKeyID), "the key use is reported")

	// Unacknowledged, the key use is sent again.
	rep.connected <- struct{}{}
	second := rep.next(t)
	assert.Equal(t, uint64(102), second.seq)
	assert.True(t, hasKey(second.report, webKeyID))

	rep.acks <- second.seq
	require.Eventually(t, func() bool {
		rep.connected <- struct{}{}
		return !hasKey(rep.next(t).report, webKeyID)
	}, 5*time.Second, 10*time.Millisecond, "the acknowledgement clears the key use")
}

func TestReceiverReportsRejectedPolicy(t *testing.T) {
	withReportEvery(t, time.Hour, time.Second)
	store := newStore(t, testPolicy(2))
	rep := newFakeReporter(t)
	host := newStatusHost(map[component.ID]component.Component{policyExtension: reporterExtension{providerExtension{source: store}, rep}})
	startReceiver(t, nil, host)

	require.Error(t, store.Apply(encodePolicy(t, testPolicy(1)), time.Now()))
	rep.connected <- struct{}{}
	r := rep.next(t).report
	assert.Equal(t, uint64(2), r.Policy.Version, "the held revision stays in force")
	assert.Equal(t, "rejected", r.Policy.Status)
	assert.Equal(t, string(ingestpolicy.CodeLowerVersion), r.Policy.Error)
}

func TestReceiverReportsPeriodically(t *testing.T) {
	withReportEvery(t, 20*time.Millisecond, time.Second)
	rep := newFakeReporter(t)
	host := newStatusHost(map[component.ID]component.Component{policyExtension: reporterExtension{providerExtension{source: newStore(t, nil)}, rep}})
	startReceiver(t, nil, host)

	a, b := rep.next(t), rep.next(t)
	assert.Less(t, a.seq, b.seq)
	assert.Equal(t, "ok", a.report.Policy.Status)
	assert.Zero(t, a.report.Policy.Version, "reported with no policy too")
}

func TestReceiverReportSequenceNotPersisted(t *testing.T) {
	withReportEvery(t, time.Hour, time.Second)
	rep := newFakeReporter(t)
	rep.seqErr = errors.New("disk full")
	host := newStatusHost(map[component.ID]component.Component{policyExtension: reporterExtension{providerExtension{source: newStore(t, testPolicy(1))}, rep}})
	startReceiver(t, nil, host)

	rep.connected <- struct{}{}
	rep.none(t, 100*time.Millisecond)

	rep.set(func(f *fakeReporter) { f.seqErr = nil })
	rep.connected <- struct{}{}
	assert.Equal(t, uint64(101), rep.next(t).seq, "no report is built without a persisted sequence")
}

func TestReceiverReportStallIsBounded(t *testing.T) {
	withReportEvery(t, time.Hour, 50*time.Millisecond)
	rep := newFakeReporter(t)
	rep.stall = true
	host := newStatusHost(map[component.ID]component.Component{policyExtension: reporterExtension{providerExtension{source: newStore(t, testPolicy(1))}, rep}})
	startReceiver(t, nil, host)

	rep.connected <- struct{}{}
	rep.next(t) // released by reportTimeout
	rep.set(func(f *fakeReporter) { f.stall = false })
	rep.connected <- struct{}{}
	assert.Equal(t, uint64(102), rep.next(t).seq, "reporting continues after a stalled send")
}

func TestReceiverShutdownCancelsReport(t *testing.T) {
	withReportEvery(t, time.Hour, time.Hour)
	rep := newFakeReporter(t)
	rep.stall = true
	store := newStore(t, testPolicy(1))
	host := newStatusHost(map[component.ID]component.Component{policyExtension: reporterExtension{providerExtension{source: store}, rep}})
	s := startReceiver(t, nil, host)

	rep.connected <- struct{}{}
	require.Eventually(t, func() bool { return len(rep.connected) == 0 }, 5*time.Second, time.Millisecond)
	done := make(chan error, 1)
	go func() { done <- s.r.Shutdown(context.Background()) }()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown waits on a stalled report")
	}
}

func TestReceiverWithoutReporter(t *testing.T) {
	host := newStatusHost(map[component.ID]component.Component{policyExtension: reporterExtension{providerExtension{source: newStore(t, testPolicy(1))}, nil}})
	s := startReceiver(t, nil, host)
	assert.Equal(t, http.StatusOK, s.post(t, tracesPath, testTraceJSON, jsonHeaders(webKey)).StatusCode)
}
