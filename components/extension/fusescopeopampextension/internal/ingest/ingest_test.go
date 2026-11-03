// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/open-telemetry/opamp-go/client/types"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
)

const (
	org    = "0191c5e2-0000-7000-8000-000000000001"
	app    = "0191c5e2-7f0a-7c3e-9a51-3c2b1d4e5f60"
	keyID  = "0191c5e2-8a1b-7d2e-b0c4-000000000001"
	rumKey = "rum_pub_webwebwebwebwebwebwebwebwebwebweb"
)

func policy(t testing.TB, version uint64) []byte {
	t.Helper()
	sum := sha256.Sum256([]byte(rumKey))
	appID := app
	b, err := json.Marshal(&ingestpolicy.Policy{
		SchemaVersion: 1, Version: version, IssuedAt: "2026-11-03T12:00:00Z",
		Keys: []ingestpolicy.Key{{ID: keyID, SHA256: hex.EncodeToString(sum[:]), OrgID: org, Kind: ingestpolicy.KindPublic,
			Signals: []string{"traces", "logs"}, ApplicationID: &appID}},
		RUMApplications: []ingestpolicy.RUMApplication{{ID: app, OrgID: org, Name: "shop-web", Environment: "production",
			Platform: ingestpolicy.PlatformWeb, AllowedOrigins: []string{"https://shop.example.com"}, AllowedPackageNames: []string{},
			PIIPolicy: ingestpolicy.PIIPolicy{DropQuery: true, QueryAllowlist: []string{}, URLAllowlist: []string{}}}},
	})
	require.NoError(t, err)
	return b
}

func message(typ string, data []byte) *protobufs.CustomMessage {
	return &protobufs.CustomMessage{Capability: ingestpolicy.Capability, Type: typ, Data: data}
}

// identity is the agent's instance UID in memory.
type identity struct {
	mu       sync.Mutex
	uid      [16]byte
	renewals int
	readErr  error
	renewErr error
}

func (id *identity) InstanceUID() ([16]byte, error) {
	id.mu.Lock()
	defer id.mu.Unlock()
	return id.uid, id.readErr
}

func (id *identity) RenewInstanceUID() ([16]byte, error) {
	id.mu.Lock()
	defer id.mu.Unlock()
	if id.renewErr != nil {
		return [16]byte{}, id.renewErr
	}
	id.renewals++
	id.uid = [16]byte{0xee, byte(id.renewals)}
	return id.uid, nil
}

func (id *identity) get() ([16]byte, int) {
	id.mu.Lock()
	defer id.mu.Unlock()
	return id.uid, id.renewals
}

func startedAs(t *testing.T, dir string, id *identity) (*Ingest, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zap.DebugLevel)
	i := New(zap.New(core), dir)
	require.NoError(t, i.Start(id))
	t.Cleanup(i.Close)
	return i, logs
}

func started(t *testing.T, dir string) (*Ingest, *observer.ObservedLogs) {
	t.Helper()
	return startedAs(t, dir, &identity{uid: [16]byte{1}})
}

func claim(t *testing.T, i *Ingest) ingestpolicy.Reporter {
	t.Helper()
	r, err := i.Reporter()
	require.NoError(t, err)
	t.Cleanup(r.Release)
	return r
}

func waitVersion(t *testing.T, s ingestpolicy.Source, version uint64) {
	t.Helper()
	require.Eventually(t, func() bool { c := s.Current(); return c != nil && c.Version == version }, 5*time.Second, time.Millisecond)
}

func TestPolicyAppliedCachedReloaded(t *testing.T) {
	dir := t.TempDir()
	i, logs := started(t, dir)
	assert.Nil(t, i.Source().Current(), "fail closed before any policy")
	i.OnCustomMessage(message(ingestpolicy.TypePolicy, policy(t, 3)))
	waitVersion(t, i.Source(), 3)
	i.Close()

	again, _ := started(t, dir)
	waitVersion(t, again.Source(), 3)
	for _, e := range logs.All() {
		assert.NotContains(t, fmt.Sprint(e.Message, e.ContextMap()), rumKey)
	}
	info, err := os.Stat(dir)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o700), info.Mode().Perm())
}

func TestNewTouchesNothing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ingest")
	New(zap.NewNop(), dir)
	_, err := os.Stat(dir)
	assert.ErrorIs(t, err, os.ErrNotExist, "validating a configuration creates no state")
}

func TestPolicyRejections(t *testing.T) {
	i, _ := started(t, t.TempDir())
	i.OnCustomMessage(message(ingestpolicy.TypePolicy, policy(t, 5)))
	waitVersion(t, i.Source(), 5)

	i.OnCustomMessage(message(ingestpolicy.TypePolicy, make([]byte, ingestpolicy.MaxSize+1)))
	require.Eventually(t, func() bool { return i.Source().Status().Rejected == ingestpolicy.CodeTooLarge }, 5*time.Second, time.Millisecond)
	i.OnCustomMessage(message(ingestpolicy.TypePolicy, policy(t, 4)))
	require.Eventually(t, func() bool { return i.Source().Status().Rejected == ingestpolicy.CodeLowerVersion }, 5*time.Second, time.Millisecond)
	assert.Equal(t, uint64(5), i.Source().Current().Version, "the policy held stays in force")
}

func TestLatestPolicyWins(t *testing.T) {
	i, _ := started(t, t.TempDir())
	for v := uint64(1); v <= 50; v++ {
		i.OnCustomMessage(message(ingestpolicy.TypePolicy, policy(t, v)))
	}
	waitVersion(t, i.Source(), 50)
}

func TestOtherMessagesIgnored(t *testing.T) {
	i, logs := started(t, t.TempDir())
	i.OnCustomMessage(nil)
	i.OnCustomMessage(&protobufs.CustomMessage{Capability: "io.example.other", Type: ingestpolicy.TypePolicy, Data: policy(t, 1)})
	i.OnCustomMessage(message("delta", []byte(rumKey)))
	i.OnCustomMessage(message(ingestpolicy.TypeReport, []byte(rumKey))) // a type the backend never sends
	time.Sleep(20 * time.Millisecond)
	assert.Nil(t, i.Source().Current())
	for _, e := range logs.All() {
		assert.NotContains(t, fmt.Sprint(e.Message, e.ContextMap()), rumKey, "no content in the logs")
	}
}

func TestAcks(t *testing.T) {
	i, logs := started(t, t.TempDir())
	r := claim(t, i)
	i.OnCustomMessage(message(ingestpolicy.TypeReportAck, []byte(`{"schemaVersion":1,"reportSequence":7}`)))
	assert.Equal(t, uint64(7), <-r.Acks())

	bad := `{"schemaVersion":1,"reportSequence":8,"key":"` + rumKey + `"}`
	i.OnCustomMessage(message(ingestpolicy.TypeReportAck, []byte(bad)))
	select {
	case seq := <-r.Acks():
		t.Fatalf("ack %d from a malformed message", seq)
	default:
	}
	for _, e := range logs.All() {
		assert.NotContains(t, fmt.Sprint(e.Message, e.ContextMap()), rumKey)
	}

	done := make(chan struct{})
	go func() {
		for n := range 10 * ackBuffer {
			i.OnCustomMessage(message(ingestpolicy.TypeReportAck, fmt.Appendf(nil, `{"schemaVersion":1,"reportSequence":%d}`, n+1)))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a receiver behind on acks blocked the client")
	}
}

func TestConnectedCoalesced(t *testing.T) {
	i, _ := started(t, t.TempDir())
	for range 5 {
		i.OnConnect()
	}
	r := claim(t, i)
	<-r.Connected()
	select {
	case <-r.Connected():
		t.Fatal("reconnections are coalesced into one pending signal")
	default:
	}
}

func readState(t *testing.T, dir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, sequenceFile))
	require.NoError(t, err)
	return string(b)
}

func TestNextSequence(t *testing.T) {
	dir := t.TempDir()
	id := &identity{uid: [16]byte{1}}
	i, _ := startedAs(t, dir, id)
	uid, renewals := id.get()
	assert.Equal(t, 1, renewals, "no state: a new UID, a new sequence domain")
	assert.Equal(t, hex.EncodeToString(uid[:])+" 0\n", readState(t, dir), "the domain is recorded before use")
	r := claim(t, i)
	first, err := r.NextSequence()
	require.NoError(t, err)
	second, err := r.NextSequence()
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 2}, []uint64{first, second})
	assert.Equal(t, hex.EncodeToString(uid[:])+" 2\n", readState(t, dir), "persisted before it is returned")
	info, err := os.Stat(filepath.Join(dir, sequenceFile))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	r.Release()
	i.Close()

	// A restart, whatever the clock says, continues the domain.
	again, _ := startedAs(t, dir, id)
	again.now = func() time.Time { return time.Unix(0, 0) }
	third, err := claim(t, again).NextSequence()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), third)
	_, renewals = id.get()
	assert.Equal(t, 1, renewals, "a valid state for this UID keeps it")
}

func TestSequenceDomainRenewed(t *testing.T) {
	for name, prepare := range map[string]func(t *testing.T, dir string, id *identity){
		"state lost": func(t *testing.T, dir string, _ *identity) {
			require.NoError(t, os.Remove(filepath.Join(dir, sequenceFile)))
		},
		"state of another UID": func(t *testing.T, _ string, id *identity) {
			id.mu.Lock()
			id.uid = [16]byte{9}
			id.mu.Unlock()
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			id := &identity{uid: [16]byte{1}}
			i, _ := startedAs(t, dir, id)
			r := claim(t, i)
			for range 5 {
				_, err := r.NextSequence()
				require.NoError(t, err)
			}
			r.Release()
			i.Close()
			before, _ := id.get()

			prepare(t, dir, id)
			again, _ := startedAs(t, dir, id)
			after, renewals := id.get()
			assert.Equal(t, 2, renewals)
			assert.NotEqual(t, before, after)
			seq, err := claim(t, again).NextSequence()
			require.NoError(t, err)
			assert.Equal(t, uint64(1), seq, "the sequence restarts only with a new UID")
			assert.Equal(t, hex.EncodeToString(after[:])+" 1\n", readState(t, dir))
		})
	}
}

func TestSequenceStateFailsClosed(t *testing.T) {
	uid := [16]byte{1}
	valid := hex.EncodeToString(uid[:])
	for name, content := range map[string]string{
		"garbage":        "garbage",
		"empty":          "",
		"no sequence":    valid + "\n",
		"no UID":         "5\n",
		"short UID":      valid[:30] + " 5\n",
		"bad hex":        "zz" + valid[2:] + " 5\n",
		"negative":       valid + " -5\n",
		"signed":         valid + " +5\n",
		"overflow":       valid + " 18446744073709551616\n",
		"two spaces":     valid + "  5\n",
		"trailing bytes": valid + " 5\nx",
		"too large":      valid + " 5" + strings.Repeat("0", maxSequenceFile),
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, sequenceFile), []byte(content), 0o600))
			id := &identity{uid: uid}
			i, logs := startedAs(t, dir, id)
			_, err := claim(t, i).NextSequence()
			assert.Error(t, err, "no report without a trusted sequence")
			_, renewals := id.get()
			assert.Zero(t, renewals, "a corrupt state is not replaced by guessing")
			assert.Equal(t, content, readState(t, dir), "left for an operator")
			assert.Equal(t, 1, logs.FilterMessageSnippet("Ingest reports disabled").Len())
		})
	}
}

func TestSequenceStateUnreadable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores file permissions")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, sequenceFile)
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o000))
	id := &identity{uid: [16]byte{1}}
	i, _ := startedAs(t, dir, id)
	_, err := claim(t, i).NextSequence()
	assert.Error(t, err)
	_, renewals := id.get()
	assert.Zero(t, renewals)
}

func TestSequenceExhausted(t *testing.T) {
	dir := t.TempDir()
	uid := [16]byte{1}
	full := hex.EncodeToString(uid[:]) + " 18446744073709551615\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, sequenceFile), []byte(full), 0o600))
	i, _ := startedAs(t, dir, &identity{uid: uid})
	_, err := claim(t, i).NextSequence()
	assert.ErrorIs(t, err, errExhausted)
	assert.Equal(t, full, readState(t, dir))
}

func TestSequenceIdentityFailures(t *testing.T) {
	dir := t.TempDir()
	i, _ := startedAs(t, dir, &identity{renewErr: errors.New("disk full")})
	_, err := claim(t, i).NextSequence()
	assert.Error(t, err, "no domain could be recorded")

	uid := [16]byte{1}
	other := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(other, sequenceFile), []byte(hex.EncodeToString(uid[:])+" 5\n"), 0o600))
	j, _ := startedAs(t, other, &identity{uid: uid, readErr: errors.New("unreadable")})
	_, err = claim(t, j).NextSequence()
	assert.Error(t, err)

	notStarted, _ := New(zap.NewNop(), t.TempDir()).Reporter()
	_, err = notStarted.NextSequence()
	assert.ErrorIs(t, err, errNotStarted)
}

func TestNextSequencePersistFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	i, _ := started(t, dir)
	r := claim(t, i)
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	_, err := r.NextSequence()
	assert.Error(t, err, "a sequence that cannot be persisted is not used")
	require.NoError(t, os.Chmod(dir, 0o700))
	seq, err := r.NextSequence()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), seq, "an unpersisted sequence reached nobody and is reserved again")
}

func TestReporterClaimedOnce(t *testing.T) {
	i, _ := started(t, t.TempDir())
	r, err := i.Reporter()
	require.NoError(t, err)
	_, err = i.Reporter()
	assert.ErrorIs(t, err, ErrReporterClaimed, "a second receiver would interleave pages")
	r.Release()
	r.Release() // idempotent: does not release a later claim
	r2, err := i.Reporter()
	require.NoError(t, err)
	r.Release()
	_, err = i.Reporter()
	assert.ErrorIs(t, err, ErrReporterClaimed, "a stale release does not free the new claim")
	r2.Release()
}

type fakeSender struct {
	mu       sync.Mutex
	sent     []*protobufs.CustomMessage
	pendings int
	block    bool
}

func (f *fakeSender) SendCustomMessage(m *protobufs.CustomMessage) (chan struct{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan struct{})
	if f.pendings > 0 {
		f.pendings--
		close(ch)
		return ch, types.ErrCustomMessagePending
	}
	f.sent = append(f.sent, m)
	if !f.block {
		close(ch)
	}
	return ch, nil
}

func TestSend(t *testing.T) {
	i, _ := started(t, t.TempDir())
	r := claim(t, i)
	assert.Error(t, r.Send(context.Background(), [][]byte{[]byte("{}")}), "no client yet")

	f := &fakeSender{pendings: 2}
	i.SetSender(f)
	pages := [][]byte{[]byte(`{"page":1}`), []byte(`{"page":2}`), []byte(`{"page":3}`)}
	require.NoError(t, r.Send(context.Background(), pages))
	require.Len(t, f.sent, 3)
	for n, m := range f.sent {
		assert.Equal(t, ingestpolicy.Capability, m.Capability)
		assert.Equal(t, ingestpolicy.TypeReport, m.Type)
		assert.Equal(t, pages[n], m.Data, "in order")
	}

	stuck := &fakeSender{block: true}
	i.SetSender(stuck)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, r.Send(ctx, pages), context.DeadlineExceeded, "a transport that never sends is bounded by ctx")
	assert.Len(t, stuck.sent, 1, "no page queued behind one not sent")
}

func TestCustomCapabilities(t *testing.T) {
	assert.Equal(t, []string{ingestpolicy.Capability}, CustomCapabilities().Capabilities)
	assert.True(t, strings.HasPrefix(ingestpolicy.Capability, "io.fuseone."))
}
