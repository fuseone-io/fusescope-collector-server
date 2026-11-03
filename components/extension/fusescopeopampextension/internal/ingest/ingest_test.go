// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

func started(t *testing.T, dir string) (*Ingest, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zap.DebugLevel)
	i := New(zap.New(core), dir)
	require.NoError(t, i.Start())
	t.Cleanup(i.Close)
	return i, logs
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
	r := i.Reporter()
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
	<-i.Reporter().Connected()
	select {
	case <-i.Reporter().Connected():
		t.Fatal("reconnections are coalesced into one pending signal")
	default:
	}
}

func TestNextSequence(t *testing.T) {
	dir := t.TempDir()
	i, _ := started(t, dir)
	i.now = func() time.Time { return time.UnixMicro(1000) }
	first, err := i.Reporter().NextSequence()
	require.NoError(t, err)
	second, err := i.Reporter().NextSequence()
	require.NoError(t, err)
	assert.Equal(t, first+1, second)
	info, err := os.Stat(filepath.Join(dir, sequenceFile))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	again, _ := started(t, dir) // a restart with a slower clock
	again.now = func() time.Time { return time.UnixMicro(1) }
	third, err := again.Reporter().NextSequence()
	require.NoError(t, err)
	assert.Greater(t, third, second, "never reused across restarts")

	require.NoError(t, os.WriteFile(filepath.Join(dir, sequenceFile), []byte("garbage"), 0o600))
	corrupt, _ := started(t, dir)
	corrupt.now = func() time.Time { return time.UnixMicro(5_000_000) }
	fourth, err := corrupt.Reporter().NextSequence()
	require.NoError(t, err)
	assert.Equal(t, uint64(5_000_000), fourth, "an unreadable state continues from the clock")
}

func TestNextSequencePersistFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	i, _ := started(t, dir)
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	_, err := i.Reporter().NextSequence()
	assert.Error(t, err, "a sequence that cannot be persisted is not used")
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
	r := i.Reporter()
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
