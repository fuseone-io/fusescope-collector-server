// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/open-telemetry/opamp-go/protobufs"

	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/agent"
	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/ingest"
	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy/ingestpolicytest"
)

// writeRole writes the base configuration with the role setting.
func writeRole(t *testing.T, endpoint, storage, role string) string {
	t.Helper()
	path := writeBase(t, endpoint, storage)
	cfg, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg = []byte(strings.Replace(string(cfg), "storage: ", fmt.Sprintf("role: %q, storage: ", role), 1))
	if err := os.WriteFile(path, cfg, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// sendCustom sends one ingest message to the connected agent.
func (b *backend) sendCustom(typ string, data []byte) {
	b.t.Helper()
	b.waitFor("a connection", func() bool { return b.conn != nil })
	b.mu.Lock()
	conn := b.conn
	b.mu.Unlock()
	err := conn.Send(context.Background(), &protobufs.ServerToAgent{CustomMessage: &protobufs.CustomMessage{
		Capability: ingestpolicy.Capability, Type: typ, Data: data,
	}})
	if err != nil {
		b.t.Fatal(err)
	}
}

func (b *backend) role() string {
	for _, kv := range append(b.description.GetIdentifyingAttributes(), b.description.GetNonIdentifyingAttributes()...) {
		if kv.GetKey() == "fusescope.collector.role" {
			return kv.GetValue().GetStringValue()
		}
	}
	return ""
}

// currentIngest waits for the running agent's ingest side, other than old.
func currentIngest(t *testing.T, old *ingest.Ingest) *ingest.Ingest {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if a := agent.Current(); a != nil && a.Ingest() != nil && a.Ingest() != old {
			return a.Ingest()
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no ingest side started")
	return nil
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestIngestPolicyOverOpAMP is the OpAMP leg of the max_policy_size test
// (issue #13, §13 decision 1): a policy of exactly 24 MiB pushed over the
// custom capability is accepted, cached and applied, applied from the cache
// after a restart, and one byte more is rejected as too_large. Reports go
// out as `report` messages and `reportAck` comes back to the receiver.
func TestIngestPolicyOverOpAMP(t *testing.T) {
	policy, err := ingestpolicytest.MaxPolicy(7)
	if err != nil {
		t.Fatal(err)
	}
	b := startBackend(t)
	storage := t.TempDir()
	path := writeRole(t, b.endpoint, storage, RoleIngress)
	col := startCollector(t, path)
	ing := currentIngest(t, nil)

	b.waitFor("the custom capability", func() bool { return b.capabilities != nil && b.description != nil })
	b.mu.Lock()
	if caps := b.capabilities.GetCapabilities(); len(caps) != 1 || caps[0] != ingestpolicy.Capability {
		t.Errorf("custom capabilities %v, want only %s", caps, ingestpolicy.Capability)
	}
	if r := b.role(); r != RoleIngress {
		t.Errorf("role %q, want %q", r, RoleIngress)
	}
	b.mu.Unlock()

	b.sendCustom(ingestpolicy.TypePolicy, policy)
	source := ing.Source()
	waitUntil(t, "the 24 MiB policy", func() bool { return source.Current() != nil })
	if v, n := source.Current().Version, source.Current().KeyCount(); v != 7 || n != ingestpolicy.MaxKeys {
		t.Fatalf("policy %d with %d keys, want 7 with %d", v, n, ingestpolicy.MaxKeys)
	}
	cached, err := os.ReadFile(filepath.Join(storage, "ingest", ingestpolicy.PolicyFile))
	if err != nil || len(cached) != ingestpolicy.MaxSize {
		t.Fatalf("cached %d bytes (%v), want %d", len(cached), err, ingestpolicy.MaxSize)
	}

	tooLarge, err := ingestpolicytest.MaxPolicy(8)
	if err != nil {
		t.Fatal(err)
	}
	b.sendCustom(ingestpolicy.TypePolicy, append(tooLarge, ' '))
	waitUntil(t, "the too_large rejection", func() bool { return source.Status().Rejected == ingestpolicy.CodeTooLarge })
	if v := source.Current().Version; v != 7 {
		t.Fatalf("policy %d after a rejection, want 7 in force", v)
	}

	// A report and its acknowledgement.
	rep, err := ing.Reporter()
	if err != nil {
		t.Fatal(err)
	}
	defer rep.Release()
	seq, err := rep.NextSequence()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pages := [][]byte{[]byte(`{"page":1}`), []byte(`{"page":2}`)}
	if err := rep.Send(ctx, pages); err != nil {
		t.Fatal(err)
	}
	b.waitFor("the report pages", func() bool {
		n := 0
		for _, c := range b.customs {
			if c.GetCapability() == ingestpolicy.Capability && c.GetType() == ingestpolicy.TypeReport {
				n++
			}
		}
		return n == len(pages)
	})
	b.sendCustom(ingestpolicy.TypeReportAck, fmt.Appendf(nil, `{"schemaVersion":1,"reportSequence":%d}`, seq))
	select {
	case got := <-rep.Acks():
		if got != seq {
			t.Fatalf("ack %d, want %d", got, seq)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no ack delivered")
	}

	b.mu.Lock()
	uid := b.describedUID
	b.mu.Unlock()
	col.col.Shutdown()
	<-col.done
	b.mu.Lock()
	b.description = nil
	b.mu.Unlock()

	// Restarted, the cached policy is applied before any push.
	startCollector(t, path)
	restarted := currentIngest(t, ing).Source()
	waitUntil(t, "the cached policy", func() bool { return restarted.Current() != nil })
	if v := restarted.Current().Version; v != 7 {
		t.Fatalf("policy %d from the cache, want 7", v)
	}
	again, err := currentIngest(t, ing).Reporter()
	if err != nil {
		t.Fatal(err)
	}
	defer again.Release()
	if next, err := again.NextSequence(); err != nil || next != seq+1 {
		t.Fatalf("reportSequence %d (%v) after a restart, want %d", next, err, seq+1)
	}
	b.waitFor("the restarted agent", func() bool { return b.description != nil })
	b.mu.Lock()
	defer b.mu.Unlock()
	if string(b.describedUID) != string(uid) {
		t.Error("the instance UID changed across a restart with a valid reportSequence state")
	}
}

// Without its reportSequence state, an agent that already has an instance
// UID takes a new one: the backend may have applied any sequence for the
// old one (rum.md §10.4).
func TestLostSequenceStateRenewsInstanceUID(t *testing.T) {
	b := startBackend(t)
	storage := t.TempDir()
	path := writeRole(t, b.endpoint, storage, RoleIngress)
	col := startCollector(t, path)
	ing := currentIngest(t, nil)
	b.waitFor("a description", func() bool { return b.description != nil })
	b.mu.Lock()
	uid := b.describedUID
	b.mu.Unlock()
	col.col.Shutdown()
	<-col.done
	b.mu.Lock()
	b.description = nil
	b.mu.Unlock()

	if err := os.Remove(filepath.Join(storage, "ingest", "report-sequence")); err != nil {
		t.Fatal(err)
	}
	startCollector(t, path)
	currentIngest(t, ing)
	b.waitFor("the restarted agent", func() bool { return b.description != nil })
	b.mu.Lock()
	defer b.mu.Unlock()
	if string(b.describedUID) == string(uid) {
		t.Error("the instance UID was kept without its sequence state")
	}
}

// The writer role neither declares the capability nor takes a policy.
func TestWriterRoleHasNoIngest(t *testing.T) {
	b := startBackend(t)
	startCollector(t, writeBase(t, b.endpoint, t.TempDir()))
	b.waitFor("a description", func() bool { return b.description != nil })
	b.mu.Lock()
	if r := b.role(); r != RoleWriter {
		t.Errorf("role %q, want %q", r, RoleWriter)
	}
	if caps := b.capabilities.GetCapabilities(); len(caps) != 0 {
		t.Errorf("writer declares custom capabilities %v", caps)
	}
	b.mu.Unlock()
	if a := agent.Current(); a == nil || a.Ingest() != nil {
		t.Fatal("writer agent has an ingest side")
	}
}

// An ingress collector with auth.token_file presents the token as a bearer
// credential on the OpAMP connection; it is never logged.
func TestIngestTokenSent(t *testing.T) {
	const token = "ingest-token-aaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	b := startBackend(t)
	path := writeRole(t, b.endpoint, t.TempDir(), RoleIngress)
	cfg, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg = []byte(strings.Replace(string(cfg), "storage: ", fmt.Sprintf("auth: {token_file: %q}, storage: ", tokenFile), 1))
	if err := os.WriteFile(path, cfg, 0o600); err != nil {
		t.Fatal(err)
	}
	startCollector(t, path)
	b.waitFor("a connection", func() bool { return b.authorization != "" })
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.authorization != "Bearer "+token {
		t.Errorf("Authorization %q", b.authorization)
	}
	if strings.Contains(string(b.effective), token) {
		t.Error("the token reached the effective configuration")
	}
}
