// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package rumreceiver

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
)

func maxKey(i int) string { return fmt.Sprintf("rum_pub_%032d", i) }

// maxPolicy is a policy at every count bound of §11.1 (50 000 keys, each
// with a rate limit, 10 000 applications with two origins), padded with
// whitespace to exactly max_policy_size.
func maxPolicy(t *testing.T, version uint64) []byte {
	t.Helper()
	p := &ingestpolicy.Policy{SchemaVersion: 1, Version: version, IssuedAt: "2026-11-03T12:00:00Z"}
	for i := range ingestpolicy.MaxApplications {
		p.RUMApplications = append(p.RUMApplications, ingestpolicy.RUMApplication{
			ID: fmt.Sprintf("0191c5e2-7f0a-7c3e-9a51-%012d", i), OrgID: testOrg,
			Name: fmt.Sprintf("app-%d", i), Environment: "production", Platform: ingestpolicy.PlatformWeb,
			AllowedOrigins:      []string{fmt.Sprintf("https://app-%d.example.com", i), fmt.Sprintf("https://*.app-%d.example.com", i)},
			AllowedPackageNames: []string{},
			PIIPolicy:           ingestpolicy.PIIPolicy{DropQuery: true, QueryAllowlist: []string{"page"}, URLAllowlist: []string{}},
		})
	}
	for i := range ingestpolicy.MaxKeys {
		app := p.RUMApplications[i%ingestpolicy.MaxApplications].ID
		p.Keys = append(p.Keys, ingestpolicy.Key{
			ID: fmt.Sprintf("0191c5e2-8a1b-7d2e-b0c4-%012d", i), SHA256: verifier(maxKey(i)), OrgID: testOrg,
			Kind: ingestpolicy.KindPublic, Signals: []string{"traces", "logs"}, ApplicationID: ptr(app),
			Limit: &ingestpolicy.Limit{Rate: 50, Burst: 100},
		})
	}
	data := encodePolicy(t, p)
	require.LessOrEqual(t, len(data), ingestpolicy.MaxSize, "the generated policy must fit before padding")
	return append(data, bytes.Repeat([]byte("\n"), ingestpolicy.MaxSize-len(data))...)
}

// TestMaxSizePolicyAcceptedCachedApplied is the step 2 part of the §13
// decision 1 requirement: a policy at max_policy_size is accepted, cached,
// applied, and applied again from the cache after a restart. The OpAMP leg
// of the end-to-end test comes with the extension (issue #13, step 3).
func TestMaxSizePolicyAcceptedCachedApplied(t *testing.T) {
	data := maxPolicy(t, 7)
	require.Len(t, data, ingestpolicy.MaxSize)
	dir := t.TempDir()

	store := ingestpolicy.NewStore(zap.NewNop(), ingestpolicy.NewFileCache(dir))
	start := time.Now()
	require.NoError(t, store.Apply(data, testNow))
	t.Logf("applied a %d-byte policy in %v", len(data), time.Since(start))
	require.NotNil(t, store.Current())
	assert.Equal(t, ingestpolicy.MaxKeys, store.Current().KeyCount())
	assert.Len(t, store.Current().Applications(), ingestpolicy.MaxApplications)

	cached, err := os.ReadFile(filepath.Join(dir, ingestpolicy.PolicyFile))
	require.NoError(t, err)
	assert.True(t, bytes.Equal(data, cached), "the data bytes are cached as received")
	info, err := os.Stat(filepath.Join(dir, ingestpolicy.PolicyFile))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	accepts := func(t *testing.T, source ingestpolicy.Source) {
		t.Helper()
		f := newFixtureWith(t, createDefaultConfig().(*Config), source)
		last := ingestpolicy.MaxKeys - 1
		origin := fmt.Sprintf("https://eu.app-%d.example.com", last%ingestpolicy.MaxApplications)
		rec := f.post(tracesPath, testTraceJSON, jsonHeaders(maxKey(last), "Origin", origin))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		resource := f.traces.AllTraces()[0].ResourceSpans().At(0).Resource().Attributes().AsRaw()
		assert.Equal(t, fmt.Sprintf("0191c5e2-8a1b-7d2e-b0c4-%012d", last), resource[attributeKeyID])
		assert.Equal(t, fmt.Sprintf("0191c5e2-7f0a-7c3e-9a51-%012d", last%ingestpolicy.MaxApplications), resource[attributeApplicationID])

		pre := f.do(http.MethodOptions, tracesPath, nil, map[string]string{"Origin": "https://x.app-1234.example.com", "Access-Control-Request-Method": "POST"})
		assert.Equal(t, http.StatusNoContent, pre.Code)
	}
	t.Run("Applied", func(t *testing.T) { accepts(t, store) })

	t.Run("OneByteMore_TooLarge", func(t *testing.T) {
		err := store.Apply(append(maxPolicy(t, 8), ' '), testNow)
		var rerr *ingestpolicy.RejectionError
		require.ErrorAs(t, err, &rerr)
		assert.Equal(t, ingestpolicy.CodeTooLarge, rerr.Code)
		assert.Equal(t, uint64(7), store.Current().Version, "the held policy stays in force")
	})

	t.Run("AppliedFromCacheAfterRestart", func(t *testing.T) {
		restarted := ingestpolicy.NewStore(zap.NewNop(), ingestpolicy.NewFileCache(dir))
		start := time.Now()
		restarted.Start()
		t.Logf("loaded the cached policy in %v", time.Since(start))
		require.NotNil(t, restarted.Current())
		assert.Equal(t, uint64(7), restarted.Current().Version)
		k := restarted.Current().Key(sha256.Sum256([]byte(maxKey(ingestpolicy.MaxKeys - 1))))
		require.NotNil(t, k)
		assert.Equal(t, &ingestpolicy.Limit{Rate: 50, Burst: 100}, k.Limit, "the per-key limits survive the cache")
		accepts(t, restarted)
	})
}
