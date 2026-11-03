// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package ingestpolicy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	orgA     = "0191c5e2-0000-7000-8000-00000000000a"
	orgB     = "0191c5e2-0000-7000-8000-00000000000b"
	webApp   = "0191c5e2-7f0a-7c3e-9a51-3c2b1d4e5f60"
	droidApp = "0191c5e2-7f0a-7c3e-9a51-3c2b1d4e5f61"
	webKeyID = "0191c5e2-8a1b-7d2e-b0c4-5e6f7a8b9c0d"
	droidKey = "0191c5e2-8a1b-7d2e-b0c4-5e6f7a8b9c0e"
	secretID = "0191c5e2-8a1b-7d2e-b0c4-5e6f7a8b9c0f"
	webKey   = "rum_pub_abcdefghijklmnopqrstuvwxyz234567"
)

func verifier(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func ptr[T any](v T) *T { return &v }

// testPolicy is a valid v1 policy: a web and an android application, their
// public keys, and a secret key.
func testPolicy(version uint64) *Policy {
	return &Policy{
		SchemaVersion: 1,
		Version:       version,
		IssuedAt:      "2026-11-03T12:00:00Z",
		Keys: []Key{
			{ID: webKeyID, SHA256: verifier(webKey), OrgID: orgA, Kind: KindPublic, Signals: []string{"traces", "logs"}, ApplicationID: ptr(webApp), Limit: &Limit{Rate: 50, Burst: 100}},
			{ID: droidKey, SHA256: verifier("rum_pub_android"), OrgID: orgA, Kind: KindPublic, Signals: []string{"logs"}, ApplicationID: ptr(droidApp), ExpiresAt: ptr("2026-11-04T00:00:00Z")},
			{ID: secretID, SHA256: verifier("fs_secret"), OrgID: orgB, Kind: KindSecret, Signals: []string{"traces", "metrics", "logs"}},
		},
		RUMApplications: []RUMApplication{
			{
				ID: webApp, OrgID: orgA, Name: "shop-web", Environment: "production", Platform: PlatformWeb,
				AllowedOrigins: []string{"https://shop.example.com", "https://*.cdn.example.com"}, AllowedPackageNames: []string{},
				PIIPolicy: PIIPolicy{DropQuery: true, QueryAllowlist: []string{"page"}, URLAllowlist: []string{}},
			},
			{
				ID: droidApp, OrgID: orgA, Name: "shop-android", Environment: "production", Platform: PlatformAndroid,
				AllowedOrigins: []string{}, AllowedPackageNames: []string{"com.example.shop"},
				PIIPolicy: PIIPolicy{DropQuery: true, QueryAllowlist: []string{}, URLAllowlist: []string{}},
			},
		},
	}
}

func encode(t testing.TB, p *Policy) []byte {
	t.Helper()
	b, err := json.Marshal(p)
	require.NoError(t, err)
	return b
}

func codeOf(t testing.TB, err error) Code {
	t.Helper()
	if err == nil {
		return ""
	}
	rerr, ok := err.(*RejectionError)
	require.True(t, ok, "%T %v", err, err)
	return rerr.Code
}
