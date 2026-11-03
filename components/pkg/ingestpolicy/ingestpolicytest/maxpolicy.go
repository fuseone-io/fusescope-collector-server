// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package ingestpolicytest builds ingest policies for tests.
package ingestpolicytest // import "github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy/ingestpolicytest"

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
)

// OrgID is the organisation of every key and application of MaxPolicy.
const OrgID = "0191c5e2-0000-7000-8000-00000000000a"

// Key is the i-th key of MaxPolicy.
func Key(i int) string { return fmt.Sprintf("rum_pub_%032d", i) }

// KeyID is the ID of the i-th key of MaxPolicy.
func KeyID(i int) string { return fmt.Sprintf("0191c5e2-8a1b-7d2e-b0c4-%012d", i) }

// ApplicationID is the ID of the i-th application of MaxPolicy; key i
// belongs to application i % MaxApplications.
func ApplicationID(i int) string { return fmt.Sprintf("0191c5e2-7f0a-7c3e-9a51-%012d", i) }

// MaxPolicy is a policy at every count bound of §11.1 (MaxKeys keys, each
// with a rate limit of 50/s burst 100, MaxApplications web applications
// allowing https://app-N.example.com and https://*.app-N.example.com),
// padded with whitespace to exactly MaxSize bytes.
func MaxPolicy(version uint64) ([]byte, error) {
	p := &ingestpolicy.Policy{SchemaVersion: 1, Version: version, IssuedAt: "2026-11-03T12:00:00Z"}
	for i := range ingestpolicy.MaxApplications {
		p.RUMApplications = append(p.RUMApplications, ingestpolicy.RUMApplication{
			ID: ApplicationID(i), OrgID: OrgID,
			Name: fmt.Sprintf("app-%d", i), Environment: "production", Platform: ingestpolicy.PlatformWeb,
			AllowedOrigins:      []string{fmt.Sprintf("https://app-%d.example.com", i), fmt.Sprintf("https://*.app-%d.example.com", i)},
			AllowedPackageNames: []string{},
			PIIPolicy:           ingestpolicy.PIIPolicy{DropQuery: true, QueryAllowlist: []string{"page"}, URLAllowlist: []string{}},
		})
	}
	for i := range ingestpolicy.MaxKeys {
		sum := sha256.Sum256([]byte(Key(i)))
		app := ApplicationID(i % ingestpolicy.MaxApplications)
		p.Keys = append(p.Keys, ingestpolicy.Key{
			ID: KeyID(i), SHA256: hex.EncodeToString(sum[:]), OrgID: OrgID,
			Kind: ingestpolicy.KindPublic, Signals: []string{"traces", "logs"}, ApplicationID: &app,
			Limit: &ingestpolicy.Limit{Rate: 50, Burst: 100},
		})
	}
	data, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	if len(data) > ingestpolicy.MaxSize {
		return nil, fmt.Errorf("the generated policy is %d bytes, over %d before padding", len(data), ingestpolicy.MaxSize)
	}
	return append(data, bytes.Repeat([]byte("\n"), ingestpolicy.MaxSize-len(data))...), nil
}
