// Copyright FuseOne
// SPDX-License-Identifier: MIT
//
// TestApplicationAllowsOrigin and TestApplicationAllowsPackageName are moved
// from fuseone-io/fusescope pkg/types/rumtypes/application_test.go at
// 6c8991257 (MIT, see LICENSE-MIT), run against compiled snapshots; the
// length-bound and preflight cases are new (docs/spec/rum.md §4, §7.1).

package ingestpolicy

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func snapshotWithOrigins(origins ...[]string) *Snapshot {
	p := &Policy{Version: 1, IssuedAt: "2026-11-03T12:00:00Z"}
	for i, o := range origins {
		p.RUMApplications = append(p.RUMApplications, RUMApplication{
			ID: strings.Replace(webApp, "60", "6"+string(rune('a'+i)), 1), OrgID: orgA, Platform: PlatformWeb,
			AllowedOrigins: o,
		})
	}
	return compile(p, [32]byte{})
}

func TestApplicationAllowsOrigin(t *testing.T) {
	testCases := []struct {
		name     string
		allowed  []string
		origin   string
		expected bool
	}{
		{name: "Exact_Allowed", allowed: []string{"https://shop.example.com"}, origin: "https://shop.example.com", expected: true},
		{name: "ExactCaseInsensitive_Allowed", allowed: []string{"https://Shop.Example.com"}, origin: "https://shop.example.com", expected: true},
		{name: "CandidateUppercase_Allowed", allowed: []string{"https://shop.example.com"}, origin: "HTTPS://SHOP.EXAMPLE.COM", expected: true},
		{name: "DifferentScheme_Denied", allowed: []string{"https://shop.example.com"}, origin: "http://shop.example.com", expected: false},
		{name: "DifferentPort_Denied", allowed: []string{"https://shop.example.com"}, origin: "https://shop.example.com:8443", expected: false},
		{name: "ExplicitPort_Allowed", allowed: []string{"http://localhost:3000"}, origin: "http://localhost:3000", expected: true},
		{name: "WildcardSubdomain_Allowed", allowed: []string{"https://*.example.com"}, origin: "https://eu.shop.example.com", expected: true},
		{name: "WildcardApex_Denied", allowed: []string{"https://*.example.com"}, origin: "https://example.com", expected: false},
		{name: "WildcardSuffixTrick_Denied", allowed: []string{"https://*.example.com"}, origin: "https://evilexample.com", expected: false},
		{name: "WildcardOtherPort_Denied", allowed: []string{"https://*.example.com"}, origin: "https://a.example.com:444", expected: false},
		{name: "Any_Allowed", allowed: []string{"*"}, origin: "https://anything.test", expected: true},
		{name: "None_Denied", allowed: []string{}, origin: "https://shop.example.com", expected: false},
		{name: "NullOrigin_Denied", allowed: []string{"https://shop.example.com"}, origin: "null", expected: false},
		{name: "NullOriginAny_Denied", allowed: []string{"*"}, origin: "null", expected: false},
		{name: "Empty_Denied", allowed: []string{"*"}, origin: "", expected: false},
		{name: "Over256Bytes_Denied", allowed: []string{"*"}, origin: "https://" + strings.Repeat("a", 250) + ".com", expected: false},
		{name: "At256Bytes_Allowed", allowed: []string{"*"}, origin: "https://" + strings.Repeat("a", 244) + ".com", expected: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			snap := snapshotWithOrigins(tc.allowed)
			assert.Equal(t, tc.expected, snap.Applications()[0].AllowsOrigin(tc.origin))
			// A preflight over the same single application agrees.
			assert.Equal(t, tc.expected, snap.AnyApplicationAllowsOrigin(tc.origin))
		})
	}
}

func TestAnyApplicationAllowsOrigin(t *testing.T) {
	snap := snapshotWithOrigins(
		[]string{"https://shop.example.com"},
		[]string{"https://*.cdn.example.com", "http://localhost:3000"},
		[]string{},
	)
	testCases := []struct {
		origin   string
		expected bool
	}{
		{"https://shop.example.com", true},
		{"https://eu.cdn.example.com", true},
		{"https://a.b.cdn.example.com", true},
		{"https://cdn.example.com", false},
		{"http://localhost:3000", true},
		{"http://localhost:3001", false},
		{"https://evil.example.org", false},
		{"https://.cdn.example.com", false},
	}
	for _, tc := range testCases {
		t.Run(tc.origin, func(t *testing.T) {
			assert.Equal(t, tc.expected, snap.AnyApplicationAllowsOrigin(tc.origin))
			any := false
			for _, a := range snap.Applications() {
				any = any || a.AllowsOrigin(tc.origin)
			}
			assert.Equal(t, any, snap.AnyApplicationAllowsOrigin(tc.origin), "index and per-application matching agree")
		})
	}
}

func TestApplicationAllowsPackageName(t *testing.T) {
	testCases := []struct {
		name        string
		allowed     []string
		packageName string
		expected    bool
	}{
		{name: "Exact_Allowed", allowed: []string{"com.example.shop"}, packageName: "com.example.shop", expected: true},
		{name: "OtherPackage_Denied", allowed: []string{"com.example.shop"}, packageName: "com.example.shopping", expected: false},
		{name: "CaseSensitive_Denied", allowed: []string{"com.example.shop"}, packageName: "com.Example.shop", expected: false},
		{name: "Wildcard_Allowed", allowed: []string{"com.example.*"}, packageName: "com.example.shop.debug", expected: true},
		{name: "WildcardApex_Denied", allowed: []string{"com.example.*"}, packageName: "com.example", expected: false},
		{name: "WildcardSuffixTrick_Denied", allowed: []string{"com.example.*"}, packageName: "com.exampleevil.shop", expected: false},
		{name: "Any_Allowed", allowed: []string{"*"}, packageName: "org.anything", expected: true},
		{name: "Missing_Denied", allowed: []string{"*"}, packageName: "", expected: false},
		{name: "None_Denied", allowed: []string{}, packageName: "com.example.shop", expected: false},
		{name: "Over256Bytes_Denied", allowed: []string{"*"}, packageName: "com." + strings.Repeat("a", 253), expected: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			app := &Application{Platform: PlatformAndroid, AllowedPackageNames: tc.allowed}
			assert.Equal(t, tc.expected, app.AllowsPackageName(tc.packageName))
		})
	}
}
