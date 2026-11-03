// Copyright FuseOne
// SPDX-License-Identifier: MIT
//
// Moved from fuseone-io/fusescope pkg/types/rumtypes/pii_test.go at
// 6c8991257 (MIT, see LICENSE-MIT), against the v1 policy type.

package rumreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
)

func defaultPII() ingestpolicy.PIIPolicy {
	return ingestpolicy.PIIPolicy{DropQuery: true, QueryAllowlist: []string{}, URLAllowlist: []string{}}
}

func TestPIIPolicyScrubURL(t *testing.T) {
	testCases := []struct {
		name     string
		policy   ingestpolicy.PIIPolicy
		input    string
		expected string
	}{
		{name: "NoQueryNoFragment_Unchanged", policy: defaultPII(), input: "https://shop.example.com/cart", expected: "https://shop.example.com/cart"},
		{name: "QueryAndFragment_Dropped", policy: defaultPII(), input: "https://shop.example.com/customers/392?email=a@b.c#tab", expected: "https://shop.example.com/customers/392"},
		{name: "FragmentBeforeQuery_Dropped", policy: defaultPII(), input: "/customers/392#tab?email=a@b.c", expected: "/customers/392"},
		{name: "AllowlistedParam_Kept", policy: ingestpolicy.PIIPolicy{DropQuery: true, QueryAllowlist: []string{"page"}}, input: "/search?q=secret&page=2&token=x", expected: "/search?page=2"},
		{name: "AllowlistedEncodedParam_Kept", policy: ingestpolicy.PIIPolicy{DropQuery: true, QueryAllowlist: []string{"sort by"}}, input: "/list?sort%20by=name&id=7", expected: "/list?sort%20by=name"},
		{name: "AllowlistedParamWithoutValue_Kept", policy: ingestpolicy.PIIPolicy{DropQuery: true, QueryAllowlist: []string{"debug"}}, input: "/x?debug&user=1", expected: "/x?debug"},
		{name: "AllParamsAllowlisted_KeptInOrder", policy: ingestpolicy.PIIPolicy{DropQuery: true, QueryAllowlist: []string{"b", "a"}}, input: "/x?a=1&b=2#f", expected: "/x?a=1&b=2"},
		{name: "DropQueryDisabled_QueryKeptFragmentDropped", policy: ingestpolicy.PIIPolicy{DropQuery: false}, input: "/search?q=shoes#results", expected: "/search?q=shoes"},
		{name: "URLAllowlisted_KeptVerbatim", policy: ingestpolicy.PIIPolicy{DropQuery: true, URLAllowlist: []string{"https://api.example.com/public/"}}, input: "https://api.example.com/public/items?id=3#top", expected: "https://api.example.com/public/items?id=3#top"},
		{name: "URLAllowlistOtherHost_Scrubbed", policy: ingestpolicy.PIIPolicy{DropQuery: true, URLAllowlist: []string{"https://api.example.com/public/"}}, input: "https://evil.example.com/public/items?id=3", expected: "https://evil.example.com/public/items"},
		{name: "EmptyQuery_Removed", policy: defaultPII(), input: "/cart?", expected: "/cart"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, scrubURL(&tc.policy, tc.input))
		})
	}
}

func TestPIIPolicyScrubQuery(t *testing.T) {
	testCases := []struct {
		name     string
		policy   ingestpolicy.PIIPolicy
		input    string
		expected string
	}{
		{name: "Default_AllDropped", policy: defaultPII(), input: "email=a@b.c&page=1", expected: ""},
		{name: "Allowlisted_Kept", policy: ingestpolicy.PIIPolicy{DropQuery: true, QueryAllowlist: []string{"page"}}, input: "email=a@b.c&page=1", expected: "page=1"},
		{name: "LeadingQuestionMark_Kept", policy: ingestpolicy.PIIPolicy{DropQuery: true, QueryAllowlist: []string{"page"}}, input: "?page=1&x=2", expected: "page=1"},
		{name: "DropQueryDisabled_Unchanged", policy: ingestpolicy.PIIPolicy{DropQuery: false}, input: "email=a@b.c", expected: "email=a@b.c"},
		{name: "EmptyParamsDropped", policy: ingestpolicy.PIIPolicy{DropQuery: true, QueryAllowlist: []string{"a", "b"}}, input: "a=1&&b=2", expected: "a=1&b=2"},
		{name: "Empty_Empty", policy: defaultPII(), input: "", expected: ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, scrubQuery(&tc.policy, tc.input))
		})
	}
}

func TestPIIPolicyScrubSpanName(t *testing.T) {
	testCases := []struct {
		name     string
		policy   ingestpolicy.PIIPolicy
		input    string
		expected string
	}{
		{name: "PlainName_Unchanged", policy: defaultPII(), input: "documentLoad", expected: "documentLoad"},
		{name: "NavigationWithFragmentAndQuery_Scrubbed", policy: defaultPII(), input: "Navigation: /customers/392#tab?email=a@b.c", expected: "Navigation: /customers/392"},
		{name: "HTTPMethodAndURL_Scrubbed", policy: defaultPII(), input: "GET https://api.example.com/users?token=abc", expected: "GET https://api.example.com/users"},
		{name: "AllowlistedParamInName_Kept", policy: ingestpolicy.PIIPolicy{DropQuery: true, QueryAllowlist: []string{"page"}}, input: "GET /items?page=3&session=x", expected: "GET /items?page=3"},
		{name: "SpacesPreserved", policy: defaultPII(), input: "click  on /a?b=c", expected: "click  on /a"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, scrubSpanName(&tc.policy, tc.input))
		})
	}
}

func TestScrubQueryAllKeptDoesNotAllocate(t *testing.T) {
	policy := ingestpolicy.PIIPolicy{DropQuery: true, QueryAllowlist: []string{"page", "sort"}}
	allocs := testing.AllocsPerRun(100, func() { _ = scrubURL(&policy, "https://a.example.com/x") })
	assert.Zero(t, allocs)
	allocs = testing.AllocsPerRun(100, func() { _ = scrubQuery(&policy, "page=2&sort=asc") })
	assert.Zero(t, allocs)
}
