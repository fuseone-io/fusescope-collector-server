// Copyright FuseOne
// SPDX-License-Identifier: MIT
//
// Moved from fuseone-io/fusescope pkg/types/rumtypes/pii.go at 6c8991257
// (MIT, see LICENSE-MIT). Changed: functions over the policy's v1 PII type
// (geoLookup, requireConsent and hashUserId are not in v1, §8.2), and
// scrubQuery keeps a query whose parameters are all allowed without
// allocating.

package rumreceiver // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver"

import (
	"net/url"
	"slices"
	"strings"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
)

// urlAttributes are the span, log and resource attributes scrubbed as URLs
// ([B] rumtypes/pii.go:10-15).
var urlAttributes = [...]string{"url.full", "http.url", "http.target", "url.original", "location.href", "page.url"}

const (
	queryAttribute    = "url.query"
	fragmentAttribute = "url.fragment"
)

// scrubURL keeps a URL without query or fragment as is, keeps one starting
// with an entry of urlAllowlist verbatim, and otherwise drops the fragment
// and filters the query.
func scrubURL(policy *ingestpolicy.PIIPolicy, raw string) string {
	cut := strings.IndexAny(raw, "?#")
	if cut < 0 {
		return raw
	}

	base := raw[:cut]
	for _, prefix := range policy.URLAllowlist {
		if strings.HasPrefix(base, prefix) {
			return raw
		}
	}

	if raw[cut] == '#' {
		return base
	}

	query := raw[cut+1:]
	fragment := false
	if hash := strings.IndexByte(query, '#'); hash >= 0 {
		query, fragment = query[:hash], true
	}

	trimmed := strings.TrimPrefix(query, "?")
	if !policy.DropQuery || allowedQuery(policy, trimmed) {
		switch {
		case trimmed == "":
			return base
		case !fragment && trimmed == query:
			return raw
		}
		return base + "?" + trimmed
	}
	// One allocation: the base and the kept parameters written together.
	var b strings.Builder
	b.Grow(len(base) + 1 + len(trimmed))
	b.WriteString(base)
	sep := byte('?')
	for param := range strings.SplitSeq(trimmed, "&") {
		if allowedParam(policy, param) {
			b.WriteByte(sep)
			b.WriteString(param)
			sep = '&'
		}
	}
	return b.String()
}

// scrubQuery filters a raw query string, keeping the original encoding and
// order of the parameters it keeps.
func scrubQuery(policy *ingestpolicy.PIIPolicy, query string) string {
	query = strings.TrimPrefix(query, "?")
	if !policy.DropQuery {
		return query
	}

	if allowedQuery(policy, query) {
		return query
	}

	var b strings.Builder
	for param := range strings.SplitSeq(query, "&") {
		if allowedParam(policy, param) {
			if b.Len() > 0 {
				b.WriteByte('&')
			}
			b.WriteString(param)
		}
	}
	return b.String()
}

// allowedQuery reports whether every parameter of query is kept.
func allowedQuery(policy *ingestpolicy.PIIPolicy, query string) bool {
	for param := range strings.SplitSeq(query, "&") {
		if !allowedParam(policy, param) {
			return false
		}
	}
	return true
}

func allowedParam(policy *ingestpolicy.PIIPolicy, param string) bool {
	name, _, _ := strings.Cut(param, "=")
	if decoded, err := url.QueryUnescape(name); err == nil {
		name = decoded
	}
	return name != "" && slices.Contains(policy.QueryAllowlist, name)
}

// scrubSpanName scrubs every space-separated token that carries a query or
// a fragment, e.g. "Navigation: /customers/392#tab?email=a@b.c".
func scrubSpanName(policy *ingestpolicy.PIIPolicy, name string) string {
	if !strings.ContainsAny(name, "?#") {
		return name
	}

	tokens := strings.Split(name, " ")
	for i, token := range tokens {
		tokens[i] = scrubURL(policy, token)
	}

	return strings.Join(tokens, " ")
}
