// Copyright FuseOne
// SPDX-License-Identifier: MIT
//
// Moved from fuseone-io/fusescope pkg/types/rumtypes/application.go at
// 6c8991257 (AllowsOrigin, AllowsPackageName, validateOrigin,
// validateURLPrefix, the package name pattern), MIT, see LICENSE-MIT.
// Changed: the allowed origins are parsed once per policy instead of per
// request, and the backend's error helpers are replaced by booleans.

package ingestpolicy // import "github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"

import (
	"net/url"
	"regexp"
	"strings"
)

const (
	anyEntry        = "*"
	packageWildcard = ".*"
	// MaxOriginLength bounds an Origin before it is matched or stored (§7.1).
	MaxOriginLength = 256
	// MaxPackageNameLength bounds an app.package.name likewise.
	MaxPackageNameLength = 256
)

// PackageNamePattern is an Android package name (§5).
var PackageNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z][A-Za-z0-9_]*)+$`)

// originEntry is one allowed origin, lower-cased and parsed.
type originEntry struct {
	any    bool
	scheme string
	port   string
	// host is the hostname, or for a wildcard entry the suffix after `*`
	// (".example.com").
	host     string
	wildcard bool
}

type originKey struct {
	scheme, port, host string
}

func compileOrigin(entry string) (originEntry, bool) {
	if entry == anyEntry {
		return originEntry{any: true}, true
	}
	allowed, err := url.Parse(strings.ToLower(entry))
	if err != nil {
		return originEntry{}, false
	}
	e := originEntry{scheme: allowed.Scheme, port: allowed.Port(), host: allowed.Hostname()}
	if suffix, ok := strings.CutPrefix(e.host, "*"); ok {
		e.host, e.wildcard = suffix, true
	}
	return e, true
}

// parseCandidate lower-cases and parses an Origin header. A candidate
// without scheme or host never matches, so `Origin: null` is refused.
func parseCandidate(origin string) (originKey, bool) {
	if origin == "" || len(origin) > MaxOriginLength {
		return originKey{}, false
	}
	candidate, err := url.Parse(strings.ToLower(origin))
	if err != nil || candidate.Scheme == "" || candidate.Host == "" {
		return originKey{}, false
	}
	return originKey{scheme: candidate.Scheme, port: candidate.Port(), host: candidate.Hostname()}, true
}

// AllowsOrigin matches an Origin header against the application's allowed
// origins. A wildcard entry matches subdomains only, never the apex.
func (a *Application) AllowsOrigin(origin string) bool {
	candidate, ok := parseCandidate(origin)
	if !ok {
		return false
	}
	for _, e := range a.origins {
		if e.matches(candidate) {
			return true
		}
	}
	return false
}

func (e originEntry) matches(c originKey) bool {
	if e.any {
		return true
	}
	if e.scheme != c.scheme || e.port != c.port {
		return false
	}
	if e.wildcard {
		return strings.HasSuffix(c.host, e.host) && len(c.host) > len(e.host)
	}
	return e.host == c.host
}

// AllowsPackageName matches an app.package.name against the allowed package
// names. A trailing .* matches sub-packages only, never the prefix itself.
func (a *Application) AllowsPackageName(packageName string) bool {
	if packageName == "" || len(packageName) > MaxPackageNameLength {
		return false
	}
	for _, entry := range a.AllowedPackageNames {
		if entry == anyEntry || entry == packageName {
			return true
		}
		if prefix, ok := strings.CutSuffix(entry, "*"); ok && strings.HasPrefix(packageName, prefix) && len(packageName) > len(prefix) {
			return true
		}
	}
	return false
}

// validOrigin is an allowed-origin entry: * or scheme://host[:port], with an
// optional leading *. in the host.
func validOrigin(origin string) bool {
	if origin == anyEntry {
		return true
	}
	parsed, err := url.Parse(origin)
	invalid := err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil
	if !invalid {
		invalid = strings.Contains(strings.TrimPrefix(parsed.Hostname(), "*."), "*")
	}
	return !invalid
}

func validPackageName(packageName string) bool {
	return packageName == anyEntry || PackageNamePattern.MatchString(strings.TrimSuffix(packageName, packageWildcard))
}

func validURLPrefix(prefix string) bool {
	parsed, err := url.Parse(prefix)
	return err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != ""
}
