// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package report keeps rumreceiver's ingest accounting for the setup screen
// and builds the bounded ingest report and its acknowledgement logic
// (docs/spec/rum.md §10). Sending it over OpAMP is the OpAMP extension's.
package report // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"

import (
	"hash/maphash"
	"net/url"
	"strings"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
)

// Reason is a per-application rejection reason (§10.1). Unknown keys are a
// global counter, not a reason.
type Reason uint8

const (
	BadKey Reason = iota
	OriginNotAllowed
	PackageNotAllowed
	RateLimited
	TooLarge
	InvalidPayload
	numReasons
)

var reasonNames = [numReasons]string{"bad_key", "origin_not_allowed", "package_not_allowed", "rate_limited", "too_large", "invalid_payload"}

func (r Reason) String() string { return reasonNames[r] }

// Bounds of §7.1 and §10.3 for client-controlled strings.
const (
	MaxSDKName    = 64
	MaxSDKVersion = 32
)

// seed is per process: session hashes mean nothing outside it.
var seed = maphash.MakeSeed()

// Batch is what one accepted request contributes (§8.6). It is reused
// across requests.
type Batch struct {
	Sessions   []uint64
	Views      int64
	Errors     int64
	Vitals     int64
	SDKName    string
	SDKVersion string
}

func (b *Batch) Reset() {
	*b = Batch{Sessions: b.Sessions[:0]}
}

// AddSession records a session.id as a 64-bit hash; the ID is not kept.
func (b *Batch) AddSession(id string) {
	b.Sessions = append(b.Sessions, maphash.String(seed, id))
}

// SetSDK keeps a name and version only within their bounds, else empty:
// strings are never truncated into a stored value.
func (b *Batch) SetSDK(name, version string) {
	if len(name) > MaxSDKName {
		name = ""
	}
	if len(version) > MaxSDKVersion {
		version = ""
	}
	b.SDKName, b.SDKVersion = name, version
}

// ObservedOrigin is the lastOrigin kept for origin_not_allowed: the Origin
// when it is an http or https scheme://host[:port] of at most 256 bytes,
// lower-cased; else empty (§10.3).
func ObservedOrigin(origin string) string {
	if origin == "" || len(origin) > ingestpolicy.MaxOriginLength {
		return ""
	}
	origin = strings.ToLower(origin)
	u, err := url.Parse(origin)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil ||
		u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || strings.ContainsAny(origin, "?#") {
		return ""
	}
	return origin
}

// ObservedPackage is the lastOrigin kept for package_not_allowed: the
// package name when it matches the §5 pattern within 256 bytes, else empty.
func ObservedPackage(name string) string {
	if len(name) > ingestpolicy.MaxPackageNameLength || !ingestpolicy.PackageNamePattern.MatchString(name) {
		return ""
	}
	return name
}
