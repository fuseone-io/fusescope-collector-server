// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package ingestpolicy holds the OpAMP ingest policy of docs/spec/rum.md §11:
// its v1 wire types, whole-snapshot strict validation, the revision and
// anti-rollback rules, the on-disk cache, and the immutable snapshot that
// rumreceiver and ingestauth read lock-free.
package ingestpolicy // import "github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"

// SchemaVersion is the only schema this package accepts (§11.1).
const SchemaVersion = 1

// MaxSize is max_policy_size (§11.2, resolved 1).
const MaxSize = 16 << 20

// Bounds of §11.1.
const (
	MaxKeys           = 50_000
	MaxApplications   = 10_000
	MaxListLength     = 50
	MaxNameLength     = 128
	MaxEnvironmentLen = 64
)

// Policy is the v1 `policy` message data (§11.1). Field names are the wire
// names; timestamps stay strings so a bad one is invalid_value, not
// parse_error.
type Policy struct {
	SchemaVersion   int              `json:"schemaVersion"`
	Version         uint64           `json:"version"`
	IssuedAt        string           `json:"issuedAt"`
	Keys            []Key            `json:"keys"`
	RUMApplications []RUMApplication `json:"rumApplications"`
}

// Key is one usable ingestion or RUM key, by verifier.
type Key struct {
	ID            string   `json:"id"`
	SHA256        string   `json:"sha256"`
	OrgID         string   `json:"orgId"`
	Kind          string   `json:"kind"`
	Signals       []string `json:"signals"`
	ExpiresAt     *string  `json:"expiresAt,omitempty"`
	Limit         *Limit   `json:"limit,omitempty"`
	ApplicationID *string  `json:"applicationId,omitempty"`
}

// Limit is this replica's share of a key's rate limit (§7).
type Limit struct {
	Rate  float64 `json:"rate"`
	Burst int64   `json:"burst"`
}

// RUMApplication is what rumreceiver needs of one RUM application.
type RUMApplication struct {
	ID                  string    `json:"id"`
	OrgID               string    `json:"orgId"`
	Name                string    `json:"name"`
	Environment         string    `json:"environment"`
	Platform            string    `json:"platform"`
	AllowedOrigins      []string  `json:"allowedOrigins"`
	AllowedPackageNames []string  `json:"allowedPackageNames"`
	PIIPolicy           PIIPolicy `json:"piiPolicy"`
}

// PIIPolicy is the v1 PII policy; geoLookup, requireConsent and hashUserId
// are not part of v1 (§8.2).
type PIIPolicy struct {
	DropQuery      bool     `json:"dropQuery"`
	QueryAllowlist []string `json:"queryAllowlist"`
	URLAllowlist   []string `json:"urlAllowlist"`
}

// Key kinds ([ADR10] §2).
const (
	KindSecret = "secret"
	KindPublic = "public"
	KindRunner = "runner"
)

// Platforms; ios is reserved and rejected (§3).
const (
	PlatformWeb     = "web"
	PlatformAndroid = "android"
)

// Signal is a bit set of the signals a key may send.
type Signal uint8

const (
	SignalTraces Signal = 1 << iota
	SignalMetrics
	SignalLogs
	SignalProfiles
	SignalArtifacts
)

var signalNames = map[string]Signal{
	"traces":    SignalTraces,
	"metrics":   SignalMetrics,
	"logs":      SignalLogs,
	"profiles":  SignalProfiles,
	"artifacts": SignalArtifacts,
}

// Code is a rejection code of §11.2, reported as policy.error.
type Code string

const (
	CodeTooLarge          Code = "too_large"
	CodeParseError        Code = "parse_error"
	CodeSchemaVersion     Code = "schema_version"
	CodeUnknownField      Code = "unknown_field"
	CodeLowerVersion      Code = "lower_version"
	CodeVersionConflict   Code = "version_conflict"
	CodeDuplicateID       Code = "duplicate_id"
	CodeDuplicateVerifier Code = "duplicate_verifier"
	CodeDanglingReference Code = "dangling_reference"
	CodeOrgMismatch       Code = "org_mismatch"
	CodeInvalidValue      Code = "invalid_value"
)

// Codes lists every rejection code, for report validation (§10.3).
var Codes = []Code{
	CodeTooLarge, CodeParseError, CodeSchemaVersion, CodeUnknownField, CodeLowerVersion,
	CodeVersionConflict, CodeDuplicateID, CodeDuplicateVerifier, CodeDanglingReference,
	CodeOrgMismatch, CodeInvalidValue,
}

// RejectionError is a rejected snapshot. Path names where, never what: the
// policy is not logged (§11.2, §11.3).
type RejectionError struct {
	Code Code
	Path string
}

func (e *RejectionError) Error() string {
	if e.Path == "" {
		return "ingest policy rejected: " + string(e.Code)
	}
	return "ingest policy rejected: " + string(e.Code) + " at " + e.Path
}

func reject(code Code, path string) *RejectionError {
	return &RejectionError{Code: code, Path: path}
}
