// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package ingestpolicy // import "github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"

import (
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

// Validate checks the values of a parsed policy (§11.1, §11.2): every field
// first (invalid_value), then the cross-references in the order of the
// §11.2 table. Revision rules need the floor and are the Store's.
func Validate(p *Policy) error {
	if err := validateFields(p); err != nil {
		return err
	}
	return validateReferences(p)
}

func validateFields(p *Policy) error {
	if p.Version < 1 {
		return reject(CodeInvalidValue, "version")
	}
	if !validTime(p.IssuedAt) {
		return reject(CodeInvalidValue, "issuedAt")
	}
	if len(p.Keys) > MaxKeys {
		return reject(CodeInvalidValue, "keys")
	}
	if len(p.RUMApplications) > MaxApplications {
		return reject(CodeInvalidValue, "rumApplications")
	}
	for i := range p.Keys {
		if f := validateKey(&p.Keys[i]); f != "" {
			return reject(CodeInvalidValue, "keys["+strconv.Itoa(i)+"]."+f)
		}
	}
	for i := range p.RUMApplications {
		if f := validateApplication(&p.RUMApplications[i]); f != "" {
			return reject(CodeInvalidValue, "rumApplications["+strconv.Itoa(i)+"]."+f)
		}
	}
	return nil
}

// validateKey returns the first invalid field, or "".
func validateKey(k *Key) string {
	switch {
	case !validUUID(k.ID):
		return "id"
	case !validVerifier(k.SHA256):
		return "sha256"
	case !validUUID(k.OrgID):
		return "orgId"
	case k.Kind != KindSecret && k.Kind != KindPublic && k.Kind != KindRunner:
		return "kind"
	case k.ExpiresAt != nil && !validTime(*k.ExpiresAt):
		return "expiresAt"
	case k.Limit != nil && (!(k.Limit.Rate > 0) || k.Limit.Burst < 1):
		return "limit"
	case k.ApplicationID != nil && *k.ApplicationID != "" && !validUUID(*k.ApplicationID):
		return "applicationId"
	}
	if _, ok := signalSet(k); !ok {
		return "signals"
	}
	return ""
}

func signalSet(k *Key) (Signal, bool) {
	if len(k.Signals) == 0 {
		return 0, false
	}
	var set Signal
	for _, name := range k.Signals {
		s, ok := signalNames[name]
		if !ok || set&s != 0 {
			return 0, false
		}
		set |= s
	}
	if k.Kind == KindPublic && set&^(SignalTraces|SignalLogs) != 0 {
		return 0, false
	}
	return set, true
}

func validateApplication(a *RUMApplication) string {
	switch {
	case !validUUID(a.ID):
		return "id"
	case !validUUID(a.OrgID):
		return "orgId"
	case !validText(a.Name, MaxNameLength):
		return "name"
	case !validText(a.Environment, MaxEnvironmentLen):
		return "environment"
	case a.Platform != PlatformWeb && a.Platform != PlatformAndroid:
		return "platform"
	case len(a.AllowedOrigins) > MaxListLength, a.Platform == PlatformAndroid && len(a.AllowedOrigins) > 0:
		return "allowedOrigins"
	case len(a.AllowedPackageNames) > MaxListLength, a.Platform == PlatformWeb && len(a.AllowedPackageNames) > 0:
		return "allowedPackageNames"
	case len(a.PIIPolicy.QueryAllowlist) > MaxListLength:
		return "piiPolicy.queryAllowlist"
	case len(a.PIIPolicy.URLAllowlist) > MaxListLength:
		return "piiPolicy.urlAllowlist"
	}
	for _, o := range a.AllowedOrigins {
		if !validOrigin(o) {
			return "allowedOrigins"
		}
	}
	for _, p := range a.AllowedPackageNames {
		if !validPackageName(p) {
			return "allowedPackageNames"
		}
	}
	for _, q := range a.PIIPolicy.QueryAllowlist {
		if strings.TrimSpace(q) == "" {
			return "piiPolicy.queryAllowlist"
		}
	}
	for _, u := range a.PIIPolicy.URLAllowlist {
		if !validURLPrefix(u) {
			return "piiPolicy.urlAllowlist"
		}
	}
	return ""
}

func validateReferences(p *Policy) error {
	apps := make(map[string]*RUMApplication, len(p.RUMApplications))
	for i := range p.RUMApplications {
		a := &p.RUMApplications[i]
		if _, dup := apps[a.ID]; dup {
			return reject(CodeDuplicateID, "rumApplications["+strconv.Itoa(i)+"].id")
		}
		apps[a.ID] = a
	}
	ids := make(map[string]struct{}, len(p.Keys))
	for i := range p.Keys {
		if _, dup := ids[p.Keys[i].ID]; dup {
			return reject(CodeDuplicateID, "keys["+strconv.Itoa(i)+"].id")
		}
		ids[p.Keys[i].ID] = struct{}{}
	}
	verifiers := make(map[string]struct{}, len(p.Keys))
	for i := range p.Keys {
		if _, dup := verifiers[p.Keys[i].SHA256]; dup {
			return reject(CodeDuplicateVerifier, "keys["+strconv.Itoa(i)+"].sha256")
		}
		verifiers[p.Keys[i].SHA256] = struct{}{}
	}
	for i := range p.Keys {
		k := &p.Keys[i]
		if k.Kind != KindPublic {
			if k.ApplicationID != nil {
				return reject(CodeDanglingReference, "keys["+strconv.Itoa(i)+"].applicationId")
			}
			continue
		}
		if k.ApplicationID == nil || apps[*k.ApplicationID] == nil {
			return reject(CodeDanglingReference, "keys["+strconv.Itoa(i)+"].applicationId")
		}
	}
	for i := range p.Keys {
		k := &p.Keys[i]
		if k.Kind == KindPublic && apps[*k.ApplicationID].OrgID != k.OrgID {
			return reject(CodeOrgMismatch, "keys["+strconv.Itoa(i)+"].orgId")
		}
	}
	return nil
}

// validUUID accepts the canonical lowercase form the backend writes.
func validUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !isLowerHex(c) {
				return false
			}
		}
	}
	return true
}

func validVerifier(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isLowerHex(s[i]) {
			return false
		}
	}
	return true
}

func isLowerHex(c byte) bool { return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' }

// validText is a non-empty, already trimmed string of at most limit bytes;
// the backend trims before it stores and sends ([B] rumtypes/application.go:332-342).
func validText(s string, limit int) bool {
	return s != "" && len(s) <= limit && strings.TrimSpace(s) == s
}

func validTime(s string) bool {
	_, err := time.Parse(time.RFC3339, s)
	return err == nil
}

func decodeVerifier(s string) (v [32]byte) {
	_, _ = hex.Decode(v[:], []byte(s))
	return v
}
