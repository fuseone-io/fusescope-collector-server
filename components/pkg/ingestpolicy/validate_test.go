// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package ingestpolicy

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateCodes(t *testing.T) {
	testCases := []struct {
		name string
		edit func(p *Policy)
		code Code
		path string
	}{
		{name: "Valid_Accepted", edit: func(*Policy) {}},
		{name: "VersionZero_InvalidValue", edit: func(p *Policy) { p.Version = 0 }, code: CodeInvalidValue, path: "version"},
		{name: "IssuedAtNotRFC3339_InvalidValue", edit: func(p *Policy) { p.IssuedAt = "yesterday" }, code: CodeInvalidValue, path: "issuedAt"},
		{name: "TooManyKeys_InvalidValue", edit: func(p *Policy) { p.Keys = make([]Key, MaxKeys+1) }, code: CodeInvalidValue, path: "keys"},
		{name: "TooManyApplications_InvalidValue", edit: func(p *Policy) { p.RUMApplications = make([]RUMApplication, MaxApplications+1) }, code: CodeInvalidValue, path: "rumApplications"},
		{name: "KeyIDNotUUID_InvalidValue", edit: func(p *Policy) { p.Keys[0].ID = "key-1" }, code: CodeInvalidValue, path: "keys[0].id"},
		{name: "KeyIDUppercase_InvalidValue", edit: func(p *Policy) { p.Keys[0].ID = strings.ToUpper(p.Keys[0].ID) }, code: CodeInvalidValue, path: "keys[0].id"},
		{name: "VerifierUppercase_InvalidValue", edit: func(p *Policy) { p.Keys[0].SHA256 = strings.ToUpper(p.Keys[0].SHA256) }, code: CodeInvalidValue, path: "keys[0].sha256"},
		{name: "VerifierShort_InvalidValue", edit: func(p *Policy) { p.Keys[0].SHA256 = p.Keys[0].SHA256[:63] }, code: CodeInvalidValue, path: "keys[0].sha256"},
		{name: "OrgNotUUID_InvalidValue", edit: func(p *Policy) { p.Keys[0].OrgID = "org" }, code: CodeInvalidValue, path: "keys[0].orgId"},
		{name: "UnknownKind_InvalidValue", edit: func(p *Policy) { p.Keys[0].Kind = "admin" }, code: CodeInvalidValue, path: "keys[0].kind"},
		{name: "NoSignals_InvalidValue", edit: func(p *Policy) { p.Keys[0].Signals = []string{} }, code: CodeInvalidValue, path: "keys[0].signals"},
		{name: "UnknownSignal_InvalidValue", edit: func(p *Policy) { p.Keys[2].Signals = []string{"events"} }, code: CodeInvalidValue, path: "keys[2].signals"},
		{name: "DuplicateSignal_InvalidValue", edit: func(p *Policy) { p.Keys[0].Signals = []string{"logs", "logs"} }, code: CodeInvalidValue, path: "keys[0].signals"},
		{name: "PublicWithMetrics_InvalidValue", edit: func(p *Policy) { p.Keys[0].Signals = []string{"traces", "metrics"} }, code: CodeInvalidValue, path: "keys[0].signals"},
		{name: "SecretWithProfiles_Accepted", edit: func(p *Policy) { p.Keys[2].Signals = []string{"profiles", "artifacts"} }},
		{name: "ExpiresAtNotRFC3339_InvalidValue", edit: func(p *Policy) { p.Keys[1].ExpiresAt = ptr("2026-11-04") }, code: CodeInvalidValue, path: "keys[1].expiresAt"},
		{name: "LimitRateZero_InvalidValue", edit: func(p *Policy) { p.Keys[0].Limit = &Limit{Rate: 0, Burst: 1} }, code: CodeInvalidValue, path: "keys[0].limit"},
		{name: "LimitBurstZero_InvalidValue", edit: func(p *Policy) { p.Keys[0].Limit = &Limit{Rate: 1, Burst: 0} }, code: CodeInvalidValue, path: "keys[0].limit"},
		{name: "ApplicationIDNotUUID_InvalidValue", edit: func(p *Policy) { p.Keys[0].ApplicationID = ptr("shop") }, code: CodeInvalidValue, path: "keys[0].applicationId"},
		{name: "ApplicationIDNotUUID_InvalidValue_App", edit: func(p *Policy) { p.RUMApplications[0].ID = "shop" }, code: CodeInvalidValue, path: "rumApplications[0].id"},
		{name: "AppOrgNotUUID_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].OrgID = "" }, code: CodeInvalidValue, path: "rumApplications[0].orgId"},
		{name: "EmptyName_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].Name = "" }, code: CodeInvalidValue, path: "rumApplications[0].name"},
		{name: "UntrimmedName_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].Name = " shop" }, code: CodeInvalidValue, path: "rumApplications[0].name"},
		{name: "NameAt128Bytes_Accepted", edit: func(p *Policy) { p.RUMApplications[0].Name = strings.Repeat("n", 128) }},
		{name: "NameOver128Bytes_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].Name = strings.Repeat("n", 129) }, code: CodeInvalidValue, path: "rumApplications[0].name"},
		{name: "EnvironmentOver64Bytes_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].Environment = strings.Repeat("e", 65) }, code: CodeInvalidValue, path: "rumApplications[0].environment"},
		{name: "PlatformIOSReserved_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].Platform = "ios" }, code: CodeInvalidValue, path: "rumApplications[0].platform"},
		{name: "OriginWithPath_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].AllowedOrigins = []string{"https://a.com/path"} }, code: CodeInvalidValue, path: "rumApplications[0].allowedOrigins"},
		{name: "OriginWithoutScheme_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].AllowedOrigins = []string{"a.com"} }, code: CodeInvalidValue, path: "rumApplications[0].allowedOrigins"},
		{name: "OriginFTP_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].AllowedOrigins = []string{"ftp://a.com"} }, code: CodeInvalidValue, path: "rumApplications[0].allowedOrigins"},
		{name: "OriginMidWildcard_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].AllowedOrigins = []string{"https://a.*.com"} }, code: CodeInvalidValue, path: "rumApplications[0].allowedOrigins"},
		{name: "OriginUserInfo_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].AllowedOrigins = []string{"https://u@a.com"} }, code: CodeInvalidValue, path: "rumApplications[0].allowedOrigins"},
		{name: "OriginQuery_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].AllowedOrigins = []string{"https://a.com?x=1"} }, code: CodeInvalidValue, path: "rumApplications[0].allowedOrigins"},
		{name: "OriginAny_Accepted", edit: func(p *Policy) { p.RUMApplications[0].AllowedOrigins = []string{"*"} }},
		{name: "TooManyOrigins_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].AllowedOrigins = repeat("https://a.com", 51) }, code: CodeInvalidValue, path: "rumApplications[0].allowedOrigins"},
		{name: "AndroidWithOrigins_InvalidValue", edit: func(p *Policy) { p.RUMApplications[1].AllowedOrigins = []string{"https://a.com"} }, code: CodeInvalidValue, path: "rumApplications[1].allowedOrigins"},
		{name: "WebWithPackageNames_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].AllowedPackageNames = []string{"com.example.shop"} }, code: CodeInvalidValue, path: "rumApplications[0].allowedPackageNames"},
		{name: "SingleSegmentPackage_InvalidValue", edit: func(p *Policy) { p.RUMApplications[1].AllowedPackageNames = []string{"shop"} }, code: CodeInvalidValue, path: "rumApplications[1].allowedPackageNames"},
		{name: "PackageDigitSegment_InvalidValue", edit: func(p *Policy) { p.RUMApplications[1].AllowedPackageNames = []string{"com.1shop"} }, code: CodeInvalidValue, path: "rumApplications[1].allowedPackageNames"},
		{name: "PackageMidWildcard_InvalidValue", edit: func(p *Policy) { p.RUMApplications[1].AllowedPackageNames = []string{"com.*.shop"} }, code: CodeInvalidValue, path: "rumApplications[1].allowedPackageNames"},
		{name: "PackageWildcardAndAny_Accepted", edit: func(p *Policy) { p.RUMApplications[1].AllowedPackageNames = []string{"com.example.*", "*"} }},
		{name: "TooManyPackages_InvalidValue", edit: func(p *Policy) { p.RUMApplications[1].AllowedPackageNames = repeat("com.a.b", 51) }, code: CodeInvalidValue, path: "rumApplications[1].allowedPackageNames"},
		{name: "EmptyQueryName_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].PIIPolicy.QueryAllowlist = []string{" "} }, code: CodeInvalidValue, path: "rumApplications[0].piiPolicy.queryAllowlist"},
		{name: "TooManyQueryNames_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].PIIPolicy.QueryAllowlist = repeat("q", 51) }, code: CodeInvalidValue, path: "rumApplications[0].piiPolicy.queryAllowlist"},
		{name: "RelativeURLPrefix_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].PIIPolicy.URLAllowlist = []string{"/api"} }, code: CodeInvalidValue, path: "rumApplications[0].piiPolicy.urlAllowlist"},
		{name: "TooManyURLPrefixes_InvalidValue", edit: func(p *Policy) { p.RUMApplications[0].PIIPolicy.URLAllowlist = repeat("https://a.com/", 51) }, code: CodeInvalidValue, path: "rumApplications[0].piiPolicy.urlAllowlist"},
		{name: "DuplicateKeyID_DuplicateID", edit: func(p *Policy) { p.Keys[1].ID = p.Keys[0].ID }, code: CodeDuplicateID, path: "keys[1].id"},
		{name: "DuplicateApplicationID_DuplicateID", edit: func(p *Policy) { p.RUMApplications[1].ID = p.RUMApplications[0].ID }, code: CodeDuplicateID, path: "rumApplications[1].id"},
		{name: "KeyAndApplicationShareID_Accepted", edit: func(p *Policy) { p.Keys[2].ID = webApp }},
		{name: "DuplicateVerifier_DuplicateVerifier", edit: func(p *Policy) { p.Keys[2].SHA256 = p.Keys[0].SHA256 }, code: CodeDuplicateVerifier, path: "keys[2].sha256"},
		{name: "PublicWithoutApplication_Dangling", edit: func(p *Policy) { p.Keys[0].ApplicationID = nil }, code: CodeDanglingReference, path: "keys[0].applicationId"},
		{name: "PublicEmptyApplication_Dangling", edit: func(p *Policy) { p.Keys[0].ApplicationID = ptr("") }, code: CodeDanglingReference, path: "keys[0].applicationId"},
		{name: "PublicUnknownApplication_Dangling", edit: func(p *Policy) { p.Keys[0].ApplicationID = ptr(secretID) }, code: CodeDanglingReference, path: "keys[0].applicationId"},
		{name: "SecretWithApplication_Dangling", edit: func(p *Policy) { p.Keys[2].ApplicationID = ptr(webApp) }, code: CodeDanglingReference, path: "keys[2].applicationId"},
		{name: "RunnerWithApplication_Dangling", edit: func(p *Policy) { p.Keys[2].Kind = KindRunner; p.Keys[2].ApplicationID = ptr("") }, code: CodeDanglingReference, path: "keys[2].applicationId"},
		{name: "KeyOrgDiffersFromApplication_OrgMismatch", edit: func(p *Policy) { p.Keys[1].OrgID = orgB }, code: CodeOrgMismatch, path: "keys[1].orgId"},
		{name: "SecretKeyOtherOrg_Accepted", edit: func(p *Policy) { p.Keys[2].OrgID = orgB }},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := testPolicy(3)
			tc.edit(p)
			err := Validate(p)
			assert.Equal(t, tc.code, codeOf(t, err), "%v", err)
			if tc.path != "" {
				assert.Equal(t, tc.path, err.(*RejectionError).Path)
			}
		})
	}
}

func repeat(s string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = s
	}
	return out
}
