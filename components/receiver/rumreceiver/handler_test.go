// Copyright FuseOne
// SPDX-License-Identifier: MIT
//
// TestGatewayAuthAndCORS, TestGatewayPreflight, TestGatewayRateLimit,
// TestGatewayDecoding, TestGatewayLogsEventName,
// TestGatewayConsumerFailures and TestGatewayAndroidPackageNames are moved
// from fuseone-io/fusescope
// pkg/modules/rumapplication/implrumapplication/gateway_test.go at
// 6c8991257 (MIT, see LICENSE-MIT), adapted to the ingest policy and the
// next consumer. As docs/spec/rum.md §14 says, the query-key cases become
// "query key ignored" and "preflight by origin only", and the per-application
// unknown-key cases become global-counter cases. The other tests are new.

package rumreceiver

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
	"github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"
)

func TestGatewayAuthAndCORS(t *testing.T) {
	f := newFixture(t, nil)

	testCases := []struct {
		name        string
		target      string
		headers     map[string]string
		status      int
		allowOrigin string
		forwarded   bool
		events      []event
	}{
		{name: "MissingKey_Unauthorized", target: tracesPath, headers: jsonHeaders(""), status: http.StatusUnauthorized},
		{name: "UnknownKey_Unauthorized", target: tracesPath, headers: jsonHeaders(unknownKey), status: http.StatusUnauthorized, events: []event{{kind: "unknown"}}},
		{name: "HeaderKeyNoOrigin_Accepted", target: tracesPath, headers: jsonHeaders(webKey), status: http.StatusOK, forwarded: true},
		{
			name: "QueryKeyIgnored_Unauthorized", target: tracesPath + "?fusescope_rum_key=" + webKey,
			headers: map[string]string{"Content-Type": "text/plain;charset=UTF-8", "Origin": "https://shop.example.com"}, status: http.StatusUnauthorized,
		},
		{name: "AllowedOrigin_EchoedBack", target: tracesPath, headers: jsonHeaders(webKey, "Origin", "https://shop.example.com"), status: http.StatusOK, allowOrigin: "https://shop.example.com", forwarded: true},
		{name: "WildcardOrigin_EchoedBack", target: tracesPath, headers: jsonHeaders(webKey, "Origin", "https://eu.cdn.example.com"), status: http.StatusOK, allowOrigin: "https://eu.cdn.example.com", forwarded: true},
		{
			name: "OtherOrigin_Forbidden", target: tracesPath, headers: jsonHeaders(webKey, "Origin", "https://evil.example.org"), status: http.StatusForbidden,
			events: []event{{kind: "reject", app: testWebApp, reason: report.OriginNotAllowed, observed: "https://evil.example.org"}},
		},
		{
			name: "NullOrigin_ForbiddenNotObserved", target: tracesPath, headers: jsonHeaders(webKey, "Origin", "null"), status: http.StatusForbidden,
			events: []event{{kind: "reject", app: testWebApp, reason: report.OriginNotAllowed}},
		},
		{
			name: "OriginOver256Bytes_ForbiddenNotObserved", target: tracesPath, headers: jsonHeaders(webKey, "Origin", "https://"+strings.Repeat("a", 250)+".cdn.example.com"), status: http.StatusForbidden,
			events: []event{{kind: "reject", app: testWebApp, reason: report.OriginNotAllowed}},
		},
		{name: "KeyWithoutPrefix_Unauthorized", target: tracesPath, headers: jsonHeaders("rum_sec_abcdefghijklmnopqrstuvwxyz234567"), status: http.StatusUnauthorized},
		{name: "PrefixOnly_Unauthorized", target: tracesPath, headers: jsonHeaders("rum_pub_"), status: http.StatusUnauthorized},
		{name: "KeyOver128Bytes_Unauthorized", target: tracesPath, headers: jsonHeaders(webKey + strings.Repeat("a", 129-len(webKey))), status: http.StatusUnauthorized},
		{name: "SecretKindKey_UnauthorizedAsUnknown", target: tracesPath, headers: jsonHeaders(secretKind), status: http.StatusUnauthorized, events: []event{{kind: "unknown"}}},
		{
			name: "ExpiredKey_UnauthorizedBadKey", target: tracesPath, headers: jsonHeaders(expiredKey, "Origin", "https://shop.example.com"), status: http.StatusUnauthorized,
			events: []event{{kind: "reject", app: testWebApp, reason: report.BadKey}},
		},
		{
			// Maintainer review of #43: authorisation, not authentication.
			name: "SignalNotAllowed_ForbiddenOwnReason", target: tracesPath, headers: jsonHeaders(logsOnlyKey, "Origin", "https://shop.example.com"), status: http.StatusForbidden,
			events: []event{{kind: "reject", app: testWebApp, reason: report.SignalNotAllowed}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			before := f.traces.SpanCount()
			response := f.post(tc.target, testTraceJSON, tc.headers)

			assert.Equal(t, tc.status, response.Code, response.Body.String())
			assert.Equal(t, tc.allowOrigin, response.Header().Get("Access-Control-Allow-Origin"))
			assert.Contains(t, response.Header().Values("Vary"), "Origin")
			assert.Empty(t, response.Header().Get("Access-Control-Allow-Credentials"))
			assert.Equal(t, tc.forwarded, f.traces.SpanCount() > before)
			events := f.acct.take()
			if tc.forwarded {
				require.Len(t, events, 1)
				assert.Equal(t, "ingest", events[0].kind)
				return
			}
			assert.Equal(t, tc.events, events)
		})
	}
}

func TestGatewayUnknownKeysGlobalCounter(t *testing.T) {
	testCases := []struct {
		name    string
		key     string
		origin  string
		counted bool
	}{
		{name: "UnknownKey_AllowedOrigin_CountedGlobally", key: unknownKey, origin: "https://shop.example.com", counted: true},
		{name: "UnknownKey_OtherOrigin_CountedGlobally", key: unknownKey, origin: "https://evil.example.org", counted: true},
		{name: "UnknownKey_NoOrigin_CountedGlobally", key: unknownKey, counted: true},
		{name: "MalformedKey_NotCounted", key: "not-a-key", origin: "https://shop.example.com"},
		{name: "MissingKey_NotCounted", origin: "https://shop.example.com"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, nil)
			headers := jsonHeaders(tc.key)
			if tc.origin != "" {
				headers["Origin"] = tc.origin
			}

			response := f.post(tracesPath, testTraceJSON, headers)
			assert.Equal(t, http.StatusUnauthorized, response.Code)
			assert.Empty(t, response.Header().Get("Access-Control-Allow-Origin"))

			pages, err := f.acct.monitor.Build(1, f.now, ingestpolicy.Status{Version: 1})
			require.NoError(t, err)
			data := bytes.Join(pages, nil)
			r, err := report.Merge(pages)
			require.NoError(t, err)
			assert.Empty(t, r.Applications, "an unknown key is never attributed to an application")
			if tc.counted {
				assert.Equal(t, int64(1), r.UnknownKeys.Count)
				assert.NotContains(t, string(data), "evil.example.org")
				assert.NotContains(t, string(data), "shop.example.com")
				return
			}
			assert.Zero(t, r.UnknownKeys.Count)
		})
	}
}

func TestGatewayPreflight(t *testing.T) {
	f := newFixture(t, nil)

	testCases := []struct {
		name    string
		target  string
		origin  string
		method  string
		status  int
		allowed bool
	}{
		{name: "KnownOrigin_Allowed", target: tracesPath, origin: "https://shop.example.com", method: http.MethodPost, status: http.StatusNoContent, allowed: true},
		{name: "KnownOriginLogs_Allowed", target: logsPath, origin: "https://a.cdn.example.com", method: http.MethodPost, status: http.StatusNoContent, allowed: true},
		{name: "UnknownOrigin_Forbidden", target: tracesPath, origin: "https://evil.example.org", method: http.MethodPost, status: http.StatusForbidden},
		{name: "WrongMethod_Forbidden", target: tracesPath, origin: "https://shop.example.com", method: http.MethodPut, status: http.StatusForbidden},
		{name: "NoOrigin_Forbidden", target: tracesPath, method: http.MethodPost, status: http.StatusForbidden},
		{name: "QueryKeyOtherOrigin_Forbidden", target: tracesPath + "?fusescope_rum_key=" + webKey, origin: "https://evil.example.org", method: http.MethodPost, status: http.StatusForbidden},
		{name: "ByOriginOnly_NoKeyNeeded", target: tracesPath, origin: "https://eu.cdn.example.com", method: http.MethodPost, status: http.StatusNoContent, allowed: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			headers := map[string]string{"Access-Control-Request-Method": tc.method, "Access-Control-Request-Headers": "content-type, fusescope-rum-key"}
			if tc.origin != "" {
				headers["Origin"] = tc.origin
			}
			response := f.do(http.MethodOptions, tc.target, nil, headers)

			assert.Equal(t, tc.status, response.Code)
			assert.Contains(t, response.Header().Values("Vary"), "Origin")
			if !tc.allowed {
				assert.Empty(t, response.Header().Get("Access-Control-Allow-Origin"))
				return
			}
			assert.Equal(t, tc.origin, response.Header().Get("Access-Control-Allow-Origin"))
			assert.Equal(t, http.MethodPost, response.Header().Get("Access-Control-Allow-Methods"))
			assert.Equal(t, "content-type, content-encoding, fusescope-rum-key", response.Header().Get("Access-Control-Allow-Headers"))
			assert.Equal(t, "86400", response.Header().Get("Access-Control-Max-Age"))
			assert.Empty(t, response.Header().Get("Access-Control-Allow-Credentials"))
		})
	}
	assert.Empty(t, f.acct.take(), "a preflight is never counted")
}

func TestGatewayRateLimit(t *testing.T) {
	f := newFixture(t, func(cfg *Config) { cfg.RateLimit.Client = Limit{Rate: 0.001, Burst: 2} })
	headers := jsonHeaders(webKey)

	codes := []int{}
	for range 3 {
		codes = append(codes, f.post(tracesPath, testTraceJSON, headers).Code)
	}
	assert.Equal(t, []int{http.StatusOK, http.StatusOK, http.StatusTooManyRequests}, codes)

	response := f.post(tracesPath, testTraceJSON, headers)
	assert.Equal(t, "1", response.Header().Get("Retry-After"))
	assert.Equal(t, 2, f.traces.SpanCount())
	events := f.acct.take()
	assert.Equal(t, event{kind: "reject", app: testWebApp, reason: report.RateLimited}, events[len(events)-1])
}

func TestGatewayRateLimitPerKeyFromPolicy(t *testing.T) {
	f := newFixture(t, nil)
	headers := jsonHeaders(limitedKey)

	// The policy gives this key {rate 0.001, burst 1}: one request, whatever the client.
	assert.Equal(t, http.StatusOK, f.post(tracesPath, testTraceJSON, headers).Code)
	req := f.do(http.MethodPost, tracesPath, strings.NewReader(testTraceJSON), headers)
	assert.Equal(t, http.StatusTooManyRequests, req.Code)
	// The default key limit applies to a key without one.
	for range 5 {
		assert.Equal(t, http.StatusOK, f.post(tracesPath, testTraceJSON, jsonHeaders(webKey)).Code)
	}
}

func TestGatewayClientBucketsByTrustedProxyAndPrefix(t *testing.T) {
	testCases := []struct {
		name      string
		proxies   []string
		requests  [][2]string // remote address, X-Forwarded-For
		allowed   []bool
		buckets   int
		forwarded bool
	}{
		{
			name:     "NoTrustedProxies_ForwardedForIgnored",
			requests: [][2]string{{"10.0.0.1:1", "203.0.113.1"}, {"10.0.0.1:2", "203.0.113.2"}},
			allowed:  []bool{true, false},
			buckets:  1,
		},
		{
			name:     "TrustedProxy_ClientsSeparated",
			proxies:  []string{"10.0.0.0/8"},
			requests: [][2]string{{"10.0.0.1:1", "203.0.113.1"}, {"10.0.0.1:2", "203.0.113.2"}, {"10.0.0.1:3", "203.0.113.1"}},
			allowed:  []bool{true, true, false},
			buckets:  2,
		},
		{
			name:     "SpoofedLeftmost_NotTrusted",
			proxies:  []string{"10.0.0.0/8"},
			requests: [][2]string{{"10.0.0.1:1", "1.1.1.1, 203.0.113.1"}, {"10.0.0.1:2", "2.2.2.2, 203.0.113.1"}},
			allowed:  []bool{true, false},
			buckets:  1,
		},
		{
			name:     "IPv6SameSlash64_OneBucket",
			requests: [][2]string{{"[2001:db8:1:2::1]:1", ""}, {"[2001:db8:1:2:ffff::2]:1", ""}, {"[2001:db8:1:3::1]:1", ""}},
			allowed:  []bool{true, false, true},
			buckets:  2,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, func(cfg *Config) {
				cfg.TrustedProxies = tc.proxies
				cfg.RateLimit.Client = Limit{Rate: 0.001, Burst: 1}
			})
			for i, r := range tc.requests {
				req := httptestRequest(tracesPath, testTraceJSON, jsonHeaders(webKey))
				req.RemoteAddr = r[0]
				if r[1] != "" {
					req.Header.Set("X-Forwarded-For", r[1])
				}
				rec := serve(f, req)
				assert.Equal(t, tc.allowed[i], rec.Code == http.StatusOK, "request %d: %d", i, rec.Code)
			}
			assert.Equal(t, tc.buckets, f.handler.clientLimiter.Len())
		})
	}
}

func TestGatewayClientBucketsBounded(t *testing.T) {
	f := newFixture(t, func(cfg *Config) { cfg.Limits.ClientBuckets = 3 })
	for i := range 20 {
		req := httptestRequest(tracesPath, testTraceJSON, jsonHeaders(webKey))
		req.RemoteAddr = fmt.Sprintf("198.51.100.%d:1", i)
		assert.Equal(t, http.StatusOK, serve(f, req).Code)
	}
	assert.Equal(t, 3, f.handler.clientLimiter.Len())
}

func TestGatewayDecoding(t *testing.T) {
	f := newFixture(t, func(cfg *Config) { cfg.MaxBodySize = 4096 })

	request := ptraceotlp.NewExportRequest()
	require.NoError(t, request.UnmarshalJSON([]byte(testTraceJSON)))
	protobuf, err := request.MarshalProto()
	require.NoError(t, err)

	gzipped := gzipBytes(t, []byte(testTraceJSON))
	bomb := gzipBytes(t, bytes.Repeat([]byte("a"), 64*1024))
	wireOver := gzipBytes(t, randomBytes(8192))

	testCases := []struct {
		name        string
		body        []byte
		contentType string
		encoding    string
		chunked     bool
		status      int
		respType    string
		reason      *report.Reason
	}{
		{name: "JSON_Accepted", body: []byte(testTraceJSON), contentType: "application/json", status: http.StatusOK, respType: "application/json"},
		{name: "Protobuf_Accepted", body: protobuf, contentType: "application/x-protobuf", status: http.StatusOK, respType: "application/x-protobuf"},
		{name: "ProtobufAlias_Accepted", body: protobuf, contentType: "application/protobuf", status: http.StatusOK, respType: "application/x-protobuf"},
		{name: "TextPlainAsJSON_Accepted", body: []byte(testTraceJSON), contentType: "text/plain;charset=UTF-8", status: http.StatusOK, respType: "application/json"},
		{name: "JSONWithCharsetUppercase_Accepted", body: []byte(testTraceJSON), contentType: "Application/JSON; charset=utf-8", status: http.StatusOK, respType: "application/json"},
		{name: "GzipJSON_Accepted", body: gzipped, contentType: "application/json", encoding: "gzip", status: http.StatusOK, respType: "application/json"},
		{name: "GzipCaseAndSpace_Accepted", body: gzipped, contentType: "application/json", encoding: " GZIP ", status: http.StatusOK, respType: "application/json"},
		{name: "Identity_Accepted", body: []byte(testTraceJSON), contentType: "application/json", encoding: "identity", status: http.StatusOK, respType: "application/json"},
		{name: "ChunkedJSON_Accepted", body: []byte(testTraceJSON), contentType: "application/json", chunked: true, status: http.StatusOK, respType: "application/json"},
		{name: "InvalidJSON_BadRequest", body: []byte(`{"resourceSpans":`), contentType: "application/json", status: http.StatusBadRequest, reason: ptr(report.InvalidPayload)},
		{name: "InvalidProtobuf_BadRequest", body: []byte{0xff, 0xff, 0xff}, contentType: "application/x-protobuf", status: http.StatusBadRequest, reason: ptr(report.InvalidPayload)},
		{name: "UnsupportedType_BadRequest", body: []byte(testTraceJSON), contentType: "application/xml", status: http.StatusBadRequest, reason: ptr(report.InvalidPayload)},
		{name: "MissingType_BadRequest", body: []byte(testTraceJSON), status: http.StatusBadRequest, reason: ptr(report.InvalidPayload)},
		{name: "TooLarge_PayloadTooLarge", body: bytes.Repeat([]byte(" "), 5000), contentType: "application/json", status: http.StatusRequestEntityTooLarge, reason: ptr(report.TooLarge)},
		{name: "ChunkedTooLarge_PayloadTooLarge", body: bytes.Repeat([]byte(" "), 5000), contentType: "application/json", chunked: true, status: http.StatusRequestEntityTooLarge, reason: ptr(report.TooLarge)},
		{name: "AtLimit_ReadNotRefusedForSize", body: append([]byte(testTraceJSON), bytes.Repeat([]byte(" "), 4096-len(testTraceJSON))...), contentType: "application/json", status: http.StatusOK, respType: "application/json"},
		{name: "GzipBomb_PayloadTooLarge", body: bomb, contentType: "application/json", encoding: "gzip", status: http.StatusRequestEntityTooLarge, reason: ptr(report.TooLarge)},
		{name: "GzipWireOverLimit_PayloadTooLarge", body: wireOver, contentType: "application/json", encoding: "gzip", chunked: true, status: http.StatusRequestEntityTooLarge, reason: ptr(report.TooLarge)},
		{name: "InvalidGzip_BadRequest", body: []byte("not gzip at all"), contentType: "application/json", encoding: "gzip", status: http.StatusBadRequest, reason: ptr(report.InvalidPayload)},
		{name: "UnsupportedEncoding_BadRequest", body: []byte(testTraceJSON), contentType: "application/json", encoding: "br", status: http.StatusBadRequest, reason: ptr(report.InvalidPayload)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			headers := jsonHeaders(webKey, "Content-Type", tc.contentType)
			if tc.contentType == "" {
				delete(headers, "Content-Type")
			}
			if tc.encoding != "" {
				headers["Content-Encoding"] = tc.encoding
			}
			req := httptestRequest(tracesPath, string(tc.body), headers)
			if tc.chunked {
				req.ContentLength = -1
			}
			response := serve(f, req)
			assert.Equal(t, tc.status, response.Code, response.Body.String())
			if tc.respType != "" {
				assert.Equal(t, tc.respType, response.Header().Get("Content-Type"))
			} else {
				assert.Equal(t, "application/json", response.Header().Get("Content-Type"), "error bodies are JSON")
			}
			events := f.acct.take()
			require.Len(t, events, 1)
			if tc.reason != nil {
				assert.Equal(t, event{kind: "reject", app: testWebApp, reason: *tc.reason}, events[0])
			}
		})
	}

	forwarded := f.traces.AllTraces()
	require.NotEmpty(t, forwarded)
	resourceSpans := forwarded[0].ResourceSpans().At(0)
	span := resourceSpans.ScopeSpans().At(0).Spans().At(0)

	assert.Equal(t, "GET /search", span.Name())
	url, _ := span.Attributes().Get("url.full")
	assert.Equal(t, "https://shop.example.com/search?page=2", url.Str())
	resource := resourceSpans.Resource().Attributes().AsRaw()
	assert.Equal(t, testWebApp, resource[attributeApplicationID])
	assert.Equal(t, "web", resource[attributePlatform])
	assert.Equal(t, webKeyID, resource[attributeKeyID])
}

func TestGatewayResponseBodies(t *testing.T) {
	f := newFixture(t, nil)
	rec := f.post(tracesPath, testTraceJSON, jsonHeaders(webKey))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"partialSuccess":{}}`, rec.Body.String(), "the backend's empty export response")

	rec = f.post(logsPath, testLogJSON, jsonHeaders(webKey))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"partialSuccess":{}}`, rec.Body.String(), "the backend's empty export response")

	rec = f.post(tracesPath, testTraceJSON, jsonHeaders(webKey, "Origin", "https://evil.example.org"))
	assert.JSONEq(t, `{"status":"error","error":{"code":"rum_origin_not_allowed","message":"origin is not allowed for this application"}}`, rec.Body.String())
	assert.NotContains(t, rec.Body.String(), "evil", "error bodies never echo request values")
}

func TestGatewayLogsEventName(t *testing.T) {
	f := newFixture(t, nil)

	response := f.post(logsPath, testLogJSON, jsonHeaders(webKey))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	forwarded := f.logs.AllLogs()
	require.Len(t, forwarded, 1)
	record := forwarded[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	eventName, ok := record.Attributes().Get(attributeEventName)
	require.True(t, ok)
	assert.Equal(t, "exception", eventName.Str())
}

func TestGatewayConsumerFailures(t *testing.T) {
	testCases := []struct {
		name       string
		err        error
		status     int
		retryAfter string
	}{
		{name: "Permanent_BadRequest", err: consumererror.NewPermanent(errors.New("refused")), status: http.StatusBadRequest},
		{name: "MemoryLimiter_ServiceUnavailable", err: errors.New("data refused due to high memory usage"), status: http.StatusServiceUnavailable, retryAfter: "5"},
		{name: "Retryable_ServiceUnavailable", err: consumererror.NewRetryableError(errors.New("queue full")), status: http.StatusServiceUnavailable, retryAfter: "5"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, nil)
			f.handler.traces = consumertest.NewErr(tc.err)

			response := f.post(tracesPath, testTraceJSON, jsonHeaders(webKey))
			assert.Equal(t, tc.status, response.Code, response.Body.String())
			assert.Equal(t, tc.retryAfter, response.Header().Get("Retry-After"))
			assert.Empty(t, f.acct.take(), "a refused forward is not counted")
		})
	}
}

func TestGatewayAndroidPackageNames(t *testing.T) {
	f := newFixture(t, nil)
	logs := func(resource string) string {
		return `{"resourceLogs":[{"resource":{"attributes":[` + resource + `]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1700000000000000000","eventName":"device.anr","attributes":[{"key":"exception.stacktrace","value":{"stringValue":"java.lang.Thread.sleep(Native Method)"}}]}]}]}]}`
	}
	pkg := func(name string) string { return `{"key":"app.package.name","value":{"stringValue":"` + name + `"}}` }

	testCases := []struct {
		name      string
		body      string
		headers   map[string]string
		status    int
		forwarded bool
		event     event
	}{
		{name: "AllowedPackageNoOrigin_Accepted", body: logs(pkg("com.example.shop")), status: http.StatusOK, forwarded: true},
		{name: "OtherPackage_Forbidden", body: logs(pkg("com.evil.app")), status: http.StatusForbidden, event: event{kind: "reject", app: testAndroidApp, reason: report.PackageNotAllowed, observed: "com.evil.app"}},
		{name: "MissingPackage_Forbidden", body: logs(``), status: http.StatusForbidden, event: event{kind: "reject", app: testAndroidApp, reason: report.PackageNotAllowed}},
		{name: "InvalidPackageName_ForbiddenNotObserved", body: logs(pkg("not a package")), status: http.StatusForbidden, event: event{kind: "reject", app: testAndroidApp, reason: report.PackageNotAllowed}},
		{name: "PackageOver256Bytes_ForbiddenNotObserved", body: logs(pkg("com.example.shop" + strings.Repeat("a", 250))), status: http.StatusForbidden, event: event{kind: "reject", app: testAndroidApp, reason: report.PackageNotAllowed}},
		{
			name: "BrowserOrigin_Forbidden", body: logs(pkg("com.example.shop")), headers: map[string]string{"Origin": "https://shop.example.com"}, status: http.StatusForbidden,
			event: event{kind: "reject", app: testAndroidApp, reason: report.OriginNotAllowed, observed: "https://shop.example.com"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			headers := jsonHeaders(androidKey)
			for k, v := range tc.headers {
				headers[k] = v
			}
			before := f.logs.LogRecordCount()
			response := f.post(logsPath, tc.body, headers)

			assert.Equal(t, tc.status, response.Code, response.Body.String())
			assert.Empty(t, response.Header().Get("Access-Control-Allow-Origin"))
			assert.Equal(t, tc.forwarded, f.logs.LogRecordCount() > before)
			events := f.acct.take()
			require.Len(t, events, 1)
			if !tc.forwarded {
				assert.Equal(t, tc.event, events[0])
			} else {
				assert.Equal(t, int64(1), events[0].batch.Errors, "the upstream ANR counts as an error")
			}
		})
	}
}

func TestGatewayRoutesAndMethods(t *testing.T) {
	f := newFixture(t, nil)
	testCases := []struct {
		name   string
		method string
		target string
		status int
		allow  string
	}{
		{name: "GetTraces_MethodNotAllowed", method: http.MethodGet, target: tracesPath, status: http.StatusMethodNotAllowed, allow: "OPTIONS, POST"},
		{name: "PutLogs_MethodNotAllowed", method: http.MethodPut, target: logsPath, status: http.StatusMethodNotAllowed, allow: "OPTIONS, POST"},
		{name: "OtherPath_NotFound", method: http.MethodPost, target: "/rum/v1/metrics", status: http.StatusNotFound},
		{name: "BrowserBundle_NotServed", method: http.MethodGet, target: "/rum/v1/sdk/browser/0.2.0/fusescope.min.js", status: http.StatusNotFound},
		{name: "TrailingSlash_NotFound", method: http.MethodPost, target: tracesPath + "/", status: http.StatusNotFound},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rec := f.do(tc.method, tc.target, strings.NewReader(testTraceJSON), jsonHeaders(webKey))
			assert.Equal(t, tc.status, rec.Code)
			assert.Equal(t, tc.allow, rec.Header().Get("Allow"))
		})
	}
	assert.Zero(t, f.traces.SpanCount())
}

func TestGatewayFailsClosedWithoutPolicy(t *testing.T) {
	testCases := []struct {
		name   string
		source ingestpolicy.Source
		setup  func(f *fixture)
	}{
		{name: "NoneReceived", source: newStore(t, nil)},
		{name: "OnlyRejectedReceived", source: func() ingestpolicy.Source {
			s := newStore(t, nil)
			p := testPolicy(1)
			p.Keys[0].OrgID = "0191c5e2-0000-7000-8000-00000000000b"
			require.Error(t, s.Apply(encodePolicy(t, p), testNow))
			return s
		}()},
		{name: "NoExtension", source: noPolicy{}},
		{name: "Stale", source: newStore(t, testPolicy(1)), setup: func(f *fixture) {
			f.handler.maxStaleness = time.Minute
			f.now = testNow.Add(time.Minute + time.Second)
		}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixtureWith(t, createDefaultConfig().(*Config), tc.source)
			if tc.setup != nil {
				tc.setup(f)
			}
			for _, path := range []string{tracesPath, logsPath} {
				rec := f.post(path, testTraceJSON, jsonHeaders(webKey, "Origin", "https://shop.example.com"))
				assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
				assert.Equal(t, "5", rec.Header().Get("Retry-After"))
				assert.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))

				rec = f.do(http.MethodOptions, path, nil, map[string]string{"Origin": "https://shop.example.com", "Access-Control-Request-Method": "POST"})
				assert.Equal(t, http.StatusForbidden, rec.Code)
			}
			assert.Empty(t, f.acct.take(), "nothing is counted without a policy")
			assert.Zero(t, f.traces.SpanCount())
		})
	}
}

func TestGatewayStalenessWithinBound(t *testing.T) {
	f := newFixture(t, func(cfg *Config) { cfg.MaxStaleness = time.Minute })
	f.now = testNow.Add(time.Minute)
	assert.Equal(t, http.StatusOK, f.post(tracesPath, testTraceJSON, jsonHeaders(webKey)).Code)
	// A repeated push of the same revision refreshes it.
	f.now = testNow.Add(2 * time.Minute)
	assert.Equal(t, http.StatusServiceUnavailable, f.post(tracesPath, testTraceJSON, jsonHeaders(webKey)).Code)
	require.NoError(t, f.store.Apply(encodePolicy(t, testPolicy(1)), f.now))
	assert.Equal(t, http.StatusOK, f.post(tracesPath, testTraceJSON, jsonHeaders(webKey)).Code)
}

func TestGatewayPolicyChangeAppliesAtOnce(t *testing.T) {
	f := newFixture(t, nil)
	require.Equal(t, http.StatusOK, f.post(tracesPath, testTraceJSON, jsonHeaders(webKey)).Code)

	// Revoked: the next policy no longer lists the key.
	p := testPolicy(2)
	p.Keys = p.Keys[1:]
	require.NoError(t, f.store.Apply(encodePolicy(t, p), testNow))
	assert.Equal(t, http.StatusUnauthorized, f.post(tracesPath, testTraceJSON, jsonHeaders(webKey)).Code)

	// Origins widened.
	p = testPolicy(3)
	p.RUMApplications[0].AllowedOrigins = append(p.RUMApplications[0].AllowedOrigins, "https://new.example.com")
	require.NoError(t, f.store.Apply(encodePolicy(t, p), testNow))
	assert.Equal(t, http.StatusOK, f.post(tracesPath, testTraceJSON, jsonHeaders(webKey, "Origin", "https://new.example.com")).Code)
}

// Codex review on #43: a request authorised by a policy the watcher has not
// passed to the monitor yet is still counted.
func TestGatewayAccountsBeforeWatcherCatchesUp(t *testing.T) {
	f := newFixture(t, nil)
	const newApp, newKey, newKeyID = "0191c5e2-7f0a-7c3e-9a51-3c2b1d4e5f62", "rum_pub_newappnewappnewappnewappnewapp", "0191c5e2-8a1b-7d2e-b0c4-000000000007"
	p := testPolicy(2)
	app := p.RUMApplications[0]
	app.ID = newApp
	p.RUMApplications = append(p.RUMApplications, app)
	p.Keys = append(p.Keys, ingestpolicy.Key{ID: newKeyID, SHA256: verifier(newKey), OrgID: testOrg, Kind: ingestpolicy.KindPublic, Signals: []string{"traces"}, ApplicationID: ptr(newApp)})
	require.NoError(t, f.store.Apply(encodePolicy(t, p), testNow))

	require.Equal(t, http.StatusOK, f.post(tracesPath, testTraceJSON, jsonHeaders(newKey)).Code)
	pages, err := f.acct.monitor.Build(1, f.now, f.store.Status())
	require.NoError(t, err)
	data := bytes.Join(pages, nil)
	assert.Contains(t, string(data), newApp)
	assert.Contains(t, string(data), newKeyID)
}

// Codex review on #43: a consumer blocked on backpressure gets a deadline,
// so it cannot hold a request slot forever.
func TestGatewayConsumerDeadline(t *testing.T) {
	f := newFixture(t, func(cfg *Config) { cfg.Limits.WriteTimeout = 100 * time.Millisecond })
	f.handler.now = time.Now
	f.handler.traces = consumerFunc(func(ctx context.Context, _ ptrace.Traces) error {
		<-ctx.Done()
		return ctx.Err()
	})
	start := time.Now()
	rec := f.post(tracesPath, testTraceJSON, jsonHeaders(webKey))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "5", rec.Header().Get("Retry-After"))
	assert.Less(t, time.Since(start), 5*time.Second)
}

// Codex review on #43: empty resource groups are dropped and the rest are
// bounded before stamping, so a small body cannot expand into millions of
// attributes.
func TestGatewayResourceGroupsBounded(t *testing.T) {
	groups := func(n int, span bool) string {
		inner := `{"resource":{},"scopeSpans":[]}`
		if span {
			inner = `{"resource":{},"scopeSpans":[{"spans":[{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174","name":"x"}]}]}`
		}
		return `{"resourceSpans":[` + strings.TrimSuffix(strings.Repeat(inner+",", n), ",") + `]}`
	}
	logGroups := `{"resourceLogs":[` + strings.TrimSuffix(strings.Repeat(`{"resource":{},"scopeLogs":[{"logRecords":[{"eventName":"x"}]}]},`, 1001), ",") + `]}`
	testCases := []struct {
		name   string
		path   string
		body   string
		status int
		spans  int
	}{
		// Maintainer review of #43: empty groups count, before decoding.
		{name: "ManyEmptyGroups_TooLarge", path: tracesPath, body: groups(200_000, false), status: http.StatusRequestEntityTooLarge},
		{name: "EmptyGroupsAtBound_AcceptedNothingForwarded", path: tracesPath, body: groups(1000, false), status: http.StatusOK},
		{name: "AtBound_Accepted", path: tracesPath, body: groups(1000, true), status: http.StatusOK, spans: 1000},
		{name: "OverBound_TooLarge", path: tracesPath, body: groups(1001, true), status: http.StatusRequestEntityTooLarge},
		{name: "LogsOverBound_TooLarge", path: logsPath, body: logGroups, status: http.StatusRequestEntityTooLarge},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, func(cfg *Config) { cfg.MaxBodySize = 16 << 20 })
			rec := f.post(tc.path, tc.body, jsonHeaders(webKey))
			assert.Equal(t, tc.status, rec.Code, rec.Body.String())
			assert.Equal(t, tc.spans, f.traces.SpanCount())
			for _, td := range f.traces.AllTraces() {
				assert.LessOrEqual(t, td.ResourceSpans().Len(), 1000)
			}
			events := f.acct.take()
			require.Len(t, events, 1)
			if tc.status == http.StatusRequestEntityTooLarge {
				assert.Equal(t, event{kind: "reject", app: testWebApp, reason: report.TooLarge}, events[0])
			}
		})
	}
}

func TestGatewayConcurrencyLimit(t *testing.T) {
	f := newFixture(t, func(cfg *Config) { cfg.Limits.Requests = 1 })
	blocked := make(chan struct{})
	release := make(chan struct{})
	f.handler.traces = blockingTraces{entered: blocked, release: release}

	done := make(chan int)
	go func() { done <- f.post(tracesPath, testTraceJSON, jsonHeaders(webKey)).Code }()
	<-blocked

	rec := f.post(tracesPath, testTraceJSON, jsonHeaders(webKey))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))
	rec = f.do(http.MethodOptions, tracesPath, nil, map[string]string{"Origin": "https://shop.example.com", "Access-Control-Request-Method": "POST"})
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, "preflights share the limit")

	close(release)
	assert.Equal(t, http.StatusOK, <-done)
	assert.Equal(t, http.StatusOK, f.post(tracesPath, testTraceJSON, jsonHeaders(webKey)).Code)
}

func TestGatewayAuthData(t *testing.T) {
	f := newFixture(t, nil)
	var info client.Info
	f.handler.traces = consumerFunc(func(ctx context.Context, _ ptrace.Traces) error {
		info = client.FromContext(ctx)
		return nil
	})
	require.Equal(t, http.StatusOK, f.post(tracesPath, testTraceJSON, jsonHeaders(webKey)).Code)
	require.NotNil(t, info.Auth)
	assert.Equal(t, testOrg, info.Auth.GetAttribute("fusescope.org.id"))
	assert.Equal(t, webKeyID, info.Auth.GetAttribute("fusescope.ingest.key_id"))
	assert.ElementsMatch(t, []string{"fusescope.org.id", "fusescope.ingest.key_id"}, info.Auth.GetAttributeNames())
	assert.Nil(t, info.Addr, "the client address is not passed on (G5)")
}

func TestGatewayAccountsAcceptedRequests(t *testing.T) {
	f := newFixture(t, nil)
	body := `{"resourceSpans":[{"resource":{"attributes":[{"key":"telemetry.distro.name","value":{"stringValue":"@fusescope/browser"}},{"key":"telemetry.distro.version","value":{"stringValue":"0.2.0"}}]},"scopeSpans":[{"spans":[` +
		`{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174","name":"view","attributes":[{"key":"session.id","value":{"stringValue":"s1"}}]},` +
		`{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b175","name":"view","attributes":[{"key":"session.id","value":{"stringValue":"s2"}}]}]}]}]}`
	require.Equal(t, http.StatusOK, f.post(tracesPath, body, jsonHeaders(webKey)).Code)
	require.Equal(t, http.StatusOK, f.post(logsPath, `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"eventName":"browser.web_vital","attributes":[{"key":"session.id","value":{"stringValue":"s1"}}]}]}]}]}`, jsonHeaders(webKey)).Code)

	pages, err := f.acct.monitor.Build(1, f.now, ingestpolicy.Status{Version: 1})
	require.NoError(t, err)
	data := bytes.Join(pages, nil)
	r, err := report.Merge(pages)
	require.NoError(t, err)
	require.Len(t, r.Applications, 1)
	assert.Equal(t, report.Counts{Sessions: 2, Views: 2, Vitals: 1}, r.Applications[0].Counts)
	assert.Equal(t, report.SDKReport{Name: "@fusescope/browser", Version: "0.2.0"}, r.Applications[0].SDK)
	assert.Equal(t, []report.KeyReport{{ID: webKeyID, LastUsedAt: "2026-11-03T12:00:30Z"}}, r.Keys)
	assert.NotContains(t, string(data), `"s1"`, "session IDs are never reported")
}

func gzipBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, err := w.Write(b)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func randomBytes(n int) []byte {
	out := make([]byte, n)
	x := uint32(2463534242)
	for i := range out {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		out[i] = byte(x)
	}
	return out
}
