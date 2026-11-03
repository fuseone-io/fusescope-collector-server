// Copyright FuseOne
// SPDX-License-Identifier: MIT
//
// TestProcessTraces, TestProcessTracesBatch, TestProcessLogs,
// TestProcessLogsAndroid, TestProcessTracesAndroidScreenRoute and
// TestAllowedResources are moved from fuseone-io/fusescope
// pkg/modules/rumapplication/implrumapplication/otlp_test.go and
// otlp_mobile_test.go at 6c8991257 (MIT, see LICENSE-MIT), adapted to the
// policy's application; the key_id, link and environment cases are new
// (docs/spec/rum.md §8.1, §8.2).

package rumreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
	"github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"
)

func testApplications(t testing.TB) (web, android *ingestpolicy.Application) {
	t.Helper()
	snap := newStore(t, testPolicy(1)).Current()
	return snap.Application(testWebApp), snap.Application(testAndroidApp)
}

func orEmpty(raw map[string]any) map[string]any {
	if raw == nil {
		return map[string]any{}
	}
	return raw
}

func TestProcessTraces(t *testing.T) {
	application, _ := testApplications(t)

	testCases := []struct {
		name      string
		spanName  string
		attrs     map[string]any
		resource  map[string]any
		wantName  string
		wantAttrs map[string]string
		absent    []string
		wantRes   map[string]string
	}{
		{
			name:      "URLAttributesAndName_Scrubbed",
			spanName:  "Navigation: /customers/392#tab?email=a@b.c",
			attrs:     map[string]any{"url.full": "https://shop.example.com/search?q=secret&page=2#x", "http.url": "/a?token=t", "url.query": "email=a&page=3", "url.fragment": "tab", "session.id": "s1"},
			wantName:  "Navigation: /customers/392",
			wantAttrs: map[string]string{"url.full": "https://shop.example.com/search?page=2", "http.url": "/a", "url.query": "page=3", "session.id": "s1"},
			absent:    []string{"url.fragment"},
		},
		{
			name:      "QueryWithNothingKept_Removed",
			spanName:  "fetch",
			attrs:     map[string]any{"url.query": "email=a", "http.target": "/p?email=a", "url.original": "/o#frag", "location.href": "https://a/?x=1", "page.url": "/p"},
			wantName:  "fetch",
			wantAttrs: map[string]string{"http.target": "/p", "url.original": "/o", "location.href": "https://a/", "page.url": "/p"},
			absent:    []string{"url.query"},
		},
		{
			name:     "NonStringURL_Untouched",
			spanName: "fetch",
			attrs:    map[string]any{"url.full": int64(3), "url.query": int64(4)},
			wantName: "fetch",
		},
		{
			name:     "ForgedApplicationResource_Overwritten",
			spanName: "view",
			resource: map[string]any{attributeApplicationID: "forged", attributePlatform: "ios", attributeKeyID: "forged", attributeServiceName: "custom-service"},
			wantName: "view",
			wantRes:  map[string]string{attributeApplicationID: application.ID, attributePlatform: "web", attributeKeyID: webKeyID, attributeServiceName: "custom-service", attributeDeploymentEnvironment: "production"},
		},
		{
			name:     "MissingServiceName_Stamped",
			spanName: "documentLoad",
			resource: map[string]any{"location.href": "https://shop.example.com/?ref=mail"},
			wantName: "documentLoad",
			wantRes:  map[string]string{attributeServiceName: "shop-web", "location.href": "https://shop.example.com/"},
		},
		{
			name:     "EmptyServiceName_Stamped",
			spanName: "view",
			resource: map[string]any{attributeServiceName: ""},
			wantName: "view",
			wantRes:  map[string]string{attributeServiceName: "shop-web"},
		},
		{
			name:     "ServiceNameIsApplicationID_Stamped",
			spanName: "view",
			resource: map[string]any{attributeServiceName: application.ID},
			wantName: "view",
			wantRes:  map[string]string{attributeServiceName: "shop-web"},
		},
		{
			name:     "EmptyEnvironmentPresent_Kept",
			spanName: "view",
			resource: map[string]any{attributeDeploymentEnvironment: ""},
			wantName: "view",
			wantRes:  map[string]string{attributeDeploymentEnvironment: ""},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			traces := ptrace.NewTraces()
			resourceSpans := traces.ResourceSpans().AppendEmpty()
			require.NoError(t, resourceSpans.Resource().Attributes().FromRaw(orEmpty(tc.resource)))
			span := resourceSpans.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
			span.SetName(tc.spanName)
			require.NoError(t, span.Attributes().FromRaw(orEmpty(tc.attrs)))
			event := span.Events().AppendEmpty()
			event.Attributes().PutStr("http.url", "/e?secret=1")
			link := span.Links().AppendEmpty()
			link.Attributes().PutStr("url.full", "https://x/?secret=1")

			processTraces(application, webKeyID, traces, &report.Batch{})

			assert.Equal(t, tc.wantName, span.Name())
			for key, value := range tc.wantAttrs {
				got, ok := span.Attributes().Get(key)
				require.True(t, ok, key)
				assert.Equal(t, value, got.Str(), key)
			}
			for _, key := range tc.absent {
				_, ok := span.Attributes().Get(key)
				assert.False(t, ok, key)
			}
			for key, value := range tc.wantRes {
				got, ok := resourceSpans.Resource().Attributes().Get(key)
				require.True(t, ok, key)
				assert.Equal(t, value, got.Str(), key)
			}
			eventURL, _ := event.Attributes().Get("http.url")
			assert.Equal(t, "/e", eventURL.Str())
			linkURL, _ := link.Attributes().Get("url.full")
			assert.Equal(t, "https://x/", linkURL.Str())
		})
	}
}

// Codex review on #43: duplicate copies of the stamped identity are all
// replaced, whatever a later reader picks.
func TestProcessDuplicateIdentityAttributes(t *testing.T) {
	application, _ := testApplications(t)
	kv := func(k, v string) []byte {
		return msgField(msgField(nil, 1, []byte(k)), 2, msgField(nil, 1, []byte(v)))
	}
	var resource []byte
	for _, k := range []string{attributeApplicationID, attributePlatform, attributeKeyID} {
		resource = msgField(resource, 1, kv(k, "first-forged"))
		resource = msgField(resource, 1, kv(k, "second-forged"))
	}
	span := msgField(nil, 5, []byte("s"))
	body := msgField(nil, 1, msgField(msgField(nil, 1, resource), 2, msgField(nil, 2, span)))
	td, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(body)
	require.NoError(t, err)
	require.Equal(t, 6, td.ResourceSpans().At(0).Resource().Attributes().Len(), "pdata keeps the duplicates")

	processTraces(application, webKeyID, td, &report.Batch{})

	attrs := td.ResourceSpans().At(0).Resource().Attributes()
	counts := map[string]int{}
	attrs.Range(func(k string, v pcommon.Value) bool {
		counts[k]++
		assert.NotContains(t, v.Str(), "forged", k)
		return true
	})
	assert.Equal(t, 1, counts[attributeApplicationID])
	assert.Equal(t, 1, counts[attributePlatform])
	assert.Equal(t, 1, counts[attributeKeyID])
	raw := attrs.AsRaw()
	assert.Equal(t, application.ID, raw[attributeApplicationID])
	assert.Equal(t, webKeyID, raw[attributeKeyID])
}

// Codex review on #43: a resource naming its package twice is refused,
// whatever the order of an allowed and a forbidden value.
func TestDuplicatePackageNameRefused(t *testing.T) {
	_, android := testApplications(t)
	kv := func(k, v string) []byte {
		return msgField(msgField(nil, 1, []byte(k)), 2, msgField(nil, 1, []byte(v)))
	}
	for _, order := range [][2]string{{"com.example.shop", "com.evil.app"}, {"com.evil.app", "com.example.shop"}, {"com.example.shop", "com.example.shop"}} {
		resource := msgField(msgField(nil, 1, kv(attributePackageName, order[0])), 1, kv(attributePackageName, order[1]))
		record := msgField(nil, 12, []byte("e"))
		body := msgField(nil, 1, msgField(msgField(nil, 1, resource), 2, msgField(nil, 2, record)))
		req := plogotlp.NewExportRequest()
		require.NoError(t, req.UnmarshalProto(body))
		name, refused := refusedLogsPackage(android, req.Logs())
		assert.True(t, refused, "%v", order)
		assert.Empty(t, name, "no single value to observe")
	}
	single := plog.NewLogs()
	single.ResourceLogs().AppendEmpty().Resource().Attributes().PutStr(attributePackageName, "com.example.shop")
	_, refused := refusedLogsPackage(android, single)
	assert.False(t, refused)
}

func TestProcessTracesBatch(t *testing.T) {
	application, _ := testApplications(t)
	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr(attributeDistroName, "@fusescope/browser")
	resourceSpans.Resource().Attributes().PutStr(attributeDistroVersion, "0.1.0")
	spans := resourceSpans.ScopeSpans().AppendEmpty().Spans()
	for _, name := range []string{"view", "view", "GET"} {
		span := spans.AppendEmpty()
		span.SetName(name)
		span.Attributes().PutStr(attributeSessionID, "s-"+name)
	}

	batch := &report.Batch{}
	processTraces(application, webKeyID, traces, batch)

	assert.Equal(t, int64(2), batch.Views)
	want := &report.Batch{}
	want.AddSession("s-view")
	want.AddSession("s-view")
	want.AddSession("s-GET")
	assert.Equal(t, want.Sessions, batch.Sessions, "sessions are hashes; duplicates collapse in the monitor")
	assert.Equal(t, "@fusescope/browser", batch.SDKName)
	assert.Equal(t, "0.1.0", batch.SDKVersion)
}

func TestProcessSDKBounds(t *testing.T) {
	application, _ := testApplications(t)
	testCases := []struct {
		name, sdkName, sdkVersion, wantName, wantVersion string
		distro                                           bool
	}{
		{name: "SDKNameFallback", sdkName: "opentelemetry", sdkVersion: "2.0.0", wantName: "opentelemetry", wantVersion: "2.0.0"},
		{name: "DistroPreferred", distro: true, sdkName: "@fusescope/browser", sdkVersion: "0.2.0", wantName: "@fusescope/browser", wantVersion: "0.2.0"},
		{name: "NameOver64Bytes_Empty", sdkName: string(make([]byte, 65)), sdkVersion: "1", wantVersion: "1"},
		{name: "VersionOver32Bytes_Empty", sdkName: "n", sdkVersion: "1.0.0-" + string(make([]byte, 27)), wantName: "n"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			traces := ptrace.NewTraces()
			attrs := traces.ResourceSpans().AppendEmpty().Resource().Attributes()
			if tc.distro {
				attrs.PutStr(attributeSDKName, "opentelemetry")
				attrs.PutStr(attributeDistroName, tc.sdkName)
				attrs.PutStr(attributeDistroVersion, tc.sdkVersion)
			} else {
				attrs.PutStr(attributeSDKName, tc.sdkName)
				attrs.PutStr(attributeSDKVersion, tc.sdkVersion)
			}
			batch := &report.Batch{}
			processTraces(application, webKeyID, traces, batch)
			assert.Equal(t, tc.wantName, batch.SDKName)
			assert.Equal(t, tc.wantVersion, batch.SDKVersion)
		})
	}
}

func TestProcessLogs(t *testing.T) {
	application, _ := testApplications(t)
	testCases := []struct {
		name          string
		eventName     string
		attrs         map[string]any
		wantEventName string
		wantErrors    int64
		wantVitals    int64
	}{
		{name: "EventNameField_CopiedToAttribute", eventName: "exception", attrs: map[string]any{"exception.type": "TypeError", "fusescope.rum.error.source": "onerror"}, wantEventName: "exception", wantErrors: 1},
		{name: "ExistingAttribute_Kept", eventName: "browser.web_vital", attrs: map[string]any{"event.name": "browser.web_vital", "browser.web_vital.name": "lcp"}, wantEventName: "browser.web_vital", wantVitals: 1},
		{name: "ExistingDifferentAttribute_Kept", eventName: "exception", attrs: map[string]any{"event.name": "rum.vital"}, wantEventName: "rum.vital", wantVitals: 1},
		{name: "AttributeOnly_Counted", attrs: map[string]any{"event.name": "browser.web_vital", "browser.web_vital.name": "inp"}, wantEventName: "browser.web_vital", wantVitals: 1},
		{name: "UserActionClick_NotCounted", eventName: "browser.user_action.click", wantEventName: "browser.user_action.click"},
		{name: "CustomAction_NotCounted", eventName: "fusescope.rum.action", wantEventName: "fusescope.rum.action"},
		{name: "NoEventName_NoAttribute"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logs := plog.NewLogs()
			resourceLogs := logs.ResourceLogs().AppendEmpty()
			record := resourceLogs.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			record.SetEventName(tc.eventName)
			require.NoError(t, record.Attributes().FromRaw(orEmpty(tc.attrs)))
			record.Attributes().PutStr("page.url", "https://shop.example.com/p?email=x")

			batch := &report.Batch{}
			processLogs(application, webKeyID, logs, batch)

			got, ok := record.Attributes().Get(attributeEventName)
			assert.Equal(t, tc.wantEventName != "", ok)
			assert.Equal(t, tc.wantEventName, got.Str())
			assert.Equal(t, tc.wantErrors, batch.Errors)
			assert.Equal(t, tc.wantVitals, batch.Vitals)

			pageURL, _ := record.Attributes().Get("page.url")
			assert.Equal(t, "https://shop.example.com/p", pageURL.Str())
			platform, _ := resourceLogs.Resource().Attributes().Get(attributePlatform)
			assert.Equal(t, "web", platform.Str())
		})
	}
}

func TestProcessLogsAndroid(t *testing.T) {
	web, android := testApplications(t)
	testCases := []struct {
		name        string
		application *ingestpolicy.Application
		eventName   string
		attrs       map[string]any
		want        map[string]string
		wantErrors  int64
	}{
		{
			name: "UpstreamCrash_Exception", application: android, eventName: "app.crash",
			attrs:      map[string]any{"exception.type": "java.lang.IllegalStateException", "exception.message": "boom", "exception.stacktrace": "java.lang.IllegalStateException: boom\n\tat a.b.c(SourceFile:1)"},
			want:       map[string]string{attributeEventName: "exception", attributeErrorSource: errorSourceCrash, "exception.type": "java.lang.IllegalStateException"},
			wantErrors: 1,
		},
		{
			name: "UpstreamANR_ExceptionWithType", application: android, eventName: "device.anr",
			attrs:      map[string]any{"exception.stacktrace": "java.lang.Thread.sleep(Native Method)\n"},
			want:       map[string]string{attributeEventName: "exception", attributeErrorSource: errorSourceANR, "exception.type": exceptionTypeANR, "exception.message": "Application Not Responding"},
			wantErrors: 1,
		},
		{
			name: "DistroANR_Kept", application: android, eventName: "exception",
			attrs:      map[string]any{"exception.type": exceptionTypeANR, "exception.message": "main thread blocked for 5s", attributeErrorSource: errorSourceANR},
			want:       map[string]string{attributeEventName: "exception", attributeErrorSource: errorSourceANR, "exception.message": "main thread blocked for 5s"},
			wantErrors: 1,
		},
		{
			name: "BetaANRType_Normalized", application: android, eventName: "exception",
			attrs:      map[string]any{"exception.type": "ApplicationNotResponding", attributeErrorSource: errorSourceANR},
			want:       map[string]string{"exception.type": exceptionTypeANR},
			wantErrors: 1,
		},
		{
			name: "CrashNamedApplicationNotResponding_Kept", application: android, eventName: "exception",
			attrs:      map[string]any{"exception.type": "ApplicationNotResponding", attributeErrorSource: errorSourceCrash},
			want:       map[string]string{"exception.type": "ApplicationNotResponding"},
			wantErrors: 1,
		},
		{
			name: "ScreenName_Route", application: android, eventName: "app.screen.click",
			attrs: map[string]any{attributeScreenName: "CheckoutScreen"},
			want:  map[string]string{attributeViewRoute: "CheckoutScreen"},
		},
		{
			name: "RouteSet_Kept", application: android, eventName: "exception",
			attrs:      map[string]any{attributeScreenName: "CheckoutScreen", attributeViewRoute: "checkout/{id}"},
			want:       map[string]string{attributeViewRoute: "checkout/{id}"},
			wantErrors: 1,
		},
		{
			name: "WebCrashName_Untouched", application: web, eventName: "app.crash",
			want: map[string]string{attributeEventName: "app.crash"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logs := plog.NewLogs()
			record := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			record.SetEventName(tc.eventName)
			require.NoError(t, record.Attributes().FromRaw(orEmpty(tc.attrs)))

			batch := &report.Batch{}
			processLogs(tc.application, androidKeyID, logs, batch)

			for key, value := range tc.want {
				got, ok := record.Attributes().Get(key)
				require.True(t, ok, key)
				assert.Equal(t, value, got.Str(), key)
			}
			assert.Equal(t, tc.wantErrors, batch.Errors)
		})
	}
}

func TestProcessTracesAndroidScreenRoute(t *testing.T) {
	web, android := testApplications(t)
	testCases := []struct {
		name        string
		application *ingestpolicy.Application
		attrs       map[string]any
		route       string
	}{
		{name: "Android_ScreenNameCopied", application: android, attrs: map[string]any{attributeScreenName: "CartScreen"}, route: "CartScreen"},
		{name: "Web_NotCopied", application: web, attrs: map[string]any{attributeScreenName: "CartScreen"}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			traces := ptrace.NewTraces()
			span := traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
			span.SetName("Resumed")
			require.NoError(t, span.Attributes().FromRaw(tc.attrs))

			processTraces(tc.application, androidKeyID, traces, &report.Batch{})

			route, ok := span.Attributes().Get(attributeViewRoute)
			assert.Equal(t, tc.route != "", ok)
			assert.Equal(t, tc.route, route.Str())
		})
	}
}

func TestAllowedResources(t *testing.T) {
	web, android := testApplications(t)
	testCases := []struct {
		name        string
		application *ingestpolicy.Application
		packages    []any
		packageName string
		refused     bool
	}{
		{name: "AllowedPackage", application: android, packages: []any{"com.example.shop"}},
		{name: "OneResourceOtherPackage_Refused", application: android, packages: []any{"com.example.shop", "com.evil.app"}, packageName: "com.evil.app", refused: true},
		{name: "MissingPackage_Refused", application: android, packages: []any{nil}, refused: true},
		{name: "Web_NotChecked", application: web, packages: []any{nil}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			traces := ptrace.NewTraces()
			logs := plog.NewLogs()
			for _, packageName := range tc.packages {
				tr := traces.ResourceSpans().AppendEmpty().Resource()
				lr := logs.ResourceLogs().AppendEmpty().Resource()
				if packageName != nil {
					tr.Attributes().PutStr(attributePackageName, packageName.(string))
					lr.Attributes().PutStr(attributePackageName, packageName.(string))
				}
			}

			packageName, refused := refusedTracesPackage(tc.application, traces)
			assert.Equal(t, tc.refused, refused)
			assert.Equal(t, tc.packageName, packageName)
			packageName, refused = refusedLogsPackage(tc.application, logs)
			assert.Equal(t, tc.refused, refused)
			assert.Equal(t, tc.packageName, packageName)
		})
	}
}
