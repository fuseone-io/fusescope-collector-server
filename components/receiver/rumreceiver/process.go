// Copyright FuseOne
// SPDX-License-Identifier: MIT
//
// Moved from fuseone-io/fusescope
// pkg/modules/rumapplication/implrumapplication/otlp.go at 6c8991257 (MIT,
// see LICENSE-MIT). Changed: the application comes from the ingest policy;
// fusescope.ingest.key_id is stamped (docs/spec/rum.md §8.1); the package
// check walks the resources in place; attributes are scrubbed in one pass;
// sessions are recorded as hashes into a reused batch (§8.6, G6).

package rumreceiver // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
	"github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"
)

// Attribute names ([B] rumtypes/attributes.go:3-19, timeline.go:16-19).
const (
	attributeApplicationID         = "fusescope.rum.application.id"
	attributePlatform              = "fusescope.rum.platform"
	attributeKeyID                 = ingestpolicy.AuthKeyID
	attributeViewRoute             = "fusescope.rum.view.route"
	attributeSessionID             = "session.id"
	attributeEventName             = "event.name"
	attributeServiceName           = "service.name"
	attributeDeploymentEnvironment = "deployment.environment.name"
	attributeDistroName            = "telemetry.distro.name"
	attributeDistroVersion         = "telemetry.distro.version"
	attributeSDKName               = "telemetry.sdk.name"
	attributeSDKVersion            = "telemetry.sdk.version"
	attributePackageName           = "app.package.name"
	attributeScreenName            = "app.screen.name"
	attributeErrorSource           = "fusescope.rum.error.source"
	attributeExceptionType         = "exception.type"
	attributeExceptionMessage      = "exception.message"
)

// Names of [B] rumtypes/attributes.go:21-39 and jvmstacktrace.go:17.
const (
	spanNameView           = "view"
	eventNameException     = "exception"
	eventNameWebVital      = "browser.web_vital"
	eventNameLegacyVital   = "rum.vital"
	eventNameAppCrash      = "app.crash"
	eventNameDeviceANR     = "device.anr"
	errorSourceCrash       = "crash"
	errorSourceANR         = "anr"
	exceptionTypeANR       = "ANR"
	exceptionTypeLegacyANR = "ApplicationNotResponding"
	anrMessage             = "Application Not Responding"
)

// processTraces scrubs and stamps the request in place and reports what it
// saw into batch.
func processTraces(app *ingestpolicy.Application, keyID string, traces ptrace.Traces, batch *report.Batch) {
	policy := &app.PIIPolicy
	android := app.Platform == ingestpolicy.PlatformAndroid
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		resourceSpans := traces.ResourceSpans().At(i)
		stampResource(app, keyID, resourceSpans.Resource(), batch)

		for j := 0; j < resourceSpans.ScopeSpans().Len(); j++ {
			spans := resourceSpans.ScopeSpans().At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				if name := span.Name(); name != "" {
					if scrubbed := scrubSpanName(policy, name); scrubbed != name {
						span.SetName(scrubbed)
					}
				}
				scrubAttributes(policy, span.Attributes())
				for l := 0; l < span.Events().Len(); l++ {
					scrubAttributes(policy, span.Events().At(l).Attributes())
				}
				for l := 0; l < span.Links().Len(); l++ {
					scrubAttributes(policy, span.Links().At(l).Attributes())
				}

				if android {
					copyScreenRoute(span.Attributes())
				}
				collectSession(span.Attributes(), batch)
				if span.Name() == spanNameView {
					batch.Views++
				}
			}
		}
	}
}

// processLogs also copies each record's event name into the event.name
// attribute, which is all the logs table keeps of it.
func processLogs(app *ingestpolicy.Application, keyID string, logs plog.Logs, batch *report.Batch) {
	policy := &app.PIIPolicy
	android := app.Platform == ingestpolicy.PlatformAndroid
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		resourceLogs := logs.ResourceLogs().At(i)
		stampResource(app, keyID, resourceLogs.Resource(), batch)

		for j := 0; j < resourceLogs.ScopeLogs().Len(); j++ {
			records := resourceLogs.ScopeLogs().At(j).LogRecords()
			for k := 0; k < records.Len(); k++ {
				record := records.At(k)
				attributes := record.Attributes()
				scrubAttributes(policy, attributes)

				if eventName := record.EventName(); eventName != "" {
					if _, ok := attributes.Get(attributeEventName); !ok {
						attributes.PutStr(attributeEventName, eventName)
					}
				}
				if android {
					normalizeAndroidEvent(attributes)
					copyScreenRoute(attributes)
				}

				collectSession(attributes, batch)
				if eventName, ok := attributes.Get(attributeEventName); ok {
					switch eventName.Str() {
					case eventNameException:
						batch.Errors++
					case eventNameWebVital, eventNameLegacyVital:
						batch.Vitals++
					}
				}
			}
		}
	}
}

// stampResource overwrites the application, platform and key a client may
// have forged, and fills service and environment only when missing; a
// service named after the application id, an SDK default, takes the
// application name.
func stampResource(app *ingestpolicy.Application, keyID string, resource pcommon.Resource, batch *report.Batch) {
	attributes := resource.Attributes()
	scrubAttributes(&app.PIIPolicy, attributes)

	attributes.PutStr(attributeApplicationID, app.ID)
	attributes.PutStr(attributePlatform, app.Platform)
	attributes.PutStr(attributeKeyID, keyID)
	if service, ok := attributes.Get(attributeServiceName); !ok || service.Str() == "" || service.Str() == app.ID {
		attributes.PutStr(attributeServiceName, app.Name)
	}
	if _, ok := attributes.Get(attributeDeploymentEnvironment); !ok {
		attributes.PutStr(attributeDeploymentEnvironment, app.Environment)
	}

	for _, keys := range [...][2]string{{attributeDistroName, attributeDistroVersion}, {attributeSDKName, attributeSDKVersion}} {
		if name, ok := attributes.Get(keys[0]); ok && name.Str() != "" {
			version, _ := attributes.Get(keys[1])
			batch.SetSDK(name.Str(), version.AsString())
			return
		}
	}
}

// refusedTracesPackage checks every resource's app.package.name against an
// android application's allowed package names, and returns the first
// refused, empty when missing. Web applications are not checked.
func refusedTracesPackage(app *ingestpolicy.Application, traces ptrace.Traces) (string, bool) {
	if app.Platform != ingestpolicy.PlatformAndroid {
		return "", false
	}
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		if name, refused := refusedPackage(app, traces.ResourceSpans().At(i).Resource()); refused {
			return name, true
		}
	}
	return "", false
}

func refusedLogsPackage(app *ingestpolicy.Application, logs plog.Logs) (string, bool) {
	if app.Platform != ingestpolicy.PlatformAndroid {
		return "", false
	}
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		if name, refused := refusedPackage(app, logs.ResourceLogs().At(i).Resource()); refused {
			return name, true
		}
	}
	return "", false
}

func refusedPackage(app *ingestpolicy.Application, resource pcommon.Resource) (string, bool) {
	packageName, _ := resource.Attributes().Get(attributePackageName)
	if !app.AllowsPackageName(packageName.Str()) {
		return packageName.Str(), true
	}
	return "", false
}

// normalizeAndroidEvent stores the crash and ANR events of
// opentelemetry-android as exceptions with their source, and gives an ANR,
// which reports only the main thread's stack, its type and message. The
// ApplicationNotResponding type of SDK 0.1.0-beta.1 becomes ANR.
func normalizeAndroidEvent(attributes pcommon.Map) {
	eventName, _ := attributes.Get(attributeEventName)
	switch eventName.Str() {
	case eventNameAppCrash:
		attributes.PutStr(attributeEventName, eventNameException)
		putIfMissing(attributes, attributeErrorSource, errorSourceCrash)
	case eventNameDeviceANR:
		attributes.PutStr(attributeEventName, eventNameException)
		putIfMissing(attributes, attributeErrorSource, errorSourceANR)
		putIfMissing(attributes, attributeExceptionType, exceptionTypeANR)
		putIfMissing(attributes, attributeExceptionMessage, anrMessage)
	}

	source, _ := attributes.Get(attributeErrorSource)
	exceptionType, _ := attributes.Get(attributeExceptionType)
	if source.Str() == errorSourceANR && exceptionType.Str() == exceptionTypeLegacyANR {
		attributes.PutStr(attributeExceptionType, exceptionTypeANR)
	}
}

// copyScreenRoute makes the screen name the view route the RUM screens group
// by, when the client set none.
func copyScreenRoute(attributes pcommon.Map) {
	if screen, ok := attributes.Get(attributeScreenName); ok && screen.Str() != "" {
		putIfMissing(attributes, attributeViewRoute, screen.Str())
	}
}

func putIfMissing(attributes pcommon.Map, key string, value string) {
	if current, ok := attributes.Get(key); !ok || current.Str() == "" {
		attributes.PutStr(key, value)
	}
}

// scrubAttributes scrubs the URL attributes, filters url.query (removed when
// nothing is left) and removes url.fragment, in one pass.
func scrubAttributes(policy *ingestpolicy.PIIPolicy, attributes pcommon.Map) {
	attributes.RemoveIf(func(key string, value pcommon.Value) bool {
		switch key {
		case fragmentAttribute:
			return true
		case queryAttribute:
			if value.Type() != pcommon.ValueTypeStr {
				return false
			}
			query := scrubQuery(policy, value.Str())
			if query == "" {
				return true
			}
			if query != value.Str() {
				value.SetStr(query)
			}
			return false
		}
		if isURLAttribute(key) && value.Type() == pcommon.ValueTypeStr {
			if scrubbed := scrubURL(policy, value.Str()); scrubbed != value.Str() {
				value.SetStr(scrubbed)
			}
		}
		return false
	})
}

func isURLAttribute(key string) bool {
	for _, k := range urlAttributes {
		if k == key {
			return true
		}
	}
	return false
}

func collectSession(attributes pcommon.Map, batch *report.Batch) {
	if sessionID, ok := attributes.Get(attributeSessionID); ok && sessionID.Str() != "" {
		batch.AddSession(sessionID.Str())
	}
}
