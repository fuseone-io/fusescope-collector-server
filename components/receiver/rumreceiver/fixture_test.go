// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package rumreceiver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
	"github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/metadata"
	"github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"
)

const (
	testOrg        = "0191c5e2-0000-7000-8000-00000000000a"
	testWebApp     = "0191c5e2-7f0a-7c3e-9a51-3c2b1d4e5f60"
	testAndroidApp = "0191c5e2-7f0a-7c3e-9a51-3c2b1d4e5f61"

	webKey       = "rum_pub_abcdefghijklmnopqrstuvwxyz234567"
	webKeyID     = "0191c5e2-8a1b-7d2e-b0c4-000000000001"
	androidKey   = "rum_pub_androidandroidandroidandroid22"
	androidKeyID = "0191c5e2-8a1b-7d2e-b0c4-000000000002"
	expiredKey   = "rum_pub_expiredexpiredexpiredexpired22"
	expiredKeyID = "0191c5e2-8a1b-7d2e-b0c4-000000000003"
	logsOnlyKey  = "rum_pub_logsonlylogsonlylogsonlylogs22"
	logsOnlyID   = "0191c5e2-8a1b-7d2e-b0c4-000000000004"
	secretKind   = "rum_pub_secretkindsecretkindsecretkind"
	secretKindID = "0191c5e2-8a1b-7d2e-b0c4-000000000005"
	limitedKey   = "rum_pub_limitedlimitedlimitedlimited22"
	limitedKeyID = "0191c5e2-8a1b-7d2e-b0c4-000000000006"
	unknownKey   = "rum_pub_nopenopenopenopenopenopenopenope"
)

var testNow = time.Date(2026, 11, 3, 12, 0, 30, 0, time.UTC)

func verifier(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func ptr[T any](v T) *T { return &v }

func testPolicy(version uint64) *ingestpolicy.Policy {
	public := func(id, key, app string, signals ...string) ingestpolicy.Key {
		return ingestpolicy.Key{ID: id, SHA256: verifier(key), OrgID: testOrg, Kind: ingestpolicy.KindPublic, Signals: signals, ApplicationID: ptr(app)}
	}
	expired := public(expiredKeyID, expiredKey, testWebApp, "traces", "logs")
	expired.ExpiresAt = ptr("2026-11-03T12:00:00Z")
	limited := public(limitedKeyID, limitedKey, testWebApp, "traces", "logs")
	limited.Limit = &ingestpolicy.Limit{Rate: 0.001, Burst: 1}
	return &ingestpolicy.Policy{
		SchemaVersion: 1,
		Version:       version,
		IssuedAt:      "2026-11-03T12:00:00Z",
		Keys: []ingestpolicy.Key{
			public(webKeyID, webKey, testWebApp, "traces", "logs"),
			public(androidKeyID, androidKey, testAndroidApp, "traces", "logs"),
			expired,
			public(logsOnlyID, logsOnlyKey, testWebApp, "logs"),
			{ID: secretKindID, SHA256: verifier(secretKind), OrgID: testOrg, Kind: ingestpolicy.KindSecret, Signals: []string{"traces", "logs"}},
			limited,
		},
		RUMApplications: []ingestpolicy.RUMApplication{
			{
				ID: testWebApp, OrgID: testOrg, Name: "shop-web", Environment: "production", Platform: ingestpolicy.PlatformWeb,
				AllowedOrigins: []string{"https://shop.example.com", "https://*.cdn.example.com"}, AllowedPackageNames: []string{},
				PIIPolicy: ingestpolicy.PIIPolicy{DropQuery: true, QueryAllowlist: []string{"page"}, URLAllowlist: []string{}},
			},
			{
				ID: testAndroidApp, OrgID: testOrg, Name: "shop-android", Environment: "production", Platform: ingestpolicy.PlatformAndroid,
				AllowedOrigins: []string{}, AllowedPackageNames: []string{"com.example.shop"},
				PIIPolicy: ingestpolicy.PIIPolicy{DropQuery: true, QueryAllowlist: []string{}, URLAllowlist: []string{}},
			},
		},
	}
}

func encodePolicy(t testing.TB, p *ingestpolicy.Policy) []byte {
	t.Helper()
	b, err := json.Marshal(p)
	require.NoError(t, err)
	return b
}

func newStore(t testing.TB, p *ingestpolicy.Policy) *ingestpolicy.Store {
	t.Helper()
	s := ingestpolicy.NewStore(zap.NewNop(), nil)
	if p != nil {
		require.NoError(t, s.Apply(encodePolicy(t, p), testNow))
	}
	return s
}

// event is one accounting call.
type event struct {
	kind     string
	app      string
	reason   report.Reason
	observed string
	key      string
	batch    report.Batch
}

// recorder forwards to a real monitor and records each call.
type recorder struct {
	mu      sync.Mutex
	events  []event
	monitor *report.Monitor
}

func (r *recorder) Ingest(app *ingestpolicy.Application, keyID string, b *report.Batch, now time.Time) {
	r.monitor.Ingest(app, keyID, b, now)
	r.mu.Lock()
	defer r.mu.Unlock()
	copied := *b
	copied.Sessions = append([]uint64(nil), b.Sessions...)
	r.events = append(r.events, event{kind: "ingest", app: app.ID, key: keyID, batch: copied})
}

func (r *recorder) Reject(appID string, reason report.Reason, observed string, now time.Time) {
	r.monitor.Reject(appID, reason, observed, now)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event{kind: "reject", app: appID, reason: reason, observed: observed})
}

func (r *recorder) UnknownKey(now time.Time) {
	r.monitor.UnknownKey(now)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event{kind: "unknown"})
}

func (r *recorder) take() []event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.events
	r.events = nil
	return out
}

type fixture struct {
	t       testing.TB
	handler *handler
	store   *ingestpolicy.Store
	traces  *consumertest.TracesSink
	logs    *consumertest.LogsSink
	acct    *recorder
	now     time.Time
}

func newFixture(t testing.TB, configure func(*Config)) *fixture {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	if configure != nil {
		configure(cfg)
	}
	return newFixtureWith(t, cfg, newStore(t, testPolicy(1)))
}

func newFixtureWith(t testing.TB, cfg *Config, source ingestpolicy.Source) *fixture {
	t.Helper()
	proxies, err := parsePrefixes(cfg.TrustedProxies)
	require.NoError(t, err)
	monitor := report.NewMonitor(report.Config{SessionsPerBucket: cfg.Limits.SessionsPerBucket, PendingKeys: cfg.Limits.PendingKeys})
	monitor.SetPolicy(source.Current())
	f := &fixture{t: t, traces: new(consumertest.TracesSink), logs: new(consumertest.LogsSink), acct: &recorder{monitor: monitor}, now: testNow}
	if s, ok := source.(*ingestpolicy.Store); ok {
		f.store = s
	}
	f.handler = newHandler(cfg, source, f.acct, proxies)
	f.handler.now = func() time.Time { return f.now }
	f.handler.traces, f.handler.logs = f.traces, f.logs
	set := receivertest.NewNopSettings(metadata.Type)
	f.handler.obsTraces, err = receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{ReceiverID: set.ID, Transport: "http", ReceiverCreateSettings: set})
	require.NoError(t, err)
	f.handler.obsLogs = f.handler.obsTraces
	return f
}

func (f *fixture) do(method, target string, body io.Reader, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, body)
	req.RemoteAddr = "203.0.113.7:51234"
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	return rec
}

func (f *fixture) post(path, body string, headers map[string]string) *httptest.ResponseRecorder {
	return f.do(http.MethodPost, path, strings.NewReader(body), headers)
}

func jsonHeaders(key string, extra ...string) map[string]string {
	h := map[string]string{"Content-Type": "application/json"}
	if key != "" {
		h[KeyHeader] = key
	}
	for i := 0; i+1 < len(extra); i += 2 {
		h[extra[i]] = extra[i+1]
	}
	return h
}

func httptestRequest(target, body string, headers map[string]string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	req.RemoteAddr = "203.0.113.7:51234"
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req
}

func serve(f *fixture, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	return rec
}

// blockingTraces holds ConsumeTraces until release is closed.
type blockingTraces struct {
	entered chan struct{}
	release chan struct{}
}

func (b blockingTraces) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }
func (b blockingTraces) ConsumeTraces(context.Context, ptrace.Traces) error {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.release
	return nil
}

type consumerFunc func(context.Context, ptrace.Traces) error

func (f consumerFunc) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }
func (f consumerFunc) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return f(ctx, td)
}

const testTraceJSON = `{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"shop-web"}}]},"scopeSpans":[{"spans":[{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174","name":"GET /search?email=a@b.c","kind":3,"startTimeUnixNano":"1700000000000000000","endTimeUnixNano":"1700000001000000000","attributes":[{"key":"url.full","value":{"stringValue":"https://shop.example.com/search?email=a@b.c&page=2"}},{"key":"session.id","value":{"stringValue":"s1"}}]}]}]}]}`

const testLogJSON = `{"resourceLogs":[{"resource":{},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1700000000000000000","eventName":"exception","body":{"stringValue":"boom"},"attributes":[{"key":"fusescope.rum.error.source","value":{"stringValue":"onerror"}}]}]}]}]}`
