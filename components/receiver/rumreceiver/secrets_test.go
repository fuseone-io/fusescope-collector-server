// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package rumreceiver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
	"github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/metadata"
)

// TestNoSecretInOwnTelemetry drives every refusal and the accepted path and
// checks that no key, verifier, request URL or client address reaches the
// receiver's logs, spans, metrics, error bodies or ingest report (G4, G5).
func TestNoSecretInOwnTelemetry(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	tel := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, tel.Shutdown(context.Background())) })

	set := receivertest.NewNopSettings(metadata.Type)
	set.TelemetrySettings = tel.NewTelemetrySettings()
	set.Logger = zap.New(core)
	cfg := createDefaultConfig().(*Config)
	cfg.TrustedProxies = []string{"203.0.113.0/24"}
	cfg.MaxBodySize = 2048
	cfg.RateLimit.Client = Limit{Rate: 0.001, Burst: 5}

	r, err := newReceiver(cfg, set)
	require.NoError(t, err)
	t.Cleanup(func() { r.telemetry.Shutdown() })
	store := ingestpolicy.NewStore(zap.New(core), nil)
	require.NoError(t, store.Apply(encodePolicy(t, testPolicy(1)), testNow))
	r.handler.source = store
	r.monitor.SetPolicy(store.Current())
	sink := new(consumertest.TracesSink)
	r.handler.traces, r.handler.logs = sink, new(consumertest.LogsSink)

	// A policy that tries to carry a key is rejected and logged without content.
	leaky := encodePolicy(t, testPolicy(2))
	leaky = []byte(strings.Replace(string(leaky), `"id":"`+webKeyID+`"`, `"plaintext":"`+webKey+`","id":"`+webKeyID+`"`, 1))
	require.Error(t, store.Apply(leaky, testNow))

	var bodies []string
	send := func(path, query, key, origin, body string) {
		req := httptest.NewRequest(http.MethodPost, path+query, strings.NewReader(body))
		req.RemoteAddr = "203.0.113.7:51234"
		req.Header.Set("X-Forwarded-For", "198.51.100.23")
		req.Header.Set("Content-Type", "application/json")
		if key != "" {
			req.Header.Set(KeyHeader, key)
		}
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		rec := httptest.NewRecorder()
		r.handler.ServeHTTP(rec, req)
		bodies = append(bodies, rec.Body.String())
	}
	query := "?fusescope_rum_key=" + webKey + "&page=secret-page"
	send(tracesPath, query, "", "", testTraceJSON)                                          // query key ignored
	send(tracesPath, "", unknownKey, "https://shop.example.com", testTraceJSON)             // unknown
	send(tracesPath, "", webKey+"x", "", testTraceJSON)                                     // unknown (near miss)
	send(tracesPath, "", "rum_pub_"+strings.Repeat("z", 200), "", testTraceJSON)            // malformed
	send(tracesPath, "", secretKind, "", testTraceJSON)                                     // wrong kind
	send(tracesPath, "", expiredKey, "", testTraceJSON)                                     // expired
	send(tracesPath, "", logsOnlyKey, "", testTraceJSON)                                    // signal not allowed
	send(tracesPath, "", webKey, "https://evil.example.org/"+webKey, testTraceJSON)         // origin
	send(tracesPath, "", webKey, "", `{"resourceSpans":`)                                   // invalid payload
	send(tracesPath, "", webKey, "", strings.Repeat(" ", 4096))                             // too large
	send(logsPath, "", androidKey, "", `{"resourceLogs":[{"resource":{},"scopeLogs":[]}]}`) // package
	r.handler.traces = consumertest.NewErr(consumererror.NewPermanent(errors.New("refused")))
	send(tracesPath, "", webKey, "", testTraceJSON)
	r.handler.traces = consumertest.NewErr(errors.New("busy"))
	send(tracesPath, "", webKey, "", testTraceJSON)
	r.handler.traces = sink
	send(tracesPath, "", webKey, "https://shop.example.com", testTraceJSON) // accepted
	send(tracesPath, "", webKey, "", testTraceJSON)                         // rate limited
	require.Equal(t, 1, sink.SpanCount())

	reportPages, err := r.monitor.Build(1, testNow, store.Status())
	require.NoError(t, err)
	reportData := bytes.Join(reportPages, nil)

	var dump strings.Builder
	for _, e := range logs.All() {
		fmt.Fprintf(&dump, "log %s %v\n", e.Message, e.ContextMap())
	}
	spans := tel.SpanRecorder.Ended()
	require.NotEmpty(t, spans, "the obsreport spans are checked")
	for _, s := range spans {
		fmt.Fprintf(&dump, "span %s %v %v %v\n", s.Name(), s.Attributes(), s.Events(), s.Status())
	}
	var rm metricdata.ResourceMetrics
	require.NoError(t, tel.Reader.Collect(context.Background(), &rm))
	metrics, err := json.Marshal(rm)
	require.NoError(t, err)
	assert.Contains(t, string(metrics), "otelcol_receiver_rum_requests_refused", "the refusal counter is checked")
	dump.Write(metrics)
	for _, b := range bodies {
		dump.WriteString(b)
	}
	dump.Write(reportData)

	secrets := []string{"203.0.113.7", "198.51.100.23", "fusescope_rum_key", "secret-page", "/rum/v1/"}
	for _, key := range []string{webKey, unknownKey, expiredKey, logsOnlyKey, secretKind, androidKey} {
		secrets = append(secrets, key, verifier(key), strings.TrimPrefix(key, "rum_pub_"))
	}
	for _, s := range secrets {
		assert.NotContains(t, dump.String(), s)
	}
}
