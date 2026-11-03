// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package rumreceiver

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
	"github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/metadata"
	"github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"
)

var policyExtension = component.MustNewID("fusescope_opamp")

// writeCert writes a self-signed certificate for localhost and returns the
// TLS settings and a client pool trusting it.
func writeCert(t testing.TB) (configtls.ServerConfig, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600))
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return configtls.ServerConfig{Config: configtls.Config{CertFile: certFile, KeyFile: keyFile, MinVersion: "1.3"}}, pool
}

type providerExtension struct {
	component.StartFunc
	component.ShutdownFunc
	source ingestpolicy.Source
}

func (p providerExtension) IngestPolicy() ingestpolicy.Source { return p.source }

type plainExtension struct {
	component.StartFunc
	component.ShutdownFunc
}

// statusHost records the status events reported to it.
type statusHost struct {
	extensions map[component.ID]component.Component
	mu         sync.Mutex
	events     []*componentstatus.Event
	changed    chan struct{}
}

func newStatusHost(extensions map[component.ID]component.Component) *statusHost {
	return &statusHost{extensions: extensions, changed: make(chan struct{}, 100)}
}

func (h *statusHost) GetExtensions() map[component.ID]component.Component { return h.extensions }

func (h *statusHost) Report(ev *componentstatus.Event) {
	h.mu.Lock()
	h.events = append(h.events, ev)
	h.mu.Unlock()
	h.changed <- struct{}{}
}

func (h *statusHost) waitFor(t *testing.T, status componentstatus.Status) *componentstatus.Event {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		h.mu.Lock()
		if n := len(h.events); n > 0 && h.events[n-1].Status() == status {
			ev := h.events[n-1]
			h.mu.Unlock()
			return ev
		}
		h.mu.Unlock()
		select {
		case <-h.changed:
		case <-deadline:
			t.Fatalf("no %v status reported", status)
			return nil
		}
	}
}

type started struct {
	r      *rumReceiver
	client *http.Client
	url    string
	traces *consumertest.TracesSink
	logs   *consumertest.LogsSink
}

func startReceiver(t *testing.T, configure func(*Config), host component.Host) *started {
	t.Helper()
	tlsCfg, pool := writeCert(t)
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = "127.0.0.1:0"
	cfg.TLS = tlsCfg
	cfg.IngestPolicy = policyExtension
	if configure != nil {
		configure(cfg)
	}
	require.NoError(t, cfg.Validate())

	set := receivertest.NewNopSettings(metadata.Type)
	s := &started{traces: new(consumertest.TracesSink), logs: new(consumertest.LogsSink)}
	tr, err := NewFactory().CreateTraces(context.Background(), set, cfg, s.traces)
	require.NoError(t, err)
	lr, err := NewFactory().CreateLogs(context.Background(), set, cfg, s.logs)
	require.NoError(t, err)
	require.Same(t, tr, lr, "one listener for both signals")
	require.NoError(t, tr.Start(context.Background(), host))
	require.NoError(t, lr.Start(context.Background(), host))
	t.Cleanup(func() {
		assert.NoError(t, tr.Shutdown(context.Background()))
		assert.NoError(t, lr.Shutdown(context.Background()))
	})
	s.r = tr.(*rumReceiver)
	s.url = "https://" + s.r.addr.String()
	s.client = &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
	t.Cleanup(s.client.CloseIdleConnections)
	return s
}

func (s *started) post(t *testing.T, path, body string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, s.url+path, strings.NewReader(body))
	require.NoError(t, err)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := s.client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	return resp
}

func TestReceiverServesOverTLS(t *testing.T) {
	store := newStore(t, testPolicy(1))
	host := newStatusHost(map[component.ID]component.Component{policyExtension: providerExtension{source: store}})
	s := startReceiver(t, nil, host)

	resp := s.post(t, tracesPath, testTraceJSON, jsonHeaders(webKey))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, uint16(tls.VersionTLS13), resp.TLS.Version)
	resp = s.post(t, logsPath, testLogJSON, jsonHeaders(webKey))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 1, s.traces.SpanCount())
	assert.Equal(t, 1, s.logs.LogRecordCount())

	old := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: s.client.Transport.(*http.Transport).TLSClientConfig.RootCAs, MaxVersion: tls.VersionTLS12}}}
	defer old.CloseIdleConnections()
	_, err := old.Post(s.url+tracesPath, "application/json", strings.NewReader(testTraceJSON))
	assert.Error(t, err, "TLS 1.2 is refused (ADR 0004)")
}

func TestReceiverReadiness(t *testing.T) {
	store := newStore(t, nil)
	host := newStatusHost(map[component.ID]component.Component{policyExtension: providerExtension{source: store}})
	s := startReceiver(t, nil, host)

	ev := host.waitFor(t, componentstatus.StatusRecoverableError)
	assert.ErrorIs(t, ev.Err(), errNoPolicy)
	assert.Equal(t, http.StatusServiceUnavailable, s.post(t, tracesPath, testTraceJSON, jsonHeaders(webKey)).StatusCode)

	require.NoError(t, store.Apply(encodePolicy(t, testPolicy(1)), time.Now()))
	host.waitFor(t, componentstatus.StatusOK)
	assert.Equal(t, http.StatusOK, s.post(t, tracesPath, testTraceJSON, jsonHeaders(webKey)).StatusCode)

	// The monitors follow the policy.
	data, err := s.r.monitor.Build(1, time.Now(), store.Status())
	require.NoError(t, err)
	assert.Contains(t, string(data), testWebApp)
}

func TestReceiverReadinessStale(t *testing.T) {
	store := newStore(t, nil)
	require.NoError(t, store.Apply(encodePolicy(t, testPolicy(1)), time.Now().Add(-time.Hour)))
	host := newStatusHost(map[component.ID]component.Component{policyExtension: providerExtension{source: store}})
	startReceiver(t, func(cfg *Config) { cfg.MaxStaleness = time.Minute }, host)

	ev := host.waitFor(t, componentstatus.StatusRecoverableError)
	assert.ErrorIs(t, ev.Err(), errStalePolicy)
	require.NoError(t, store.Apply(encodePolicy(t, testPolicy(1)), time.Now()))
	host.waitFor(t, componentstatus.StatusOK)
}

func TestReceiverWithoutIngestPolicyFailsClosed(t *testing.T) {
	host := newStatusHost(nil)
	s := startReceiver(t, func(cfg *Config) { cfg.IngestPolicy = component.ID{} }, host)
	host.waitFor(t, componentstatus.StatusRecoverableError)
	resp := s.post(t, tracesPath, testTraceJSON, jsonHeaders(webKey))
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Equal(t, "5", resp.Header.Get("Retry-After"))
}

func TestReceiverStartErrors(t *testing.T) {
	tlsCfg, _ := writeCert(t)
	testCases := []struct {
		name       string
		extensions map[component.ID]component.Component
		configure  func(*Config)
		err        string
	}{
		{name: "ExtensionMissing", err: `extension "fusescope_opamp" not found`},
		{name: "ExtensionWithoutPolicy", extensions: map[component.ID]component.Component{policyExtension: plainExtension{}}, err: "does not hold an ingest policy"},
		{
			name:       "CertificateUnreadable",
			extensions: map[component.ID]component.Component{policyExtension: providerExtension{source: newStore(t, nil)}},
			configure:  func(cfg *Config) { cfg.TLS.CertFile = filepath.Join(t.TempDir(), "missing.pem") },
			err:        "TLS",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			cfg.Endpoint, cfg.TLS, cfg.IngestPolicy = "127.0.0.1:0", tlsCfg, policyExtension
			if tc.configure != nil {
				tc.configure(cfg)
			}
			r, err := NewFactory().CreateTraces(context.Background(), receivertest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
			require.NoError(t, err)
			err = r.Start(context.Background(), newStatusHost(tc.extensions))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.err)
			require.NoError(t, r.Shutdown(context.Background()))
		})
	}
}

func TestReceiverHeaderBytesLimit(t *testing.T) {
	store := newStore(t, testPolicy(1))
	host := newStatusHost(map[component.ID]component.Component{policyExtension: providerExtension{source: store}})
	s := startReceiver(t, func(cfg *Config) { cfg.Limits.MaxHeaderBytes = 4 << 10 }, host)

	headers := jsonHeaders(webKey, "X-Padding", strings.Repeat("p", 16<<10))
	resp := s.post(t, tracesPath, testTraceJSON, headers)
	assert.Equal(t, http.StatusRequestHeaderFieldsTooLarge, resp.StatusCode)
}

func TestReceiverTimeoutsCloseConnections(t *testing.T) {
	store := newStore(t, testPolicy(1))
	host := newStatusHost(map[component.ID]component.Component{policyExtension: providerExtension{source: store}})
	s := startReceiver(t, func(cfg *Config) {
		cfg.Limits.HeaderReadTimeout = 200 * time.Millisecond
		cfg.Limits.BodyReadTimeout = 200 * time.Millisecond
		cfg.Limits.IdleTimeout = 200 * time.Millisecond
	}, host)
	pool := s.client.Transport.(*http.Transport).TLSClientConfig.RootCAs

	t.Run("HeaderRead", func(t *testing.T) {
		conn, err := tls.Dial("tcp", s.r.addr.String(), &tls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{"http/1.1"}})
		require.NoError(t, err)
		defer conn.Close()
		_, _ = conn.Write([]byte("POST /rum/v1/traces HTTP/1.1\r\nHost: x\r\n"))
		assertClosed(t, conn)
	})
	t.Run("BodyRead", func(t *testing.T) {
		conn, err := tls.Dial("tcp", s.r.addr.String(), &tls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{"http/1.1"}})
		require.NoError(t, err)
		defer conn.Close()
		_, _ = conn.Write([]byte("POST /rum/v1/traces HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n" + KeyHeader + ": " + webKey + "\r\nContent-Length: 100\r\n\r\n{\"res"))
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.True(t, resp.Close, "the connection is closed after a body timeout")
	})
	// Codex review on #43: a request refused before the body is read (here,
	// no key) must not leave the server draining an unsent body forever.
	for _, path := range []string{tracesPath, "/elsewhere"} {
		t.Run("BodyNeverSentAfterEarlyRefusal"+path, func(t *testing.T) {
			conn, err := tls.Dial("tcp", s.r.addr.String(), &tls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{"http/1.1"}})
			require.NoError(t, err)
			defer conn.Close()
			_, _ = conn.Write([]byte("POST " + path + " HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 1\r\n\r\n"))
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			br := bufio.NewReader(conn)
			resp, err := http.ReadResponse(br, nil)
			require.NoError(t, err)
			resp.Body.Close()
			assert.Contains(t, []int{http.StatusUnauthorized, http.StatusNotFound}, resp.StatusCode)
			_, err = io.Copy(io.Discard, br)
			var ne net.Error
			require.False(t, asNetError(err, &ne) && ne.Timeout(), "the server must close the connection, not wait for the body: %v", err)
		})
	}
	t.Run("Idle", func(t *testing.T) {
		conn, err := tls.Dial("tcp", s.r.addr.String(), &tls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{"http/1.1"}})
		require.NoError(t, err)
		defer conn.Close()
		assertClosed(t, conn)
	})
}

func TestReceiverConnectionLimit(t *testing.T) {
	store := newStore(t, testPolicy(1))
	host := newStatusHost(map[component.ID]component.Component{policyExtension: providerExtension{source: store}})
	s := startReceiver(t, func(cfg *Config) { cfg.Limits.Connections = 1 }, host)
	pool := s.client.Transport.(*http.Transport).TLSClientConfig.RootCAs
	dial := func() (*tls.Conn, error) {
		d := &net.Dialer{Timeout: time.Second}
		conn, err := d.Dial("tcp", s.r.addr.String())
		if err != nil {
			return nil, err
		}
		tc := tls.Client(conn, &tls.Config{RootCAs: pool, ServerName: "localhost"})
		_ = tc.SetDeadline(time.Now().Add(300 * time.Millisecond))
		return tc, tc.Handshake()
	}

	first, err := dial()
	require.NoError(t, err)
	second, err := dial()
	assert.Error(t, err, "the second connection waits in the backlog, unserved")
	if second != nil {
		second.Close()
	}
	first.Close()
	assert.Eventually(t, func() bool {
		c, err := dial()
		if c != nil {
			c.Close()
		}
		return err == nil
	}, 5*time.Second, 50*time.Millisecond, "a slot frees when a connection closes")
}

// Codex review on #43: the receiver releases expired window state without
// waiting for a report to be built.
func TestReceiverExpiresWindowState(t *testing.T) {
	// Restored after the receiver's own cleanup has stopped it.
	saved := expireEvery
	t.Cleanup(func() { expireEvery = saved })
	expireEvery = 20 * time.Millisecond
	store := newStore(t, testPolicy(1))
	host := newStatusHost(map[component.ID]component.Component{policyExtension: providerExtension{source: store}})
	s := startReceiver(t, nil, host)
	host.waitFor(t, componentstatus.StatusOK)

	s.r.monitor.Reject(testWebApp, report.OriginNotAllowed, "https://evil.example.org", time.Now().Add(-time.Hour))
	before, err := s.r.monitor.Build(1, time.Now().Add(-time.Hour), store.Status())
	require.NoError(t, err)
	require.Contains(t, string(before), "evil.example.org")
	assert.Eventually(t, func() bool {
		data, err := s.r.monitor.Build(uint64(time.Now().UnixNano()), time.Now().Add(-time.Hour), store.Status())
		return err == nil && !strings.Contains(string(data), "evil.example.org")
	}, 5*time.Second, 50*time.Millisecond)
}

// ctxTraces blocks until its request's context ends.
type ctxTraces struct {
	entered chan struct{}
	exited  chan struct{}
}

func (c ctxTraces) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }
func (c ctxTraces) ConsumeTraces(ctx context.Context, _ ptrace.Traces) error {
	c.entered <- struct{}{}
	<-ctx.Done()
	close(c.exited)
	return ctx.Err()
}

// Codex review on #43: Shutdown that times out closes active connections and
// returns only once their handlers are done.
func TestReceiverShutdownWaitsForActiveRequests(t *testing.T) {
	tlsCfg, pool := writeCert(t)
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint, cfg.TLS, cfg.IngestPolicy = "127.0.0.1:0", tlsCfg, policyExtension
	block := ctxTraces{entered: make(chan struct{}, 1), exited: make(chan struct{})}
	r, err := NewFactory().CreateTraces(context.Background(), receivertest.NewNopSettings(metadata.Type), cfg, block)
	require.NoError(t, err)
	host := newStatusHost(map[component.ID]component.Component{policyExtension: providerExtension{source: newStore(t, testPolicy(1))}})
	require.NoError(t, r.Start(context.Background(), host))
	host.waitFor(t, componentstatus.StatusOK)

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	defer client.CloseIdleConnections()
	go func() {
		req, _ := http.NewRequest(http.MethodPost, "https://"+r.(*rumReceiver).addr.String()+tracesPath, strings.NewReader(testTraceJSON))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(KeyHeader, webKey)
		if resp, err := client.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	<-block.entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	assert.Error(t, r.Shutdown(ctx), "the graceful shutdown timed out")
	select {
	case <-block.exited:
	default:
		t.Fatal("Shutdown returned while a handler was still in the pipeline")
	}
}

func assertClosed(t *testing.T, conn *tls.Conn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1024)
	for {
		_, err := conn.Read(buf)
		if err != nil {
			var ne net.Error
			if assert.Error(t, err) && (!asNetError(err, &ne) || !ne.Timeout()) {
				return
			}
			t.Fatal("the server did not close the connection")
		}
	}
}

func asNetError(err error, target *net.Error) bool {
	ne, ok := err.(net.Error)
	if ok {
		*target = ne
	}
	return ok
}

var _ receiver.Factory = NewFactory()
