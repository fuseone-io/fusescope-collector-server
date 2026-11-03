// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package rumreceiver // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
	"github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/metadata"
	"github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/ratelimit"
	"github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"
)

var (
	errNoPolicy    = errors.New("no valid ingest policy: every RUM request is refused")
	errStalePolicy = errors.New("the ingest policy is older than max_staleness: every RUM request is refused")
)

// stalenessPoll rechecks staleness, which a repeated push refreshes without
// a new snapshot.
const stalenessPoll = time.Second

// expireEvery is a variable for tests.
var expireEvery = time.Minute

type rumReceiver struct {
	cfg       *Config
	set       receiver.Settings
	handler   *handler
	monitor   *report.Monitor
	telemetry *metadata.TelemetryBuilder
	forget    func()

	mu      sync.Mutex
	hosts   []component.Host
	ready   *bool
	server  *http.Server
	addr    net.Addr
	started bool

	stopOnce sync.Once
	done     chan struct{}
	wg       sync.WaitGroup
}

func newReceiver(cfg *Config, set receiver.Settings) (*rumReceiver, error) {
	proxies, err := parsePrefixes(cfg.TrustedProxies)
	if err != nil {
		return nil, err
	}
	obsTraces, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{ReceiverID: set.ID, Transport: "http", ReceiverCreateSettings: set})
	if err != nil {
		return nil, err
	}
	obsLogs, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{ReceiverID: set.ID, Transport: "http", ReceiverCreateSettings: set})
	if err != nil {
		return nil, err
	}
	tb, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	monitor := report.NewMonitor(report.Config{SessionsPerBucket: cfg.Limits.SessionsPerBucket, PendingKeys: cfg.Limits.PendingKeys})
	r := &rumReceiver{cfg: cfg, set: set, monitor: monitor, telemetry: tb, done: make(chan struct{}), forget: func() {}}
	r.handler = newHandler(cfg, noPolicy{}, monitor, proxies)
	r.handler.obsTraces, r.handler.obsLogs = obsTraces, obsLogs
	r.handler.refused = refusedCounter(tb)
	return r, nil
}

func newHandler(cfg *Config, source ingestpolicy.Source, acct accounting, proxies []netip.Prefix) *handler {
	return &handler{
		source:        source,
		maxStaleness:  cfg.MaxStaleness,
		acct:          acct,
		now:           time.Now,
		keyLimiter:    ratelimit.NewLimiter[string](ingestpolicy.MaxKeys),
		clientLimiter: ratelimit.NewLimiter[clientIdentity](cfg.Limits.ClientBuckets),
		keyLimit:      ratelimit.Limit{Rate: cfg.RateLimit.Key.Rate, Burst: cfg.RateLimit.Key.Burst},
		clientLimit:   ratelimit.Limit{Rate: cfg.RateLimit.Client.Rate, Burst: cfg.RateLimit.Client.Burst},
		proxies:       ratelimit.TrustedProxies(proxies),
		maxBody:       cfg.MaxBodySize,
		maxResources:  cfg.Limits.ResourcesPerRequest,
		bodyTimeout:   cfg.Limits.BodyReadTimeout,
		writeTimeout:  cfg.Limits.WriteTimeout,
		slots:         make(chan struct{}, cfg.Limits.Requests),
	}
}

// refusedCounter counts refusals with one precomputed attribute set each.
func refusedCounter(tb *metadata.TelemetryBuilder) func(context.Context, refusal) {
	var opts [numRefusals][]metric.AddOption
	for i := refusalBusy; i < numRefusals; i++ {
		opts[i] = []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(attribute.String("reason", refusalSpecs[i].reason)))}
	}
	return func(ctx context.Context, rf refusal) {
		tb.ReceiverRumRequestsRefused.Add(ctx, 1, opts[rf]...)
	}
}

func (r *rumReceiver) setTraces(next consumer.Traces) error {
	if next == nil {
		return errors.New("nil traces consumer")
	}
	r.handler.traces = next
	return nil
}

func (r *rumReceiver) setLogs(next consumer.Logs) error {
	if next == nil {
		return errors.New("nil logs consumer")
	}
	r.handler.logs = next
	return nil
}

// Start runs once per signal; the first call starts the listener.
func (r *rumReceiver) Start(ctx context.Context, host component.Host) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hosts = append(r.hosts, host)
	if r.started {
		if r.ready != nil {
			reportStatus(host, *r.ready, r.handler.maxStaleness)
		}
		return nil
	}

	source, err := resolveSource(host, r.cfg.IngestPolicy)
	if err != nil {
		return err
	}
	if source == nil {
		r.set.Logger.Warn("No ingest_policy configured: every RUM request is refused and the receiver is not ready")
		source = noPolicy{}
	}
	tlsCfg, err := r.cfg.TLS.LoadTLSConfig(ctx)
	if err != nil {
		return fmt.Errorf("rum receiver TLS: %w", err)
	}
	if err := r.telemetry.RegisterReceiverRumPolicyAgeCallback(func(_ context.Context, o metric.Float64Observer) error {
		if received := source.LastReceived(); source.Current() != nil && !received.IsZero() {
			o.Observe(time.Since(received).Seconds())
		}
		return nil
	}); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", r.cfg.Endpoint)
	if err != nil {
		return err
	}
	r.addr = ln.Addr()
	r.handler.source = source
	r.server = &http.Server{
		Handler:           r.handler,
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: r.cfg.Limits.HeaderReadTimeout,
		// An outer bound; the handler sets the body deadline per request.
		ReadTimeout: r.cfg.Limits.HeaderReadTimeout + r.cfg.Limits.BodyReadTimeout,
		// The body read and the write have per-request deadlines (handler.go).
		WriteTimeout:   r.cfg.Limits.BodyReadTimeout + r.cfg.Limits.WriteTimeout,
		IdleTimeout:    r.cfg.Limits.IdleTimeout,
		MaxHeaderBytes: r.cfg.Limits.MaxHeaderBytes,
		// net/http logs handshake errors with the client address (G5).
		ErrorLog: log.New(io.Discard, "", 0),
	}
	r.started = true

	limited := newLimitListener(ln, r.cfg.Limits.Connections)
	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		if err := r.server.ServeTLS(limited, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(err))
		}
	}()
	go func() {
		defer r.wg.Done()
		r.watch(source)
	}()
	return nil
}

// watch keeps the monitors to the policy's applications and reports the
// receiver ready only while it holds a valid policy (§11.4).
func (r *rumReceiver) watch(source ingestpolicy.Source) {
	var poll <-chan time.Time
	if r.handler.maxStaleness > 0 {
		t := time.NewTicker(stalenessPoll)
		defer t.Stop()
		poll = t.C
	}
	// Window values are released on time even if no report is built (§10.3).
	expire := time.NewTicker(expireEvery)
	defer expire.Stop()
	var held *ingestpolicy.Snapshot
	for {
		changed := source.Changed()
		snap := source.Current()
		if snap != held {
			r.monitor.SetPolicy(snap)
			held = snap
		}
		r.setReady(r.handler.policy(r.handler.now()) != nil)
		select {
		case <-changed:
		case <-poll:
		case <-expire.C:
			r.monitor.Expire(r.handler.now())
		case <-r.done:
			return
		}
	}
}

func (r *rumReceiver) setReady(ready bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ready != nil && *r.ready == ready {
		return
	}
	r.ready = &ready
	for _, host := range r.hosts {
		reportStatus(host, ready, r.handler.maxStaleness)
	}
}

func reportStatus(host component.Host, ready bool, maxStaleness time.Duration) {
	if ready {
		componentstatus.ReportStatus(host, componentstatus.NewEvent(componentstatus.StatusOK))
		return
	}
	err := errNoPolicy
	if maxStaleness > 0 {
		err = fmt.Errorf("%w (or %w)", errNoPolicy, errStalePolicy)
	}
	componentstatus.ReportStatus(host, componentstatus.NewRecoverableErrorEvent(err))
}

func (r *rumReceiver) Shutdown(ctx context.Context) error {
	var err error
	r.stopOnce.Do(func() {
		close(r.done)
		r.mu.Lock()
		server := r.server
		r.mu.Unlock()
		if server != nil {
			if err = server.Shutdown(ctx); err != nil {
				// Graceful shutdown timed out: close the active connections,
				// which cancels their requests' contexts.
				_ = server.Close()
			}
		}
		r.handler.active.closeAndWait()
		r.wg.Wait()
		r.telemetry.Shutdown()
		r.forget()
	})
	return err
}

// resolveSource finds the extension that holds the ingest policy.
func resolveSource(host component.Host, id component.ID) (ingestpolicy.Source, error) {
	if id == (component.ID{}) {
		return nil, nil
	}
	ext, ok := host.GetExtensions()[id]
	if !ok {
		return nil, fmt.Errorf("ingest_policy: extension %q not found", id)
	}
	provider, ok := ext.(ingestpolicy.Provider)
	if !ok {
		return nil, fmt.Errorf("ingest_policy: extension %q does not hold an ingest policy", id)
	}
	return provider.IngestPolicy(), nil
}

// noPolicy is the source without a policy: fail closed.
type noPolicy struct{}

var never = make(chan struct{})

func (noPolicy) Current() *ingestpolicy.Snapshot { return nil }
func (noPolicy) LastReceived() time.Time         { return time.Time{} }
func (noPolicy) Changed() <-chan struct{}        { return never }
func (noPolicy) Status() ingestpolicy.Status     { return ingestpolicy.Status{} }
