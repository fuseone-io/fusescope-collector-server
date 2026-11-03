// Copyright FuseOne
// SPDX-License-Identifier: MIT
//
// Moved from fuseone-io/fusescope
// pkg/modules/rumapplication/implrumapplication/gateway.go and
// gateway_otlp.go at 6c8991257 (MIT, see LICENSE-MIT). Changed, per
// docs/spec/rum.md §12: keys are looked up in the ingest policy, never a
// database, and only from the header; with no valid policy every request is
// refused; preflight is allowed by origin alone; X-Forwarded-For is read only
// from trusted proxies; a concurrency limit applies; unknown keys are one
// global counter; the request goes to the next consumer, whose errors map
// to 400 or 503; responses are precomputed and buffers pooled.

package rumreceiver // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver"

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"io"
	"mime"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"
	"unsafe"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/collector/receiver/receiverhelper"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
	"github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/ratelimit"
	"github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/report"
)

// KeyHeader carries the public key; there is no query-string key (§2, G4).
const KeyHeader = "FuseScope-RUM-Key"

const (
	tracesPath     = "/rum/v1/traces"
	logsPath       = "/rum/v1/logs"
	keyPrefix      = "rum_pub_"
	maxKeyLength   = 128
	allowedHeaders = "content-type, content-encoding, fusescope-rum-key"
	preflightAge   = "86400"
)

// accounting is the report's view of each request (§10.1); report.Monitor
// implements it.
type accounting interface {
	// SetPolicy advances accounting to the snapshot that authorised a
	// request, before the request is recorded.
	SetPolicy(snap *ingestpolicy.Snapshot)
	Ingest(app *ingestpolicy.Application, keyID string, batch *report.Batch, now time.Time)
	Reject(applicationID string, reason report.Reason, observed string, now time.Time)
	UnknownKey(now time.Time)
}

type clientIdentity struct {
	key   string
	group netip.Addr
}

type handler struct {
	source       ingestpolicy.Source
	maxStaleness time.Duration
	traces       consumer.Traces
	logs         consumer.Logs
	obsTraces    *receiverhelper.ObsReport
	obsLogs      *receiverhelper.ObsReport
	acct         accounting
	refused      func(context.Context, refusal)
	now          func() time.Time

	keyLimiter    *ratelimit.Limiter[string]
	clientLimiter *ratelimit.Limiter[clientIdentity]
	keyLimit      ratelimit.Limit
	clientLimit   ratelimit.Limit
	proxies       ratelimit.TrustedProxies

	maxBody      int64
	maxResources int
	bodyTimeout  time.Duration
	writeTimeout time.Duration
	slots        chan struct{}
	active       activeRequests
}

// activeRequests lets Shutdown wait for every handler, including ones a
// forced close interrupted, before the receiver's state is released.
type activeRequests struct {
	mu     sync.Mutex
	n      int
	closed bool
	idle   chan struct{}
}

func (a *activeRequests) enter() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return false
	}
	a.n++
	return true
}

func (a *activeRequests) leave() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.n--
	if a.closed && a.n == 0 {
		close(a.idle)
	}
}

// closeAndWait refuses new requests and waits for the active ones.
func (a *activeRequests) closeAndWait() {
	a.mu.Lock()
	if !a.closed {
		a.closed = true
		a.idle = make(chan struct{})
		if a.n == 0 {
			close(a.idle)
		}
	}
	idle := a.idle
	a.mu.Unlock()
	<-idle
}

type encoding uint8

const (
	encodingProtobuf encoding = iota
	encodingJSON
)

func (e encoding) contentType() string {
	if e == encodingJSON {
		return "application/json"
	}
	return "application/x-protobuf"
}

var (
	tracesResponse [2][]byte
	logsResponse   [2][]byte
)

func init() {
	tracesResponse[encodingProtobuf], _ = ptraceotlp.NewExportResponse().MarshalProto()
	tracesResponse[encodingJSON], _ = ptraceotlp.NewExportResponse().MarshalJSON()
	logsResponse[encodingProtobuf], _ = plogotlp.NewExportResponse().MarshalProto()
	logsResponse[encodingJSON], _ = plogotlp.NewExportResponse().MarshalJSON()
}

// work is the per-request state reused across requests. pdata copies every
// string and byte slice it decodes, so the body buffer can be reused while
// the decoded request travels on.
type work struct {
	body  bytes.Buffer
	gzip  *gzip.Reader
	wire  io.LimitedReader
	plain io.LimitedReader
	batch report.Batch
}

// maxPooledBody keeps rare large bodies from pinning memory in the pool.
const maxPooledBody = 1 << 20

var workPool = sync.Pool{New: func() any { return new(work) }}

func releaseWork(wk *work) {
	if wk.body.Cap() > maxPooledBody {
		wk.body = bytes.Buffer{}
	}
	wk.wire, wk.plain = io.LimitedReader{}, io.LimitedReader{}
	workPool.Put(wk)
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Before any refusal: net/http drains an unread body after the handler
	// returns, which without a deadline a client could stall forever.
	_ = http.NewResponseController(w).SetReadDeadline(h.now().Add(h.bodyTimeout))
	if !h.active.enter() {
		w.Header().Set("Connection", "close")
		h.refuse(w, r, refusalBusy)
		return
	}
	defer h.active.leave()
	var traces bool
	switch r.URL.Path {
	case tracesPath:
		traces = true
		if h.traces == nil {
			http.NotFound(w, r)
			return
		}
	case logsPath:
		if h.logs == nil {
			http.NotFound(w, r)
			return
		}
	default:
		http.NotFound(w, r)
		return
	}
	w.Header().Add("Vary", "Origin")

	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		h.refuse(w, r, refusalBusy)
		return
	}

	switch r.Method {
	case http.MethodOptions:
		h.preflight(w, r)
	case http.MethodPost:
		h.post(w, r, traces)
	default:
		w.Header().Set("Allow", "OPTIONS, POST")
		h.refuse(w, r, refusalMethod)
	}
}

// policy is the snapshot in force, nil with none or a stale one (§11.4).
func (h *handler) policy(now time.Time) *ingestpolicy.Snapshot {
	snap := h.source.Current()
	if snap == nil {
		return nil
	}
	if h.maxStaleness > 0 && now.Sub(h.source.LastReceived()) > h.maxStaleness {
		return nil
	}
	return snap
}

// preflight carries no key, so an origin is accepted when any application
// allows it; the POST is checked against the key's application (§4).
func (h *handler) preflight(w http.ResponseWriter, r *http.Request) {
	snap := h.policy(h.now())
	origin := r.Header.Get("Origin")
	if snap == nil || origin == "" || r.Header.Get("Access-Control-Request-Method") != http.MethodPost ||
		!snap.AnyApplicationAllowsOrigin(origin) {
		h.refuse(w, r, refusalPreflight)
		return
	}

	header := w.Header()
	header.Set("Access-Control-Allow-Origin", origin)
	header.Set("Access-Control-Allow-Methods", http.MethodPost)
	header.Set("Access-Control-Allow-Headers", allowedHeaders)
	header.Set("Access-Control-Max-Age", preflightAge)
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) post(w http.ResponseWriter, r *http.Request, traces bool) {
	now := h.now()
	snap := h.policy(now)
	if snap == nil {
		h.refuse(w, r, refusalNoPolicy)
		return
	}

	key := r.Header.Get(KeyHeader)
	if len(key) <= len(keyPrefix) || len(key) > maxKeyLength || !strings.HasPrefix(key, keyPrefix) {
		h.refuse(w, r, refusalKeyInvalid)
		return
	}
	// Sum256 neither keeps nor modifies its input.
	k := snap.Key(sha256.Sum256(unsafe.Slice(unsafe.StringData(key), len(key))))
	if k == nil || k.Kind != ingestpolicy.KindPublic || k.Application == nil {
		h.acct.UnknownKey(now)
		h.refuse(w, r, refusalKeyUnknown)
		return
	}
	app := k.Application
	h.acct.SetPolicy(snap)
	signal := ingestpolicy.SignalLogs
	if traces {
		signal = ingestpolicy.SignalTraces
	}
	if !k.UsableAt(now) {
		h.acct.Reject(app.ID, report.BadKey, "", now)
		h.refuse(w, r, refusalKeyExpired)
		return
	}
	// Authorisation, not authentication: the key is valid but may not send
	// this signal. A public key gains nothing from the difference (§2).
	if k.Signals&signal == 0 {
		h.acct.Reject(app.ID, report.SignalNotAllowed, "", now)
		h.refuse(w, r, refusalSignal)
		return
	}

	if origin := r.Header.Get("Origin"); origin != "" {
		if !app.AllowsOrigin(origin) {
			h.acct.Reject(app.ID, report.OriginNotAllowed, report.ObservedOrigin(origin), now)
			h.refuse(w, r, refusalOrigin)
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
	}

	keyLimit := h.keyLimit
	if k.Limit != nil {
		keyLimit = ratelimit.Limit{Rate: k.Limit.Rate, Burst: k.Limit.Burst}
	}
	identity := clientIdentity{key: k.ID, group: ratelimit.Group(h.proxies.ClientIP(r.RemoteAddr, r.Header.Values("X-Forwarded-For")))}
	if !h.keyLimiter.Allow(k.ID, keyLimit, now) || !h.clientLimiter.Allow(identity, h.clientLimit, now) {
		h.acct.Reject(app.ID, report.RateLimited, "", now)
		h.refuse(w, r, refusalRateLimited)
		return
	}

	enc, ok := encodingOf(r.Header.Get("Content-Type"))
	if !ok {
		h.acct.Reject(app.ID, report.InvalidPayload, "", now)
		h.refuse(w, r, refusalInvalid)
		return
	}

	wk := workPool.Get().(*work)
	defer releaseWork(wk)
	if refused := wk.readBody(r, h.maxBody); refused != refusalNone {
		reason := report.InvalidPayload
		if refused == refusalTooLarge {
			reason = report.TooLarge
		}
		h.acct.Reject(app.ID, reason, "", now)
		// The rest of the body is not read: the connection closes.
		w.Header().Set("Connection", "close")
		h.refuse(w, r, refused)
		return
	}
	_ = http.NewResponseController(w).SetWriteDeadline(h.now().Add(h.writeTimeout))

	wk.batch.Reset()
	// A deadline for the pipeline: the write deadline does not cancel the
	// request's context, and a consumer blocked on backpressure would hold
	// a request slot and its payload indefinitely.
	ctx, cancel := context.WithTimeout(r.Context(), h.writeTimeout)
	defer cancel()
	ctx = client.NewContext(ctx, client.Info{Auth: authData{orgID: k.OrgID, keyID: k.ID}})
	var refused refusal
	var observed string
	if traces {
		observed, refused = h.consumeTraces(ctx, app, k.ID, wk, enc)
	} else {
		observed, refused = h.consumeLogs(ctx, app, k.ID, wk, enc)
	}
	switch refused {
	case refusalNone:
	case refusalInvalid:
		h.acct.Reject(app.ID, report.InvalidPayload, "", now)
		h.refuse(w, r, refused)
		return
	case refusalTooLarge:
		h.acct.Reject(app.ID, report.TooLarge, "", now)
		h.refuse(w, r, refused)
		return
	case refusalPackage:
		h.acct.Reject(app.ID, report.PackageNotAllowed, report.ObservedPackage(observed), now)
		h.refuse(w, r, refused)
		return
	default:
		h.refuse(w, r, refused)
		return
	}

	h.acct.Ingest(app, k.ID, &wk.batch, now)
	response := logsResponse[enc]
	if traces {
		response = tracesResponse[enc]
	}
	w.Header().Set("Content-Type", enc.contentType())
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(response)
}

func (h *handler) consumeTraces(ctx context.Context, app *ingestpolicy.Application, keyID string, wk *work, enc encoding) (string, refusal) {
	var td ptrace.Traces
	var err error
	if enc == encodingJSON {
		td, err = (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(wk.body.Bytes())
	} else {
		td, err = (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(wk.body.Bytes())
	}
	if err != nil {
		return "", refusalInvalid
	}
	// Empty groups carry nothing; the rest are bounded before stamping
	// multiplies their attributes (§7.1).
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		for i := 0; i < rs.ScopeSpans().Len(); i++ {
			if rs.ScopeSpans().At(i).Spans().Len() > 0 {
				return false
			}
		}
		return true
	})
	if td.ResourceSpans().Len() > h.maxResources {
		return "", refusalTooLarge
	}
	if name, refused := refusedTracesPackage(app, td); refused {
		return name, refusalPackage
	}
	processTraces(app, keyID, td, &wk.batch)

	ctx = h.obsTraces.StartTracesOp(ctx)
	spans := td.SpanCount()
	err = h.traces.ConsumeTraces(ctx, td)
	h.obsTraces.EndTracesOp(ctx, format(enc), spans, err)
	return "", consumerRefusal(err)
}

func (h *handler) consumeLogs(ctx context.Context, app *ingestpolicy.Application, keyID string, wk *work, enc encoding) (string, refusal) {
	var ld plog.Logs
	var err error
	if enc == encodingJSON {
		ld, err = (&plog.JSONUnmarshaler{}).UnmarshalLogs(wk.body.Bytes())
	} else {
		ld, err = (&plog.ProtoUnmarshaler{}).UnmarshalLogs(wk.body.Bytes())
	}
	if err != nil {
		return "", refusalInvalid
	}
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		for i := 0; i < rl.ScopeLogs().Len(); i++ {
			if rl.ScopeLogs().At(i).LogRecords().Len() > 0 {
				return false
			}
		}
		return true
	})
	if ld.ResourceLogs().Len() > h.maxResources {
		return "", refusalTooLarge
	}
	if name, refused := refusedLogsPackage(app, ld); refused {
		return name, refusalPackage
	}
	processLogs(app, keyID, ld, &wk.batch)

	ctx = h.obsLogs.StartLogsOp(ctx)
	records := ld.LogRecordCount()
	err = h.logs.ConsumeLogs(ctx, ld)
	h.obsLogs.EndLogsOp(ctx, format(enc), records, err)
	return "", consumerRefusal(err)
}

// consumerRefusal maps a consumer error the way upstream OTLP receivers do:
// permanent 400, anything else 503 with Retry-After (§9).
func consumerRefusal(err error) refusal {
	switch {
	case err == nil:
		return refusalNone
	case consumererror.IsPermanent(err):
		return refusalConsumerPermanent
	default:
		return refusalConsumerRetryable
	}
}

func format(enc encoding) string {
	if enc == encodingJSON {
		return "json"
	}
	return "protobuf"
}

// encodingOf maps the Content-Type to an encoding. text/plain is read as
// JSON, as the backend did for navigator.sendBeacon (§6).
func encodingOf(contentType string) (encoding, bool) {
	mediaType := strings.ToLower(strings.TrimSpace(contentType))
	if i := strings.IndexByte(mediaType, ';'); i >= 0 {
		parsed, _, err := mime.ParseMediaType(contentType)
		if err == nil {
			mediaType = parsed
		}
	}
	switch mediaType {
	case "application/x-protobuf", "application/protobuf":
		return encodingProtobuf, true
	case "application/json", "text/plain":
		return encodingJSON, true
	}
	return encodingProtobuf, false
}

// readBody caps the body before and after gzip decoding, and never reads it
// without a bound (§6, G3).
func (wk *work) readBody(r *http.Request, limit int64) refusal {
	if r.ContentLength > limit {
		return refusalTooLarge
	}
	wk.body.Reset()
	wk.wire = io.LimitedReader{R: r.Body, N: limit + 1}

	var err error
	switch enc := strings.TrimSpace(r.Header.Get("Content-Encoding")); {
	case enc == "", strings.EqualFold(enc, "identity"):
		_, err = wk.body.ReadFrom(&wk.wire)
	case strings.EqualFold(enc, "gzip"):
		if wk.gzip == nil {
			wk.gzip, err = gzip.NewReader(&wk.wire)
		} else {
			err = wk.gzip.Reset(&wk.wire)
		}
		if err == nil {
			wk.plain = io.LimitedReader{R: wk.gzip, N: limit + 1}
			_, err = wk.body.ReadFrom(&wk.plain)
		}
	default:
		return refusalInvalid
	}
	switch {
	case wk.wire.N <= 0 || int64(wk.body.Len()) > limit:
		return refusalTooLarge
	case err != nil:
		return refusalInvalid
	}
	return refusalNone
}

// authData is the request's identity for the pipeline: org and key from
// the policy, never from the payload (§8.1).
type authData struct {
	orgID string
	keyID string
}

func (a authData) GetAttribute(name string) any {
	switch name {
	case "fusescope.org.id":
		return a.orgID
	case attributeKeyID:
		return a.keyID
	}
	return nil
}

func (a authData) GetAttributeNames() []string {
	return []string{"fusescope.org.id", attributeKeyID}
}
