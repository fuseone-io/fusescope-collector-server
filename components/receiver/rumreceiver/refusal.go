// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package rumreceiver // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver"

import (
	"encoding/json"
	"net/http"
)

// refusal is one way a request is refused (§9). Each has a fixed status,
// Retry-After and JSON body, built once: nothing from the request, and so
// never the key, is echoed back.
type refusal uint8

const (
	refusalNone refusal = iota
	refusalBusy
	refusalMethod
	refusalPreflight
	refusalNoPolicy
	refusalKeyInvalid
	refusalKeyUnknown
	refusalKeyExpired
	refusalOrigin
	refusalRateLimited
	refusalInvalid
	refusalTooLarge
	refusalPackage
	refusalConsumerPermanent
	refusalConsumerRetryable
	numRefusals
)

type refusalSpec struct {
	status     int
	retryAfter string
	// code follows the backend's error codes ([B] impl/gateway.go:29-37).
	code    string
	message string
	// reason is the telemetry attribute value.
	reason string
}

var refusalSpecs = [numRefusals]refusalSpec{
	refusalBusy:              {http.StatusServiceUnavailable, "1", "rum_busy", "too many concurrent requests", "busy"},
	refusalMethod:            {http.StatusMethodNotAllowed, "", "rum_method_not_allowed", "use POST", "method_not_allowed"},
	refusalPreflight:         {http.StatusForbidden, "", "rum_origin_not_allowed", "origin is not allowed", "preflight_refused"},
	refusalNoPolicy:          {http.StatusServiceUnavailable, "5", "rum_policy_unavailable", "no ingest policy is held", "no_policy"},
	refusalKeyInvalid:        {http.StatusUnauthorized, "", "rum_application_key_invalid", "invalid rum application key", "key_invalid"},
	refusalKeyUnknown:        {http.StatusUnauthorized, "", "rum_application_key_invalid", "invalid rum application key", "unknown_key"},
	refusalKeyExpired:        {http.StatusUnauthorized, "", "rum_application_key_invalid", "invalid rum application key", "bad_key"},
	refusalOrigin:            {http.StatusForbidden, "", "rum_origin_not_allowed", "origin is not allowed for this application", "origin_not_allowed"},
	refusalRateLimited:       {http.StatusTooManyRequests, "1", "rum_rate_limited", "rate limit exceeded", "rate_limited"},
	refusalInvalid:           {http.StatusBadRequest, "", "rum_payload_invalid", "invalid otlp payload, content type or encoding", "invalid_payload"},
	refusalTooLarge:          {http.StatusRequestEntityTooLarge, "", "rum_payload_too_large", "request body is too large", "too_large"},
	refusalPackage:           {http.StatusForbidden, "", "rum_package_not_allowed", "app.package.name is not allowed for this application", "package_not_allowed"},
	refusalConsumerPermanent: {http.StatusBadRequest, "", "rum_payload_invalid", "the pipeline refused the payload", "consumer_permanent"},
	refusalConsumerRetryable: {http.StatusServiceUnavailable, "5", "rum_collector_unavailable", "the pipeline is busy", "consumer_retryable"},
}

var refusalBodies [numRefusals][]byte

func init() {
	type body struct {
		Status string `json:"status"`
		Error  struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	for i := refusalBusy; i < numRefusals; i++ {
		var b body
		b.Status, b.Error.Code, b.Error.Message = "error", refusalSpecs[i].code, refusalSpecs[i].message
		refusalBodies[i], _ = json.Marshal(b)
	}
}

func (h *handler) refuse(w http.ResponseWriter, r *http.Request, rf refusal) {
	spec := &refusalSpecs[rf]
	header := w.Header()
	header.Set("Content-Type", "application/json")
	if spec.retryAfter != "" {
		header.Set("Retry-After", spec.retryAfter)
	}
	w.WriteHeader(spec.status)
	_, _ = w.Write(refusalBodies[rf])
	if h.refused != nil {
		h.refused(r.Context(), rf)
	}
}
