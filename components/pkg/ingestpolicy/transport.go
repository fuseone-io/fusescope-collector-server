// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package ingestpolicy // import "github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// The OpAMP custom capability and message types of the ingest policy and
// report (§10.2, §10.4, §11.1).
const (
	Capability      = "io.fuseone.fusescope.ingest.v1"
	TypePolicy      = "policy"
	TypeReport      = "report"
	TypeReportAck   = "reportAck"
	MaxReportAckLen = 1 << 10
)

// Reporter carries the ingest report to the backend (§10.2–10.4); the
// OpAMP extension implements it.
type Reporter interface {
	// NextSequence reserves the next reportSequence, persisted before it
	// returns, so a sequence is never reused across restarts.
	NextSequence() (uint64, error)
	// Send hands the pages of one report to the transport, in order. It
	// returns once every page is handed over, ctx ends or the transport
	// fails; a failure leaves the report's values pending.
	Send(ctx context.Context, pages [][]byte) error
	// Acks delivers the sequences the backend acknowledged.
	Acks() <-chan uint64
	// Connected is signalled after each (re)connection, when a report is
	// sent at once.
	Connected() <-chan struct{}
	// Release gives the transport back, at the receiver's shutdown.
	Release()
}

// ReporterProvider is implemented by the component that owns the Reporter.
// It hands the Reporter to one receiver at a time and returns an error to a
// second; nil, nil means it carries no report.
type ReporterProvider interface {
	IngestReporter() (Reporter, error)
}

// ParseReportAck decodes a reportAck strictly: one object with exactly
// the fields schemaVersion, equal to 1, and reportSequence, a uint64 above
// 0, each once and spelled exactly, at most MaxReportAckLen bytes.
// encoding/json alone would match names case-insensitively and take the
// last of duplicate fields.
func ParseReportAck(data []byte) (uint64, error) {
	if len(data) > MaxReportAckLen {
		return 0, fmt.Errorf("reportAck: %d bytes, over %d", len(data), MaxReportAckLen)
	}
	malformed := errors.New("reportAck: not {\"schemaVersion\":1,\"reportSequence\":N} with N above 0")
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return 0, malformed
	}
	var version, seq json.Number
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return 0, malformed
		}
		var field *json.Number
		switch tok {
		case "schemaVersion":
			field = &version
		case "reportSequence":
			field = &seq
		default:
			return 0, malformed
		}
		if *field != "" {
			return 0, malformed // a duplicate
		}
		tok, err = dec.Token()
		n, ok := tok.(json.Number)
		if err != nil || !ok {
			return 0, malformed
		}
		*field = n
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return 0, malformed
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return 0, malformed
	}
	if version != json.Number(strconv.Itoa(SchemaVersion)) {
		return 0, malformed
	}
	n, err := strconv.ParseUint(string(seq), 10, 64)
	if err != nil || n == 0 {
		return 0, malformed
	}
	return n, nil
}
