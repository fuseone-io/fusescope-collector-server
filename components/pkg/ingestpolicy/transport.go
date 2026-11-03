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
}

// ReporterProvider is implemented by the component that owns the Reporter.
type ReporterProvider interface {
	IngestReporter() Reporter
}

// ParseReportAck decodes a reportAck strictly, {"schemaVersion":1,
// "reportSequence":N} and nothing else, at most MaxReportAckLen bytes.
func ParseReportAck(data []byte) (uint64, error) {
	if len(data) > MaxReportAckLen {
		return 0, fmt.Errorf("reportAck: %d bytes, over %d", len(data), MaxReportAckLen)
	}
	var ack struct {
		SchemaVersion  *int    `json:"schemaVersion"`
		ReportSequence *uint64 `json:"reportSequence"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ack); err != nil {
		return 0, fmt.Errorf("reportAck: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return 0, errors.New("reportAck: trailing data")
	}
	if ack.SchemaVersion == nil || *ack.SchemaVersion != SchemaVersion || ack.ReportSequence == nil || *ack.ReportSequence == 0 {
		return 0, errors.New("reportAck: schemaVersion 1 and a reportSequence above 0 are required")
	}
	return *ack.ReportSequence, nil
}
