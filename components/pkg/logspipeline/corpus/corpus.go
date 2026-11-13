// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package corpus publishes the canonical log pipelines corpus (ADR 0012,
// decision 5): the cases, their inputs and what is expected of them.
//
// Two kinds of data are kept apart:
//   - observed/ holds black-box observations of the earlier collector
//     (fusescope-otel-collector 0.145.0, ADR 0002): processors, outputs and
//     startup outcomes. They are kept exactly as observed;
//   - library/ holds what this module's library produces and nobody observed:
//     the messages it logs while processing records (messages.json).
//
// Every function decodes its file anew, so callers own what they get. Numbers
// are kept as json.Number, and processor configurations as raw JSON for the
// caller to decode.
package corpus // import "github.com/fuseone-io/fusescope-collector-server/components/pkg/logspipeline/corpus"

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
)

//go:embed observed/cases.json observed/outputs.json observed/startup.json observed/run.yaml library/messages.json
var files embed.FS

// Case is one observed case: processors as the backend deploys them, applied
// in ProcessorNames order to the records of the log set LogSet.
type Case struct {
	Name string `json:"name"`
	// Origin is "backend" for a case of the backend's corpus and "hand-written"
	// for one written for the observation.
	Origin         string                     `json:"origin"`
	LogSet         string                     `json:"logSet"`
	ProcessorNames []string                   `json:"processorNames"`
	Processors     map[string]json.RawMessage `json:"processors"`
}

// Cases is observed/cases.json.
type Cases struct {
	// Inputs holds each log set as OTLP/JSON documents.
	Inputs map[string][]json.RawMessage `json:"inputs"`
	// Cases are run on records.
	Cases []Case `json:"cases"`
	// Startup are only started.
	Startup []Case `json:"startup"`
}

// StartupResult is whether the reference started with a case's processors,
// and the first error it logged.
type StartupResult struct {
	Name   string `json:"name"`
	Origin string `json:"origin"`
	Starts bool   `json:"starts"`
	Error  string `json:"error"`
}

// Message is one message the library logs while processing a record, at warn
// level or above, after the logs of building the pipelines are dropped.
type Message struct {
	// Input is the record's position in its log set, counting the records of
	// every document, resource and scope in order, from 0.
	Input      int    `json:"input"`
	OperatorID string `json:"operator_id"`
	Error      string `json:"error"`
}

// Messages is library/messages.json.
type Messages struct {
	// Cases maps every case the library evaluated to its messages, ordered by
	// input and then as logged. An evaluated case without messages maps to an
	// empty list.
	Cases map[string][]Message `json:"cases"`
}

func decode(name string, v any) error {
	b, err := files.ReadFile(name)
	if err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return fmt.Errorf("corpus %s: %w", name, err)
	}
	return nil
}

// Observed returns observed/cases.json.
func Observed() (Cases, error) {
	var c Cases
	return c, decode("observed/cases.json", &c)
}

// ObservedOutputs returns, per case, the OTLP/JSON documents the reference
// wrote. Observed timestamps the reference took from its clock read
// "set-by-collector". A case the reference refused has no entry.
func ObservedOutputs() (map[string][]json.RawMessage, error) {
	var o map[string][]json.RawMessage
	return o, decode("observed/outputs.json", &o)
}

// ObservedStartup returns observed/startup.json.
func ObservedStartup() ([]StartupResult, error) {
	var r []StartupResult
	return r, decode("observed/startup.json", &r)
}

// LibraryMessages returns library/messages.json.
func LibraryMessages() (Messages, error) {
	var m Messages
	return m, decode("library/messages.json", &m)
}

// File returns a copy of one of the corpus's files, by its path in this
// package (for example "observed/run.yaml").
func File(name string) ([]byte, error) {
	return files.ReadFile(name)
}
