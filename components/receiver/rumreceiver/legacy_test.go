// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package rumreceiver

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

// Maintainer review of #43: OTLP's deprecated scope lists (field 1000,
// deprecatedScopeSpans / deprecatedScopeLogs) carry data that must be
// delivered, not dropped with a 200, in both encodings.
func TestLegacyScopeListsDelivered(t *testing.T) {
	str := func(num protowire.Number, v string) []byte { return msgField(nil, num, []byte(v)) }
	span := append(str(1, string(make([]byte, 16))), str(2, string(make([]byte, 8)))...)
	span = append(span, str(5, "legacy-span")...)

	testCases := []struct {
		name        string
		path        string
		contentType string
		body        []byte
	}{
		{
			name: "TracesProtobuf", path: tracesPath, contentType: "application/x-protobuf",
			body: msgField(nil, 1, msgField(nil, 1000, msgField(nil, 2, span))),
		},
		{
			name: "LogsProtobuf", path: logsPath, contentType: "application/x-protobuf",
			body: msgField(nil, 1, msgField(nil, 1000, msgField(nil, 2, append(protowire.AppendFixed64(protowire.AppendTag(nil, 1, protowire.Fixed64Type), 1), str(12, "legacy-log")...)))),
		},
		{
			name: "TracesJSON", path: tracesPath, contentType: "application/json",
			body: []byte(`{"resourceSpans":[{"resource":{},"deprecatedScopeSpans":[{"spans":[{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174","name":"legacy-span"}]}]}]}`),
		},
		{
			name: "LogsJSON", path: logsPath, contentType: "application/json",
			body: []byte(`{"resourceLogs":[{"resource":{},"deprecatedScopeLogs":[{"logRecords":[{"timeUnixNano":"1","eventName":"legacy-log"}]}]}]}`),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, nil)
			req := httptestRequest(tc.path, string(tc.body), jsonHeaders(webKey, "Content-Type", tc.contentType))
			rec := serve(f, req)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			if tc.path == tracesPath {
				require.Equal(t, 1, f.traces.SpanCount(), "the legacy span is delivered")
				span := f.traces.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
				assert.Equal(t, "legacy-span", span.Name())
				return
			}
			require.Equal(t, 1, f.logs.LogRecordCount(), "the legacy record is delivered")
			record := f.logs.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
			assert.Equal(t, "legacy-log", record.EventName())
		})
	}
}
