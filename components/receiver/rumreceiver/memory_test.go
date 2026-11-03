// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package rumreceiver

import (
	"bytes"
	"net/http"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

// emptyGroupsJSON is n empty resource groups as OTLP JSON.
func emptyGroupsJSON(n int) []byte {
	return []byte(`{"resourceSpans":[` + strings.TrimSuffix(strings.Repeat(`{"resource":{},"scopeSpans":[]},`, n), ",") + `]}`)
}

// emptyGroupsProto is n empty resource groups as OTLP protobuf: two bytes each.
func emptyGroupsProto(n int) []byte {
	var b []byte
	for range n {
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendVarint(b, 0)
	}
	return b
}

// allocated is the bytes allocated while serving one request.
func allocated(t *testing.T, f *fixture, contentType string, body []byte) (int, uint64) {
	t.Helper()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	req := httptestRequest(tracesPath, "", jsonHeaders(webKey, "Content-Type", contentType))
	req.Body = readCloser{bytes.NewReader(body)}
	req.ContentLength = int64(len(body))
	rec := serve(f, req)
	runtime.ReadMemStats(&after)
	return rec.Code, after.TotalAlloc - before.TotalAlloc
}

type readCloser struct{ *bytes.Reader }

func (readCloser) Close() error { return nil }

// TestEmptyResourceGroupsMemory is the maintainer's case on #43: 200 000
// empty resource groups. Refused before pdata decoding, a request costs a
// small multiple of its body, whatever its structure.
func TestEmptyResourceGroupsMemory(t *testing.T) {
	testCases := []struct {
		name        string
		contentType string
		body        []byte
	}{
		{name: "JSON", contentType: "application/json", body: emptyGroupsJSON(200_000)},
		{name: "Protobuf", contentType: "application/x-protobuf", body: emptyGroupsProto(200_000)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, func(cfg *Config) { cfg.MaxBodySize = 16 << 20 })
			code, bytes := allocated(t, f, tc.contentType, tc.body)
			t.Logf("%s: body %d bytes, status %d, allocated %.1f MiB", tc.name, len(tc.body), code, float64(bytes)/(1<<20))
			require.Equal(t, http.StatusRequestEntityTooLarge, code)
			// The body buffer and a little more: no pdata was built.
			assert.Less(t, bytes, uint64(2*len(tc.body)+(1<<20)))
		})
	}
}
