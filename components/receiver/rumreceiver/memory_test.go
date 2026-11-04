// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package rumreceiver

import (
	"bytes"
	"math"
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

// allocatedRuns is how many times allocated serves the request. TotalAlloc
// counts the whole process, so a run can include another goroutine's
// allocations; they only add, so the smallest run is the request's (#52).
const allocatedRuns = 3

// allocated is the bytes allocated while serving one request.
func allocated(t *testing.T, f *fixture, contentType string, body []byte) (int, uint64) {
	t.Helper()
	code, least := 0, uint64(math.MaxUint64)
	for run := range allocatedRuns {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		req := httptestRequest(tracesPath, "", jsonHeaders(webKey, "Content-Type", contentType))
		req.Body = readCloser{bytes.NewReader(body)}
		req.ContentLength = int64(len(body))
		rec := serve(f, req)
		runtime.ReadMemStats(&after)
		if run > 0 {
			require.Equal(t, code, rec.Code, "the same request, run %d", run+1)
		}
		code, least = rec.Code, min(least, after.TotalAlloc-before.TotalAlloc)
	}
	return code, least
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

// Maintainer review of #43: the worst a request can allocate at the
// structural ceiling (elements_per_request 25 000), whatever the kind of
// element, and with one large string filling the body. Together with
// limits.requests this bounds the memory in flight (rum.md §7.1).
func TestWorstCaseAllocationAtCeiling(t *testing.T) {
	m := maxStructure
	shapes := map[string]shape{
		"Attributes":    {resources: 1, scopes: 1, items: 193, itemAttrs: m.attributes},
		"Events":        {resources: 1, scopes: 1, items: 193, events: m.eventsPerSpan},
		"Links":         {resources: 1, scopes: 1, items: 193, links: m.linksPerSpan},
		"EmptySpans":    {resources: 1, scopes: 1, items: m.items},
		"ResourceAttrs": {resources: m.resources, resourceAttrs: 23},
		"EmptyScopes":   {resources: 240, scopes: m.scopesPerResource},
		"ArrayValues":   {resources: 1, scopes: 1, items: 1, arrayLen: m.attributes},
	}
	for name, s := range shapes {
		for _, enc := range []string{"application/json", "application/x-protobuf"} {
			t.Run(name+"/"+enc, func(t *testing.T) {
				body := []byte(s.json())
				if enc != "application/json" {
					body = s.proto()
				}
				f := newFixture(t, func(cfg *Config) { cfg.MaxBodySize = 64 << 20 })
				code, bytes := allocated(t, f, enc, body)
				t.Logf("%s %s: body %d bytes, status %d, allocated %.1f MiB", name, enc, len(body), code, float64(bytes)/(1<<20))
				require.Equal(t, http.StatusOK, code, "within the ceiling")
				limit := uint64(worstCaseElements)
				if raceEnabled {
					limit += 2 * uint64(len(body))
				}
				assert.Less(t, bytes, limit, "allocation at the ceiling")
			})
		}
	}
	t.Run("OneLargeString", func(t *testing.T) {
		value := strings.Repeat("v", 4<<20-200)
		body := []byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"name":"s","attributes":[{"key":"k","value":{"stringValue":"` + value + `"}}]}]}]}]}`)
		f := newFixture(t, nil)
		code, bytes := allocated(t, f, "application/json", body)
		t.Logf("one large string: body %d bytes, status %d, allocated %.1f MiB", len(body), code, float64(bytes)/(1<<20))
		require.Equal(t, http.StatusOK, code)
		limit := uint64(worstCaseBody)
		if raceEnabled {
			limit += 2 * uint64(len(body))
		}
		assert.Less(t, bytes, limit, "the body and its decoded copy")
	})
}

// Maintainer review of #43: the structural ceiling and a full body at
// once, the most a valid request allocates: 999 resource groups of 23
// attributes (23 976 elements) and a schemaUrl padding the body to 4 MiB.
func TestWorstCaseAllocationCombined(t *testing.T) {
	for _, enc := range []string{"application/json", "application/x-protobuf"} {
		t.Run(enc, func(t *testing.T) {
			s := shape{resources: 999, resourceAttrs: 23}
			render := func(s shape) []byte {
				if enc == "application/json" {
					return []byte(s.json())
				}
				return s.proto()
			}
			s.schemaURL = 4<<20 - len(render(s)) - 16
			body := render(s)
			require.LessOrEqual(t, len(body), 4<<20)
			require.Greater(t, len(body), 4<<20-64)
			f := newFixture(t, nil)
			code, bytes := allocated(t, f, enc, body)
			t.Logf("combined %s: body %d bytes, status %d, allocated %.1f MiB", enc, len(body), code, float64(bytes)/(1<<20))
			require.Equal(t, http.StatusOK, code, "a valid request")
			limit := uint64(worstCaseCombined)
			if raceEnabled {
				limit += 2 * uint64(len(body))
			}
			assert.Less(t, bytes, limit)
		})
	}
}

// Thresholds the in-flight bound of rum.md §7.1 is built on.
const (
	worstCaseElements = 6 << 20  // a request at the structural ceiling: measured up to 5.0 MiB
	worstCaseBody     = 10 << 20 // a 4 MiB body, its buffer and decoded copy: measured 8.0 MiB
	worstCaseCombined = 12 << 20 // both at once: the per-request guardrail of §7.1
)
