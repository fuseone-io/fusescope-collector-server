// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package rumreceiver

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/protobuf/encoding/protowire"
)

// Builders of OTLP traces and logs in both encodings, for the structural
// budget. A shape is described once and rendered as JSON and protobuf.
type shape struct {
	resources, scopes, items, events, links  int
	resourceAttrs, scopeAttrs, itemAttrs     int
	eventAttrs, linkAttrs, arrayLen, nesting int
	logs, deprecated, snake                  bool
	// schemaURL pads the first resource group with a long string field.
	schemaURL int
}

func (s shape) json() string {
	name := func(camel, snake string) string {
		if s.snake {
			return snake
		}
		return camel
	}
	attrs := func(n int) string {
		var b strings.Builder
		for i := range n {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `{"key":"k%d","value":{"stringValue":"v"}}`, i)
		}
		return b.String()
	}
	value := `{"intValue":"1"}`
	if s.arrayLen > 0 {
		value = `{"` + name("arrayValue", "array_value") + `":{"values":[` + strings.TrimSuffix(strings.Repeat(`{"intValue":"1"},`, s.arrayLen), ",") + `]}}`
	}
	for range s.nesting {
		value = `{"` + name("kvlistValue", "kvlist_value") + `":{"values":[{"key":"n","value":` + value + `}]}}`
	}
	itemAttrs := attrs(s.itemAttrs)
	if s.arrayLen > 0 || s.nesting > 0 {
		itemAttrs = `{"key":"deep","value":` + value + `}`
	}
	var item string
	if s.logs {
		item = `{"eventName":"x","attributes":[` + itemAttrs + `]}`
	} else {
		events := strings.TrimSuffix(strings.Repeat(`{"name":"e","attributes":[`+attrs(s.eventAttrs)+`]},`, s.events), ",")
		links := strings.TrimSuffix(strings.Repeat(`{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174","attributes":[`+attrs(s.linkAttrs)+`]},`, s.links), ",")
		item = `{"name":"s","attributes":[` + itemAttrs + `],"events":[` + events + `],"links":[` + links + `]}`
	}
	items := strings.TrimSuffix(strings.Repeat(item+",", s.items), ",")
	scopeList, itemList, resList := name("scopeSpans", "scope_spans"), "spans", name("resourceSpans", "resource_spans")
	if s.logs {
		scopeList, itemList, resList = name("scopeLogs", "scope_logs"), name("logRecords", "log_records"), name("resourceLogs", "resource_logs")
	}
	if s.deprecated {
		scopeList = name("deprecatedScopeSpans", "deprecated_scope_spans")
		if s.logs {
			scopeList = name("deprecatedScopeLogs", "deprecated_scope_logs")
		}
	}
	scope := `{"scope":{"name":"x","attributes":[` + attrs(s.scopeAttrs) + `]},"` + itemList + `":[` + items + `]}`
	scopes := strings.TrimSuffix(strings.Repeat(scope+",", s.scopes), ",")
	resource := `{"resource":{"attributes":[` + attrs(s.resourceAttrs) + `]},"` + scopeList + `":[` + scopes + `]}`
	groups := strings.TrimSuffix(strings.Repeat(resource+",", s.resources), ",")
	if s.schemaURL > 0 {
		groups = `{"schemaUrl":"` + strings.Repeat("u", s.schemaURL) + `",` + groups[1:]
	}
	return `{"` + resList + `":[` + groups + `]}`
}

func msgField(b []byte, num protowire.Number, inner []byte) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, inner)
}

func (s shape) proto() []byte {
	attrs := func(num protowire.Number, n int) []byte {
		var b []byte
		for i := range n {
			kv := msgField(nil, 1, []byte(fmt.Sprintf("k%d", i)))
			kv = msgField(kv, 2, msgField(nil, 1, []byte("v")))
			b = msgField(b, num, kv)
		}
		return b
	}
	value := protowire.AppendVarint(protowire.AppendTag(nil, 3, protowire.VarintType), 1)
	if s.arrayLen > 0 {
		var arr []byte
		for range s.arrayLen {
			arr = msgField(arr, 1, value)
		}
		value = msgField(nil, 5, arr)
	}
	for range s.nesting {
		kv := msgField(msgField(nil, 1, []byte("n")), 2, value)
		value = msgField(nil, 6, msgField(nil, 1, kv))
	}
	var item []byte
	itemAttrNum, itemNum, scopeNum, resNum := protowire.Number(9), protowire.Number(2), protowire.Number(2), protowire.Number(1)
	if s.logs {
		itemAttrNum = 6
	}
	if s.deprecated {
		scopeNum = 1000
	}
	if s.arrayLen > 0 || s.nesting > 0 {
		item = msgField(nil, itemAttrNum, msgField(msgField(nil, 1, []byte("deep")), 2, value))
	} else {
		item = attrs(itemAttrNum, s.itemAttrs)
	}
	if !s.logs {
		for range s.events {
			item = msgField(item, 11, attrs(3, s.eventAttrs))
		}
		for range s.links {
			item = msgField(item, 13, attrs(4, s.linkAttrs))
		}
	}
	scope := msgField(nil, 1, attrs(3, s.scopeAttrs))
	for range s.items {
		scope = msgField(scope, itemNum, item)
	}
	resource := msgField(nil, 1, attrs(1, s.resourceAttrs))
	for range s.scopes {
		resource = msgField(resource, scopeNum, scope)
	}
	var req []byte
	for i := range s.resources {
		if i == 0 && s.schemaURL > 0 {
			req = msgField(req, resNum, msgField(append([]byte{}, resource...), 3, []byte(strings.Repeat("u", s.schemaURL))))
			continue
		}
		req = msgField(req, resNum, resource)
	}
	return req
}

func smallLimits() structureLimits {
	return structureLimits{resources: 3, scopesPerResource: 2, items: 4, eventsPerSpan: 2, linksPerSpan: 2, attributes: 3, elements: 60, valueDepth: 3}
}

// TestStructureBudget checks every limit at and just over it, in JSON and
// protobuf, traces and logs (maintainer review of #43). Accepted shapes must
// also decode with pdata, so the scan follows what pdata allocates.
func TestStructureBudget(t *testing.T) {
	testCases := []struct {
		name string
		s    shape
		over bool
	}{
		{name: "EmptyResourcesAtLimit", s: shape{resources: 3}},
		{name: "EmptyResourcesOver", s: shape{resources: 4}, over: true},
		{name: "EmptyScopesAtLimit", s: shape{resources: 1, scopes: 2}},
		{name: "EmptyScopesOver", s: shape{resources: 1, scopes: 3}, over: true},
		{name: "DeprecatedScopesOver", s: shape{resources: 1, scopes: 3, deprecated: true}, over: true},
		{name: "EmptyItemsAtLimit", s: shape{resources: 2, scopes: 2, items: 1}},
		{name: "EmptyItemsOver", s: shape{resources: 1, scopes: 1, items: 5}, over: true},
		{name: "ItemsAcrossResourcesOver", s: shape{resources: 3, scopes: 1, items: 2}, over: true},
		{name: "EventsAtLimit", s: shape{resources: 1, scopes: 1, items: 1, events: 2}},
		{name: "EventsOver", s: shape{resources: 1, scopes: 1, items: 1, events: 3}, over: true},
		{name: "LinksAtLimit", s: shape{resources: 1, scopes: 1, items: 1, links: 2}},
		{name: "LinksOver", s: shape{resources: 1, scopes: 1, items: 1, links: 3}, over: true},
		{name: "ResourceAttrsAtLimit", s: shape{resources: 1, resourceAttrs: 3}},
		{name: "ResourceAttrsOver", s: shape{resources: 1, resourceAttrs: 4}, over: true},
		{name: "ScopeAttrsOver", s: shape{resources: 1, scopes: 1, scopeAttrs: 4}, over: true},
		{name: "ItemAttrsAtLimit", s: shape{resources: 1, scopes: 1, items: 1, itemAttrs: 3}},
		{name: "ItemAttrsOver", s: shape{resources: 1, scopes: 1, items: 1, itemAttrs: 4}, over: true},
		{name: "EventAttrsOver", s: shape{resources: 1, scopes: 1, items: 1, events: 1, eventAttrs: 4}, over: true},
		{name: "LinkAttrsOver", s: shape{resources: 1, scopes: 1, items: 1, links: 1, linkAttrs: 4}, over: true},
		{name: "ArrayAtLimit", s: shape{resources: 1, scopes: 1, items: 1, arrayLen: 3}},
		{name: "ArrayOver", s: shape{resources: 1, scopes: 1, items: 1, arrayLen: 4}, over: true},
		{name: "NestingAtLimit", s: shape{resources: 1, scopes: 1, items: 1, nesting: 3}},
		{name: "NestingOver", s: shape{resources: 1, scopes: 1, items: 1, nesting: 4}, over: true},
		{name: "ElementsOver", s: shape{resources: 3, scopes: 2, items: 1, itemAttrs: 3, resourceAttrs: 3, scopeAttrs: 3}, over: true},
	}
	for _, tc := range testCases {
		for _, logs := range []bool{false, true} {
			s := tc.s
			s.logs = logs
			if logs && (s.events > 0 || s.links > 0 || s.eventAttrs > 0 || s.linkAttrs > 0) {
				continue
			}
			for _, snake := range []bool{false, true} {
				s.snake = snake
				root := msgTracesRequest
				if logs {
					root = msgLogsRequest
				}
				t.Run(fmt.Sprintf("%s/logs=%v/snake=%v", tc.name, logs, snake), func(t *testing.T) {
					want := structureOK
					if tc.over {
						want = structureOverBudget
					}
					js, pb := s.json(), s.proto()
					assert.Equal(t, want, scanJSON([]byte(js), root, smallLimits()), "JSON")
					assert.Equal(t, want, scanProto(pb, root, smallLimits()), "protobuf")
					if !snake {
						// The shapes are real OTLP: pdata decodes them.
						if logs {
							_, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs([]byte(js))
							require.NoError(t, err)
							_, err = (&plog.ProtoUnmarshaler{}).UnmarshalLogs(pb)
							require.NoError(t, err)
						} else {
							_, err := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces([]byte(js))
							require.NoError(t, err)
							_, err = (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(pb)
							require.NoError(t, err)
						}
					}
				})
			}
		}
	}
}

func TestStructureJSONEdgeCases(t *testing.T) {
	lim := smallLimits()
	testCases := []struct {
		name string
		body string
		want structure
	}{
		{name: "EscapedFieldName_Counted", body: `{"resourceSpans":[{},{},{},{}]}`, want: structureOverBudget},
		{name: "EscapedQuoteInValue_Skipped", body: `{"resourceSpans":[{"resource":{"attributes":[{"key":"a\"b","value":{"stringValue":"x\\yé"}}]}}]}`, want: structureOK},
		{name: "UnknownFieldsSkipped", body: `{"resourceSpans":[{"other":[1,2,{"x":[true,false,null,-1.5e3]}]}],"x":"y"}`, want: structureOK},
		{name: "LongKeyNotAField", body: `{"` + strings.Repeat("r", 100) + `":[{},{},{},{}]}`, want: structureOK},
		{name: "Whitespace", body: " {\n\t\"resourceSpans\" : [ { } , { } ] \r\n} ", want: structureOK},
		{name: "Empty", body: ``, want: structureMalformed},
		{name: "Truncated", body: `{"resourceSpans":[{}`, want: structureMalformed},
		{name: "TrailingData", body: `{} {}`, want: structureMalformed},
		{name: "BadEscape", body: `{"a\q":1}`, want: structureMalformed},
		{name: "ControlInString", body: "{\"a\x01\":1}", want: structureMalformed},
		{name: "MissingColon", body: `{"a" 1}`, want: structureMalformed},
		{name: "DeepUnknownNesting", body: `{"x":` + strings.Repeat("[", 100) + strings.Repeat("]", 100) + `}`, want: structureOverBudget},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, scanJSON([]byte(tc.body), msgTracesRequest, lim))
		})
	}
}

// Codex review on #43: a repeated single message field (resource, scope,
// value, body, array or map value) would be merged by the decoder and
// escape its budget in parts; it is refused in both encodings.
func TestStructureRepeatedSingularField(t *testing.T) {
	kv := func(i int) []byte {
		return msgField(msgField(nil, 1, []byte(fmt.Sprintf("k%d", i))), 2, msgField(nil, 1, []byte("v")))
	}
	resourcePart := func(from, to int) []byte {
		var b []byte
		for i := from; i < to; i++ {
			b = msgField(b, 1, kv(i))
		}
		return b
	}
	split := msgField(nil, 1, msgField(msgField(nil, 1, resourcePart(0, 100)), 1, resourcePart(100, 129)))
	assert.Equal(t, structureMalformed, scanProto(split, msgTracesRequest, maxStructure), "resource in two parts")
	whole := msgField(nil, 1, msgField(nil, 1, resourcePart(0, 128)))
	assert.Equal(t, structureOK, scanProto(whole, msgTracesRequest, maxStructure), "one resource at the limit")

	testCases := []string{
		`{"resourceSpans":[{"resource":{},"resource":{}}]}`,
		`{"resourceSpans":[{"scopeSpans":[{"scope":{},"scope":{}}]}]}`,
		`{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{},"body":{}}]}]}]}`,
		`{"resourceSpans":[{"resource":{"attributes":[{"key":"k","value":{},"value":{}}]}}]}`,
		`{"resourceSpans":[{"resource":{"attributes":[{"key":"k","value":{"arrayValue":{},"array_value":{}}}]}}]}`,
	}
	for _, body := range testCases {
		root := msgTracesRequest
		if strings.Contains(body, "resourceLogs") {
			root = msgLogsRequest
		}
		assert.Equal(t, structureMalformed, scanJSON([]byte(body), root, maxStructure), body)
	}
}

func TestStructureProtoMalformed(t *testing.T) {
	for _, body := range [][]byte{{0x0a}, {0x0a, 0x05, 0x01}, {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}} {
		assert.Equal(t, structureMalformed, scanProto(body, msgTracesRequest, maxStructure))
	}
	// Unknown fields and scalars are skipped, as pdata skips them.
	unknown := protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 7)
	assert.Equal(t, structureOK, scanProto(unknown, msgTracesRequest, maxStructure))
}

// The scan allocates nothing, whatever the body.
func TestStructureScanDoesNotAllocate(t *testing.T) {
	big := shape{resources: 3, scopes: 2, items: 2, itemAttrs: 3, events: 2, links: 2}
	js, pb := []byte(big.json()), big.proto()
	escaped := []byte(`{"resourceSpans":[{"resource":{"attributes":[{"key":"a\"b"}]}}]}`)
	assert.Zero(t, testing.AllocsPerRun(20, func() { scanJSON(js, msgTracesRequest, maxStructure) }))
	assert.Zero(t, testing.AllocsPerRun(20, func() { scanJSON(escaped, msgTracesRequest, maxStructure) }))
	assert.Zero(t, testing.AllocsPerRun(20, func() { scanProto(pb, msgTracesRequest, maxStructure) }))
}

// Through the handler with the default budget: over each limit the request
// is refused with 413 and costs little more than its body; at the limit it
// is accepted (maintainer review of #43).
func TestStructureRefusedBeforeDecoding(t *testing.T) {
	m := maxStructure
	testCases := []struct {
		name string
		s    shape
		code int
	}{
		{name: "ResourcesAtMax", s: shape{resources: m.resources}, code: http.StatusOK},
		{name: "ResourcesOverMax", s: shape{resources: m.resources + 1}, code: http.StatusRequestEntityTooLarge},
		{name: "EmptyScopesOverMax", s: shape{resources: 1, scopes: m.scopesPerResource + 1}, code: http.StatusRequestEntityTooLarge},
		{name: "EmptySpansAtMax", s: shape{resources: 1, scopes: 1, items: m.items}, code: http.StatusOK},
		{name: "EmptySpansOverMax", s: shape{resources: 1, scopes: 1, items: m.items + 1}, code: http.StatusRequestEntityTooLarge},
		{name: "EventsOverMax", s: shape{resources: 1, scopes: 1, items: 1, events: m.eventsPerSpan + 1}, code: http.StatusRequestEntityTooLarge},
		{name: "AttributesOverMax", s: shape{resources: 1, scopes: 1, items: 1, itemAttrs: m.attributes + 1}, code: http.StatusRequestEntityTooLarge},
		{name: "ElementsOverMax", s: shape{resources: 1, scopes: 1, items: 1000, itemAttrs: 100}, code: http.StatusRequestEntityTooLarge},
	}
	for _, tc := range testCases {
		for _, enc := range []string{"application/json", "application/x-protobuf"} {
			t.Run(tc.name+"/"+enc, func(t *testing.T) {
				body := []byte(tc.s.json())
				if enc != "application/json" {
					body = tc.s.proto()
				}
				f := newFixture(t, func(cfg *Config) { cfg.MaxBodySize = 64 << 20 })
				code, bytes := allocated(t, f, enc, body)
				assert.Equal(t, tc.code, code)
				if code != http.StatusOK {
					// The body buffer (at most twice the body while it grows) and a little more: no pdata.
					assert.Less(t, bytes, uint64(2*len(body)+(256<<10)), "refused before pdata (%d bytes for a %d-byte body)", bytes, len(body))
				}
			})
		}
	}
}
