// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package ingestpolicy

import (
	"bytes"
	"encoding/json"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mutate edits the generic form of the test policy.
func mutate(t *testing.T, edit func(m map[string]any)) []byte {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(encode(t, testPolicy(1)), &m))
	edit(m)
	b, err := json.Marshal(m)
	require.NoError(t, err)
	return b
}

func firstKey(m map[string]any) map[string]any { return m["keys"].([]any)[0].(map[string]any) }
func firstApp(m map[string]any) map[string]any {
	return m["rumApplications"].([]any)[0].(map[string]any)
}

func TestParseCodes(t *testing.T) {
	valid := encode(t, testPolicy(1))
	testCases := []struct {
		name string
		data []byte
		code Code
		path string
	}{
		{name: "Valid_Accepted", data: valid},
		{name: "OverMaxSize_TooLarge", data: append(bytes.Repeat([]byte(" "), MaxSize), valid...), code: CodeTooLarge},
		{name: "Truncated_ParseError", data: valid[:len(valid)-1], code: CodeParseError},
		{name: "TrailingValue_ParseError", data: append(append([]byte{}, valid...), []byte(` {}`)...), code: CodeParseError},
		{name: "TopLevelArray_ParseError", data: []byte(`[1]`), code: CodeParseError},
		{name: "TopLevelString_ParseError", data: []byte(`"policy"`), code: CodeParseError},
		{name: "Empty_ParseError", data: []byte(``), code: CodeParseError},
		{name: "SchemaVersionMissing_SchemaVersion", data: mutate(t, func(m map[string]any) { delete(m, "schemaVersion") }), code: CodeSchemaVersion},
		{name: "SchemaVersionTwo_SchemaVersion", data: mutate(t, func(m map[string]any) { m["schemaVersion"] = 2 }), code: CodeSchemaVersion},
		{name: "SchemaVersionString_SchemaVersion", data: mutate(t, func(m map[string]any) { m["schemaVersion"] = "1" }), code: CodeSchemaVersion},
		{name: "SchemaVersionFloat_SchemaVersion", data: []byte(strings.Replace(string(valid), `"schemaVersion":1`, `"schemaVersion":1.0`, 1)), code: CodeSchemaVersion},
		{name: "SchemaVersionObject_SchemaVersion", data: mutate(t, func(m map[string]any) { m["schemaVersion"] = map[string]any{"v": 1} }), code: CodeSchemaVersion},
		{
			name: "NewerSchemaWithNewFields_SchemaVersion",
			data: mutate(t, func(m map[string]any) { m["schemaVersion"] = 2; m["revocations"] = []any{}; m["keys"] = "changed" }),
			code: CodeSchemaVersion,
		},
		{name: "DuplicateSchemaVersion_ParseError", data: []byte(strings.Replace(string(valid), `"schemaVersion":1`, `"schemaVersion":1,"schemaVersion":1`, 1)), code: CodeParseError},
		{name: "UnknownTopField_UnknownField", data: mutate(t, func(m map[string]any) { m["revocations"] = []any{} }), code: CodeUnknownField, path: "revocations"},
		{name: "UnknownKeyField_UnknownField", data: mutate(t, func(m map[string]any) { firstKey(m)["plaintext"] = "x" }), code: CodeUnknownField, path: "keys[0].plaintext"},
		{name: "UnknownLimitField_UnknownField", data: mutate(t, func(m map[string]any) { firstKey(m)["limit"].(map[string]any)["window"] = 1 }), code: CodeUnknownField, path: "keys[0].limit.window"},
		{name: "GeoLookupNotInV1_UnknownField", data: mutate(t, func(m map[string]any) { firstApp(m)["piiPolicy"].(map[string]any)["geoLookup"] = true }), code: CodeUnknownField, path: "rumApplications[0].piiPolicy.geoLookup"},
		{name: "CaseVariantField_UnknownField", data: []byte(strings.Replace(string(valid), `"version":1`, `"Version":1,"version":1`, 1)), code: CodeUnknownField, path: "Version"},
		{name: "DuplicateField_ParseError", data: []byte(strings.Replace(string(valid), `"version":1`, `"version":1,"version":9`, 1)), code: CodeParseError},
		{name: "VersionMissing_ParseError", data: mutate(t, func(m map[string]any) { delete(m, "version") }), code: CodeParseError},
		{name: "KeysNull_ParseError", data: mutate(t, func(m map[string]any) { m["keys"] = nil }), code: CodeParseError},
		{name: "KeyIDNumber_ParseError", data: mutate(t, func(m map[string]any) { firstKey(m)["id"] = 7 }), code: CodeParseError},
		{name: "SignalsString_ParseError", data: mutate(t, func(m map[string]any) { firstKey(m)["signals"] = "traces" }), code: CodeParseError},
		{name: "PIIPolicyMissingField_ParseError", data: mutate(t, func(m map[string]any) { delete(firstApp(m)["piiPolicy"].(map[string]any), "dropQuery") }), code: CodeParseError},
		{name: "NegativeVersion_ParseError", data: mutate(t, func(m map[string]any) { m["version"] = -1 }), code: CodeParseError},
		{name: "FractionalBurst_ParseError", data: mutate(t, func(m map[string]any) { firstKey(m)["limit"].(map[string]any)["burst"] = 1.5 }), code: CodeParseError},
		{name: "ExpiresAtNull_Accepted", data: mutate(t, func(m map[string]any) { firstKey(m)["expiresAt"] = nil })},
		{name: "ApplicationIDNull_ParseError", data: mutate(t, func(m map[string]any) { firstKey(m)["applicationId"] = nil }), code: CodeParseError},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := Parse(tc.data)
			assert.Equal(t, tc.code, codeOf(t, err), "%v", err)
			if tc.code == "" {
				require.NotNil(t, p)
			}
			if tc.path != "" {
				assert.Equal(t, tc.path, err.(*RejectionError).Path)
			}
		})
	}
}

// Codex review on #43: nesting is bounded before descending, even inside an
// unknown field, so a small policy cannot exhaust the stack.
func TestParseDeepNestingRejected(t *testing.T) {
	const levels = 2_000_000
	deep := `{"schemaVersion":1,"x":` + strings.Repeat("[", levels) + strings.Repeat("]", levels) + `}`
	require.Less(t, len(deep), MaxSize)
	_, err := Parse([]byte(deep))
	assert.Equal(t, CodeParseError, codeOf(t, err))

	valid := encode(t, testPolicy(1))
	nested := strings.Replace(string(valid), `"keys":[`, `"x":`+strings.Repeat(`{"a":`, maxDepth)+`1`+strings.Repeat(`}`, maxDepth)+`,"keys":[`, 1)
	_, err = Parse([]byte(nested))
	assert.Equal(t, CodeParseError, codeOf(t, err), "too deep, though only inside an unknown field")
}

// Codex review on #43: list bounds are enforced while walking, so an
// oversized list never reaches the typed decoder.
func TestParseListBoundsBeforeDecoding(t *testing.T) {
	valid := string(encode(t, testPolicy(1)))
	many := func(n int, item string) string { return strings.TrimSuffix(strings.Repeat(item+",", n), ",") }
	testCases := []struct {
		name, body, path string
	}{
		{name: "QueryAllowlist", body: strings.Replace(valid, `"queryAllowlist":["page"]`, `"queryAllowlist":[`+many(2_000_000, `""`)+`]`, 1), path: "rumApplications[0].piiPolicy.queryAllowlist"},
		{name: "Origins", body: strings.Replace(valid, `"allowedOrigins":["https://shop.example.com"`, `"allowedOrigins":[`+many(51, `"https://a.com"`)+`,"https://shop.example.com"`, 1), path: "rumApplications[0].allowedOrigins"},
		{name: "Signals", body: strings.Replace(valid, `"signals":["traces","logs"]`, `"signals":[`+many(6, `"logs"`)+`]`, 1), path: "keys[0].signals"},
		{name: "Keys", body: strings.Replace(valid, `"keys":[`, `"keys":[`+many(MaxKeys, `{}`)+`,`, 1), path: "keys"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Less(t, len(tc.body), MaxSize)
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			_, err := Parse([]byte(tc.body))
			runtime.ReadMemStats(&after)
			require.Equal(t, CodeInvalidValue, codeOf(t, err), "%v", err)
			assert.Equal(t, tc.path, err.(*RejectionError).Path)
			assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(len(tc.body))+(8<<20), "stopped before decoding the list")
		})
	}
	_, err := Parse([]byte(strings.Replace(valid, `"queryAllowlist":["page"]`, `"queryAllowlist":[`+many(50, `"q"`)+`]`, 1)))
	assert.NoError(t, err, "50 entries are allowed")
}

func TestParseAtMaxSizeAccepted(t *testing.T) {
	valid := encode(t, testPolicy(1))
	data := append(valid, bytes.Repeat([]byte(" "), MaxSize-len(valid))...)
	require.Len(t, data, MaxSize)
	_, err := Parse(data)
	require.NoError(t, err)
}

func TestRejectionErrorHasNoContent(t *testing.T) {
	data := mutate(t, func(m map[string]any) { firstKey(m)["plaintext"] = webKey })
	_, err := Parse(data)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), webKey)
	assert.NotContains(t, err.Error(), verifier(webKey))
}
