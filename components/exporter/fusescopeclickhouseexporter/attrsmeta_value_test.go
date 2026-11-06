// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

func valueOf(raw any) pcommon.Value {
	v := pcommon.NewValueEmpty()
	if err := v.FromRaw(raw); err != nil {
		panic(err)
	}
	return v
}

// Within its budget the bounded conversion gives exactly AsString
// (attributes-metadata §2.1). Invalid UTF-8 inside a map or list is left to
// TestBoundedStringCanonical: encoding/json changed its replacement after
// Go 1.26, and the canonical form must not follow the toolchain.
func TestBoundedStringMatchesAsString(t *testing.T) {
	values := []pcommon.Value{
		pcommon.NewValueEmpty(),
		pcommon.NewValueStr("plain"),
		pcommon.NewValueStr(`<a href="x">&</a>` + "\u2028\xff\t"),
		pcommon.NewValueInt(-42),
		pcommon.NewValueBool(true),
		pcommon.NewValueDouble(0.1),
		pcommon.NewValueDouble(1e21),
		pcommon.NewValueDouble(1e-7),
		pcommon.NewValueDouble(-0.0),
		pcommon.NewValueDouble(123456789.25),
		pcommon.NewValueDouble(math.NaN()),
		pcommon.NewValueDouble(math.Inf(1)),
		pcommon.NewValueDouble(math.Inf(-1)),
		valueOf([]byte{0, 1, 2, 250}),
		valueOf(map[string]any{"b": 1, "a": "x<y&z", "c": []any{1.5, true, nil}, "d": map[string]any{"z": []byte("hi")}}),
		valueOf([]any{map[string]any{}, []any{}, 1e-9, int64(math.MaxInt64), "\u2029"}),
		valueOf(map[string]any{"bad": math.NaN()}),
		valueOf([]any{1, math.Inf(1)}),
		valueOf(map[string]any{"k\"\n": "v\\"}),
	}
	for i, v := range values {
		var b boundedString
		if !b.append(v, 1<<20) {
			t.Errorf("%d (%s): refused within budget", i, v.Type())
			continue
		}
		if got, want := string(b.buf), v.AsString(); got != want {
			t.Errorf("%d (%s):\n got %q\nwant %q", i, v.Type(), got, want)
		}
	}
}

// The canonical form, pinned whatever the toolchain: encoding/json's with
// SetEscapeHTML(false) as of Go 1.26 (attributes-metadata §2.1).
func TestBoundedStringCanonical(t *testing.T) {
	tests := []struct {
		v    pcommon.Value
		want string
	}{
		{pcommon.NewValueStr("raw \xff <&>"), "raw \xff <&>"},
		{valueOf(map[string]any{"c": []any{"\xff", "a\xffb"}}), `{"c":["\ufffd","a\ufffdb"]}`},
		{valueOf([]any{"\xe2\x80\xa8", "\xe2\x80\xa9"}), `["\u2028","\u2029"]`},
		{valueOf(map[string]any{"a": "x<y&z", "b": "q\"\\\n\x01"}), `{"a":"x<y&z","b":"q\"\\\n\u0001"}`},
		{valueOf(map[string]any{"z": []byte("hi"), "f": 1e21, "g": 1e-7, "h": 0.5}), `{"f":1e+21,"g":1e-7,"h":0.5,"z":"aGk="}`},
		{valueOf(map[string]any{"bad": math.NaN()}), ""},
		{valueOf([]any{nil, map[string]any{}, []any{}}), `[null,{},[]]`},
	}
	for i, tt := range tests {
		var b boundedString
		if !b.append(tt.v, 1<<20) {
			t.Errorf("%d: refused", i)
			continue
		}
		if got := string(b.buf); got != tt.want {
			t.Errorf("%d:\n got %q\nwant %q", i, got, tt.want)
		}
	}
}

// The budget bounds the work: an escape-heavy string stops once its encoded
// form passes the budget, and a map's keys stop being collected once they
// alone cannot fit, before any sort.
func TestBoundedStringStopsAtTheBudget(t *testing.T) {
	const budget = 16 << 10
	escapes := valueOf(map[string]any{"k": strings.Repeat("\x01", budget/2)})
	var b boundedString
	if b.append(escapes, budget) {
		t.Fatal("accepted an escaped string six times its raw size over the budget")
	}
	if len(b.buf) > budget+8 {
		t.Errorf("buffer grew to %d bytes for a budget of %d", len(b.buf), budget)
	}
	keys := pcommon.NewValueMap()
	for i := range 10_000 {
		keys.Map().PutStr(strings.Repeat("k", 100)+strconv.Itoa(i), "")
	}
	b = boundedString{}
	if b.append(keys, budget) {
		t.Fatal("accepted keys over the budget")
	}
	if n := cap(b.keys[0]); n > budget/100 {
		t.Errorf("collected %d keys for a budget of %d bytes", n, budget)
	}
}

func TestBoundedStringRefuses(t *testing.T) {
	deep := map[string]any{"leaf": 1}
	for range maxValueDepth {
		deep = map[string]any{"d": deep}
	}
	deepList := []any{1}
	for range maxValueDepth {
		deepList = []any{deepList}
	}
	bigMap := pcommon.NewValueMap()
	for i := range 100_000 {
		bigMap.Map().PutStr(strconv.Itoa(i), "v")
	}
	bigList := pcommon.NewValueSlice()
	for range 100_000 {
		bigList.Slice().AppendEmpty().SetInt(1)
	}
	tests := []struct {
		name   string
		v      pcommon.Value
		budget int
	}{
		{"long string", pcommon.NewValueStr(strings.Repeat("x", 101)), 100},
		{"long bytes", valueOf(make([]byte, 100)), 100},
		{"deep map", valueOf(deep), 1 << 20},
		{"deep list", valueOf(deepList), 1 << 20},
		{"huge map", bigMap, 16 << 10},
		{"huge list", bigList, 16 << 10},
		{"map that grows past the budget", valueOf(map[string]any{"a": strings.Repeat("x", 50), "b": strings.Repeat("y", 50)}), 80},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b boundedString
			if b.append(tt.v, tt.budget) {
				t.Fatalf("accepted %d bytes over a budget of %d", len(b.buf), tt.budget)
			}
			// The refusal is cheap: no materialization of the whole value.
			allocs := testing.AllocsPerRun(10, func() {
				b.buf = b.buf[:0]
				b.append(tt.v, tt.budget)
			})
			if allocs > 1 {
				t.Errorf("allocations per refusal = %v", allocs)
			}
		})
	}
	// Exactly at the depth limit is accepted.
	ok := map[string]any{"leaf": 1}
	for range maxValueDepth - 1 {
		ok = map[string]any{"d": ok}
	}
	var b boundedString
	if !b.append(valueOf(ok), 1<<20) {
		t.Error("refused a map at the depth limit")
	}
}
