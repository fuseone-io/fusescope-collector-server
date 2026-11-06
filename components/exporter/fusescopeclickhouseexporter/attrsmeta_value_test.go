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
// (attributes-metadata §2.1).
func TestBoundedStringMatchesAsString(t *testing.T) {
	values := []pcommon.Value{
		pcommon.NewValueEmpty(),
		pcommon.NewValueStr("plain"),
		pcommon.NewValueStr(`<a href="x">&</a>` + " \xff\t"),
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
		valueOf(map[string]any{"b": 1, "a": "x<y&z", "c": []any{1.5, true, nil, "\xff"}, "d": map[string]any{"z": []byte("hi")}}),
		valueOf([]any{map[string]any{}, []any{}, 1e-9, int64(math.MaxInt64), " "}),
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
