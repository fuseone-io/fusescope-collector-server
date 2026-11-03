// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package ingestreport

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeContract(t *testing.T) {
	pages := sequences(t, 1)[1]
	p1, p2 := string(pages[0]), string(pages[1])
	rest := pages[2:]
	with := func(first ...string) [][]byte {
		out := [][]byte{}
		for _, f := range first {
			out = append(out, []byte(f))
		}
		return append(out, rest...)
	}
	sub := func(page, old, new string) string {
		require.Contains(t, page, old)
		return strings.Replace(page, old, new, 1)
	}
	var r1 Report
	require.NoError(t, json.Unmarshal(pages[0], &r1))
	require.NotEmpty(t, r1.Keys)
	firstKey := `{"id":"` + r1.Keys[0].ID + `"`

	testCases := []struct {
		name  string
		pages [][]byte
		ok    bool
	}{
		{name: "Complete", pages: pages, ok: true},
		{name: "OutOfOrder", pages: append(append([][]byte{}, pages[1:]...), pages[0]), ok: true},
		{name: "IdenticalDuplicate", pages: append([][]byte{pages[1]}, pages...), ok: true},
		{name: "NoPage", pages: [][]byte{}},
		{name: "MissingPage", pages: pages[1:]},
		{name: "DifferentDuplicate", pages: append(with(p1, p2), []byte(sub(p2, `"keys":[`, `"keys":[{"id":"0191c5e2-8a1b-7d2e-b0c4-ffffffffffff","lastUsedAt":"2026-11-03T12:00:00Z"},`))), ok: false},
		{name: "PageOverSize", pages: with(p1[:len(p1)-1]+strings.Repeat(" ", MaxPageBytes)+"}", p2)},
		{name: "UnknownField", pages: with(sub(p1, `"page":`, `"extra":1,"page":`), p2)},
		{name: "CaseVariantField", pages: with(sub(p1, `"page":1`, `"Page":1`), p2)},
		{name: "DuplicateField", pages: with(sub(p1, `"page":1`, `"page":1,"page":1`), p2)},
		{name: "TrailingData", pages: with(p1+" {}", p2)},
		{name: "SchemaVersion2", pages: with(sub(p1, `"schemaVersion":1`, `"schemaVersion":2`), p2)},
		{name: "HeaderDiffers", pages: with(p1, sub(p2, `"generatedAt":"`, `"generatedAt":"1`))},
		{name: "PagesDiffer", pages: with(p1, sub(p2, fmt.Sprintf(`"pages":%d`, len(pages)), fmt.Sprintf(`"pages":%d`, len(pages)+1)))},
		{name: "PageOutOfRange", pages: with(sub(p1, `"page":1`, `"page":0`), p2)},
		{name: "OverMaxPages", pages: with(sub(sub(p1, fmt.Sprintf(`"pages":%d`, len(pages)), `"pages":129`), `"page":1`, `"page":1`), p2)},
		{name: "KeyInTwoPages", pages: with(p1, sub(p2, `"keys":[`, `"keys":[`+firstKey+`,"lastUsedAt":"2026-11-03T12:00:00Z"},`))},
		{name: "ApplicationInTwoPages", pages: with(p1, sub(p2, `"applications":[`, `"applications":[{"id":"`+appA+`","counts":{"sessions":0,"views":0,"errors":0,"vitals":0},"sdk":{"name":"","version":""},"rejections":[]}`+map[bool]string{true: "", false: ","}[strings.Contains(p2, `"applications":[]`)]))},
		{name: "MissingRequiredField", pages: with(sub(p1, `"windowSeconds":300,`, ``), p2)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Merge(tc.pages)
			if tc.ok {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
