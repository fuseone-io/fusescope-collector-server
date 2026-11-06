// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"encoding/base64"
	"math"
	"slices"
	"strconv"
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// maxValueDepth is the deepest map or list nesting converted
// (attributes-metadata §2.1).
const maxValueDepth = 16

// boundedString appends the string form of v that pdata's Value.AsString
// gives (attributes-metadata §2.1), but stops as soon as the output would
// exceed budget bytes or nest deeper than maxValueDepth, and then reports
// false. Unlike AsString it never materializes a map or list (AsRaw) nor
// encodes it whole first, so its cost is bounded by budget.
//
// keys is scratch for sorting map keys, one slice per depth.
type boundedString struct {
	buf    []byte
	keys   [maxValueDepth][]string
	budget int
}

// append writes v at the end of b.buf and reports whether it fit.
func (b *boundedString) append(v pcommon.Value, budget int) bool {
	b.budget = len(b.buf) + budget
	switch v.Type() {
	case pcommon.ValueTypeEmpty:
		return true
	case pcommon.ValueTypeStr:
		s := v.Str()
		if len(s) > budget {
			return false
		}
		b.buf = append(b.buf, s...)
	case pcommon.ValueTypeBool:
		b.buf = strconv.AppendBool(b.buf, v.Bool())
	case pcommon.ValueTypeInt:
		b.buf = strconv.AppendInt(b.buf, v.Int(), 10)
	case pcommon.ValueTypeDouble:
		b.buf = appendES6Float(b.buf, v.Double(), true)
	case pcommon.ValueTypeBytes:
		raw := v.Bytes().AsRaw()
		if base64.StdEncoding.EncodedLen(len(raw)) > budget {
			return false
		}
		b.buf = base64.StdEncoding.AppendEncode(b.buf, raw)
	case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
		start := len(b.buf)
		ok, valid := b.appendJSON(v, 0)
		if !ok {
			return false
		}
		if !valid {
			// encoding/json refuses NaN and infinities, and AsString then
			// gives an empty string.
			b.buf = b.buf[:start]
		}
		return true
	default:
		return false
	}
	return len(b.buf) <= b.budget
}

// appendJSON writes v as encoding/json does for AsRaw's result, without HTML
// escaping. ok is false when the budget or the depth is exceeded; valid is
// false when encoding/json would fail.
func (b *boundedString) appendJSON(v pcommon.Value, depth int) (ok, valid bool) {
	valid = true
	switch v.Type() {
	case pcommon.ValueTypeEmpty:
		b.buf = append(b.buf, "null"...)
	case pcommon.ValueTypeStr:
		s := v.Str()
		if len(b.buf)+len(s)+2 > b.budget {
			return false, valid
		}
		b.buf = appendJSONNoHTML(b.buf, s)
	case pcommon.ValueTypeBool:
		b.buf = strconv.AppendBool(b.buf, v.Bool())
	case pcommon.ValueTypeInt:
		b.buf = strconv.AppendInt(b.buf, v.Int(), 10)
	case pcommon.ValueTypeDouble:
		f := v.Double()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			valid = false
			b.buf = append(b.buf, "null"...)
			break
		}
		b.buf = appendES6Float(b.buf, f, false)
	case pcommon.ValueTypeBytes:
		raw := v.Bytes().AsRaw()
		if len(b.buf)+base64.StdEncoding.EncodedLen(len(raw))+2 > b.budget {
			return false, valid
		}
		b.buf = append(b.buf, '"')
		b.buf = base64.StdEncoding.AppendEncode(b.buf, raw)
		b.buf = append(b.buf, '"')
	case pcommon.ValueTypeMap:
		if depth >= maxValueDepth {
			return false, valid
		}
		m := v.Map()
		// Each entry takes at least `"":0,`; refuse before sorting a map that
		// cannot fit.
		if len(b.buf)+2+4*m.Len() > b.budget {
			return false, valid
		}
		keys := b.keys[depth][:0]
		for k := range m.All() {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		b.keys[depth] = keys
		b.buf = append(b.buf, '{')
		for i, k := range keys {
			if i > 0 {
				b.buf = append(b.buf, ',')
			}
			if len(b.buf)+len(k)+3 > b.budget {
				clear(keys)
				return false, valid
			}
			b.buf = appendJSONNoHTML(b.buf, k)
			b.buf = append(b.buf, ':')
			e, _ := m.Get(k)
			eok, evalid := b.appendJSON(e, depth+1)
			if !eok {
				clear(keys)
				return false, valid
			}
			valid = valid && evalid
		}
		clear(keys)
		b.buf = append(b.buf, '}')
	case pcommon.ValueTypeSlice:
		if depth >= maxValueDepth {
			return false, valid
		}
		s := v.Slice()
		if len(b.buf)+2*s.Len()+1 > b.budget {
			return false, valid
		}
		b.buf = append(b.buf, '[')
		for i, e := range s.All() {
			if i > 0 {
				b.buf = append(b.buf, ',')
			}
			eok, evalid := b.appendJSON(e, depth+1)
			if !eok {
				return false, valid
			}
			valid = valid && evalid
		}
		b.buf = append(b.buf, ']')
	default:
		return false, valid
	}
	return len(b.buf) <= b.budget, valid
}

// appendES6Float formats f as encoding/json and pdata do: like %g, with
// exponents for magnitudes below 1e-6 or from 1e21, unpadded. top selects
// AsString's spelling of NaN and infinities.
func appendES6Float(dst []byte, f float64, top bool) []byte {
	switch {
	case top && math.IsNaN(f):
		return append(dst, "NaN"...)
	case top && math.IsInf(f, 1):
		return append(dst, "Infinity"...)
	case top && math.IsInf(f, -1):
		return append(dst, "-Infinity"...)
	}
	abs := math.Abs(f)
	format := byte('f')
	if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	start := len(dst)
	dst = strconv.AppendFloat(dst, f, format, -1, 64)
	if format == 'e' {
		// clean up e-09 to e-9
		if n := len(dst) - start; n >= 4 && dst[len(dst)-4] == 'e' && dst[len(dst)-3] == '-' && dst[len(dst)-2] == '0' {
			dst[len(dst)-2] = dst[len(dst)-1]
			dst = dst[:len(dst)-1]
		}
	}
	return dst
}

// appendJSONNoHTML appends s as a JSON string as encoding/json does with
// SetEscapeHTML(false): invalid UTF-8 replaced, U+2028 and U+2029 escaped.
func appendJSONNoHTML(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			switch c {
			case '\\', '"':
				dst = append(dst, '\\', c)
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xF])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, `�`...)
			i += size
			start = i
			continue
		}
		if r == ' ' || r == ' ' {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', hexDigits[r&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}
