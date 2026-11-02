// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"unicode/utf8"
)

const hexDigits = "0123456789abcdef"

// appendJSONString appends s as a JSON string with the escaping of Go's
// encoding/json (HTML-safe, U+2028/U+2029 escaped, invalid UTF-8 replaced).
// String columns such as events and links store this text verbatim, so it must
// match byte for byte (docs/spec/clickhouse-traces.md §3).
func appendJSONString(dst []byte, s string) []byte {
	return appendJSON(dst, s, true)
}

// appendJSONLabel appends s as appendJSONString does, but keeps invalid UTF-8
// raw, as metric labels hold it (docs/spec/clickhouse-metrics.md §3).
func appendJSONLabel(dst []byte, s string) []byte {
	return appendJSON(dst, s, false)
}

func appendJSON(dst []byte, s string, replaceInvalid bool) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		b := s[i]
		if b < utf8.RuneSelf {
			if jsonSafe[b] {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			switch b {
			case '\\', '"':
				dst = append(dst, '\\', b)
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
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[b>>4], hexDigits[b&0xF])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 && replaceInvalid {
			dst = append(dst, s[start:i]...)
			dst = append(dst, `\ufffd`...)
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

var jsonSafe = func() (t [utf8.RuneSelf]bool) {
	for b := 0x20; b < utf8.RuneSelf; b++ {
		t[b] = true
	}
	for _, b := range []byte{'"', '\\', '<', '>', '&'} {
		t[b] = false
	}
	return t
}()
