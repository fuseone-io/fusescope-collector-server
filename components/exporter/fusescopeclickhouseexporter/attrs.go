// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"encoding/base64"
	"math"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

type dataType uint8

const (
	dataString dataType = iota + 1
	dataBool
	dataFloat64
)

func (d dataType) String() string {
	switch d {
	case dataBool:
		return "bool"
	case dataFloat64:
		return "float64"
	default:
		return "string"
	}
}

// leafType is the map column a flattened attribute value lands in (§2.2).
func leafType(v pcommon.Value) dataType {
	switch v.Type() {
	case pcommon.ValueTypeBool:
		return dataBool
	case pcommon.ValueTypeInt, pcommon.ValueTypeDouble:
		return dataFloat64
	default:
		return dataString
	}
}

func asFloat(v pcommon.Value) float64 {
	if v.Type() == pcommon.ValueTypeInt {
		return float64(v.Int())
	}
	return v.Double()
}

// walkLeaves calls f for every attribute, flattening kvlists into dotted keys.
func walkLeaves(prefix string, m pcommon.Map, f func(key string, v pcommon.Value)) {
	for k, v := range m.All() {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		if v.Type() == pcommon.ValueTypeMap {
			walkLeaves(key, v.Map(), f)
			continue
		}
		f(key, v)
	}
}

// appendAttributesJSON writes m as a flat JSON object for a ClickHouse JSON
// column, which nests dotted keys itself. A key that is also the prefix of
// another key is left out, as observed (§2.2).
func appendAttributesJSON(dst []byte, m pcommon.Map) []byte {
	dst = append(dst, '{')
	first := true
	for k, v := range m.All() {
		if isNumberNaNOrInf(v) || shadowedByObject(m, k) {
			continue
		}
		if !first {
			dst = append(dst, ',')
		}
		first = false
		dst = appendJSONString(dst, k)
		dst = append(dst, ':')
		dst = appendJSONValue(dst, v)
	}
	return append(dst, '}')
}

func shadowedByObject(m pcommon.Map, key string) bool {
	for other := range m.All() {
		if len(other) > len(key) && other[len(key)] == '.' && strings.HasPrefix(other, key) {
			return true
		}
	}
	return false
}

func isNumberNaNOrInf(v pcommon.Value) bool {
	if v.Type() != pcommon.ValueTypeDouble {
		return false
	}
	d := v.Double()
	return math.IsNaN(d) || math.IsInf(d, 0)
}

func appendJSONValue(dst []byte, v pcommon.Value) []byte {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return appendJSONString(dst, v.Str())
	case pcommon.ValueTypeInt:
		return strconv.AppendInt(dst, v.Int(), 10)
	case pcommon.ValueTypeDouble:
		if isNumberNaNOrInf(v) {
			return append(dst, "null"...)
		}
		return strconv.AppendFloat(dst, v.Double(), 'g', -1, 64)
	case pcommon.ValueTypeBool:
		return strconv.AppendBool(dst, v.Bool())
	case pcommon.ValueTypeBytes:
		dst = append(dst, '"')
		dst = base64.StdEncoding.AppendEncode(dst, v.Bytes().AsRaw())
		return append(dst, '"')
	case pcommon.ValueTypeMap:
		dst = append(dst, '{')
		first := true
		for k, e := range v.Map().All() {
			if !first {
				dst = append(dst, ',')
			}
			first = false
			dst = appendJSONString(dst, k)
			dst = append(dst, ':')
			dst = appendJSONValue(dst, e)
		}
		return append(dst, '}')
	case pcommon.ValueTypeSlice:
		dst = append(dst, '[')
		for i, e := range v.Slice().All() {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendJSONValue(dst, e)
		}
		return append(dst, ']')
	default:
		return append(dst, "null"...)
	}
}
