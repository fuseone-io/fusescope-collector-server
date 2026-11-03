// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"cmp"
	"slices"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// fingerprintPrefix lists the identifying attributes that lead the resource
// fingerprint, in order; within a group the first present key wins
// (docs/spec/clickhouse-traces.md §2.1).
var fingerprintPrefix = [][]string{
	{"cloud.region"},
	{"k8s.cluster.name"},
	{"service.namespace", "k8s.namespace.name"},
	{"service.name"},
	{"deployment.environment"},
	{"k8s.pod.name", "host.name"},
	{"container.name"},
}

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

type keyValue struct {
	key, value string
}

// resourceInfo is everything derived from one OTLP resource, computed once per
// ResourceSpans and shared by all its spans.
type resourceInfo struct {
	attrs       []keyValue // sorted by key, values stringified
	fingerprint string
	labels      string // compact JSON object of attrs
	json        string // labels without keys a longer key extends, for the resource JSON column
	serviceName string
}

func newResourceInfo(res pcommon.Resource, buf []byte) (resourceInfo, []byte) {
	m := res.Attributes()
	attrs := sortedStrings(make([]keyValue, 0, m.Len()), m)

	info := resourceInfo{attrs: attrs}
	buf = buf[:0]
	for _, group := range fingerprintPrefix {
		for _, key := range group {
			if v, ok := info.get(key); ok {
				buf = append(buf, key...)
				buf = append(buf, '=')
				buf = append(buf, v...)
				buf = append(buf, ';')
				break
			}
		}
	}
	buf = append(buf, "hash="...)
	buf = strconv.AppendUint(buf, labelsHash(attrs), 10)
	info.fingerprint = string(buf)

	buf = buf[:0]
	buf = append(buf, '{')
	for i, kv := range attrs {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = appendJSONString(buf, kv.key)
		buf = append(buf, ':')
		buf = appendJSONString(buf, kv.value)
	}
	buf = append(buf, '}')
	info.labels = string(buf)
	info.json = info.labels
	if slices.ContainsFunc(attrs, func(kv keyValue) bool { return shadowed(attrs, kv.key) }) {
		buf = buf[:0]
		buf = append(buf, '{')
		for _, kv := range attrs {
			if shadowed(attrs, kv.key) {
				continue
			}
			if len(buf) > 1 {
				buf = append(buf, ',')
			}
			buf = appendJSONString(buf, kv.key)
			buf = append(buf, ':')
			buf = appendJSONString(buf, kv.value)
		}
		buf = append(buf, '}')
		info.json = string(buf)
	}
	info.serviceName, _ = info.get("service.name")
	return info, buf
}

// shadowed reports whether another key of sorted extends key with a ".", which
// the resource JSON column would nest under it (docs/spec/clickhouse-traces.md §2.2).
func shadowed(sorted []keyValue, key string) bool {
	i, _ := slices.BinarySearchFunc(sorted, key, func(kv keyValue, k string) int { return cmp.Compare(kv.key, k) })
	// Keys that extend key follow it, those extending it with a byte below "." first.
	for _, kv := range sorted[i:] {
		if !strings.HasPrefix(kv.key, key) {
			return false
		}
		if len(kv.key) > len(key) && kv.key[len(key)] == '.' {
			return true
		}
	}
	return false
}

// sortedStrings appends m's attributes stringified and sorted by key.
func sortedStrings(dst []keyValue, m pcommon.Map) []keyValue {
	for k, v := range m.All() {
		dst = append(dst, keyValue{key: k, value: valueString(v)})
	}
	slices.SortFunc(dst, func(a, b keyValue) int { return cmp.Compare(a.key, b.key) })
	return dst
}

func (r *resourceInfo) get(key string) (string, bool) {
	i, ok := slices.BinarySearchFunc(r.attrs, key, func(kv keyValue, k string) int { return cmp.Compare(kv.key, k) })
	if !ok {
		return "", false
	}
	return r.attrs[i].value, true
}

// labelsHash is FNV-1a 64 over "key 0xFF value 0xFF" for each attribute in key order.
func labelsHash(sorted []keyValue) uint64 {
	h := uint64(fnvOffset64)
	add := func(s string) {
		for i := 0; i < len(s); i++ {
			h ^= uint64(s[i])
			h *= fnvPrime64
		}
		h ^= 0xFF
		h *= fnvPrime64
	}
	for _, kv := range sorted {
		add(kv.key)
		add(kv.value)
	}
	return h
}
