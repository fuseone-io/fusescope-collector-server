// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"github.com/ClickHouse/ch-go/proto"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

const (
	tsBucketSeconds = 1800
	tagBucketMillis = 3_600_000
	maxTagValueLen  = 256
)

type tagType uint8

const (
	tagAttribute tagType = iota + 1
	tagResource
	tagScope
	tagSpanField
	tagLogField
)

var tagTypeNames = [...]string{
	tagAttribute: "tag",
	tagResource:  "resource",
	tagScope:     "scope",
	tagSpanField: "spanfield",
	tagLogField:  "logfield",
}

type tagRow struct {
	hour     int64
	key      string
	tagType  tagType
	dataType dataType
	str      string
	num      float64
}

// tagSet builds one tag_attributes_v2 block, deduplicated per batch.
type tagSet struct {
	cols *tagColumns
	seen map[tagRow]struct{}
}

func newTagSet() *tagSet {
	return &tagSet{cols: newTagColumns(), seen: map[tagRow]struct{}{}}
}

func (s *tagSet) reset() {
	s.cols.input().Reset()
	clear(s.seen)
}

// addValue adds an attribute value typed like the map columns. Empty strings
// are kept only for record and span attributes.
func (s *tagSet) addValue(hour int64, key string, t tagType, v pcommon.Value) {
	switch dt := leafType(v); dt {
	case dataFloat64:
		s.add(tagRow{hour: hour, key: key, tagType: t, dataType: dt, num: asFloat(v)})
	case dataBool:
		s.add(tagRow{hour: hour, key: key, tagType: t, dataType: dt})
	default:
		str := valueString(v)
		if t != tagAttribute && str == "" {
			return
		}
		s.add(tagRow{hour: hour, key: key, tagType: t, dataType: dt, str: str})
	}
}

// addString adds a non-empty string value.
func (s *tagSet) addString(hour int64, key string, t tagType, str string) {
	if str == "" {
		return
	}
	s.add(tagRow{hour: hour, key: key, tagType: t, dataType: dataString, str: str})
}

// addResource adds a resource attribute, kept even when empty (traces §6).
func (s *tagSet) addResource(hour int64, key, str string) {
	s.add(tagRow{hour: hour, key: key, tagType: tagResource, dataType: dataString, str: str})
}

func (s *tagSet) addNumber(hour int64, key string, t tagType, n float64) {
	s.add(tagRow{hour: hour, key: key, tagType: t, dataType: dataFloat64, num: n})
}

// add deduplicates without the hour: the first row carrying a value decides
// its bucket (traces §6).
func (s *tagSet) add(r tagRow) {
	if len(r.str) > maxTagValueLen {
		return
	}
	seen := r
	seen.hour = 0
	if _, ok := s.seen[seen]; ok {
		return
	}
	s.seen[seen] = struct{}{}
	c := s.cols
	c.unixMilli.Append(r.hour)
	c.tagKey.Append(r.key)
	c.tagType.Append(tagTypeNames[r.tagType])
	c.dataType.Append(r.dataType.String())
	c.stringValue.Append(r.str)
	if r.dataType == dataFloat64 {
		c.numberValue.Append(proto.NewNullable(r.num))
	} else {
		c.numberValue.Append(proto.Null[float64]())
	}
}

type resourceRow struct {
	fingerprint string
	bucket      int64
}

// resourceSet builds one *_v3_resource / *_v2_resource block: one row per
// resource per time bucket.
type resourceSet struct {
	cols resourceColumns
	seen map[resourceRow]struct{}
}

func newResourceSet() *resourceSet {
	return &resourceSet{seen: map[resourceRow]struct{}{}}
}

func (s *resourceSet) reset() {
	s.cols.input().Reset()
	clear(s.seen)
}

func (s *resourceSet) add(res *resourceInfo, bucket int64) {
	r := resourceRow{res.fingerprint, bucket}
	if _, ok := s.seen[r]; ok {
		return
	}
	s.seen[r] = struct{}{}
	s.cols.labels.Append(res.labels)
	s.cols.fingerprint.Append(res.fingerprint)
	s.cols.seenAt.Append(bucket)
}

func tsBucket(unixNanos uint64) int64 {
	return int64(unixNanos/1e9) / tsBucketSeconds * tsBucketSeconds
}

func hourBucket(unixNanos uint64) int64 {
	return int64(unixNanos/1e6) / tagBucketMillis * tagBucketMillis
}
