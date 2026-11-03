// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package rumreceiver // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver"

import (
	"google.golang.org/protobuf/encoding/protowire"
)

// structureLimits is the structural budget of one request (§7.1), checked
// on the encoded body before pdata decodes it: a few bytes of protobuf or
// JSON can name millions of empty messages, each of which decoding would
// allocate. Every limit is a maximum the operator can only lower.
type structureLimits struct {
	resources         int // resource groups, empty ones included
	scopesPerResource int
	items             int // spans or log records
	eventsPerSpan     int
	linksPerSpan      int
	attributes        int // per resource, scope, item, event, link, and per nested array or map
	elements          int // every repeated entry of the request, all kinds together
	valueDepth        int // nesting of array and map attribute values
}

// The maxima, also the defaults.
var maxStructure = structureLimits{
	resources:         1000,
	scopesPerResource: 100,
	items:             10_000,
	eventsPerSpan:     128,
	linksPerSpan:      128,
	attributes:        128,
	elements:          100_000,
	valueDepth:        8,
}

// msg is a message kind of OTLP traces and logs that holds repeated fields.
type msg uint8

const (
	msgOther msg = iota
	msgTracesRequest
	msgLogsRequest
	msgResourceSpans
	msgResourceLogs
	msgResource
	msgEntityRef
	msgScopeSpans
	msgScopeLogs
	msgScope
	msgSpan
	msgLogRecord
	msgEvent
	msgLink
	msgKeyValue
	msgAnyValue
	msgArrayValue
	msgKeyValueList
)

// count is what a repeated entry counts against.
type count uint8

const (
	countNone count = iota
	countResource
	countScope
	countItem
	countEvent
	countLink
	countAttribute // an attribute, or an entry of a nested array or map
	countElement   // only the request-wide budget (entity reference keys)
)

// field is a field the budget follows: the child message kind (msgOther
// for a scalar), and what each occurrence counts against.
type field struct {
	child msg
	count count
}

// protoFields maps (message, field number); JSON names are in jsonFields.
// The numbers are those pdata decodes (opentelemetry-proto, as pdata v1.70
// generates them), deprecated scope lists included.
func protoField(m msg, num protowire.Number) (field, bool) {
	switch m {
	case msgTracesRequest:
		if num == 1 {
			return field{msgResourceSpans, countResource}, true
		}
	case msgLogsRequest:
		if num == 1 {
			return field{msgResourceLogs, countResource}, true
		}
	case msgResourceSpans:
		switch num {
		case 1:
			return field{msgResource, countNone}, true
		case 2, 1000:
			return field{msgScopeSpans, countScope}, true
		}
	case msgResourceLogs:
		switch num {
		case 1:
			return field{msgResource, countNone}, true
		case 2, 1000:
			return field{msgScopeLogs, countScope}, true
		}
	case msgResource:
		switch num {
		case 1:
			return field{msgKeyValue, countAttribute}, true
		case 3:
			return field{msgEntityRef, countElement}, true
		}
	case msgEntityRef:
		if num == 3 || num == 4 {
			return field{msgOther, countElement}, true
		}
	case msgScopeSpans:
		switch num {
		case 1:
			return field{msgScope, countNone}, true
		case 2:
			return field{msgSpan, countItem}, true
		}
	case msgScopeLogs:
		switch num {
		case 1:
			return field{msgScope, countNone}, true
		case 2:
			return field{msgLogRecord, countItem}, true
		}
	case msgScope:
		if num == 3 {
			return field{msgKeyValue, countAttribute}, true
		}
	case msgSpan:
		switch num {
		case 9:
			return field{msgKeyValue, countAttribute}, true
		case 11:
			return field{msgEvent, countEvent}, true
		case 13:
			return field{msgLink, countLink}, true
		}
	case msgLogRecord:
		switch num {
		case 5:
			return field{msgAnyValue, countNone}, true
		case 6:
			return field{msgKeyValue, countAttribute}, true
		}
	case msgEvent:
		if num == 3 {
			return field{msgKeyValue, countAttribute}, true
		}
	case msgLink:
		if num == 4 {
			return field{msgKeyValue, countAttribute}, true
		}
	case msgKeyValue:
		if num == 2 {
			return field{msgAnyValue, countNone}, true
		}
	case msgAnyValue:
		switch num {
		case 5:
			return field{msgArrayValue, countNone}, true
		case 6:
			return field{msgKeyValueList, countNone}, true
		}
	case msgArrayValue:
		if num == 1 {
			return field{msgAnyValue, countAttribute}, true
		}
	case msgKeyValueList:
		if num == 1 {
			return field{msgKeyValue, countAttribute}, true
		}
	}
	return field{}, false
}

// jsonField is protoField by the names pdata's JSON decoder accepts, both
// lowerCamelCase and snake_case.
func jsonField(m msg, name []byte) (field, bool) {
	switch m {
	case msgTracesRequest:
		if eq(name, "resourceSpans", "resource_spans") {
			return protoField(m, 1)
		}
	case msgLogsRequest:
		if eq(name, "resourceLogs", "resource_logs") {
			return protoField(m, 1)
		}
	case msgResourceSpans:
		switch {
		case eq(name, "resource", ""):
			return protoField(m, 1)
		case eq(name, "scopeSpans", "scope_spans"), eq(name, "deprecatedScopeSpans", "deprecated_scope_spans"):
			return protoField(m, 2)
		}
	case msgResourceLogs:
		switch {
		case eq(name, "resource", ""):
			return protoField(m, 1)
		case eq(name, "scopeLogs", "scope_logs"), eq(name, "deprecatedScopeLogs", "deprecated_scope_logs"):
			return protoField(m, 2)
		}
	case msgResource:
		switch {
		case eq(name, "attributes", ""):
			return protoField(m, 1)
		case eq(name, "entityRefs", "entity_refs"):
			return protoField(m, 3)
		}
	case msgEntityRef:
		if eq(name, "idKeys", "id_keys") || eq(name, "descriptionKeys", "description_keys") {
			return protoField(m, 3)
		}
	case msgScopeSpans:
		switch {
		case eq(name, "scope", ""):
			return protoField(m, 1)
		case eq(name, "spans", ""):
			return protoField(m, 2)
		}
	case msgScopeLogs:
		switch {
		case eq(name, "scope", ""):
			return protoField(m, 1)
		case eq(name, "logRecords", "log_records"):
			return protoField(m, 2)
		}
	case msgScope, msgEvent:
		if eq(name, "attributes", "") {
			return field{msgKeyValue, countAttribute}, true
		}
	case msgLink:
		if eq(name, "attributes", "") {
			return protoField(m, 4)
		}
	case msgSpan:
		switch {
		case eq(name, "attributes", ""):
			return protoField(m, 9)
		case eq(name, "events", ""):
			return protoField(m, 11)
		case eq(name, "links", ""):
			return protoField(m, 13)
		}
	case msgLogRecord:
		switch {
		case eq(name, "body", ""):
			return protoField(m, 5)
		case eq(name, "attributes", ""):
			return protoField(m, 6)
		}
	case msgKeyValue:
		if eq(name, "value", "") {
			return protoField(m, 2)
		}
	case msgAnyValue:
		switch {
		case eq(name, "arrayValue", "array_value"):
			return protoField(m, 5)
		case eq(name, "kvlistValue", "kvlist_value"):
			return protoField(m, 6)
		}
	case msgArrayValue, msgKeyValueList:
		if eq(name, "values", "") {
			return protoField(m, 1)
		}
	}
	return field{}, false
}

func eq(name []byte, a, b string) bool {
	return string(name) == a || (b != "" && string(name) == b)
}

// budget is one request's running count.
type budget struct {
	lim       structureLimits
	elements  int
	resources int
	items     int
}

// node is the per-message state: what its direct repeated fields used.
type node struct {
	scopes, events, links, entries int
	// singular marks the single (not repeated) message fields seen. Both
	// protobuf and pdata's JSON decoder merge a repeated occurrence into
	// the first, which would let a message exceed its budget in parts; a
	// repeat is refused instead, as no conforming encoder writes one.
	singular uint32
}

// once records a single message field; false if it was seen before.
func (n *node) once(f field) bool {
	bit := uint32(1) << f.child
	if n.singular&bit != 0 {
		return false
	}
	n.singular |= bit
	return true
}

// take counts one occurrence of f inside parent n; false when over budget.
func (b *budget) take(n *node, f field) bool {
	if f.count == countNone {
		return true
	}
	b.elements++
	if b.elements > b.lim.elements {
		return false
	}
	switch f.count {
	case countResource:
		b.resources++
		return b.resources <= b.lim.resources
	case countScope:
		n.scopes++
		return n.scopes <= b.lim.scopesPerResource
	case countItem:
		b.items++
		return b.items <= b.lim.items
	case countEvent:
		n.events++
		return n.events <= b.lim.eventsPerSpan
	case countLink:
		n.links++
		return n.links <= b.lim.linksPerSpan
	case countAttribute:
		n.entries++
		return n.entries <= b.lim.attributes
	}
	return true
}

// structure is the outcome of a scan.
type structure uint8

const (
	structureOK structure = iota
	structureOverBudget
	structureMalformed
)

// scanProto walks a protobuf request without building anything.
func scanProto(data []byte, root msg, lim structureLimits) structure {
	b := budget{lim: lim}
	return b.protoMessage(data, root, 0)
}

func (b *budget) protoMessage(data []byte, m msg, depth int) structure {
	var n node
	for len(data) > 0 {
		num, typ, l := protowire.ConsumeTag(data)
		if l < 0 {
			return structureMalformed
		}
		data = data[l:]
		f, ok := protoField(m, num)
		if !ok || typ != protowire.BytesType {
			l = protowire.ConsumeFieldValue(num, typ, data)
			if l < 0 {
				return structureMalformed
			}
			data = data[l:]
			continue
		}
		v, l := protowire.ConsumeBytes(data)
		if l < 0 {
			return structureMalformed
		}
		data = data[l:]
		if f.count == countNone && !n.once(f) {
			return structureMalformed
		}
		if !b.take(&n, f) {
			return structureOverBudget
		}
		if f.child == msgOther {
			continue
		}
		d := depth
		if f.child == msgArrayValue || f.child == msgKeyValueList {
			if d++; d > b.lim.valueDepth {
				return structureOverBudget
			}
		}
		if s := b.protoMessage(v, f.child, d); s != structureOK {
			return s
		}
	}
	return structureOK
}

// maxJSONDepth bounds the JSON nesting the scanner follows; OTLP at the
// value depth bound needs well under it.
const maxJSONDepth = 64

// scanJSON walks an OTLP JSON request with no allocation: only the
// structure is followed, and values are skipped.
func scanJSON(data []byte, root msg, lim structureLimits) structure {
	s := jsonScanner{data: data, budget: budget{lim: lim}}
	s.space()
	if r := s.value(root, 0, 0); r != structureOK {
		return r
	}
	s.space()
	if s.pos != len(s.data) {
		return structureMalformed
	}
	return structureOK
}

type jsonScanner struct {
	data   []byte
	pos    int
	budget budget
	// key is the object key being read; long ones are cut and match nothing.
	key      [64]byte
	keyLen   int
	keyLong  bool
	readsKey bool
}

func (s *jsonScanner) space() {
	for s.pos < len(s.data) {
		switch s.data[s.pos] {
		case ' ', '\t', '\n', '\r':
			s.pos++
		default:
			return
		}
	}
}

// value scans one value of message kind m (msgOther: anything), at JSON
// nesting depth and attribute value depth vdepth.
func (s *jsonScanner) value(m msg, depth, vdepth int) structure {
	if depth > maxJSONDepth {
		return structureOverBudget
	}
	if s.pos >= len(s.data) {
		return structureMalformed
	}
	switch s.data[s.pos] {
	case '{':
		return s.object(m, depth, vdepth)
	case '[':
		// An array where a message is expected is not OTLP; pdata refuses it.
		return s.array(nil, field{}, depth, vdepth)
	case '"':
		s.readsKey = false
		if !s.str() {
			return structureMalformed
		}
		return structureOK
	default:
		return s.literal()
	}
}

func (s *jsonScanner) object(m msg, depth, vdepth int) structure {
	var n node
	s.pos++ // {
	s.space()
	if s.pos < len(s.data) && s.data[s.pos] == '}' {
		s.pos++
		return structureOK
	}
	for {
		s.space()
		s.readsKey = true
		if s.pos >= len(s.data) || s.data[s.pos] != '"' || !s.str() {
			return structureMalformed
		}
		var k []byte
		if !s.keyLong {
			k = s.key[:s.keyLen]
		}
		s.space()
		if s.pos >= len(s.data) || s.data[s.pos] != ':' {
			return structureMalformed
		}
		s.pos++
		s.space()
		f, ok := jsonField(m, k)
		var r structure
		switch {
		case !ok:
			r = s.value(msgOther, depth+1, vdepth)
		case f.count == countNone:
			if !n.once(f) {
				return structureMalformed
			}
			r = s.child(&n, f, depth+1, vdepth)
		default:
			// A repeated field: its value is an array of children.
			if s.pos >= len(s.data) || s.data[s.pos] != '[' {
				r = s.value(msgOther, depth+1, vdepth)
			} else {
				r = s.array(&n, f, depth+1, vdepth)
			}
		}
		if r != structureOK {
			return r
		}
		s.space()
		if s.pos >= len(s.data) {
			return structureMalformed
		}
		switch s.data[s.pos] {
		case ',':
			s.pos++
		case '}':
			s.pos++
			return structureOK
		default:
			return structureMalformed
		}
	}
}

// child scans a single (not repeated) child message.
func (s *jsonScanner) child(n *node, f field, depth, vdepth int) structure {
	if f.child == msgArrayValue || f.child == msgKeyValueList {
		if vdepth++; vdepth > s.budget.lim.valueDepth {
			return structureOverBudget
		}
	}
	return s.value(f.child, depth, vdepth)
}

// array scans an array; with n set, each entry counts as f in n.
func (s *jsonScanner) array(n *node, f field, depth, vdepth int) structure {
	if depth > maxJSONDepth {
		return structureOverBudget
	}
	s.pos++ // [
	s.space()
	if s.pos < len(s.data) && s.data[s.pos] == ']' {
		s.pos++
		return structureOK
	}
	for {
		s.space()
		var r structure
		if n == nil {
			r = s.value(msgOther, depth+1, vdepth)
		} else {
			if !s.budget.take(n, f) {
				return structureOverBudget
			}
			r = s.value(f.child, depth+1, vdepth)
		}
		if r != structureOK {
			return r
		}
		s.space()
		if s.pos >= len(s.data) {
			return structureMalformed
		}
		switch s.data[s.pos] {
		case ',':
			s.pos++
		case ']':
			s.pos++
			return structureOK
		default:
			return structureMalformed
		}
	}
}

// str consumes a string; when readsKey is set it decodes it into key.
func (s *jsonScanner) str() bool {
	s.pos++ // "
	s.keyLen, s.keyLong = 0, false
	for s.pos < len(s.data) {
		c := s.data[s.pos]
		switch {
		case c == '"':
			s.pos++
			return true
		case c == '\\':
			if s.pos+1 >= len(s.data) {
				return false
			}
			e := s.data[s.pos+1]
			s.pos += 2
			switch e {
			case '"', '\\', '/':
				s.put(e)
			case 'b', 'f', 'n', 'r', 't':
				s.put(0) // control characters are in no field name
			case 'u':
				if s.pos+4 > len(s.data) {
					return false
				}
				r, ok := hex4(s.data[s.pos : s.pos+4])
				if !ok {
					return false
				}
				s.pos += 4
				if r < 0x80 {
					s.put(byte(r))
				} else {
					s.put(0)
				}
			default:
				return false
			}
		case c < 0x20:
			return false
		default:
			s.put(c)
			s.pos++
		}
	}
	return false
}

func (s *jsonScanner) put(c byte) {
	if !s.readsKey || s.keyLong {
		return
	}
	if s.keyLen == len(s.key) {
		s.keyLong = true
		return
	}
	s.key[s.keyLen] = c
	s.keyLen++
}

func hex4(b []byte) (rune, bool) {
	var r rune
	for _, c := range b {
		r <<= 4
		switch {
		case '0' <= c && c <= '9':
			r |= rune(c - '0')
		case 'a' <= c && c <= 'f':
			r |= rune(c-'a') + 10
		case 'A' <= c && c <= 'F':
			r |= rune(c-'A') + 10
		default:
			return 0, false
		}
	}
	return r, true
}

// literal consumes a number, true, false or null.
func (s *jsonScanner) literal() structure {
	start := s.pos
	for s.pos < len(s.data) {
		switch c := s.data[s.pos]; {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '+', c == '.', c == 'E':
			s.pos++
		default:
			if s.pos == start {
				return structureMalformed
			}
			return structureOK
		}
	}
	if s.pos == start {
		return structureMalformed
	}
	return structureOK
}
