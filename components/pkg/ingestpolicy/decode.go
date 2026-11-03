// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package ingestpolicy // import "github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
)

// The v1 shape, walked before decoding: encoding/json matches field names
// case-insensitively, takes the last of duplicate keys and ignores unknown
// fields, none of which a strict schema allows (§11.2).
type kind uint8

const (
	kindObject kind = iota + 1
	kindArray
	kindString
	kindNumber
	kindBool
)

type node struct {
	kind     kind
	nullable bool
	// max bounds an array's entries, checked while walking so an oversized
	// list never reaches the typed decoder; 0 is unbounded.
	max    int
	fields []field
	elem   *node
}

type field struct {
	name     string
	node     *node
	required bool
}

var (
	stringNode   = &node{kind: kindString}
	numberNode   = &node{kind: kindNumber}
	boolNode     = &node{kind: kindBool}
	stringsNode  = &node{kind: kindArray, elem: stringNode, max: MaxListLength}
	signalsNode  = &node{kind: kindArray, elem: stringNode, max: len(signalNames)}
	nullableTime = &node{kind: kindString, nullable: true}

	limitNode = &node{kind: kindObject, fields: []field{
		{"rate", numberNode, true},
		{"burst", numberNode, true},
	}}
	keyNode = &node{kind: kindObject, fields: []field{
		{"id", stringNode, true},
		{"sha256", stringNode, true},
		{"orgId", stringNode, true},
		{"kind", stringNode, true},
		{"signals", signalsNode, true},
		{"expiresAt", nullableTime, false},
		{"limit", limitNode, false},
		{"applicationId", stringNode, false},
	}}
	piiNode = &node{kind: kindObject, fields: []field{
		{"dropQuery", boolNode, true},
		{"queryAllowlist", stringsNode, true},
		{"urlAllowlist", stringsNode, true},
	}}
	applicationNode = &node{kind: kindObject, fields: []field{
		{"id", stringNode, true},
		{"orgId", stringNode, true},
		{"name", stringNode, true},
		{"environment", stringNode, true},
		{"platform", stringNode, true},
		{"allowedOrigins", stringsNode, true},
		{"allowedPackageNames", stringsNode, true},
		{"piiPolicy", piiNode, true},
	}}
	// schemaVersion is checked on its own, before the shape, so a newer schema
	// is reported as schema_version rather than as its first new field.
	policyNode = &node{kind: kindObject, fields: []field{
		{"schemaVersion", nil, false},
		{"version", numberNode, true},
		{"issuedAt", stringNode, true},
		{"keys", &node{kind: kindArray, elem: keyNode, max: MaxKeys}, true},
		{"rumApplications", &node{kind: kindArray, elem: applicationNode, max: MaxApplications}, true},
	}}
)

// Parse decodes data as a v1 policy, strictly. It checks size, syntax,
// schemaVersion, unknown fields and the shape, in that order; values are
// checked by Validate.
func Parse(data []byte) (*Policy, error) {
	if len(data) > MaxSize {
		return nil, reject(CodeTooLarge, "")
	}
	w := walker{dec: json.NewDecoder(bytes.NewReader(data))}
	w.dec.UseNumber()
	if err := w.walk(policyNode, true); err != nil {
		if errors.Is(err, errTooMany) {
			return nil, reject(CodeInvalidValue, w.tooMany)
		}
		return nil, reject(CodeParseError, w.render())
	}
	if _, err := w.dec.Token(); !errors.Is(err, io.EOF) {
		return nil, reject(CodeParseError, "")
	}
	switch {
	case !w.schemaVersionOK:
		return nil, reject(CodeSchemaVersion, "schemaVersion")
	case w.unknown != "":
		return nil, reject(CodeUnknownField, w.unknown)
	case w.tooMany != "":
		return nil, reject(CodeInvalidValue, w.tooMany)
	case w.shape != "":
		return nil, reject(CodeParseError, w.shape)
	}
	var p Policy
	if err := json.Unmarshal(data, &p); err != nil {
		// Numbers out of their Go type's range, for example a negative version.
		return nil, reject(CodeParseError, "")
	}
	return &p, nil
}

// maxDepth bounds nesting before the walker descends. v1 needs four levels
// (policy, keys, key, limit); anything far deeper is not a v1 policy.
const maxDepth = 16

type walker struct {
	dec             *json.Decoder
	depth           int
	path            []string
	unknown         string
	tooMany         string
	shape           string
	schemaVersionOK bool
}

var (
	errSyntax  = errors.New("syntax")
	errTooMany = errors.New("too many entries")
)

func (w *walker) render() string { return strings.Join(w.path, "") }

func (w *walker) shapeError() {
	if w.shape == "" {
		w.shape = w.render()
	}
}

// walk consumes one value. n nil skips it. Only syntax errors stop the walk.
func (w *walker) walk(n *node, top bool) error {
	tok, err := w.dec.Token()
	if err != nil {
		return errSyntax
	}
	switch v := tok.(type) {
	case json.Delim:
		if top && v != '{' {
			return errSyntax
		}
		switch v {
		case '{':
			if n != nil && n.kind != kindObject {
				w.shapeError()
				n = nil
			}
			return w.object(n, top)
		case '[':
			if n != nil && n.kind != kindArray {
				w.shapeError()
				n = nil
			}
			return w.array(n)
		}
		return errSyntax
	case nil:
		if n != nil && !n.nullable {
			w.shapeError()
		}
	case string:
		if n != nil && n.kind != kindString {
			w.shapeError()
		}
	case json.Number:
		if n != nil && n.kind != kindNumber {
			w.shapeError()
		}
	case bool:
		if n != nil && n.kind != kindBool {
			w.shapeError()
		}
	}
	if top {
		// The top-level value must be an object.
		return errSyntax
	}
	return nil
}

func (w *walker) object(n *node, top bool) error {
	if w.depth++; w.depth > maxDepth {
		return errSyntax
	}
	defer func() { w.depth-- }()
	var seen uint32
	for w.dec.More() {
		tok, err := w.dec.Token()
		if err != nil {
			return errSyntax
		}
		name, ok := tok.(string)
		if !ok {
			return errSyntax
		}
		w.path = append(w.path, "."+name)
		if top && len(w.path) == 1 {
			w.path[0] = name
		}
		var child *node
		dup := false
		if n != nil {
			i := fieldIndex(n, name)
			switch {
			case i < 0:
				if w.unknown == "" {
					w.unknown = w.render()
				}
			case seen&(1<<i) != 0:
				// A duplicate key: two parsers could read different values.
				dup = true
				w.shapeError()
			default:
				seen |= 1 << i
				child = n.fields[i].node
			}
			if top && i >= 0 && name == "schemaVersion" {
				ok, err := w.schemaVersion()
				if err != nil {
					return err
				}
				if !dup {
					w.schemaVersionOK = ok
				}
				w.path = w.path[:len(w.path)-1]
				continue
			}
		}
		if err := w.walk(child, false); err != nil {
			return err
		}
		w.path = w.path[:len(w.path)-1]
	}
	if _, err := w.dec.Token(); err != nil {
		return errSyntax
	}
	if n != nil {
		for i, f := range n.fields {
			if f.required && seen&(1<<i) == 0 {
				w.path = append(w.path, "."+f.name)
				w.shapeError()
				w.path = w.path[:len(w.path)-1]
			}
		}
	}
	return nil
}

func (w *walker) array(n *node) error {
	if w.depth++; w.depth > maxDepth {
		return errSyntax
	}
	defer func() { w.depth-- }()
	var elem *node
	if n != nil {
		elem = n.elem
	}
	for i := 0; w.dec.More(); i++ {
		if n != nil && n.max > 0 && i == n.max && w.tooMany == "" {
			w.tooMany = w.render()
			if w.schemaVersionOK {
				// Stop at once: the rest of the list is never read.
				return errTooMany
			}
			// Keep walking for a schemaVersion that may follow and take
			// precedence, without descending or tracking paths.
			elem = nil
		}
		if elem == nil {
			if err := w.walk(nil, false); err != nil {
				return err
			}
			continue
		}
		w.path = append(w.path, "["+strconv.Itoa(i)+"]")
		if err := w.walk(elem, false); err != nil {
			return err
		}
		w.path = w.path[:len(w.path)-1]
	}
	if _, err := w.dec.Token(); err != nil {
		return errSyntax
	}
	return nil
}

// schemaVersion consumes the value of schemaVersion: exactly the number 1.
func (w *walker) schemaVersion() (bool, error) {
	tok, err := w.dec.Token()
	if err != nil {
		return false, errSyntax
	}
	switch v := tok.(type) {
	case json.Number:
		return v.String() == strconv.Itoa(SchemaVersion), nil
	case json.Delim:
		if v == '{' {
			return false, w.object(nil, false)
		}
		if v == '[' {
			return false, w.array(nil)
		}
		return false, errSyntax
	}
	return false, nil
}

func fieldIndex(n *node, name string) int {
	for i, f := range n.fields {
		if f.name == name {
			return i
		}
	}
	return -1
}
