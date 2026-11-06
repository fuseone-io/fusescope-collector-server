// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"errors"
	"fmt"
	"strings"
)

// Engine is engine_full split into what §4.1 compares. The keys come from
// their own system.tables columns; TTL and settings only from engine_full.
type Engine struct {
	Name     string
	Args     []string
	TTL      string
	Settings []string
}

// clauses are the top-level clauses that may follow the engine in engine_full,
// in the order ClickHouse prints them.
var clauses = []string{" PARTITION BY ", " PRIMARY KEY ", " ORDER BY ", " SAMPLE BY ", " TTL ", " SETTINGS "}

// scanner walks ClickHouse text, tracking string literals, quoted identifiers
// and parentheses.
type scanner struct {
	s     string
	i     int
	depth int
	quote byte // ' or ` while inside one
}

// next advances one byte and reports whether that byte is at depth 0 outside
// any quote, before it is consumed.
func (sc *scanner) next() (top bool) {
	c := sc.s[sc.i]
	top = sc.depth == 0 && sc.quote == 0
	switch {
	case sc.quote != 0:
		if c == '\\' && sc.quote == '\'' && sc.i+1 < len(sc.s) {
			sc.i++
		} else if c == sc.quote {
			sc.quote = 0
		}
	case c == '\'' || c == '`':
		sc.quote = c
	case c == '(':
		sc.depth++
	case c == ')':
		sc.depth--
	}
	sc.i++
	return top
}

func (sc *scanner) balanced() bool { return sc.depth == 0 && sc.quote == 0 }

// SplitTop splits s at the occurrences of sep outside quotes and parentheses.
func SplitTop(s, sep string) ([]string, error) {
	var out []string
	sc := &scanner{s: s}
	start := 0
	for sc.i < len(s) {
		if sc.depth == 0 && sc.quote == 0 && strings.HasPrefix(s[sc.i:], sep) {
			out = append(out, strings.TrimSpace(s[start:sc.i]))
			sc.i += len(sep)
			start = sc.i
			continue
		}
		sc.next()
		if sc.depth < 0 {
			return nil, fmt.Errorf("unbalanced parentheses in %q", s)
		}
	}
	if !sc.balanced() {
		return nil, fmt.Errorf("unbalanced quotes or parentheses in %q", s)
	}
	return append(out, strings.TrimSpace(s[start:])), nil
}

// ParseEngine splits engine_full.
func ParseEngine(full string) (Engine, error) {
	if full == "" {
		return Engine{}, errors.New("empty engine")
	}
	var e Engine
	end := strings.IndexAny(full, "( ")
	if end < 0 {
		e.Name = full
		return e, nil
	}
	e.Name = full[:end]
	rest := full[end:]
	if rest[0] == '(' {
		sc := &scanner{s: rest}
		sc.next()
		for sc.i < len(rest) && sc.depth > 0 {
			sc.next()
		}
		if !sc.balanced() {
			return Engine{}, fmt.Errorf("unbalanced engine arguments in %q", full)
		}
		if inner := strings.TrimSpace(rest[1 : sc.i-1]); inner != "" {
			args, err := SplitTop(inner, ",")
			if err != nil {
				return Engine{}, err
			}
			e.Args = args
		}
		rest = rest[sc.i:]
	}
	// Find each top-level clause; a clause's text runs to the next one.
	type at struct {
		clause string
		pos    int
	}
	var found []at
	sc := &scanner{s: rest}
	for sc.i < len(rest) {
		if sc.depth == 0 && sc.quote == 0 {
			for _, c := range clauses {
				if strings.HasPrefix(rest[sc.i:], c) {
					found = append(found, at{c, sc.i})
				}
			}
		}
		sc.next()
	}
	if !sc.balanced() {
		return Engine{}, fmt.Errorf("unbalanced quotes or parentheses in %q", full)
	}
	if strings.TrimSpace(rest) != "" && (len(found) == 0 || strings.TrimSpace(rest[:found[0].pos]) != "") {
		return Engine{}, fmt.Errorf("unexpected text after the engine in %q", full)
	}
	for i, f := range found {
		stop := len(rest)
		if i+1 < len(found) {
			stop = found[i+1].pos
		}
		text := strings.TrimSpace(rest[f.pos+len(f.clause) : stop])
		switch f.clause {
		case " TTL ":
			e.TTL = text
		case " SETTINGS ":
			settings, err := SplitTop(text, ",")
			if err != nil {
				return Engine{}, err
			}
			e.Settings = settings
		}
	}
	return e, nil
}

// ReplaceDatabases replaces each database qualifier in canonical text, a name
// in dbs, bare or quoted, directly followed by '.', with its role placeholder.
// String literals and names inside a longer qualified name are kept.
func ReplaceDatabases(text string, dbs map[string]string) string {
	var b strings.Builder
	sc := &scanner{s: text}
	isIdent := func(c byte) bool {
		return c == '_' || c == '$' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
	}
	for sc.i < len(text) {
		start := sc.i
		if sc.quote == 0 && (start == 0 || text[start-1] != '.' && !isIdent(text[start-1])) {
			// A candidate qualifier begins here.
			var name string
			end := start
			switch c := text[start]; {
			case c == '`':
				if j := strings.IndexByte(text[start+1:], '`'); j >= 0 {
					name, end = text[start+1:start+1+j], start+j+2
				}
			case isIdent(c) && !(c >= '0' && c <= '9'):
				for end < len(text) && isIdent(text[end]) {
					end++
				}
				name = text[start:end]
			}
			if role, ok := dbs[name]; ok && name != "" && end < len(text) && text[end] == '.' {
				b.WriteString(RolePlaceholder(role))
				sc.i = end
				continue
			}
			if end > start && text[start] != '`' {
				b.WriteString(text[start:end])
				sc.i = end
				continue
			}
		}
		sc.next()
		b.WriteString(text[start:sc.i])
	}
	return b.String()
}
