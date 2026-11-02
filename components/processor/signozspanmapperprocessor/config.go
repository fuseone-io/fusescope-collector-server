// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozspanmapperprocessor // import "github.com/fuseone-io/fusescope-collector-server/components/processor/signozspanmapperprocessor"

import (
	"errors"
	"fmt"
)

// Config is the block the FuseScope backend pushes (signozspanmapper spec §2).
type Config struct {
	Groups []Group `mapstructure:"groups"`
}

// Group runs its rules, in order, on spans that match ExistsAny.
type Group struct {
	// ID is a label only: it may be empty or repeated.
	ID         string    `mapstructure:"id"`
	ExistsAny  ExistsAny `mapstructure:"exists_any"`
	Attributes []Rule    `mapstructure:"attributes"`
}

// ExistsAny lists substrings of span (Attributes) or resource (Resource)
// attribute keys.
type ExistsAny struct {
	Attributes []string `mapstructure:"attributes"`
	Resource   []string `mapstructure:"resource"`
}

// Rule writes Target, in the span or the resource per Context, from the first
// present source.
type Rule struct {
	Target  string   `mapstructure:"target"`
	Context string   `mapstructure:"context"`
	Sources []Source `mapstructure:"sources"`
}

// Source is a span attribute key, or a resource attribute key prefixed with
// "resource.".
type Source struct {
	Key    string `mapstructure:"key"`
	Action string `mapstructure:"action"`
}

const (
	ContextAttribute = "attribute"
	ContextResource  = "resource"
	ActionCopy       = "copy"
	ActionMove       = "move"

	resourcePrefix = "resource."
)

func (cfg *Config) Validate() error {
	var errs []error
	for i, g := range cfg.Groups {
		at := fmt.Sprintf("groups[%d] (id %q)", i, g.ID)
		if len(g.ExistsAny.Attributes)+len(g.ExistsAny.Resource) == 0 {
			errs = append(errs, fmt.Errorf("%s: exists_any needs at least one attributes or resource substring", at))
		}
		if hasEmpty(g.ExistsAny.Attributes) {
			errs = append(errs, fmt.Errorf("%s: exists_any.attributes has an empty substring", at))
		}
		if hasEmpty(g.ExistsAny.Resource) {
			errs = append(errs, fmt.Errorf("%s: exists_any.resource has an empty substring", at))
		}
		for j, r := range g.Attributes {
			at := fmt.Sprintf("%s: attributes[%d] (target %q)", at, j, r.Target)
			if r.Target == "" {
				errs = append(errs, fmt.Errorf("%s: target must not be empty", at))
			}
			switch r.Context {
			case "", ContextAttribute, ContextResource:
			default:
				errs = append(errs, fmt.Errorf("%s: context %q is not %q or %q", at, r.Context, ContextAttribute, ContextResource))
			}
			if len(r.Sources) == 0 {
				errs = append(errs, fmt.Errorf("%s: sources must not be empty", at))
			}
			for k, s := range r.Sources {
				at := fmt.Sprintf("%s: sources[%d]", at, k)
				if s.Key == "" {
					errs = append(errs, fmt.Errorf("%s: key must not be empty", at))
				}
				switch s.Action {
				case "", ActionCopy, ActionMove:
				default:
					errs = append(errs, fmt.Errorf("%s: action %q is not %q or %q", at, s.Action, ActionCopy, ActionMove))
				}
			}
		}
	}
	return errors.Join(errs...)
}

func hasEmpty(substrings []string) bool {
	for _, s := range substrings {
		if s == "" {
			return true
		}
	}
	return false
}
