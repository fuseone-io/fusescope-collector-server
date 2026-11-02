// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozllmpricingprocessor // import "github.com/fuseone-io/fusescope-collector-server/components/processor/signozllmpricingprocessor"

import (
	"errors"
	"fmt"
	"path"
)

// Config is the block the FuseScope backend pushes (signozllmpricing spec §2).
type Config struct {
	Attrs          Attrs          `mapstructure:"attrs"`
	DefaultPricing DefaultPricing `mapstructure:"default_pricing"`
	OutputAttrs    OutputAttrs    `mapstructure:"output_attrs"`
}

// Attrs names the span attributes read; an empty name reads nothing.
type Attrs struct {
	Model      string `mapstructure:"model"`
	In         string `mapstructure:"in"`
	Out        string `mapstructure:"out"`
	CacheRead  string `mapstructure:"cache_read"`
	CacheWrite string `mapstructure:"cache_write"`
}

// DefaultPricing holds the rules, first match wins.
type DefaultPricing struct {
	Rules []Rule `mapstructure:"rules"`
}

// Rule prices the models matching any of its patterns, in USD per million tokens.
type Rule struct {
	Name    string   `mapstructure:"name"`
	Pattern []string `mapstructure:"pattern"`
	Cache   *Cache   `mapstructure:"cache"`
	In      float64  `mapstructure:"in"`
	Out     float64  `mapstructure:"out"`
}

// Cache holds the cache token prices. Mode is validated but does not change
// the cost (spec §4).
type Cache struct {
	Mode  string  `mapstructure:"mode"`
	Read  float64 `mapstructure:"read"`
	Write float64 `mapstructure:"write"`
}

// OutputAttrs names the span attributes written; an empty name except Total
// writes nothing.
type OutputAttrs struct {
	In         string `mapstructure:"in"`
	Out        string `mapstructure:"out"`
	CacheRead  string `mapstructure:"cache_read"`
	CacheWrite string `mapstructure:"cache_write"`
	Total      string `mapstructure:"total"`
}

// Validate refuses what the reference refuses (spec §2.1).
func (cfg *Config) Validate() error {
	var errs []error
	if cfg.Attrs.Model == "" {
		errs = append(errs, errors.New("attrs.model must not be empty"))
	}
	if cfg.OutputAttrs.Total == "" {
		errs = append(errs, errors.New("output_attrs.total must not be empty"))
	}
	for i, r := range cfg.DefaultPricing.Rules {
		at := fmt.Sprintf("default_pricing.rules[%d]", i)
		if len(r.Pattern) == 0 {
			errs = append(errs, fmt.Errorf("%s: pattern must list at least one pattern", at))
		}
		for j, p := range r.Pattern {
			if p == "" {
				errs = append(errs, fmt.Errorf("%s.pattern[%d] must not be empty", at, j))
			} else if _, err := path.Match(p, ""); err != nil {
				errs = append(errs, fmt.Errorf("%s.pattern[%d] %q is not a valid glob: %w", at, j, p, err))
			}
		}
		if r.In < 0 || r.Out < 0 {
			errs = append(errs, fmt.Errorf("%s: in and out must not be negative", at))
		}
		if c := r.Cache; c != nil {
			if c.Read < 0 || c.Write < 0 {
				errs = append(errs, fmt.Errorf("%s: cache.read and cache.write must not be negative", at))
			}
			switch c.Mode {
			case "", "subtract", "additive":
			default:
				errs = append(errs, fmt.Errorf("%s: cache.mode must be subtract, additive or empty, not %q", at, c.Mode))
			}
		}
	}
	return errors.Join(errs...)
}
