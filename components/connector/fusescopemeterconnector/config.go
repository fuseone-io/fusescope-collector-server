// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopemeterconnector // import "github.com/fuseone-io/fusescope-collector-server/components/connector/fusescopemeterconnector"

import (
	"errors"
	"time"
)

// Config defines the fusescope_meter connector (docs/spec/meter.md).
type Config struct {
	// FlushInterval is how often the counts are emitted, from the start (meter §5).
	FlushInterval time.Duration `mapstructure:"flush_interval"`
	// MaxSeries and MaxSeriesBytes bound the label sets held between flushes (meter §7).
	MaxSeries      int `mapstructure:"max_series"`
	MaxSeriesBytes int `mapstructure:"max_series_bytes"`
	// MaxAuthenticatedKeys bounds the per-key overflow sets (meter §7).
	MaxAuthenticatedKeys int `mapstructure:"max_authenticated_keys"`
	// MaxLabelBytes is the longest label value kept (meter §7).
	MaxLabelBytes int `mapstructure:"max_label_bytes"`
}

func defaultConfig() *Config {
	return &Config{
		FlushInterval:        time.Hour,
		MaxSeries:            10_000,
		MaxSeriesBytes:       4 << 20,
		MaxAuthenticatedKeys: 1_000,
		MaxLabelBytes:        256,
	}
}

func (cfg *Config) Validate() error {
	var errs []error
	if cfg.FlushInterval <= 0 {
		errs = append(errs, errors.New("flush_interval must be positive"))
	}
	if cfg.MaxSeries < 1 {
		errs = append(errs, errors.New("max_series must be at least 1"))
	}
	if cfg.MaxSeriesBytes < 1 {
		errs = append(errs, errors.New("max_series_bytes must be at least 1"))
	}
	if cfg.MaxAuthenticatedKeys < 0 {
		errs = append(errs, errors.New("max_authenticated_keys must not be negative"))
	}
	if cfg.MaxLabelBytes < 4 {
		errs = append(errs, errors.New("max_label_bytes must be at least 4, the longest UTF-8 character"))
	}
	return errors.Join(errs...)
}
