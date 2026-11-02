// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopespanmetricsconnector // import "github.com/fuseone-io/fusescope-collector-server/components/connector/fusescopespanmetricsconnector"

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

// Config defines the fusescope_spanmetrics connector (apm-metrics spec).
type Config struct {
	// Dimensions become point attributes of every metric (apm-metrics §3.3).
	Dimensions []Dimension `mapstructure:"dimensions"`
	// LatencyHistogramBuckets are the explicit bounds of signoz_latency.
	LatencyHistogramBuckets []time.Duration `mapstructure:"latency_histogram_buckets"`
	// MetricsFlushInterval is how often the deltas are emitted.
	MetricsFlushInterval time.Duration `mapstructure:"metrics_flush_interval"`
	// MaxSeries and MaxSeriesBytes bound the series kept between flushes,
	// in number and in bytes (apm-metrics §6).
	MaxSeries      int `mapstructure:"max_series"`
	MaxSeriesBytes int `mapstructure:"max_series_bytes"`
}

// Dimension is a span or resource attribute copied to the points.
type Dimension struct {
	Name    string  `mapstructure:"name"`
	Default *string `mapstructure:"default"`
}

// reserved are the attributes the connector sets itself (apm-metrics §3).
var reserved = []string{"service.name", "operation", "span.kind", "status.code", "http.status_code", "db.system", "db.name", "address"}

func defaultConfig() *Config {
	def := func(s string) *string { return &s }
	cfg := &Config{
		LatencyHistogramBuckets: []time.Duration{
			100 * time.Microsecond, time.Millisecond, 2 * time.Millisecond, 6 * time.Millisecond,
			10 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond,
			500 * time.Millisecond, time.Second, 1400 * time.Millisecond, 2 * time.Second, 5 * time.Second,
			10 * time.Second, 20 * time.Second, 40 * time.Second, 60 * time.Second,
		},
		MetricsFlushInterval: 10 * time.Second,
		MaxSeries:            100_000,
		MaxSeriesBytes:       64 << 20,
	}
	cfg.Dimensions = append(cfg.Dimensions,
		Dimension{Name: "service.namespace", Default: def("default")},
		Dimension{Name: "deployment.environment", Default: def("default")})
	for _, name := range []string{"signoz.collector.id", "service.version", "browser.platform", "browser.mobile",
		"k8s.cluster.name", "k8s.node.name", "k8s.namespace.name", "host.name", "host.type", "container.name"} {
		cfg.Dimensions = append(cfg.Dimensions, Dimension{Name: name})
	}
	return cfg
}

func (cfg *Config) Validate() error {
	var errs []error
	seen := map[string]bool{}
	for _, d := range cfg.Dimensions {
		switch {
		case d.Name == "":
			errs = append(errs, errors.New("dimension name must not be empty"))
		case slices.Contains(reserved, d.Name):
			errs = append(errs, fmt.Errorf("dimension %q is set by the connector itself", d.Name))
		case seen[d.Name]:
			errs = append(errs, fmt.Errorf("dimension %q is listed twice", d.Name))
		}
		seen[d.Name] = true
	}
	if len(cfg.LatencyHistogramBuckets) == 0 {
		errs = append(errs, errors.New("latency_histogram_buckets must not be empty"))
	}
	for i, b := range cfg.LatencyHistogramBuckets {
		if b <= 0 || (i > 0 && b <= cfg.LatencyHistogramBuckets[i-1]) {
			errs = append(errs, errors.New("latency_histogram_buckets must be positive and increasing"))
			break
		}
	}
	if cfg.MetricsFlushInterval <= 0 {
		errs = append(errs, errors.New("metrics_flush_interval must be positive"))
	}
	if cfg.MaxSeries < 1 {
		errs = append(errs, errors.New("max_series must be at least 1"))
	}
	if cfg.MaxSeriesBytes < 1 {
		errs = append(errs, errors.New("max_series_bytes must be at least 1"))
	}
	return errors.Join(errs...)
}
