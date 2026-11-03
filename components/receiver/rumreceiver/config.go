// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package rumreceiver // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver"

import (
	"errors"
	"fmt"
	"net/netip"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configtls"
)

// Config is local configuration only: listeners, limits and trusted proxies
// never come over OpAMP (ADR 0006 decision 3).
type Config struct {
	// Endpoint is the TLS listener for /rum/v1/traces and /rum/v1/logs.
	Endpoint string `mapstructure:"endpoint"`
	// TLS is required, at least 1.3 (ADR 0004).
	TLS configtls.ServerConfig `mapstructure:"tls"`
	// IngestPolicy names the extension holding the ingest policy. Unset, the
	// receiver refuses everything and reports not ready (§11.4).
	IngestPolicy component.ID `mapstructure:"ingest_policy"`
	// MaxBodySize applies to the wire and to the decompressed body (§6).
	MaxBodySize int64 `mapstructure:"max_body_size"`
	// RateLimit.Key applies when the policy gives a key no limit (§7).
	RateLimit RateLimitConfig `mapstructure:"rate_limit"`
	// TrustedProxies are the CIDRs X-Forwarded-For is read from; empty
	// trusts none (§7, G2).
	TrustedProxies []string `mapstructure:"trusted_proxies"`
	// MaxStaleness, when set, refuses requests once the policy is older (§11.4).
	MaxStaleness time.Duration `mapstructure:"max_staleness"`
	// Limits are the bounds of §7.1.
	Limits LimitsConfig `mapstructure:"limits"`

	_ struct{}
}

// Limit is a token bucket: rate per second, up to burst.
type Limit struct {
	Rate  float64 `mapstructure:"rate"`
	Burst int64   `mapstructure:"burst"`
}

type RateLimitConfig struct {
	Key    Limit `mapstructure:"key"`
	Client Limit `mapstructure:"client"`
}

type LimitsConfig struct {
	ClientBuckets     int           `mapstructure:"client_buckets"`
	SessionsPerBucket int           `mapstructure:"sessions_per_bucket"`
	PendingKeys       int           `mapstructure:"pending_keys"`
	Connections       int           `mapstructure:"connections"`
	Requests          int           `mapstructure:"requests"`
	HeaderReadTimeout time.Duration `mapstructure:"header_read_timeout"`
	BodyReadTimeout   time.Duration `mapstructure:"body_read_timeout"`
	WriteTimeout      time.Duration `mapstructure:"write_timeout"`
	IdleTimeout       time.Duration `mapstructure:"idle_timeout"`
	MaxHeaderBytes    int           `mapstructure:"max_header_bytes"`
	// ResourcesPerRequest bounds the resource groups of one request, each
	// of which processing stamps with several attributes.
	ResourcesPerRequest int `mapstructure:"resources_per_request"`
}

const minTLSVersion = "1.3"

func createDefaultConfig() component.Config {
	return &Config{
		Endpoint:    "localhost:4319",
		TLS:         configtls.ServerConfig{Config: configtls.Config{MinVersion: minTLSVersion}},
		MaxBodySize: 4 << 20,
		RateLimit: RateLimitConfig{
			Key:    Limit{Rate: 500, Burst: 1000},
			Client: Limit{Rate: 20, Burst: 60},
		},
		Limits: LimitsConfig{
			ClientBuckets:       100_000,
			SessionsPerBucket:   10_000,
			PendingKeys:         10_000,
			Connections:         4096,
			Requests:            512,
			HeaderReadTimeout:   10 * time.Second,
			BodyReadTimeout:     30 * time.Second,
			WriteTimeout:        30 * time.Second,
			IdleTimeout:         120 * time.Second,
			MaxHeaderBytes:      64 << 10,
			ResourcesPerRequest: 1000,
		},
	}
}

// Validate implements xconfmap.Validator.
func (c *Config) Validate() error {
	var errs []error
	if c.Endpoint == "" {
		errs = append(errs, errors.New("endpoint must be set"))
	}
	if err := c.TLS.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("tls is required on the RUM listener (ADR 0004): %w", err))
	}
	// Credentials come from files only (ADR 0004): an inline key would be in
	// the configuration the OpAMP extension reports as effective.
	if len(c.TLS.KeyPem) > 0 || len(c.TLS.CertPem) > 0 || len(c.TLS.CAPem) > 0 {
		errs = append(errs, errors.New("tls: use cert_file and key_file; inline cert_pem, key_pem and ca_pem are refused (ADR 0004)"))
	}
	if c.TLS.CertFile == "" || c.TLS.KeyFile == "" {
		errs = append(errs, errors.New("tls.cert_file and tls.key_file are required"))
	}
	if c.TLS.MinVersion != minTLSVersion {
		errs = append(errs, fmt.Errorf("tls.min_version must be %s (ADR 0004)", minTLSVersion))
	}
	if c.MaxBodySize <= 0 {
		errs = append(errs, errors.New("max_body_size must be positive"))
	}
	for name, l := range map[string]Limit{"rate_limit.key": c.RateLimit.Key, "rate_limit.client": c.RateLimit.Client} {
		if !(l.Rate > 0) || l.Burst < 1 {
			errs = append(errs, fmt.Errorf("%s needs a positive rate and a burst of at least 1", name))
		}
	}
	if _, err := parsePrefixes(c.TrustedProxies); err != nil {
		errs = append(errs, err)
	}
	if c.MaxStaleness < 0 {
		errs = append(errs, errors.New("max_staleness must not be negative"))
	}
	l := c.Limits
	for name, v := range map[string]int{
		"limits.client_buckets": l.ClientBuckets, "limits.sessions_per_bucket": l.SessionsPerBucket,
		"limits.pending_keys": l.PendingKeys, "limits.connections": l.Connections,
		"limits.requests": l.Requests, "limits.max_header_bytes": l.MaxHeaderBytes,
		"limits.resources_per_request": l.ResourcesPerRequest,
	} {
		if v <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive", name))
		}
	}
	for name, v := range map[string]time.Duration{
		"limits.header_read_timeout": l.HeaderReadTimeout, "limits.body_read_timeout": l.BodyReadTimeout,
		"limits.write_timeout": l.WriteTimeout, "limits.idle_timeout": l.IdleTimeout,
	} {
		if v <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive and finite", name))
		}
	}
	return errors.Join(errs...)
}

func parsePrefixes(cidrs []string) ([]netip.Prefix, error) {
	out := make([]netip.Prefix, 0, len(cidrs))
	for _, s := range cidrs {
		p, err := netip.ParsePrefix(s)
		if err != nil {
			return nil, fmt.Errorf("trusted_proxies: %q is not a CIDR", s)
		}
		out = append(out, p.Masked())
	}
	return out, nil
}
