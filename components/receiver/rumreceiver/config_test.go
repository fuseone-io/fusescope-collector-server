// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package rumreceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap"
)

func TestConfigDefaults(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	assert.Equal(t, int64(4<<20), cfg.MaxBodySize)
	assert.Equal(t, Limit{Rate: 500, Burst: 1000}, cfg.RateLimit.Key)
	assert.Equal(t, Limit{Rate: 20, Burst: 60}, cfg.RateLimit.Client)
	assert.Empty(t, cfg.TrustedProxies, "no proxy is trusted by default (G2)")
	assert.Zero(t, cfg.MaxStaleness, "max_staleness has no default")
	assert.Equal(t, LimitsConfig{
		ClientBuckets: 100_000, SessionsPerBucket: 10_000, PendingKeys: 10_000,
		Connections: 4096, Requests: 512,
		HeaderReadTimeout: 10 * time.Second, BodyReadTimeout: 30 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 120 * time.Second, MaxHeaderBytes: 64 << 10,
	}, cfg.Limits)
	assert.Equal(t, "1.3", cfg.TLS.MinVersion)
}

func TestConfigUnmarshal(t *testing.T) {
	conf := confmap.NewFromStringMap(map[string]any{
		"endpoint":        "0.0.0.0:4319",
		"tls":             map[string]any{"cert_file": "/c.pem", "key_file": "/k.pem"},
		"ingest_policy":   "fusescope_opamp",
		"trusted_proxies": []any{"10.0.0.0/8", "2001:db8::/32"},
		"rate_limit":      map[string]any{"client": map[string]any{"rate": 5, "burst": 10}},
		"limits":          map[string]any{"requests": 64},
		"max_staleness":   "1h",
	})
	cfg := createDefaultConfig().(*Config)
	require.NoError(t, conf.Unmarshal(cfg))
	assert.Equal(t, component.MustNewID("fusescope_opamp"), cfg.IngestPolicy)
	assert.Equal(t, Limit{Rate: 5, Burst: 10}, cfg.RateLimit.Client)
	assert.Equal(t, Limit{Rate: 500, Burst: 1000}, cfg.RateLimit.Key)
	assert.Equal(t, 64, cfg.Limits.Requests)
	assert.Equal(t, 4096, cfg.Limits.Connections)
	assert.Equal(t, time.Hour, cfg.MaxStaleness)
	assert.Equal(t, "1.3", cfg.TLS.MinVersion)
	require.NoError(t, cfg.Validate())

	require.Error(t, confmap.NewFromStringMap(map[string]any{"query_key": true}).Unmarshal(createDefaultConfig()), "unknown keys are refused")
}

func TestConfigValidate(t *testing.T) {
	valid := func() *Config {
		cfg := createDefaultConfig().(*Config)
		cfg.TLS = configtls.ServerConfig{Config: configtls.Config{CertFile: "/c.pem", KeyFile: "/k.pem", MinVersion: "1.3"}}
		return cfg
	}
	testCases := []struct {
		name string
		edit func(*Config)
		err  string
	}{
		{name: "Valid", edit: func(*Config) {}},
		{name: "NoTLS", edit: func(c *Config) { c.TLS = configtls.ServerConfig{Config: configtls.Config{MinVersion: "1.3"}} }, err: "tls is required"},
		{name: "TLS12", edit: func(c *Config) { c.TLS.MinVersion = "1.2" }, err: "tls.min_version must be 1.3"},
		// Codex review on #43: credentials from files only.
		{name: "InlineKey", edit: func(c *Config) { c.TLS.KeyFile, c.TLS.KeyPem = "", "-----BEGIN EC PRIVATE KEY-----" }, err: "inline cert_pem, key_pem and ca_pem are refused"},
		{name: "InlineCert", edit: func(c *Config) { c.TLS.CertFile, c.TLS.CertPem = "", "-----BEGIN CERTIFICATE-----" }, err: "inline cert_pem, key_pem and ca_pem are refused"},
		{name: "InlineClientCA", edit: func(c *Config) { c.TLS.CAPem = "-----BEGIN CERTIFICATE-----" }, err: "inline cert_pem, key_pem and ca_pem are refused"},
		{name: "NoKeyFile", edit: func(c *Config) { c.TLS.KeyFile = "" }, err: "tls.cert_file and tls.key_file are required"},
		{name: "NoEndpoint", edit: func(c *Config) { c.Endpoint = "" }, err: "endpoint"},
		{name: "BodySizeZero", edit: func(c *Config) { c.MaxBodySize = 0 }, err: "max_body_size"},
		{name: "KeyRateZero", edit: func(c *Config) { c.RateLimit.Key.Rate = 0 }, err: "rate_limit.key"},
		{name: "ClientBurstZero", edit: func(c *Config) { c.RateLimit.Client.Burst = 0 }, err: "rate_limit.client"},
		{name: "ProxyNotCIDR", edit: func(c *Config) { c.TrustedProxies = []string{"10.0.0.1"} }, err: "not a CIDR"},
		{name: "NegativeStaleness", edit: func(c *Config) { c.MaxStaleness = -time.Second }, err: "max_staleness"},
		{name: "NoRequestLimit", edit: func(c *Config) { c.Limits.Requests = 0 }, err: "limits.requests"},
		{name: "NoBucketCap", edit: func(c *Config) { c.Limits.ClientBuckets = 0 }, err: "limits.client_buckets"},
		{name: "NoHeaderTimeout", edit: func(c *Config) { c.Limits.HeaderReadTimeout = 0 }, err: "limits.header_read_timeout"},
		{name: "NoWriteTimeout", edit: func(c *Config) { c.Limits.WriteTimeout = 0 }, err: "limits.write_timeout"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid()
			tc.edit(cfg)
			err := cfg.Validate()
			if tc.err == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.err)
		})
	}
}
