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
		Connections: 4096, Requests: 16,
		HeaderReadTimeout: 10 * time.Second, BodyReadTimeout: 30 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 120 * time.Second, MaxHeaderBytes: 64 << 10,
		ResourcesPerRequest: 1000, ScopesPerResource: 100, ItemsPerRequest: 10_000, EventsPerSpan: 128,
		LinksPerSpan: 128, AttributesPerEntity: 128, ElementsPerRequest: 25_000, ValueDepth: 8,
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
		// Maintainer review of #43: report and structure bounds only go down.
		{name: "PendingKeysOverMax", edit: func(c *Config) { c.Limits.PendingKeys = 10_001 }, err: "limits.pending_keys must be between 1 and 10000"},
		{name: "PendingKeysLowered", edit: func(c *Config) { c.Limits.PendingKeys = 100 }},
		{name: "PendingKeysZero", edit: func(c *Config) { c.Limits.PendingKeys = 0 }, err: "limits.pending_keys"},
		{name: "SessionsOverMax", edit: func(c *Config) { c.Limits.SessionsPerBucket = 10_001 }, err: "limits.sessions_per_bucket must be between 1 and 10000"},
		{name: "ResourcesOverMax", edit: func(c *Config) { c.Limits.ResourcesPerRequest = 1001 }, err: "limits.resources_per_request must be between 1 and 1000"},
		{name: "ResourcesLowered", edit: func(c *Config) { c.Limits.ResourcesPerRequest = 10 }},
		{name: "ScopesOverMax", edit: func(c *Config) { c.Limits.ScopesPerResource = 101 }, err: "limits.scopes_per_resource"},
		{name: "ItemsOverMax", edit: func(c *Config) { c.Limits.ItemsPerRequest = 10_001 }, err: "limits.items_per_request"},
		{name: "EventsOverMax", edit: func(c *Config) { c.Limits.EventsPerSpan = 129 }, err: "limits.events_per_span"},
		{name: "LinksOverMax", edit: func(c *Config) { c.Limits.LinksPerSpan = 129 }, err: "limits.links_per_span"},
		{name: "AttributesOverMax", edit: func(c *Config) { c.Limits.AttributesPerEntity = 129 }, err: "limits.attributes_per_entity"},
		{name: "ElementsOverMax", edit: func(c *Config) { c.Limits.ElementsPerRequest = 25_001 }, err: "limits.elements_per_request"},
		{name: "ValueDepthOverMax", edit: func(c *Config) { c.Limits.ValueDepth = 9 }, err: "limits.attribute_value_depth"},
		{name: "ValueDepthZero", edit: func(c *Config) { c.Limits.ValueDepth = 0 }, err: "limits.attribute_value_depth"},
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
