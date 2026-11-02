// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"
	"crypto/tls"
	"testing"

	"go.opentelemetry.io/collector/config/configtls"
)

func TestSettings(t *testing.T) {
	tests := []struct {
		name    string
		s       Settings
		wantErr bool
	}{
		{"wss", Settings{Endpoint: "wss://b:4320/v1/opamp", Storage: "/s"}, false},
		{"ws needs insecure", Settings{Endpoint: "ws://b:4320/v1/opamp", Storage: "/s"}, true},
		{"ws with insecure", Settings{Endpoint: "ws://b:4320/v1/opamp", Storage: "/s", TLS: configtls.ClientConfig{Insecure: true}}, false},
		{"wss with insecure", Settings{Endpoint: "wss://b:4320/v1/opamp", Storage: "/s", TLS: configtls.ClientConfig{Insecure: true}}, true},
		{"TLS 1.2 refused", Settings{Endpoint: "wss://b:4320/v1/opamp", Storage: "/s",
			TLS: configtls.ClientConfig{Config: configtls.Config{MinVersion: "1.2"}}}, true},
		{"no storage", Settings{Endpoint: "wss://b:4320/v1/opamp"}, true},
		{"http endpoint", Settings{Endpoint: "http://b:4320/v1/opamp", Storage: "/s"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.s.validate(); (err != nil) != tt.wantErr {
				t.Errorf("validate = %v, want error %v", err, tt.wantErr)
			}
		})
	}
}

// Without min_version the connection still requires TLS 1.3.
func TestTLSFloor(t *testing.T) {
	cfg, err := clientTLS(context.Background(), &Settings{Endpoint: "wss://b/v1/opamp", Storage: "/s"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion = %x, want TLS 1.3", cfg.MinVersion)
	}
}

// The provider's own section resolves environment variables, since the
// collector never sees it.
func TestSettingsResolveEnv(t *testing.T) {
	t.Setenv("OPAMP_ENDPOINT", "wss://backend:4320/v1/opamp")
	t.Setenv("OPAMP_STORAGE", "/var/lib/opamp")
	base, s, err := split([]byte(`
fusescope_opamp:
  endpoint: ${env:OPAMP_ENDPOINT}
  storage: ${OPAMP_STORAGE}
  tls: {server_name_override: "literal $$dollar"}
receivers: {otlp: {}}
`))
	if err != nil {
		t.Fatal(err)
	}
	if s.Endpoint != "wss://backend:4320/v1/opamp" || s.Storage != "/var/lib/opamp" || s.TLS.ServerName != "literal $dollar" {
		t.Errorf("settings %+v, want the variables resolved and $$ unescaped", s)
	}
	if _, ok := base[settingsKey]; ok {
		t.Error("the settings section reached the collector")
	}
}
