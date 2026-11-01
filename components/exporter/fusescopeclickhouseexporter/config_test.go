// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.uber.org/zap"
)

func loadConfig(t *testing.T, name string) *Config {
	t.Helper()
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := cm.Sub(name)
	if err != nil {
		t.Fatal(err)
	}
	cfg := createDefaultConfig()
	if err := sub.Unmarshal(cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		wantErr string
	}{
		{name: "fusescope_clickhouse"},
		{name: "fusescope_clickhouse/full"},
		{name: "fusescope_clickhouse/insecure"},
		{name: "fusescope_clickhouse/bad_database", wantErr: "not a valid ClickHouse identifier"},
		{name: "fusescope_clickhouse/old_tls", wantErr: "tls.min_version must be at least 1.3"},
		{name: "fusescope_clickhouse/no_port", wantErr: "endpoint must be host:port"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := loadConfig(t, tt.name).Validate()
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultsAreSecure(t *testing.T) {
	cfg := createDefaultConfig()
	if cfg.TLS.Insecure || cfg.TLS.MinVersion != "1.3" {
		t.Fatalf("default TLS = insecure %v, min %q; want TLS 1.3", cfg.TLS.Insecure, cfg.TLS.MinVersion)
	}
}

func TestClientOptions(t *testing.T) {
	cfg := loadConfig(t, "fusescope_clickhouse/full")
	cfg.PasswordFile = filepath.Join("testdata", "password")
	cfg.TLS.CAFile = ""
	opts, err := newTracesExporter(cfg, zap.NewNop()).clientOptions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if opts.Password != "test-password" {
		t.Errorf("password = %q, want the file content without its trailing newline", opts.Password)
	}
	if opts.TLS == nil {
		t.Error("TLS is off, want on by default")
	}
	if opts.User != "collector" || opts.Database != "shadow_traces" || opts.DialTimeout != 2*time.Second {
		t.Errorf("options = %+v", opts)
	}

	cfg.PasswordFile = filepath.Join("testdata", "missing")
	if _, err := newTracesExporter(cfg, zap.NewNop()).clientOptions(context.Background()); err == nil {
		t.Error("missing password_file accepted")
	}
}

func TestInsertBodiesUseConfiguredDatabase(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.Traces.Database = "shadow_traces"
	for _, body := range newTracesExporter(cfg, zap.NewNop()).bodies {
		if !strings.HasPrefix(body, "INSERT INTO shadow_traces.distributed_") || !strings.HasSuffix(body, " VALUES") {
			t.Errorf("body = %q", body)
		}
	}
}
