// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package chstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Dial refuses an invalid configuration; a valid one is accepted (the pool
// connects on first use).
func TestDialValidatesTheConfig(t *testing.T) {
	valid := Config{Endpoint: "127.0.0.1:1", Username: "default", Cluster: "cluster", TLS: TLSConfig{Insecure: true}, DialTimeout: time.Second}
	tests := []struct {
		name   string
		change func(*Config)
		want   error
	}{
		{"an endpoint without a port", func(c *Config) { c.Endpoint = "clickhouse" }, ErrInvalidConfig},
		{"no username", func(c *Config) { c.Username = "" }, ErrInvalidConfig},
		{"no cluster", func(c *Config) { c.Cluster = "" }, ErrInvalidConfig},
		{"insecure with a CA file", func(c *Config) { c.TLS.CAFile = "/nonexistent/ca.pem" }, ErrInvalidConfig},
		{"insecure without verification", func(c *Config) { c.TLS.InsecureSkipVerify = true }, ErrInvalidConfig},
		{"valid", func(*Config) {}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.change(&cfg)
			c, err := Dial(context.Background(), cfg)
			if c != nil {
				c.Close()
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("Dial = %v, want %v", err, tt.want)
			}
		})
	}
}
