// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"time"

	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter/internal/metadata"
)

const minTLSVersion = "1.3"

// Modes: signals writes traces, logs and metrics to their databases; meter
// writes only the usage metrics of fusescope_meter to signoz_meter (meter §6).
const (
	modeSignals = "signals"
	modeMeter   = "meter"
)

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Config defines the fusescope_clickhouse exporter. The password is read from
// PasswordFile only; there is deliberately no inline password field.
type Config struct {
	Endpoint     string                 `mapstructure:"endpoint"`
	Username     string                 `mapstructure:"username"`
	PasswordFile string                 `mapstructure:"password_file"`
	TLS          configtls.ClientConfig `mapstructure:"tls"`
	DialTimeout  time.Duration          `mapstructure:"dial_timeout"`
	// MaxConnections bounds concurrent inserts towards ClickHouse.
	MaxConnections int32 `mapstructure:"max_connections"`
	// Mode is signals or meter.
	Mode    string       `mapstructure:"mode"`
	Traces  SignalConfig `mapstructure:"traces"`
	Logs    SignalConfig `mapstructure:"logs"`
	Metrics SignalConfig `mapstructure:"metrics"`
	// Meter is the database of mode meter.
	Meter SignalConfig `mapstructure:"meter"`
	// AttributesMetadata records attribute sets for related values
	// (docs/spec/attributes-metadata.md); mode signals only.
	AttributesMetadata AttributesMetadataConfig `mapstructure:"attributes_metadata"`

	exporterhelper.TimeoutConfig `mapstructure:",squash"`
	QueueConfig                  configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
	RetryConfig                  configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`
}

// AttributesMetadataConfig bounds the attribute metadata of each signal
// (attributes-metadata §3, §5). The limits apply per signal.
type AttributesMetadataConfig struct {
	Enabled  bool   `mapstructure:"enabled"`
	Database string `mapstructure:"database"`
	// FlushInterval is how often queued rows are inserted.
	FlushInterval       time.Duration `mapstructure:"flush_interval"`
	MaxWindows          int           `mapstructure:"max_windows"`
	MaxSetsPerWindow    int           `mapstructure:"max_sets_per_window"`
	MaxSetsPerResource  int           `mapstructure:"max_sets_per_resource"`
	MaxAttributesPerRow int           `mapstructure:"max_attributes_per_row"`
	MaxRowBytes         int           `mapstructure:"max_row_bytes"`
	MaxQueueRows        int           `mapstructure:"max_queue_rows"`
	MaxQueueBytes       int           `mapstructure:"max_queue_bytes"`
	MaxStateBytes       int           `mapstructure:"max_state_bytes"`
}

func defaultAttributesMetadata() AttributesMetadataConfig {
	return AttributesMetadataConfig{
		Database:            "signoz_metadata",
		FlushInterval:       time.Second,
		MaxWindows:          4,
		MaxSetsPerWindow:    100_000,
		MaxSetsPerResource:  1_000,
		MaxAttributesPerRow: 256,
		MaxRowBytes:         16 << 10,
		MaxQueueRows:        20_000,
		MaxQueueBytes:       8 << 20,
		MaxStateBytes:       32 << 20,
	}
}

func (c *AttributesMetadataConfig) validate() []error {
	var errs []error
	if !identifier.MatchString(c.Database) {
		errs = append(errs, fmt.Errorf("attributes_metadata.database %q is not a valid ClickHouse identifier", c.Database))
	}
	if c.FlushInterval <= 0 {
		errs = append(errs, errors.New("attributes_metadata.flush_interval must be positive"))
	}
	for name, v := range map[string]int{
		"max_windows": c.MaxWindows, "max_sets_per_window": c.MaxSetsPerWindow, "max_sets_per_resource": c.MaxSetsPerResource,
		"max_attributes_per_row": c.MaxAttributesPerRow, "max_queue_rows": c.MaxQueueRows, "max_state_bytes": c.MaxStateBytes,
	} {
		if v < 1 {
			errs = append(errs, fmt.Errorf("attributes_metadata.%s must be at least 1", name))
		}
	}
	if c.MaxRowBytes <= amRowOverhead {
		errs = append(errs, fmt.Errorf("attributes_metadata.max_row_bytes must be more than the row overhead, %d", amRowOverhead))
	}
	if c.MaxQueueBytes < c.MaxRowBytes {
		errs = append(errs, errors.New("attributes_metadata.max_queue_bytes must hold at least one row of max_row_bytes"))
	}
	return errs
}

// SignalConfig selects the database a signal is written to.
type SignalConfig struct {
	Database string `mapstructure:"database"`
}

func createDefaultConfig() *Config {
	tls := configtls.NewDefaultClientConfig()
	tls.MinVersion = minTLSVersion

	timeout := exporterhelper.NewDefaultTimeoutConfig()
	timeout.Timeout = 30 * time.Second

	return &Config{
		Username:           "default",
		TLS:                tls,
		DialTimeout:        5 * time.Second,
		MaxConnections:     8,
		Mode:               modeSignals,
		Traces:             SignalConfig{Database: "signoz_traces"},
		Logs:               SignalConfig{Database: "signoz_logs"},
		Metrics:            SignalConfig{Database: "signoz_metrics"},
		Meter:              SignalConfig{Database: "signoz_meter"},
		AttributesMetadata: defaultAttributesMetadata(),
		TimeoutConfig:      timeout,
		QueueConfig:        metadata.NewDefaultSendingQueueConfig(),
		RetryConfig:        configretry.NewDefaultBackOffConfig(),
	}
}

func (cfg *Config) Validate() error {
	var errs []error
	if _, _, err := net.SplitHostPort(cfg.Endpoint); err != nil {
		errs = append(errs, fmt.Errorf("endpoint must be host:port of the ClickHouse native protocol: %w", err))
	}
	if cfg.Username == "" {
		errs = append(errs, errors.New("username must not be empty"))
	}
	if cfg.Mode != modeSignals && cfg.Mode != modeMeter {
		errs = append(errs, fmt.Errorf("mode %q must be %s or %s", cfg.Mode, modeSignals, modeMeter))
	}
	if cfg.AttributesMetadata.Enabled {
		if cfg.Mode == modeMeter {
			errs = append(errs, errors.New("attributes_metadata must not be enabled in mode meter"))
		}
		errs = append(errs, cfg.AttributesMetadata.validate()...)
	}
	for name, db := range map[string]string{"traces": cfg.Traces.Database, "logs": cfg.Logs.Database, "metrics": cfg.Metrics.Database, "meter": cfg.Meter.Database} {
		if !identifier.MatchString(db) {
			errs = append(errs, fmt.Errorf("%s.database %q is not a valid ClickHouse identifier", name, db))
		}
	}
	if !cfg.TLS.Insecure && cfg.TLS.MinVersion != "" && cfg.TLS.MinVersion < minTLSVersion {
		errs = append(errs, fmt.Errorf("tls.min_version must be at least %s", minTLSVersion))
	}
	if cfg.MaxConnections < 1 {
		errs = append(errs, errors.New("max_connections must be at least 1"))
	}
	return errors.Join(errs...)
}
