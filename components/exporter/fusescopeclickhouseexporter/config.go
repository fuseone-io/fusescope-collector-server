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

	exporterhelper.TimeoutConfig `mapstructure:",squash"`
	QueueConfig                  configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
	RetryConfig                  configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`
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
		Username:       "default",
		TLS:            tls,
		DialTimeout:    5 * time.Second,
		MaxConnections: 8,
		Mode:           modeSignals,
		Traces:         SignalConfig{Database: "signoz_traces"},
		Logs:           SignalConfig{Database: "signoz_logs"},
		Metrics:        SignalConfig{Database: "signoz_metrics"},
		Meter:          SignalConfig{Database: "signoz_meter"},
		TimeoutConfig:  timeout,
		QueueConfig:    metadata.NewDefaultSendingQueueConfig(),
		RetryConfig:    configretry.NewDefaultBackOffConfig(),
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
