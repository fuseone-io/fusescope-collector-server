// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"go.uber.org/zap"
)

type fileStamp struct {
	modTime time.Time
	size    int64
}

// clickhouseClient owns the connection pool of one signal. It recreates the
// pool when the password file changes, so rotating the secret needs no restart.
type clickhouseClient struct {
	cfg      *Config
	database string
	logger   *zap.Logger

	pool  atomic.Pointer[chpool.Pool]
	mu    sync.Mutex
	stamp fileStamp
}

func (c *clickhouseClient) start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dial(ctx)
}

// dial creates a pool from the current configuration; c.mu must be held.
func (c *clickhouseClient) dial(ctx context.Context) error {
	opts, err := c.clientOptions(ctx)
	if err != nil {
		return err
	}
	stamp, _ := c.passwordStamp()
	pool, err := chpool.New(ctx, chpool.Options{ClientOptions: opts, MaxConns: c.cfg.MaxConnections})
	if err != nil {
		return fmt.Errorf("create ClickHouse pool: %w", err)
	}
	c.stamp = stamp
	if old := c.pool.Swap(pool); old != nil {
		// Close waits for inserts still running on the old pool.
		old.Close()
	}
	return nil
}

func (c *clickhouseClient) shutdown(context.Context) error {
	if p := c.pool.Swap(nil); p != nil {
		p.Close()
	}
	return nil
}

func (c *clickhouseClient) passwordStamp() (fileStamp, error) {
	if c.cfg.PasswordFile == "" {
		return fileStamp{}, nil
	}
	fi, err := os.Stat(c.cfg.PasswordFile)
	if err != nil {
		return fileStamp{}, err
	}
	return fileStamp{modTime: fi.ModTime(), size: fi.Size()}, nil
}

// refresh recreates the pool if the password file changed since it was read.
func (c *clickhouseClient) refresh(ctx context.Context) error {
	stamp, err := c.passwordStamp()
	if err != nil {
		return fmt.Errorf("stat password_file: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if stamp == c.stamp {
		return nil
	}
	c.logger.Info("ClickHouse password file changed; reconnecting")
	return c.dial(ctx)
}

func (c *clickhouseClient) clientOptions(ctx context.Context) (ch.Options, error) {
	cfg := c.cfg
	opts := ch.Options{
		Logger:      c.logger,
		Address:     cfg.Endpoint,
		Database:    c.database,
		User:        cfg.Username,
		Compression: ch.CompressionLZ4,
		ClientName:  "fusescope-collector",
		DialTimeout: cfg.DialTimeout,
		ReadTimeout: cfg.Timeout,
	}
	if cfg.PasswordFile != "" {
		b, err := os.ReadFile(cfg.PasswordFile)
		if err != nil {
			return opts, fmt.Errorf("read password_file: %w", err)
		}
		opts.Password = strings.TrimRight(string(b), "\r\n")
	}
	if cfg.TLS.Insecure {
		c.logger.Warn("TLS to ClickHouse is disabled; credentials and telemetry travel in clear text", zap.String("endpoint", cfg.Endpoint))
		return opts, nil
	}
	tlsCfg, err := cfg.TLS.LoadTLSConfig(ctx)
	if err != nil {
		return opts, fmt.Errorf("load TLS config: %w", err)
	}
	if tlsCfg.MinVersion < tls.VersionTLS13 {
		tlsCfg.MinVersion = tls.VersionTLS13
	}
	opts.TLS = tlsCfg
	return opts, nil
}

// insert runs one INSERT. The deduplication token makes ClickHouse drop the
// block when a retry resends the same batch, even though re-encoding changes
// random IDs and insert times; dependent materialized views are deduplicated too.
func (c *clickhouseClient) insert(ctx context.Context, q ch.Query, token string) error {
	q.Settings = append(q.Settings,
		ch.Setting{Key: "insert_deduplicate", Value: "1"},
		ch.Setting{Key: "deduplicate_blocks_in_dependent_materialized_views", Value: "1"},
		ch.Setting{Key: "insert_deduplication_token", Value: token},
	)
	pool := c.pool.Load()
	if pool == nil {
		return fmt.Errorf("ClickHouse pool is not started")
	}
	return pool.Do(ctx, q)
}

// batchToken identifies a batch by its content, so every retry of the same
// batch yields the same token.
func batchToken(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:16])
}
