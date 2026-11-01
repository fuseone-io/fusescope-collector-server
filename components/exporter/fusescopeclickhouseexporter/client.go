// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package fusescopeclickhouseexporter // import "github.com/fuseone-io/fusescope-collector-server/components/exporter/fusescopeclickhouseexporter"

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
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
	// do replaces the pool's query in tests.
	do func(context.Context, ch.Query) error
}

func (c *clickhouseClient) start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dial(ctx)
}

// dial creates a pool from the current configuration; c.mu must be held.
func (c *clickhouseClient) dial(ctx context.Context) error {
	opts, stamp, err := c.options(ctx)
	if err != nil {
		return err
	}
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
	opts, _, err := c.options(ctx)
	return opts, err
}

// readPassword reads the password file and stamps the file it read, from the
// same open file: a replacement right after the read changes the stamp seen by
// the next refresh.
func readPassword(path string) (string, fileStamp, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fileStamp{}, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return "", fileStamp{}, err
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return "", fileStamp{}, err
	}
	return strings.TrimRight(string(b), "\r\n"), fileStamp{modTime: fi.ModTime(), size: fi.Size()}, nil
}

// options builds the client options and the stamp of the password file they hold.
func (c *clickhouseClient) options(ctx context.Context) (ch.Options, fileStamp, error) {
	cfg := c.cfg
	var stamp fileStamp
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
		password, s, err := readPassword(cfg.PasswordFile)
		if err != nil {
			return opts, stamp, fmt.Errorf("read password_file: %w", err)
		}
		opts.Password, stamp = password, s
	}
	if cfg.TLS.Insecure {
		c.logger.Warn("TLS to ClickHouse is disabled; credentials and telemetry travel in clear text", zap.String("endpoint", cfg.Endpoint))
		return opts, stamp, nil
	}
	tlsCfg, err := cfg.TLS.LoadTLSConfig(ctx)
	if err != nil {
		return opts, stamp, fmt.Errorf("load TLS config: %w", err)
	}
	if tlsCfg.MinVersion < tls.VersionTLS13 {
		tlsCfg.MinVersion = tls.VersionTLS13
	}
	opts.TLS = tlsCfg
	return opts, stamp, nil
}

// insert runs one INSERT. The deduplication token, unless empty, makes
// ClickHouse drop the block when a retry resends it; dependent materialized
// views are deduplicated too.
func (c *clickhouseClient) insert(ctx context.Context, q ch.Query, token string) error {
	q.Settings = append(q.Settings,
		ch.Setting{Key: "insert_deduplicate", Value: "1"},
		ch.Setting{Key: "deduplicate_blocks_in_dependent_materialized_views", Value: "1"},
	)
	if token != "" {
		q.Settings = append(q.Settings, ch.Setting{Key: "insert_deduplication_token", Value: token})
	}
	if c.do != nil {
		return c.do(ctx, q)
	}
	pool := c.pool.Load()
	if pool == nil {
		return fmt.Errorf("ClickHouse pool is not started")
	}
	return pool.Do(ctx, q)
}
