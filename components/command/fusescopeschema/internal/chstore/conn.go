// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package chstore runs fusescope-schema's statements on ClickHouse: the
// connection (docs/spec/fusescope-schema.md §2) and the coordination store of
// §3 and §6.
package chstore

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"
)

// Config is the connection, as fusescope_clickhouse takes it (§2). The
// password comes only from PasswordFile (ADR 0004).
type Config struct {
	Endpoint     string
	Username     string
	PasswordFile string
	TLS          TLSConfig
	DialTimeout  time.Duration
	Cluster      string
}

// TLSConfig enables TLS 1.3 unless Insecure, the explicit plaintext opt-in of
// ADR 0007. InsecureSkipVerify is the chart's verify: false.
type TLSConfig struct {
	Insecure           bool
	CAFile             string
	InsecureSkipVerify bool
}

// ErrInvalidConfig: the connection configuration is refused before dialing.
var ErrInvalidConfig = errors.New("invalid ClickHouse connection configuration")

func (c Config) Validate() error {
	var errs []error
	if _, _, err := net.SplitHostPort(c.Endpoint); err != nil {
		errs = append(errs, fmt.Errorf("endpoint must be host:port of the ClickHouse native protocol: %w", err))
	}
	if c.Username == "" {
		errs = append(errs, errors.New("username must not be empty"))
	}
	if c.Cluster == "" {
		errs = append(errs, errors.New("cluster must name the cluster in remote_servers"))
	}
	if c.TLS.Insecure && (c.TLS.CAFile != "" || c.TLS.InsecureSkipVerify) {
		errs = append(errs, errors.New("tls.insecure disables TLS: ca_file and insecure_skip_verify do not apply"))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	return nil
}

func (c Config) options() (ch.Options, error) {
	opts := ch.Options{
		Address:     c.Endpoint,
		User:        c.Username,
		Compression: ch.CompressionLZ4,
		ClientName:  "fusescope-schema",
		DialTimeout: c.DialTimeout,
	}
	if c.PasswordFile != "" {
		b, err := os.ReadFile(c.PasswordFile)
		if err != nil {
			return opts, fmt.Errorf("read password_file: %w", err)
		}
		opts.Password = strings.TrimRight(string(b), "\r\n")
	}
	if c.TLS.Insecure {
		return opts, nil
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: c.TLS.InsecureSkipVerify} //nolint:gosec // only with the chart's verify: false
	if c.TLS.CAFile != "" {
		pem, err := os.ReadFile(c.TLS.CAFile)
		if err != nil {
			return opts, fmt.Errorf("read tls.ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return opts, errors.New("tls.ca_file holds no certificate")
		}
		tlsCfg.RootCAs = pool
	}
	opts.TLS = tlsCfg
	return opts, nil
}

// Conn runs statements on one server through a small pool, so the lease
// renewal can run beside a DDL statement.
type Conn struct {
	pool *chpool.Pool
}

func Dial(ctx context.Context, c Config) (*Conn, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	opts, err := c.options()
	if err != nil {
		return nil, err
	}
	pool, err := chpool.New(ctx, chpool.Options{ClientOptions: opts, MaxConns: 4})
	if err != nil {
		return nil, fmt.Errorf("connect to ClickHouse: %w", err)
	}
	return &Conn{pool: pool}, nil
}

func (c *Conn) Close() { c.pool.Close() }

// Exec runs a statement with the given settings.
func (c *Conn) Exec(ctx context.Context, query string, settings ...ch.Setting) error {
	return c.pool.Do(ctx, ch.Query{Body: query, Settings: settings})
}

// Rows runs a query whose n columns are all String, and returns its rows.
func (c *Conn) Rows(ctx context.Context, query string, n int) ([][]string, error) {
	cols := make([]*proto.ColStr, n)
	res := make(proto.Results, n)
	for i := range cols {
		cols[i] = new(proto.ColStr)
		res[i] = proto.ResultColumn{Data: cols[i]}
	}
	var rows [][]string
	err := c.pool.Do(ctx, ch.Query{
		Body:   query,
		Result: res,
		OnResult: func(context.Context, proto.Block) error {
			for r := range cols[0].Rows() {
				row := make([]string, n)
				for i, col := range cols {
					row[i] = col.Row(r)
				}
				rows = append(rows, row)
			}
			for _, col := range cols {
				col.Reset()
			}
			return nil
		},
	})
	return rows, err
}

// quote makes s a ClickHouse string literal.
func quote(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

// ident makes s a ClickHouse identifier.
func ident(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

// isCode reports a ClickHouse exception with one of codes.
func isCode(err error, codes ...proto.Error) bool { return ch.IsErr(err, codes...) }
