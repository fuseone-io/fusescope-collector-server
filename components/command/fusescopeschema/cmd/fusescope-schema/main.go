// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Command fusescope-schema migrates the telemetry schema
// (docs/spec/fusescope-schema.md).
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/fuseone-io/fusescope-collector-server/components/command/fusescopeschema/internal/cli"
)

// version is set at build time with -ldflags "-X main.version=…".
var version = "dev"

func main() {
	// The first SIGINT or SIGTERM cancels the run: no statement is started
	// after it, and the lease is released. A second one ends the process.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		stop()
	}()
	host, _ := os.Hostname()
	code := cli.Run(ctx, cli.Env{Args: os.Args[1:], Getenv: os.Getenv, Stdout: os.Stdout, Stderr: os.Stderr, Hostname: host, Version: version})
	stop()
	os.Exit(code)
}
