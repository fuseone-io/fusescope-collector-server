// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package preflight builds the allow-listed processors of a merged
// configuration on their own before the collector reloads it (ADR 0008,
// reload lifecycle), so a push that would fail is rejected while the
// running graph is untouched.
package preflight // import "github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/preflight"

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/processor"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

// Check builds, starts and shuts down every processor of procs, in the
// signal of each pipeline that lists it, within timeout each. A processor
// whose Start outlasts timeout fails the check; its Shutdown still runs
// once Start returns.
func Check(ctx context.Context, logger *zap.Logger, factories map[component.Type]processor.Factory,
	procs map[string]any, pipelines map[string][]string, timeout time.Duration,
) error {
	var errs []error
	for name, raw := range procs {
		signals := signalsOf(name, pipelines)
		if len(signals) == 0 {
			continue // declared but unused: the collector does not build it
		}
		for _, sig := range signals {
			if err := check(ctx, logger, factories, name, raw, sig, timeout); err != nil {
				errs = append(errs, fmt.Errorf("processor %q in %s pipelines: %w", name, sig, err))
			}
		}
	}
	return errors.Join(errs...)
}

func signalsOf(name string, pipelines map[string][]string) []pipeline.Signal {
	var out []pipeline.Signal
	seen := map[pipeline.Signal]bool{}
	for p, procs := range pipelines {
		for _, proc := range procs {
			if proc != name {
				continue
			}
			var sig pipeline.Signal
			if err := sig.UnmarshalText([]byte(strings.SplitN(p, "/", 2)[0])); err == nil && !seen[sig] {
				seen[sig] = true
				out = append(out, sig)
			}
		}
	}
	return out
}

func check(ctx context.Context, logger *zap.Logger, factories map[component.Type]processor.Factory,
	name string, raw any, sig pipeline.Signal, timeout time.Duration,
) error {
	var id component.ID
	if err := id.UnmarshalText([]byte(name)); err != nil {
		return err
	}
	f, ok := factories[id.Type()]
	if !ok {
		return fmt.Errorf("type %q is not in this build", id.Type())
	}
	cfg := f.CreateDefaultConfig()
	m, _ := raw.(map[string]any)
	if err := confmap.NewFromStringMap(m).Unmarshal(cfg); err != nil {
		return err
	}
	if err := confmap.Validate(cfg); err != nil {
		return err
	}
	set := processor.Settings{
		ID: id,
		TelemetrySettings: component.TelemetrySettings{
			Logger:         logger.With(zap.String("preflight", name)),
			TracerProvider: tracenoop.NewTracerProvider(),
			MeterProvider:  metricnoop.NewMeterProvider(),
		},
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var comp component.Component
	var err error
	switch sig {
	case pipeline.SignalLogs:
		comp, err = f.CreateLogs(ctx, set, cfg, consumertest.NewNop())
	case pipeline.SignalTraces:
		comp, err = f.CreateTraces(ctx, set, cfg, consumertest.NewNop())
	case pipeline.SignalMetrics:
		comp, err = f.CreateMetrics(ctx, set, cfg, consumertest.NewNop())
	default:
		return fmt.Errorf("signal %s is not checked", sig)
	}
	if err != nil {
		return err
	}
	return startAndShutdown(ctx, comp, timeout)
}

// startAndShutdown starts comp within ctx and always shuts it down: at once
// after Start, or once a Start that outlasted ctx returns.
func startAndShutdown(ctx context.Context, comp component.Component, timeout time.Duration) error {
	started := make(chan error, 1)
	go func() { started <- comp.Start(context.Background(), nopHost{}) }()
	shutdown := func() error {
		sctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return comp.Shutdown(sctx)
	}
	select {
	case err := <-started:
		return errors.Join(err, shutdown())
	case <-ctx.Done():
		go func() { <-started; _ = shutdown() }()
		return fmt.Errorf("start did not finish within %s", timeout)
	}
}

type nopHost struct{}

func (nopHost) GetExtensions() map[component.ID]component.Component { return nil }
