// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package preflight

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"
)

type testCfg struct {
	FailStart bool `mapstructure:"fail_start"`
	Hang      bool `mapstructure:"hang"`
}

var (
	testType  = component.MustNewType("signozlogspipeline")
	shutdowns atomic.Int32
)

func testFactory() processor.Factory {
	return processor.NewFactory(testType, func() component.Config { return &testCfg{} },
		processor.WithLogs(func(ctx context.Context, set processor.Settings, c component.Config, next consumer.Logs) (processor.Logs, error) {
			cfg := c.(*testCfg)
			return processorhelper.NewLogs(ctx, set, c, next,
				func(_ context.Context, ld plog.Logs) (plog.Logs, error) { return ld, nil },
				processorhelper.WithStart(func(context.Context, component.Host) error {
					if cfg.Hang {
						time.Sleep(200 * time.Millisecond)
					}
					if cfg.FailStart {
						return errors.New("cannot start")
					}
					return nil
				}),
				processorhelper.WithShutdown(func(context.Context) error { shutdowns.Add(1); return nil }))
		}, component.StabilityLevelAlpha))
}

func TestCheck(t *testing.T) {
	factories := map[component.Type]processor.Factory{testType: testFactory()}
	pipelines := map[string][]string{"logs": {"signozlogspipeline/pipeline_a"}}
	tests := []struct {
		name    string
		raw     map[string]any
		types   map[component.Type]processor.Factory
		wantErr string
	}{
		{"good", map[string]any{}, factories, ""},
		{"fails at start", map[string]any{"fail_start": true}, factories, "cannot start"},
		{"unknown field", map[string]any{"no_such": 1}, factories, "no_such"},
		{"type not in this build", map[string]any{}, map[component.Type]processor.Factory{}, "not in this build"},
		{"start outlasts the timeout", map[string]any{"hang": true}, factories, "within"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := shutdowns.Load()
			err := Check(context.Background(), zap.NewNop(), tt.types,
				map[string]any{"signozlogspipeline/pipeline_a": tt.raw}, pipelines, 50*time.Millisecond)
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("Check = %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("Check = %v, want an error with %q", err, tt.wantErr)
			}
			// Every processor that was created is shut down, also after a timeout.
			if tt.types[testType] != nil && tt.name != "unknown field" {
				deadline := time.Now().Add(2 * time.Second)
				for shutdowns.Load() == before && time.Now().Before(deadline) {
					time.Sleep(10 * time.Millisecond)
				}
				if shutdowns.Load() == before {
					t.Error("processor was not shut down")
				}
			}
		})
	}
}

// A declaration no pipeline lists is still decoded by the collector, so it
// must be valid; it is not started.
func TestUnusedDeclarations(t *testing.T) {
	unused := map[string][]string{"logs": {"batch"}}
	factories := map[component.Type]processor.Factory{testType: testFactory()}
	tests := []struct {
		name    string
		types   map[component.Type]processor.Factory
		raw     map[string]any
		wantErr bool
	}{
		{"valid", factories, map[string]any{}, false},
		{"not started, even if it would fail", factories, map[string]any{"fail_start": true}, false},
		{"type not in this build", map[component.Type]processor.Factory{}, map[string]any{}, true},
		{"unknown field", factories, map[string]any{"no_such": 1}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Check(context.Background(), zap.NewNop(), tt.types,
				map[string]any{"signozlogspipeline/pipeline_a": tt.raw}, unused, time.Second)
			if (err != nil) != tt.wantErr {
				t.Errorf("Check = %v, want error %v", err, tt.wantErr)
			}
		})
	}
}
