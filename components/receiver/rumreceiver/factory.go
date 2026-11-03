// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package rumreceiver // import "github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver"

import (
	"context"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/fuseone-io/fusescope-collector-server/components/receiver/rumreceiver/internal/metadata"
)

// NewFactory returns the rum receiver factory.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithTraces(createTraces, metadata.TracesStability),
		receiver.WithLogs(createLogs, metadata.LogsStability),
	)
}

// One listener serves both signals of a receiver ID, as otlpreceiver does.
var (
	sharedMu sync.Mutex
	shared   = map[*Config]*rumReceiver{}
)

func getOrCreate(cfg *Config, set receiver.Settings) (*rumReceiver, error) {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if r, ok := shared[cfg]; ok {
		return r, nil
	}
	r, err := newReceiver(cfg, set)
	if err != nil {
		return nil, err
	}
	r.forget = func() {
		sharedMu.Lock()
		defer sharedMu.Unlock()
		delete(shared, cfg)
	}
	shared[cfg] = r
	return r, nil
}

func createTraces(_ context.Context, set receiver.Settings, cfg component.Config, next consumer.Traces) (receiver.Traces, error) {
	r, err := getOrCreate(cfg.(*Config), set)
	if err != nil {
		return nil, err
	}
	if err := r.setTraces(next); err != nil {
		return nil, err
	}
	return r, nil
}

func createLogs(_ context.Context, set receiver.Settings, cfg component.Config, next consumer.Logs) (receiver.Logs, error) {
	r, err := getOrCreate(cfg.(*Config), set)
	if err != nil {
		return nil, err
	}
	if err := r.setLogs(next); err != nil {
		return nil, err
	}
	return r, nil
}
