// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package fusescopeopampextension is the fusescope_opamp extension: its Ready
// confirms that the configuration revision the fusescopeopamp provider
// injected runs, so the provider reports the push APPLIED (ADR 0008).
package fusescopeopampextension // import "github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensioncapabilities"

	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/agent"
	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/metadata"
)

// Config is set by the provider; nothing in it is meant to be written by hand.
type Config struct {
	// Revision is the configuration revision this collector instance runs.
	Revision string `mapstructure:"revision"`
}

// NewFactory returns the fusescope_opamp extension factory.
func NewFactory() extension.Factory {
	return extension.NewFactory(metadata.Type, func() component.Config { return &Config{} },
		func(_ context.Context, _ extension.Settings, cfg component.Config) (extension.Extension, error) {
			return &watcher{revision: cfg.(*Config).Revision}, nil
		}, metadata.ExtensionStability)
}

type watcher struct{ revision string }

var _ extensioncapabilities.PipelineWatcher = (*watcher)(nil)

func (*watcher) Start(context.Context, component.Host) error { return nil }
func (*watcher) Shutdown(context.Context) error              { return nil }

// Ready is called once every pipeline is built and every receiver started.
func (w *watcher) Ready() error {
	if a := agent.Current(); a != nil && w.revision != "" {
		a.Confirm(w.revision)
	}
	return nil
}

func (*watcher) NotReady() error { return nil }
