// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package fusescopeopampextension is the fusescope_opamp extension: its Ready
// confirms that the configuration revision the fusescopeopamp provider
// injected runs, so the provider reports the push APPLIED (ADR 0008).
package fusescopeopampextension // import "github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/extensioncapabilities"

	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/agent"
	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/metadata"
	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
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

// Start tells the provider's agent that the collector really runs, as
// opposed to validating its configuration.
func (*watcher) Start(ctx context.Context, _ component.Host) error {
	if a := agent.Current(); a != nil {
		return a.Activate(ctx)
	}
	return nil
}

func (*watcher) Shutdown(context.Context) error { return nil }

// Ready is called once every pipeline is built and every receiver started.
func (w *watcher) Ready() error {
	if a := agent.Current(); a != nil && w.revision != "" {
		a.Confirm(w.revision)
	}
	return nil
}

func (*watcher) NotReady() error { return nil }

var (
	_ ingestpolicy.Provider         = (*watcher)(nil)
	_ ingestpolicy.ReporterProvider = (*watcher)(nil)
)

// IngestPolicy is the ingest policy for receivers (ADR 0006). Without an
// ingress-role OpAMP agent it is a source that never holds a policy, so
// they fail closed.
func (*watcher) IngestPolicy() ingestpolicy.Source {
	if a := agent.Current(); a != nil && a.Ingest() != nil {
		return a.Ingest().Source()
	}
	return noPolicy{}
}

// IngestReporter carries the ingest report, nil without an ingress agent.
func (*watcher) IngestReporter() ingestpolicy.Reporter {
	if a := agent.Current(); a != nil && a.Ingest() != nil {
		return a.Ingest().Reporter()
	}
	return nil
}

type noPolicy struct{}

var never = make(chan struct{})

func (noPolicy) Current() *ingestpolicy.Snapshot { return nil }
func (noPolicy) LastReceived() time.Time         { return time.Time{} }
func (noPolicy) Changed() <-chan struct{}        { return never }
func (noPolicy) Status() ingestpolicy.Status     { return ingestpolicy.Status{} }
