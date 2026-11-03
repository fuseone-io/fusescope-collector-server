// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package rumreceiver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckv2extension"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/yamlprovider"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/nopexporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"
	"go.uber.org/zap"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/ingestpolicy"
)

// healthStore is the policy the test extension of TestHealthEndpoint holds.
var healthStore *ingestpolicy.Store

var testPolicyType = component.MustNewType("testpolicy")

type testPolicyExtension struct {
	component.StartFunc
	component.ShutdownFunc
}

func (testPolicyExtension) IngestPolicy() ingestpolicy.Source { return healthStore }

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// TestHealthEndpoint runs a real collector with healthcheckv2 configured as
// the chart must (docs/spec/rum.md §11.4): include_recoverable_errors with a
// zero recovery_duration, so a missing or stale policy takes the collector
// out of traffic and a valid one puts it back.
func TestHealthEndpoint(t *testing.T) {
	healthStore = ingestpolicy.NewStore(zap.NewNop(), nil)
	tlsCfg, _ := writeCert(t)
	health := freePort(t)
	config := fmt.Sprintf(`
extensions:
  testpolicy:
  healthcheckv2:
    use_v2: true
    component_health:
      include_recoverable_errors: true
      recovery_duration: 0s
    http:
      endpoint: %s
      status:
        enabled: true
        path: /health/status
receivers:
  rum:
    endpoint: 127.0.0.1:0
    tls:
      cert_file: %s
      key_file: %s
    ingest_policy: testpolicy
    max_staleness: 3s
exporters:
  nop:
service:
  extensions: [testpolicy, healthcheckv2]
  pipelines:
    traces:
      receivers: [rum]
      exporters: [nop]
  telemetry:
    metrics:
      level: none
    logs:
      level: error
`, health, tlsCfg.CertFile, tlsCfg.KeyFile)

	factories := func() (otelcol.Factories, error) {
		f := otelcol.Factories{Telemetry: otelconftelemetry.NewFactory()}
		var err error
		if f.Receivers, err = otelcol.MakeFactoryMap[receiver.Factory](NewFactory()); err != nil {
			return f, err
		}
		if f.Exporters, err = otelcol.MakeFactoryMap[exporter.Factory](nopexporter.NewFactory()); err != nil {
			return f, err
		}
		f.Extensions, err = otelcol.MakeFactoryMap[extension.Factory](
			healthcheckv2extension.NewFactory(),
			extension.NewFactory(testPolicyType, func() component.Config { return &struct{}{} },
				func(context.Context, extension.Settings, component.Config) (extension.Extension, error) {
					return testPolicyExtension{}, nil
				}, component.StabilityLevelDevelopment),
		)
		return f, err
	}
	col, err := otelcol.NewCollector(otelcol.CollectorSettings{
		BuildInfo: component.NewDefaultBuildInfo(),
		Factories: factories,
		ConfigProviderSettings: otelcol.ConfigProviderSettings{ResolverSettings: confmap.ResolverSettings{
			URIs:              []string{"yaml:" + config},
			ProviderFactories: []confmap.ProviderFactory{yamlprovider.NewFactory()},
		}},
	})
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- col.Run(context.Background()) }()
	t.Cleanup(func() {
		col.Shutdown()
		assert.NoError(t, <-done)
	})

	status := func() int {
		resp, err := http.Get("http://" + health + "/health/status")
		if err != nil {
			return 0
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	eventually := func(want int, why string) {
		t.Helper()
		assert.Eventually(t, func() bool { return status() == want }, 15*time.Second, 50*time.Millisecond, why)
	}

	eventually(http.StatusInternalServerError, "not ready without a policy")
	require.NoError(t, healthStore.Apply(encodePolicy(t, testPolicy(1)), time.Now()))
	eventually(http.StatusOK, "ready once a valid policy is held")
	eventually(http.StatusInternalServerError, "not ready once the policy is older than max_staleness")
	require.NoError(t, healthStore.Apply(encodePolicy(t, testPolicy(1)), time.Now()))
	eventually(http.StatusOK, "ready again after a fresh push")
}
