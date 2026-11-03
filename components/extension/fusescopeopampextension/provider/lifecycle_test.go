// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/open-telemetry/opamp-go/server"
	srvtypes "github.com/open-telemetry/opamp-go/server/types"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/nopexporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/nopreceiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"
	"go.uber.org/zap"
	yaml "go.yaml.in/yaml/v3"

	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension"
	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/agent"
)

// --- a managed processor for the tests, under the log pipelines' type ---

type testProcCfg struct {
	FailStart   bool `mapstructure:"fail_start"`
	FailInGraph bool `mapstructure:"fail_in_graph"`
}

var (
	logsPipelineType = component.MustNewType("signozlogspipeline")
	graphStarts      atomic.Int32
)

// testProcessor fails at every Start with fail_start, and with
// fail_in_graph at every second Start: preflight passes, the graph fails.
func testProcessor() processor.Factory {
	return processor.NewFactory(logsPipelineType, func() component.Config { return &testProcCfg{} },
		processor.WithLogs(func(ctx context.Context, set processor.Settings, c component.Config, next consumer.Logs) (processor.Logs, error) {
			cfg := c.(*testProcCfg)
			return processorhelper.NewLogs(ctx, set, c, next,
				func(_ context.Context, ld plog.Logs) (plog.Logs, error) { return ld, nil },
				processorhelper.WithStart(func(context.Context, component.Host) error {
					switch {
					case cfg.FailStart:
						return errors.New("cannot start")
					case cfg.FailInGraph && graphStarts.Add(1)%2 == 0:
						return errors.New("fails in the real graph")
					}
					return nil
				}))
		}, component.StabilityLevelAlpha))
}

// --- a fake backend, an opamp-go server that pushes like the FuseScope one ---

type backend struct {
	t        *testing.T
	srv      server.OpAMPServer
	endpoint string

	mu        sync.Mutex
	conn      srvtypes.Connection
	first     *protobufs.AgentToServer
	effective []byte
	statuses  []*protobufs.RemoteConfigStatus
	// The ingest capability's traffic (ingest_test.go).
	description   *protobufs.AgentDescription
	describedUID  []byte
	authorization string
	capabilities  *protobufs.CustomCapabilities
	customs       []*protobufs.CustomMessage
}

func startBackend(t *testing.T) *backend {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	b := &backend{t: t, srv: server.New(nil), endpoint: "ws://" + addr + "/v1/opamp"}
	err = b.srv.Start(server.StartSettings{ListenEndpoint: addr, Settings: server.Settings{Callbacks: srvtypes.Callbacks{
		OnConnecting: func(r *http.Request) srvtypes.ConnectionResponse {
			b.mu.Lock()
			b.authorization = r.Header.Get("Authorization")
			b.mu.Unlock()
			return srvtypes.ConnectionResponse{Accept: true, ConnectionCallbacks: srvtypes.ConnectionCallbacks{
				OnMessage: func(_ context.Context, conn srvtypes.Connection, msg *protobufs.AgentToServer) *protobufs.ServerToAgent {
					b.mu.Lock()
					defer b.mu.Unlock()
					b.conn = conn
					if b.first == nil {
						b.first = msg
					}
					if f := msg.GetEffectiveConfig().GetConfigMap().GetConfigMap()[configFile]; f != nil {
						b.effective = f.GetBody()
					}
					if s := msg.GetRemoteConfigStatus(); s != nil {
						b.statuses = append(b.statuses, s)
					}
					if d := msg.GetAgentDescription(); d != nil {
						b.description, b.describedUID = d, msg.InstanceUid
					}
					if c := msg.GetCustomCapabilities(); c != nil {
						b.capabilities = c
					}
					if c := msg.GetCustomMessage(); c != nil {
						b.customs = append(b.customs, c)
					}
					return &protobufs.ServerToAgent{InstanceUid: msg.InstanceUid}
				},
			}}
		},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.srv.Stop(context.Background()) })
	return b
}

// push sends the agent's effective configuration edited by edit.
func (b *backend) push(hash string, edit func(cfg map[string]any)) {
	b.t.Helper()
	b.waitFor("an effective configuration", func() bool { return b.effective != nil && b.conn != nil })
	b.mu.Lock()
	var cfg map[string]any
	if err := yaml.Unmarshal(b.effective, &cfg); err != nil {
		b.t.Fatal(err)
	}
	conn := b.conn
	b.mu.Unlock()
	edit(cfg)
	body, err := yaml.Marshal(cfg)
	if err != nil {
		b.t.Fatal(err)
	}
	err = conn.Send(context.Background(), &protobufs.ServerToAgent{RemoteConfig: &protobufs.AgentRemoteConfig{
		ConfigHash: []byte(hash),
		Config: &protobufs.AgentConfigMap{ConfigMap: map[string]*protobufs.AgentConfigObject{
			configFile: {Body: body, ContentType: "application/x-yaml"},
		}},
	}})
	if err != nil {
		b.t.Fatal(err)
	}
}

// status waits for the status of hash and returns it.
func (b *backend) status(hash string) *protobufs.RemoteConfigStatus {
	b.t.Helper()
	var got *protobufs.RemoteConfigStatus
	b.waitFor("the status of "+hash, func() bool {
		for _, s := range b.statuses {
			if string(s.GetLastRemoteConfigHash()) == hash && s.GetStatus() != protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLYING {
				got = s
				return true
			}
		}
		return false
	})
	return got
}

func (b *backend) waitFor(what string, cond func() bool) {
	b.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		b.mu.Lock()
		ok := cond()
		b.mu.Unlock()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			b.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// --- the collector under test ---

func writeBase(t *testing.T, endpoint, storage string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg := fmt.Sprintf(`
fusescope_opamp: {endpoint: %q, tls: {insecure: true}, storage: %q}
receivers: {nop: {}}
exporters: {nop: {}}
extensions: {fusescope_opamp: {}}
service:
  extensions: [fusescope_opamp]
  telemetry: {metrics: {level: none}}
  pipelines:
    logs: {receivers: [nop], processors: [], exporters: [nop]}
`, endpoint, storage)
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

type running struct {
	col  *otelcol.Collector
	done chan error
}

func startCollector(t *testing.T, path string) *running {
	t.Helper()
	return startCollectorWith(t, path, map[component.Type]processor.Factory{logsPipelineType: testProcessor()})
}

// startCollectorWith runs the collector with managed the processors the
// provider can preflight.
func startCollectorWith(t *testing.T, path string, managed map[component.Type]processor.Factory) *running {
	t.Helper()
	col, err := otelcol.NewCollector(collectorSettings(path, managed))
	if err != nil {
		t.Fatal(err)
	}
	r := &running{col: col, done: make(chan error, 1)}
	go func() { r.done <- col.Run(context.Background()) }()
	t.Cleanup(func() {
		col.Shutdown()
		select {
		case <-r.done:
		case <-time.After(5 * time.Second):
		}
	})
	return r
}

func collectorSettings(path string, managed map[component.Type]processor.Factory) otelcol.CollectorSettings {
	managedFactories = managed
	return otelcol.CollectorSettings{
		BuildInfo: component.BuildInfo{Command: "test", Version: "0"},
		Factories: func() (otelcol.Factories, error) {
			f := otelcol.Factories{Telemetry: otelconftelemetry.NewFactory()}
			var err error
			if f.Receivers, err = otelcol.MakeFactoryMap[receiver.Factory](nopreceiver.NewFactory()); err != nil {
				return f, err
			}
			if f.Exporters, err = otelcol.MakeFactoryMap[exporter.Factory](nopexporter.NewFactory()); err != nil {
				return f, err
			}
			if f.Processors, err = otelcol.MakeFactoryMap[processor.Factory](testProcessor()); err != nil {
				return f, err
			}
			f.Extensions, err = otelcol.MakeFactoryMap[extension.Factory](fusescopeopampextension.NewFactory())
			return f, err
		},
		ConfigProviderSettings: otelcol.ConfigProviderSettings{ResolverSettings: confmap.ResolverSettings{
			URIs:              []string{Scheme + ":" + path},
			ProviderFactories: []confmap.ProviderFactory{NewFactory()},
		}},
	}
}

func (r *running) stillRunning(t *testing.T) {
	t.Helper()
	select {
	case err := <-r.done:
		t.Fatalf("collector exited: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
}

func addLogsPipeline(cfg map[string]any, block map[string]any) {
	procs, _ := cfg["processors"].(map[string]any)
	if procs == nil {
		procs = map[string]any{}
		cfg["processors"] = procs
	}
	procs["signozlogspipeline/pipeline_test"] = block
	logs := cfg["service"].(map[string]any)["pipelines"].(map[string]any)["logs"].(map[string]any)
	logs["processors"] = []any{"signozlogspipeline/pipeline_test"}
}

// --- tests ---

// The first message carries the description and the effective configuration
// together, as the backend requires (opamp spec §3).
func TestFirstMessage(t *testing.T) {
	b := startBackend(t)
	startCollector(t, writeBase(t, b.endpoint, t.TempDir()))
	b.waitFor("a first message", func() bool { return b.first != nil })
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.first.GetAgentDescription() == nil || b.first.GetEffectiveConfig().GetConfigMap().GetConfigMap()[configFile] == nil {
		t.Fatalf("first message lacks the description or the effective configuration: %v", b.first)
	}
	var eff map[string]any
	if err := yaml.Unmarshal(b.effective, &eff); err != nil {
		t.Fatal(err)
	}
	if _, ok := eff[settingsKey]; ok {
		t.Error("effective configuration leaks the provider's settings section")
	}
	if ext, _ := eff["extensions"].(map[string]any)[agent.ExtensionName].(map[string]any); ext["revision"] != nil {
		t.Error("effective configuration leaks the injected revision")
	}
}

func TestPushLifecycle(t *testing.T) {
	unused := func(block map[string]any) func(cfg map[string]any) {
		return func(cfg map[string]any) {
			procs, _ := cfg["processors"].(map[string]any)
			if procs == nil {
				procs = map[string]any{}
				cfg["processors"] = procs
			}
			procs["signozlogspipeline/pipeline_unused"] = block
		}
	}
	// A mounted credential the push points at; resolved, it would be a valid
	// value and the push would apply.
	credential := filepath.Join(t.TempDir(), "engine-token")
	if err := os.WriteFile(credential, []byte("false"), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name        string
		edit        func(cfg map[string]any)
		wantApplied bool
		wantMessage string
		forbidden   string
		noFactory   bool
	}{
		{"provider reference to a local file", func(cfg map[string]any) {
			addLogsPipeline(cfg, map[string]any{"fail_start": "${file:" + credential + "}"})
		}, false, "processors.signozlogspipeline/pipeline_test.fail_start", credential, false},
		{"log pipeline applied", func(cfg map[string]any) { addLogsPipeline(cfg, map[string]any{}) }, true, "", "", false},
		{"exporter changed", func(cfg map[string]any) {
			cfg["exporters"].(map[string]any)["nop"] = map[string]any{"secret": "s3cr3t-value"}
		}, false, "exporters.nop", "s3cr3t-value", false},
		{"preflight failure", func(cfg map[string]any) { addLogsPipeline(cfg, map[string]any{"fail_start": true}) }, false, "preflight", "", false},
		{"unused declaration with a bad field", unused(map[string]any{"no_such": 1}), false, "no_such", "", false},
		{"unused declaration of a type not in this build", unused(map[string]any{}), false, "not in this build", "", true},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := startBackend(t)
			storage := t.TempDir()
			managed := map[component.Type]processor.Factory{logsPipelineType: testProcessor()}
			if tt.noFactory {
				managed = map[component.Type]processor.Factory{}
			}
			r := startCollectorWith(t, writeBase(t, b.endpoint, storage), managed)
			hash := fmt.Sprintf("log_pipelines:%d", i+1)
			b.push(hash, tt.edit)
			st := b.status(hash)
			applied := st.GetStatus() == protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED
			if applied != tt.wantApplied || !strings.Contains(st.GetErrorMessage(), tt.wantMessage) {
				t.Fatalf("status %v %q, want applied %v with %q", st.GetStatus(), st.GetErrorMessage(), tt.wantApplied, tt.wantMessage)
			}
			if tt.forbidden != "" && strings.Contains(st.GetErrorMessage(), tt.forbidden) {
				t.Errorf("FAILED message shows a configuration value: %q", st.GetErrorMessage())
			}
			r.stillRunning(t)
			_, cached := os.Stat(filepath.Join(storage, "applied.json"))
			if (cached == nil) != tt.wantApplied {
				t.Errorf("applied push cached = %v, want %v", cached == nil, tt.wantApplied)
			}
			if tt.wantApplied {
				b.waitFor("the new effective configuration", func() bool {
					return strings.Contains(string(b.effective), "signozlogspipeline/pipeline_test")
				})
			}
		})
	}
}

// A push that passes preflight and fails in the real graph stops the
// collector once; the restart reports it FAILED and runs the last applied push.
func TestFailureDespitePreflight(t *testing.T) {
	graphStarts.Store(0)
	b := startBackend(t)
	storage := t.TempDir()
	path := writeBase(t, b.endpoint, storage)
	r := startCollector(t, path)
	b.push("log_pipelines:1", func(cfg map[string]any) { addLogsPipeline(cfg, map[string]any{}) })
	if st := b.status("log_pipelines:1"); st.GetStatus() != protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED {
		t.Fatalf("first push %v", st.GetStatus())
	}
	b.push("log_pipelines:2", func(cfg map[string]any) {
		addLogsPipeline(cfg, map[string]any{"fail_in_graph": true})
	})
	select {
	case err := <-r.done:
		if err == nil {
			t.Fatal("collector exited without an error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("collector kept running a graph that failed to start")
	}

	// A restart while the backend is unreachable keeps the failure to report.
	offline := startCollector(t, writeBase(t, "ws://127.0.0.1:1/v1/opamp", storage))
	failedFile := filepath.Join(storage, "failed.json")
	deadline := time.Now().Add(10 * time.Second)
	for _, err := os.Stat(failedFile); err != nil; _, err = os.Stat(failedFile) {
		if time.Now().After(deadline) {
			t.Fatal("the failure was not recorded at start")
		}
		time.Sleep(20 * time.Millisecond)
	}
	offline.col.Shutdown()
	<-offline.done

	b2 := startBackend(t)
	path2 := writeBase(t, b2.endpoint, storage) // same storage, new backend address
	r2 := startCollector(t, path2)
	st := b2.status("log_pipelines:2")
	if st.GetStatus() != protobufs.RemoteConfigStatuses_RemoteConfigStatuses_FAILED || !strings.Contains(st.GetErrorMessage(), "exited") {
		t.Fatalf("after restart: %v %q, want FAILED for the push that stopped the collector", st.GetStatus(), st.GetErrorMessage())
	}
	r2.stillRunning(t)
	b2.waitFor("the last applied push to run", func() bool {
		return strings.Contains(string(b2.effective), "signozlogspipeline/pipeline_test") &&
			!strings.Contains(string(b2.effective), "fail_in_graph")
	})
	b2.waitFor("the reported failure to be dropped", func() bool {
		_, err := os.Stat(failedFile)
		return err != nil
	})
}

// With the backend gone when the new graph comes up, the graph keeps running
// and the push is still recorded as applied.
func TestBackendGoneAtConfirmation(t *testing.T) {
	b := startBackend(t)
	storage := t.TempDir()
	r := startCollector(t, writeBase(t, b.endpoint, storage))
	b.push("log_pipelines:1", func(cfg map[string]any) { addLogsPipeline(cfg, map[string]any{}) })
	_ = b.srv.Stop(context.Background())
	r.stillRunning(t)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(storage, "applied.json")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the applied push was not recorded while the backend was gone")
		}
		time.Sleep(20 * time.Millisecond)
	}
	r.stillRunning(t)
}

// A change of the provider's own settings is refused at the next reload
// instead of being ignored.
func TestSettingsChangeNeedsRestart(t *testing.T) {
	b := startBackend(t)
	storage := t.TempDir()
	path := writeBase(t, b.endpoint, storage)
	p := NewFactory().Create(confmap.ProviderSettings{Logger: zap.NewNop()})
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	if _, err := p.Retrieve(context.Background(), Scheme+":"+path, nil); err != nil {
		t.Fatal(err)
	}
	b2 := startBackend(t)
	if err := os.WriteFile(path, []byte(strings.Replace(mustRead(t, path), b.endpoint, b2.endpoint, 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Retrieve(context.Background(), Scheme+":"+path, nil); err == nil || !strings.Contains(err.Error(), "restart") {
		t.Errorf("Retrieve after a settings change = %v, want a restart-required error", err)
	}
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Validating the configuration, as `validate` does, neither connects to the
// backend nor records a start, so the next start still runs the cached push.
func TestValidateKeepsState(t *testing.T) {
	b := startBackend(t)
	storage := t.TempDir()
	path := writeBase(t, b.endpoint, storage)
	managed := map[component.Type]processor.Factory{logsPipelineType: testProcessor()}
	r := startCollectorWith(t, path, managed)
	b.push("log_pipelines:1", func(cfg map[string]any) { addLogsPipeline(cfg, map[string]any{}) })
	if st := b.status("log_pipelines:1"); st.GetStatus() != protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED {
		t.Fatalf("status %v %q", st.GetStatus(), st.GetErrorMessage())
	}
	r.col.Shutdown()
	<-r.done

	other := startBackend(t)
	for range 2 {
		col, err := otelcol.NewCollector(collectorSettings(writeBase(t, other.endpoint, storage), managed))
		if err != nil {
			t.Fatal(err)
		}
		if err := col.DryRun(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(200 * time.Millisecond)
	other.mu.Lock()
	connected := other.first != nil
	other.mu.Unlock()
	if connected {
		t.Error("validating the configuration connected to the backend")
	}
	if _, err := os.Stat(filepath.Join(storage, "starting.json")); err == nil {
		t.Fatal("validating the configuration left a startup marker")
	}

	startCollectorWith(t, writeBase(t, other.endpoint, storage), managed)
	other.waitFor("the cached push to run", func() bool {
		return strings.Contains(string(other.effective), "signozlogspipeline/pipeline_test")
	})
}
