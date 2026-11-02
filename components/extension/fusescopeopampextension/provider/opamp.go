// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package provider // import "github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/provider"

import (
	"context"
	"fmt"
	"os"
	"runtime"

	"github.com/open-telemetry/opamp-go/client"
	"github.com/open-telemetry/opamp-go/client/types"
	"github.com/open-telemetry/opamp-go/protobufs"
	"go.uber.org/zap"

	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/agent"
)

// configFile is the one effective and remote config entry (opamp spec §3).
const configFile = "collector.yaml"

type opampClient struct {
	logger *zap.Logger
	c      client.OpAMPClient
}

func startClient(ctx context.Context, logger *zap.Logger, s *Settings, a *agent.Agent, failed *agent.Status) (*opampClient, error) {
	uid, err := a.InstanceUID()
	if err != nil {
		return nil, err
	}
	oc := &opampClient{logger: logger, c: client.NewWebSocket(zapLogger{logger})}
	host, _ := os.Hostname()
	desc := &protobufs.AgentDescription{
		IdentifyingAttributes: []*protobufs.KeyValue{
			str("service.name", "fusescope-collector"),
			str("service.instance.id", formatUUID(uid)),
		},
		NonIdentifyingAttributes: []*protobufs.KeyValue{
			str("host.name", host),
			str("os.type", runtime.GOOS),
		},
	}
	if err := oc.c.SetAgentDescription(desc); err != nil {
		return nil, err
	}
	if err := oc.c.SetCapabilities(ptr(protobufs.AgentCapabilities_AgentCapabilities_ReportsStatus |
		protobufs.AgentCapabilities_AgentCapabilities_AcceptsRemoteConfig |
		protobufs.AgentCapabilities_AgentCapabilities_ReportsEffectiveConfig |
		protobufs.AgentCapabilities_AgentCapabilities_ReportsRemoteConfig)); err != nil {
		return nil, err
	}
	settings := types.StartSettings{
		OpAMPServerURL: s.Endpoint,
		InstanceUid:    types.InstanceUid(uid),
		Callbacks: types.Callbacks{
			OnMessage: func(ctx context.Context, msg *types.MessageData) {
				if msg.RemoteConfig != nil {
					oc.onRemoteConfig(ctx, a, msg.RemoteConfig)
				}
			},
			GetEffectiveConfig: func(context.Context) (*protobufs.EffectiveConfig, error) {
				body, err := agent.MarshalYAML(a.Effective())
				if err != nil {
					return nil, err
				}
				return &protobufs.EffectiveConfig{ConfigMap: &protobufs.AgentConfigMap{ConfigMap: map[string]*protobufs.AgentConfigObject{
					configFile: {Body: body, ContentType: "application/x-yaml"},
				}}}, nil
			},
		},
	}
	if failed != nil {
		settings.RemoteConfigStatus = remoteStatus(*failed)
	}
	if !s.TLS.Insecure {
		tlsCfg, err := s.TLS.LoadTLSConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("fusescope_opamp.tls: %w", err)
		}
		settings.TLSConfig = tlsCfg
	}
	if err := oc.c.Start(ctx, settings); err != nil {
		return nil, fmt.Errorf("start OpAMP client: %w", err)
	}
	return oc, nil
}

func (oc *opampClient) onRemoteConfig(ctx context.Context, a *agent.Agent, rc *protobufs.AgentRemoteConfig) {
	file := rc.GetConfig().GetConfigMap()[configFile]
	if file == nil {
		oc.Report(agent.Status{Hash: rc.GetConfigHash(), Message: "the pushed configuration has no " + configFile})
		return
	}
	if st := a.Push(ctx, rc.GetConfigHash(), file.GetBody()); st != nil {
		oc.Report(*st)
	}
}

// Report sets the remote config status; the client sends it when it can.
// A network failure only delays it and never touches the running graph.
func (oc *opampClient) Report(st agent.Status) {
	if err := oc.c.SetRemoteConfigStatus(remoteStatus(st)); err != nil {
		oc.logger.Warn("could not report the OpAMP config status", zap.Error(err))
	}
	if !st.Applied {
		oc.logger.Warn("OpAMP configuration rejected", zap.String("reason", st.Message))
	}
}

func (oc *opampClient) EffectiveConfigChanged() {
	go func() {
		if err := oc.c.UpdateEffectiveConfig(context.Background()); err != nil {
			oc.logger.Warn("could not report the effective configuration", zap.Error(err))
		}
	}()
}

func (oc *opampClient) stop(ctx context.Context) error { return oc.c.Stop(ctx) }

func remoteStatus(st agent.Status) *protobufs.RemoteConfigStatus {
	rs := &protobufs.RemoteConfigStatus{LastRemoteConfigHash: st.Hash, Status: protobufs.RemoteConfigStatuses_RemoteConfigStatuses_APPLIED}
	if !st.Applied {
		rs.Status = protobufs.RemoteConfigStatuses_RemoteConfigStatuses_FAILED
		rs.ErrorMessage = st.Message
	}
	return rs
}

func str(k, v string) *protobufs.KeyValue {
	return &protobufs.KeyValue{Key: k, Value: &protobufs.AnyValue{Value: &protobufs.AnyValue_StringValue{StringValue: v}}}
}

func ptr[T any](v T) *T { return &v }

func formatUUID(b [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// zapLogger adapts opamp-go's logger.
type zapLogger struct{ l *zap.Logger }

func (z zapLogger) Debugf(_ context.Context, format string, v ...any) {
	z.l.Debug(fmt.Sprintf(format, v...))
}

func (z zapLogger) Errorf(_ context.Context, format string, v ...any) {
	z.l.Warn(fmt.Sprintf(format, v...))
}
