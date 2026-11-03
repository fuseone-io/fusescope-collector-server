// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package provider // import "github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/provider"

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"

	"github.com/open-telemetry/opamp-go/client"
	"github.com/open-telemetry/opamp-go/client/types"
	"github.com/open-telemetry/opamp-go/protobufs"
	"go.uber.org/zap"

	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/agent"
	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/ingest"
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
			str("fusescope.collector.role", role(s)),
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
	ing := a.Ingest()
	if ing != nil {
		// Only the ingress role declares the ingest capability (ADR 0006).
		if err := oc.c.SetCustomCapabilities(ingest.CustomCapabilities()); err != nil {
			return nil, err
		}
	}
	settings := types.StartSettings{
		OpAMPServerURL: s.Endpoint,
		InstanceUid:    types.InstanceUid(uid),
		// Every message read is bounded: the largest policy and a margin;
		// remote configuration is smaller (agent.MaxPushBytes).
		MaxMessageSize: ingest.MaxMessageSize,
		Callbacks: types.Callbacks{
			// No redirect is followed: one would resend the token and could
			// move the connection to another host or from wss to ws.
			CheckRedirect: func(*http.Request, []*http.Request, []*http.Response) error {
				return errRedirect
			},
			OnConnect: func(context.Context) {
				if ing != nil {
					ing.OnConnect()
				}
			},
			OnMessage: func(ctx context.Context, msg *types.MessageData) {
				// The server answers each message; the first one carried the
				// status of a push that failed before the last exit.
				a.FailedReported()
				if msg.RemoteConfig != nil {
					oc.onRemoteConfig(ctx, a, msg.RemoteConfig)
				}
				if ing != nil && msg.CustomMessage != nil {
					ing.OnCustomMessage(msg.CustomMessage)
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
	if s.Auth.TokenFile != "" {
		token, err := readToken(s.Auth.TokenFile)
		if err != nil {
			return nil, err
		}
		settings.Header = http.Header{"Authorization": {"Bearer " + token}}
	}
	if !s.TLS.Insecure {
		tlsCfg, err := clientTLS(ctx, s)
		if err != nil {
			return nil, err
		}
		settings.TLSConfig = tlsCfg
	}
	if err := oc.c.Start(ctx, settings); err != nil {
		return nil, fmt.Errorf("start OpAMP client: %w", err)
	}
	if ing != nil {
		ing.SetSender(oc.c)
	}
	return oc, nil
}

func role(s *Settings) string {
	if s.Role == "" {
		return RoleWriter
	}
	return s.Role
}

// clientTLS loads the TLS settings with the TLS 1.3 floor of ADR 0004.
func clientTLS(ctx context.Context, s *Settings) (*tls.Config, error) {
	tlsCfg, err := s.TLS.LoadTLSConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("fusescope_opamp.tls: %w", err)
	}
	if tlsCfg.MinVersion < tls.VersionTLS13 {
		tlsCfg.MinVersion = tls.VersionTLS13
	}
	return tlsCfg, nil
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

var errRedirect = errors.New("the OpAMP endpoint answered with a redirect, which is not followed")

// The backend's bounds of an ingest token, its file read with room for a
// newline.
const (
	minTokenLength = 32
	maxTokenLength = 256
	maxTokenFile   = maxTokenLength + 2
)

// readToken reads the one token of path; neither it nor its content is
// ever in an error or a log.
func readToken(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("fusescope_opamp.auth.token_file: %w", err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxTokenFile+1))
	if err != nil {
		return "", fmt.Errorf("fusescope_opamp.auth.token_file: %w", err)
	}
	token := strings.TrimSpace(string(b))
	if len(b) > maxTokenFile || len(token) < minTokenLength || len(token) > maxTokenLength || strings.ContainsAny(token, " \t\r\n") {
		return "", fmt.Errorf("fusescope_opamp.auth.token_file must hold one token of %d to %d characters", minTokenLength, maxTokenLength)
	}
	return token, nil
}
