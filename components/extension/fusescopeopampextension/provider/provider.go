// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

// Package provider is the fusescopeopamp confmap provider (ADR 0008): run
// as --config fusescopeopamp:<file>, it returns the local file merged with
// what the FuseScope backend pushes over OpAMP, within the allow-list.
package provider // import "github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/provider"

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/agent"
	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/ingest"
	"github.com/fuseone-io/fusescope-collector-server/components/processor/signozllmpricingprocessor"
	"github.com/fuseone-io/fusescope-collector-server/components/processor/signozlogspipelineprocessor"
	"github.com/fuseone-io/fusescope-collector-server/components/processor/signozspanmapperprocessor"
)

// Scheme is the provider's URI scheme.
const Scheme = "fusescopeopamp"

// settingsKey is the local file's top-level section for this provider; it
// never reaches the collector.
const settingsKey = "fusescope_opamp"

// Settings are the provider's own settings, from the local file.
type Settings struct {
	Endpoint string                 `mapstructure:"endpoint"`
	TLS      configtls.ClientConfig `mapstructure:"tls"`
	// Storage is the directory of the persisted state (ADR 0008 §4).
	Storage string `mapstructure:"storage"`
	// Role is reported as fusescope.collector.role (FuseScope ADR 0009 line 79):
	// writer, the default, or ingress, which alone takes the ingest policy
	// and sends the ingest report (ADR 0006).
	Role string `mapstructure:"role"`
	// Auth is the ingress collector's credential for the ingest policy.
	Auth AuthSettings `mapstructure:"auth"`
}

// AuthSettings authenticate the ingress collector to the backend, which
// sends the ingest policy only to a connection that presents a token it
// holds (FuseScope ADR 0010 §3). The token is read from a file (ADR 0004).
type AuthSettings struct {
	TokenFile string `mapstructure:"token_file"`
}

// The collector roles of FuseScope ADR 0009 line 79.
const (
	RoleWriter  = "writer"
	RoleIngress = "ingress"
)

func (s *Settings) validate() error {
	u, err := url.Parse(s.Endpoint)
	switch {
	case s.Endpoint == "" || err != nil || (u.Scheme != "ws" && u.Scheme != "wss"):
		return errors.New("fusescope_opamp.endpoint must be a ws:// or wss:// URL")
	case u.Scheme == "ws" && !s.TLS.Insecure:
		return errors.New("fusescope_opamp.endpoint uses ws:// without tls.insecure: true (ADR 0008 §3)")
	case u.Scheme == "wss" && s.TLS.Insecure:
		return errors.New("fusescope_opamp: wss:// with tls.insecure: true is contradictory")
	case s.Storage == "":
		return errors.New("fusescope_opamp.storage must name a directory")
	case s.Role != "" && s.Role != RoleWriter && s.Role != RoleIngress:
		return fmt.Errorf("fusescope_opamp.role must be %s or %s", RoleWriter, RoleIngress)
	case s.Auth.TokenFile != "" && s.Role != RoleIngress:
		return errors.New("fusescope_opamp.auth.token_file is for the ingress role")
	case !s.TLS.Insecure && s.TLS.MinVersion != "" && s.TLS.MinVersion < minTLSVersion:
		return fmt.Errorf("fusescope_opamp.tls.min_version must be at least %s (ADR 0004)", minTLSVersion)
	}
	return nil
}

// minTLSVersion is the floor of ADR 0004, also when min_version is unset.
const minTLSVersion = "1.3"

// managedFactories are the allow-listed processors this build can preflight;
// each processor's PR adds itself.
var managedFactories = map[component.Type]processor.Factory{
	signozspanmapperprocessor.NewFactory().Type():   signozspanmapperprocessor.NewFactory(),
	signozllmpricingprocessor.NewFactory().Type():   signozllmpricingprocessor.NewFactory(),
	signozlogspipelineprocessor.NewFactory().Type(): signozlogspipelineprocessor.NewFactory(),
}

// NewFactory returns the fusescopeopamp provider factory.
func NewFactory() confmap.ProviderFactory {
	return confmap.NewProviderFactory(func(set confmap.ProviderSettings) confmap.Provider {
		return &provider{logger: set.Logger, factories: managedFactories}
	})
}

type provider struct {
	logger    *zap.Logger
	factories map[component.Type]processor.Factory

	once     sync.Once
	initErr  error
	settings *Settings // the settings the client started with
	agent    *agent.Agent
	mu       sync.Mutex
	client   *opampClient
	watcher  atomic.Pointer[confmap.WatcherFunc]
}

func (p *provider) Scheme() string { return Scheme }

func (p *provider) Retrieve(ctx context.Context, uri string, watcher confmap.WatcherFunc) (*confmap.Retrieved, error) {
	path, ok := strings.CutPrefix(uri, Scheme+":")
	if !ok {
		return nil, fmt.Errorf("%q is not a %s URI", uri, Scheme)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	base, settings, err := split(b)
	if err != nil {
		return nil, err
	}
	if watcher != nil {
		p.watcher.Store(&watcher)
	}
	p.once.Do(func() {
		p.settings = settings
		p.initErr = p.start(base, settings)
	})
	if p.initErr != nil {
		return nil, p.initErr
	}
	if !reflect.DeepEqual(settings, p.settings) {
		return nil, errors.New("the fusescope_opamp settings changed; restart the collector to apply them")
	}
	return confmap.NewRetrieved(p.agent.Config(base))
}

func (p *provider) start(base map[string]any, s *Settings) error {
	a, err := agent.New(p.logger, s.Storage, p.factories)
	if err != nil {
		return err
	}
	if !agent.ExtensionEnabled(base) {
		return fmt.Errorf("the %s provider needs the %s extension declared and listed in service.extensions", Scheme, agent.ExtensionName)
	}
	failed, err := a.Load(base)
	if err != nil {
		return fmt.Errorf("read OpAMP state: %w", err)
	}
	if s.Role == RoleIngress {
		// Nothing on disk until the collector starts.
		a.SetIngest(ingest.New(p.logger, filepath.Join(s.Storage, "ingest")))
	}
	p.agent = a
	agent.SetCurrent(a)
	// The client starts with the collector, not when the configuration is
	// only validated, so validation neither records a start nor takes pushes.
	a.OnActivate(func(ctx context.Context) error {
		if s.TLS.Insecure {
			p.logger.Warn("OpAMP to the backend is in plaintext; remote configuration, the ingest policy and any ingest token travel unencrypted",
				zap.String("endpoint", redactURL(s.Endpoint)))
		}
		if ing := a.Ingest(); ing != nil {
			// The cached policy is in force before any receiver starts.
			if err := ing.Start(a); err != nil {
				return err
			}
		}
		c, err := startClient(ctx, p.logger, s, a, failed)
		if err != nil {
			return err
		}
		p.mu.Lock()
		p.client = c
		p.mu.Unlock()
		a.SetReloader(p.reload, c)
		return nil
	})
	return nil
}

func (p *provider) reload() {
	if w := p.watcher.Load(); w != nil {
		(*w)(&confmap.ChangeEvent{})
	}
}

func (p *provider) Shutdown(ctx context.Context) error {
	if p.agent != nil {
		p.agent.Close()
	}
	p.mu.Lock()
	c := p.client
	p.mu.Unlock()
	var err error
	if c != nil {
		err = c.stop(ctx)
	}
	if p.agent != nil && p.agent.Ingest() != nil {
		p.agent.Ingest().Close()
	}
	return err
}

// split separates the provider's settings section from the base configuration.
func split(b []byte) (map[string]any, *Settings, error) {
	r, err := confmap.NewRetrievedFromYAML(b)
	if err != nil {
		return nil, nil, err
	}
	raw, err := r.AsRaw()
	if err != nil {
		return nil, nil, err
	}
	base, _ := raw.(map[string]any)
	section, ok := base[settingsKey].(map[string]any)
	if !ok {
		return nil, nil, agent.ErrNoSettings
	}
	delete(base, settingsKey)
	// The collector resolves substitutions in what it receives; this section
	// it never receives, so environment references are resolved here.
	section = expandEnv(section).(map[string]any)
	var s Settings
	if err := confmap.NewFromStringMap(section).Unmarshal(&s); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", settingsKey, err)
	}
	if err := s.validate(); err != nil {
		return nil, nil, err
	}
	return base, &s, nil
}

// redactURL keeps a URL's scheme, host and path: no user, password or query.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "(unparsable endpoint)"
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}).String()
}

// envRef matches $$ and ${env:NAME} or ${NAME}, as the collector writes them.
var envRef = regexp.MustCompile(`\$\$|\$\{(?:env:)?([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEnv resolves environment references in the string values of v.
func expandEnv(v any) any {
	switch x := v.(type) {
	case string:
		return envRef.ReplaceAllStringFunc(x, func(m string) string {
			if m == "$$" {
				return "$"
			}
			return os.Getenv(envRef.FindStringSubmatch(m)[1])
		})
	case map[string]any:
		for k, e := range x {
			x[k] = expandEnv(e)
		}
		return x
	case []any:
		for i, e := range x {
			x[i] = expandEnv(e)
		}
		return x
	default:
		return v
	}
}
