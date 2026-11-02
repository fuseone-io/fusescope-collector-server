// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package agent // import "github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/agent"

import (
	"reflect"

	"go.opentelemetry.io/collector/confmap"
	yaml "go.yaml.in/yaml/v3"

	"github.com/fuseone-io/fusescope-collector-server/components/extension/fusescopeopampextension/internal/merge"
)

func parseYAML(b []byte) (merge.Config, error) {
	r, err := confmap.NewRetrievedFromYAML(b)
	if err != nil {
		return nil, err
	}
	raw, err := r.AsRaw()
	if err != nil {
		return nil, err
	}
	m, _ := raw.(map[string]any)
	if m == nil {
		m = map[string]any{}
	}
	return m, nil
}

// MarshalYAML renders a configuration for the effective config report.
func MarshalYAML(cfg merge.Config) ([]byte, error) { return yaml.Marshal(cfg) }

func equalConfig(a, b merge.Config) bool { return reflect.DeepEqual(a, b) }
