// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozlogspipelineprocessor // import "github.com/fuseone-io/fusescope-collector-server/components/processor/signozlogspipelineprocessor"

import (
	"github.com/fuseone-io/fusescope-collector-server/components/pkg/logspipeline"
)

// Config is one log pipeline as the FuseScope backend pushes it: stanza
// operators, run in order unless an operator names its output
// (docs/spec/signozlogspipeline.md §1).
type Config struct {
	Operators []map[string]any `mapstructure:"operators"`
}

// Validate builds the operators, so a configuration the processor cannot
// run is refused before the collector starts it.
func (cfg *Config) Validate() error {
	return logspipeline.Validate(cfg.Operators)
}
