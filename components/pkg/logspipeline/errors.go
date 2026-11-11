// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package logspipeline // import "github.com/fuseone-io/fusescope-collector-server/components/pkg/logspipeline"

import "errors"

// Stage is where a configuration fails to build.
type Stage string

const (
	// StageOperators: the operator list itself, a missing or unsupported
	// type, a field that must be a string, an id used twice, or no operators.
	StageOperators Stage = "operators"
	// StageDecode: an operator's fields do not decode.
	StageDecode Stage = "decode"
	// StageCompile: an if or route expression does not compile.
	StageCompile Stage = "compile"
	// StageBuild: the operator refuses its configuration when it is built.
	StageBuild Stage = "build"
	// StageGraph: an output names no operator, or the outputs form a cycle.
	StageGraph Stage = "graph"
)

// ConfigError is every configuration New and Validate refuse. Its message is
// the one the processor gives; OperatorID and OperatorType are set when the
// error belongs to one operator.
type ConfigError struct {
	Stage        Stage
	OperatorID   string
	OperatorType string
	err          error
}

func (e *ConfigError) Error() string { return e.err.Error() }

func (e *ConfigError) Unwrap() error { return e.err }

func configError(stage Stage, id, typ string, err error) *ConfigError {
	return &ConfigError{Stage: stage, OperatorID: id, OperatorType: typ, err: err}
}

// stageError marks the stage of an error inside an operator's build, before
// newPipeline names the operator; its message is the error's.
type stageError struct {
	stage Stage
	err   error
}

func (e *stageError) Error() string { return e.err.Error() }

func (e *stageError) Unwrap() error { return e.err }

func staged(stage Stage, err error) error {
	if err == nil {
		return nil
	}
	return &stageError{stage: stage, err: err}
}

func stageOf(err error, fallback Stage) Stage {
	var s *stageError
	if errors.As(err, &s) {
		return s.stage
	}
	return fallback
}
