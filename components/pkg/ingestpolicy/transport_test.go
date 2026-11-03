// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package ingestpolicy

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseReportAck(t *testing.T) {
	seq, err := ParseReportAck([]byte(`{"schemaVersion":1,"reportSequence":1874}`))
	require.NoError(t, err)
	assert.Equal(t, uint64(1874), seq)

	for name, data := range map[string]string{
		"empty":            ``,
		"not an object":    `[]`,
		"unknown field":    `{"schemaVersion":1,"reportSequence":1,"x":1}`,
		"no sequence":      `{"schemaVersion":1}`,
		"zero sequence":    `{"schemaVersion":1,"reportSequence":0}`,
		"negative":         `{"schemaVersion":1,"reportSequence":-1}`,
		"no schema":        `{"reportSequence":1}`,
		"other schema":     `{"schemaVersion":2,"reportSequence":1}`,
		"trailing object":  `{"schemaVersion":1,"reportSequence":1}{}`,
		"trailing brace":   `{"schemaVersion":1,"reportSequence":1}}`,
		"trailing garbage": `{"schemaVersion":1,"reportSequence":1} x`,
		"string sequence":  `{"schemaVersion":1,"reportSequence":"1"}`,
		"too large":        `{"schemaVersion":1,"reportSequence":1}` + strings.Repeat(" ", MaxReportAckLen),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseReportAck([]byte(data))
			assert.Error(t, err)
		})
	}
}
