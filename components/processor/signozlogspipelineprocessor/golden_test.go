// Copyright FuseOne
// SPDX-License-Identifier: Apache-2.0

package signozlogspipelineprocessor

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/fuseone-io/fusescope-collector-server/components/pkg/logspipeline/corpus"
	"github.com/fuseone-io/fusescope-collector-server/components/processor/signozlogspipelineprocessor/internal/metadata"
)

// collectorTime marks an observed timestamp the reference set from its clock.
const collectorTime = "set-by-collector"

type observedCase struct {
	Name           string                    `json:"name"`
	Origin         string                    `json:"origin"`
	LogSet         string                    `json:"logSet"`
	Processors     map[string]map[string]any `json:"processors"`
	ProcessorNames []string                  `json:"processorNames"`
}

type observedCases struct {
	Inputs  map[string][]json.RawMessage `json:"inputs"`
	Cases   []observedCase               `json:"cases"`
	Startup []observedCase               `json:"startup"`
}

// unsupported are cases this processor does not reproduce, with the reason
// (spec §7).
var unsupported = map[string]string{
	// The reference keeps body_map in a pooled environment, so an expression
	// naming it reads another record's body; here it is nil.
	"X_Expr_BodyMapVariable": "body_map leaks between records in the reference (spec §4.3)",
}

// stricter are configurations the reference starts with and this processor
// refuses, with the reason (spec §1.2).
var stricter = map[string]string{
	"R_UnknownOperatorField": "a field the operator does not have",
	"R_UnknownTraceField":    "a field the operator does not have",
	"R_AddNoValue":           "add without value",
	"X_Expr_Helpers":         "env() is refused (spec §4.1)",
}

// withoutEnv rewrites the cases using env() so their outputs are still
// compared: env("OBSERVE_MARKER") becomes the value the observation's compose
// file gave the reference.
var withoutEnv = map[string]func(string) string{
	"X_Expr_Helpers": func(processors string) string {
		return strings.ReplaceAll(processors, `EXPR(env(\"OBSERVE_MARKER\"))`, "observed-env-value")
	},
}

// readObserved returns the observed cases of the published corpus
// (components/pkg/logspipeline/corpus, ADR 0012 step 4), with the processor
// configurations decoded keeping their numbers.
func readObserved(t *testing.T) observedCases {
	t.Helper()
	c, err := corpus.Observed()
	require.NoError(t, err)
	convert := func(in []corpus.Case) []observedCase {
		out := make([]observedCase, 0, len(in))
		for _, k := range in {
			oc := observedCase{Name: k.Name, Origin: k.Origin, LogSet: k.LogSet, ProcessorNames: k.ProcessorNames, Processors: map[string]map[string]any{}}
			for name, raw := range k.Processors {
				d := json.NewDecoder(bytes.NewReader(raw))
				d.UseNumber()
				var cfg map[string]any
				require.NoError(t, d.Decode(&cfg))
				oc.Processors[name] = cfg
			}
			out = append(out, oc)
		}
		return out
	}
	return observedCases{Inputs: c.Inputs, Cases: convert(c.Cases), Startup: convert(c.Startup)}
}

// unescape resolves `$$` as the collector's confmap does for the configs
// the backend pushes (spec §1.1), and types numbers as YAML does.
func unescape(v any) any {
	switch v := v.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return int(i)
		}
		f, _ := v.Float64()
		return f
	case string:
		return strings.ReplaceAll(v, "$$", "$")
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			out[k] = unescape(item)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = unescape(item)
		}
		return out
	}
	return v
}

func loadConfig(raw map[string]any) (*Config, error) {
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	if err := confmap.NewFromStringMap(unescape(raw).(map[string]any)).Unmarshal(cfg); err != nil {
		return nil, err
	}
	return cfg, cfg.Validate()
}

// chain builds the case's processors in order, ending in sink.
func chain(t *testing.T, c observedCase, sink consumer.Logs) consumer.Logs {
	t.Helper()
	next := sink
	for _, name := range slices.Backward(c.ProcessorNames) {
		cfg, err := loadConfig(c.Processors[name])
		require.NoError(t, err, name)
		p, err := NewFactory().CreateLogs(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, next)
		require.NoError(t, err)
		next = p
	}
	return next
}

// TestObservedOutputs replays every observed case and compares the records
// grouped by resource and scope (spec §5.2: group order is not part of the
// contract).
func TestObservedOutputs(t *testing.T) {
	cases := readObserved(t)
	outputs, err := corpus.ObservedOutputs()
	require.NoError(t, err)

	for _, c := range cases.Cases {
		t.Run(c.Name, func(t *testing.T) {
			if reason, ok := unsupported[c.Name]; ok {
				t.Skip(reason)
			}
			if rewrite, ok := withoutEnv[c.Name]; ok {
				raw, err := json.Marshal(c.Processors)
				require.NoError(t, err)
				rewritten := rewrite(string(raw))
				require.NotEqual(t, string(raw), rewritten, "the rewrite no longer applies")
				d := json.NewDecoder(strings.NewReader(rewritten))
				d.UseNumber()
				c.Processors = nil
				require.NoError(t, d.Decode(&c.Processors))
			} else if reason, ok := stricter[c.Name]; ok {
				t.Skip("this processor refuses the configuration: " + reason)
			}
			want, ok := outputs[c.Name]
			if !ok {
				t.Skip("the reference refuses this configuration; TestObservedStartup covers it")
			}
			sink := &consumertest.LogsSink{}
			first := chain(t, c, sink)
			for _, raw := range cases.Inputs[c.LogSet] {
				ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs(raw)
				require.NoError(t, err)
				require.NoError(t, first.ConsumeLogs(context.Background(), ld))
			}
			// The reference's file exporter batches calls and merges their
			// groups, so records are compared per resource and scope.
			got, wantGroups := map[string][]string{}, map[string][]string{}
			for _, ld := range sink.AllLogs() {
				canonicalGroups(t, ld, got)
			}
			for _, raw := range want {
				ld, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs(withoutCollectorTime(t, raw))
				require.NoError(t, err)
				canonicalGroups(t, ld, wantGroups)
			}
			for _, records := range got {
				slices.Sort(records)
			}
			for _, records := range wantGroups {
				slices.Sort(records)
			}
			if !assert.Equal(t, slices.Sorted(maps.Keys(wantGroups)), slices.Sorted(maps.Keys(got)), "resource and scope groups") {
				return
			}
			for key, records := range wantGroups {
				assert.Empty(t, without(records, got[key]), "observed, not produced, in %s", key)
				assert.Empty(t, without(got[key], records), "produced, not observed, in %s", key)
			}
		})
	}
}

// without is a minus b, as multisets of sorted slices.
func without(a, b []string) []string {
	var out []string
	i, j := 0, 0
	for i < len(a) {
		switch {
		case j < len(b) && a[i] == b[j]:
			i++
			j++
		case j < len(b) && b[j] < a[i]:
			j++
		default:
			out = append(out, a[i])
			i++
		}
	}
	return out
}

// TestObservedStartup checks that a configuration is accepted exactly when
// the reference starts with it, except where this processor is stricter.
func TestObservedStartup(t *testing.T) {
	cases := readObserved(t)
	results, err := corpus.ObservedStartup()
	require.NoError(t, err)
	byName := map[string]observedCase{}
	for _, c := range append(cases.Startup, cases.Cases...) {
		byName[c.Name] = c
	}
	for _, r := range results {
		t.Run(r.Name, func(t *testing.T) {
			c := byName[r.Name]
			var err error
			for _, name := range c.ProcessorNames {
				if _, err = loadConfig(c.Processors[name]); err != nil {
					break
				}
			}
			switch {
			case stricter[r.Name] != "":
				assert.Error(t, err)
			case r.Starts:
				assert.NoError(t, err)
			default:
				assert.Error(t, err, "the reference refuses it: %s", r.Error)
			}
		})
	}
}

// withoutCollectorTime replaces observed timestamps the reference took from
// its clock by zero; canonicalDoc then compares them only as present.
func withoutCollectorTime(t *testing.T, raw json.RawMessage) []byte {
	t.Helper()
	return []byte(strings.ReplaceAll(string(raw), `"`+collectorTime+`"`, `"0"`))
}

// canonicalGroups adds the logs' records to groups, keyed by everything of
// their resource and scope; attribute order is not compared.
func canonicalGroups(t *testing.T, ld plog.Logs, groups map[string][]string) {
	t.Helper()
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			key, err := json.Marshal(map[string]any{
				"resource": typedMap(rl.Resource().Attributes().AsRaw()), "resourceDropped": rl.Resource().DroppedAttributesCount(),
				"resourceSchema": rl.SchemaUrl(), "scope": sl.Scope().Name(), "scopeVersion": sl.Scope().Version(),
				"scopeAttributes": typedMap(sl.Scope().Attributes().AsRaw()), "scopeSchema": sl.SchemaUrl(),
			})
			require.NoError(t, err)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				r := sl.LogRecords().At(k)
				// Observed timestamps after 2025 come from a clock: the reference's
				// (zeroed by withoutCollectorTime) or this processor's.
				if r.ObservedTimestamp() > 1_750_000_000_000_000_000 {
					r.SetObservedTimestamp(0)
				}
				groups[string(key)] = append(groups[string(key)], canonicalRecord(t, r))
			}
		}
	}
}

func canonicalRecord(t *testing.T, r plog.LogRecord) string {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"time": r.Timestamp(), "observed": r.ObservedTimestamp(), "severity": r.SeverityNumber(), "severityText": r.SeverityText(),
		"body": valueJSON(t, r.Body().AsRaw()), "bodyType": r.Body().Type().String(), "attributes": typedMap(r.Attributes().AsRaw()),
		"trace": r.TraceID().String(), "span": r.SpanID().String(), "flags": r.Flags(), "event": r.EventName(),
		"dropped": r.DroppedAttributesCount(),
	})
	require.NoError(t, err)
	return string(b)
}

func valueJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(typed(v))
	require.NoError(t, err)
	return string(b)
}

// typed keeps the value types apart in JSON (1 and 1.0, a string and bytes).
func typed(v any) any {
	switch v := v.(type) {
	case map[string]any:
		return typedMap(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = typed(item)
		}
		return out
	case int64:
		return map[string]any{"int": v}
	case float64:
		return map[string]any{"double": v}
	case []byte:
		return map[string]any{"bytes": v}
	}
	return v
}

func typedMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = typed(v)
	}
	return out
}
