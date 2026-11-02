# signozllmpricing · LLM cost on spans

What the `signozllmpricing` processor must do so that the LLM cost the FuseScope backend shows
(AI observability pages, the AI overview dashboard, trace details) matches the existing
collector (phase 5). Clean-room (ADR 0002). Sources:

- **[be]**: the FuseScope backend (`fuseone-io/fusescope`, read at `c42fdd3db`); paths below
  are relative to it.
- **[obs]**: [`observed/signozllmpricing/`](observed/signozllmpricing/), the reference image
  `ghcr.io/fuseone-io/fusescope-otel-collector:0.145.0` run as a black box by
  `test/observe/signozllmpricing/observe.sh`. Each case of `cases.py` starts the reference with
  one configuration (`configs/<case>.yaml`), records whether it starts (`results.yaml`, and the
  error in `logs/<case>.log` when it does not), sends `corpus/traces.json` and records what the
  processor passed on (`outputs/<case>.jsonl`). `[obs main: x]` names span `x` of the corpus
  under the `main` configuration.
- **[otel]**: OpenTelemetry semantic conventions v1.40.0, GenAI registry
  (`gen_ai.usage.*`).

## 1. Placement and signals

- Component type `signozllmpricing`; the backend writes one block under that exact key
  [be `pkg/types/llmpricingruletypes/processorconfig.go:11,138`] and never wires it into a
  pipeline, so the base configuration does (opamp §5, §6).
- Traces only. In a logs or metrics pipeline the reference fails to start with "telemetry type
  is not supported" [obs `logs-pipeline`, `metrics-pipeline`].
- The processor changes span and resource attributes only; span fields, events, links and
  scope attributes pass unchanged, and no span is dropped [obs main: `removal-events-links`,
  `model-on-resource`; every span of the corpus is in the output].

## 2. Configuration

The backend pushes exactly this shape, with these attribute names
[be `processorconfig.go:15-58,89-107`, `pkg/types/aiobservabilitytypes/keys.go:8-37`,
`testdata/collector_with_rule.yaml`]:

```yaml
signozllmpricing:
  attrs:                       # span attributes read
    model: gen_ai.request.model
    in: gen_ai.usage.input_tokens
    out: gen_ai.usage.output_tokens
    cache_read: gen_ai.usage.cache_read.input_tokens
    cache_write: gen_ai.usage.cache_creation.input_tokens
  default_pricing:
    rules:                     # in the backend's order, see §3
      - name: gpt-4o           # informational
        pattern: [gpt-4o*]
        cache: {mode: subtract, read: 2.5, write: 0}   # optional; mode optional
        in: 5                  # USD per million tokens
        out: 15
  output_attrs:                # span attributes written
    in: signoz.gen_ai.usage.input_tokens.cost
    out: signoz.gen_ai.usage.output_tokens.cost
    cache_read: signoz.gen_ai.usage.cache_read.input_tokens.cost
    cache_write: signoz.gen_ai.usage.cache_write.input_tokens.cost
    total: signoz.gen_ai.usage.tokens.cost
```

- **Prices** are per million tokens: the backend's only unit is `per_million_tokens`
  [be `pricing.go:28-34`]; the observed costs are `tokens × price / 10⁶` (§4).
- **Rules may be empty**: the backend deploys the block with `rules: []` so rules can be added
  later [be `processorconfig_test.go:68-74`, `testdata/collector_no_rules.yaml`]. It starts
  [obs `no-rules`], as does a block without `default_pricing` [obs `no-default-pricing`].
- **`cache`** is omitted for a rule without cache prices; `mode` is `subtract`, `additive` or
  omitted (the backend's `unknown`) [be `processorconfig.go:68-79`, `pricing.go:40-48`,
  `processorconfig_test.go:76-92`].
- **What the backend never sends**, so the collector must refuse rather than guess: the
  backend drops rules with a negative `in` or `out` and the cache block of a negative cache
  price, saying the collector rejects negative prices [be `processorconfig.go:64-69`]; it
  accepts only non-empty patterns that are valid `path.Match` globs, "mirroring the
  collector's pattern check" [be `pricing.go:223-238`].

### 2.1 Validation

The reference refuses to start, with a configuration error, on [obs, each case's
`logs/<case>.log`]:

| Condition | Case |
|---|---|
| a key not in the shape above, at any level | `unknown-field`, `unknown-top-field` |
| a value of the wrong type (`in: "5"`) | `string-price` |
| `attrs.model` empty or missing, including a null block | `empty-attrs-model`, `no-attrs`, `null-config` |
| `output_attrs.total` empty or missing | `empty-output-attrs`, `no-output-attrs` |
| a rule without patterns, or with an empty pattern | `no-pattern`, `empty-pattern-list`, `empty-pattern` |
| a pattern that is not a valid `path.Match` glob | `bad-glob` |
| a negative `in`, `out`, `cache.read` or `cache.write` | `negative-in`, `negative-cache-read` |
| `cache.mode` other than `subtract`, `additive` or empty (case-sensitive) | `bad-cache-mode`, `uppercase-cache-mode` |

It starts with: the other `attrs` and `output_attrs` keys empty [obs `empty-attrs-in`,
`empty-attrs-out`, `empty-attrs-cache`, `empty-output-in`, `empty-output-cache-read`], a rule
without `name` or `out` [obs `no-rule-name`, `no-rule-out`; a missing price is 0], `cache: {}`
and `mode: ""` [obs `empty-cache-block`, `empty-cache-mode`].

The FuseScope collector refuses the same configurations. Its error messages are its own.

## 3. Model matching

- The model is the span attribute named by `attrs.model`, and only a string value counts: an
  int model is not matched even by the pattern `"42"` [obs main: `int-model`]. Nothing else
  stands in for it: not `gen_ai.response.model`, not a resource attribute [obs main:
  `response-model-only`, `request-and-response-model`, `model-on-resource`].
- A rule matches when any of its patterns matches the whole model under Go's `path.Match`
  [obs main: `glob-*`; be `pricing.go:223-238,270-279` uses the same function]: `*` is any run
  of characters except `/`, `?` exactly one such character, `[...]` a class (`[!3]` is the
  class of `!` and `3`, not a negation: `gemini-3-flash` matches `gemini-[!3]*-flash` and
  `gemini-2.5-flash` does not), `\` escapes. Case-sensitive, with no trimming [obs main:
  `case-sensitive`, `leading-space`, `trailing-space`].
- **First match wins**, in the order of `rules` [obs main: `overlap-general-first` is priced
  by `gpt-4o*`, listed before `gpt-4o-mini*`; `overlap-specific-first`]. Names play no part
  [obs main: `duplicate-rule-name`]. The backend lists rules newest first
  [be `pkg/modules/llmpricingrule/impllmpricingrule/module.go:132-163`,
  `impllmpricingrule/store.go:44`]; the collector takes the order as given.
- A model no rule matches gets no cost [obs main: `unknown-model`, `empty-model`]. The backend
  reports such models from trace data as "unmapped", by the same matching
  [be `module.go:41-61`].

## 4. Cost

Token counts are read from the attributes named by `attrs.in`, `attrs.out`,
`attrs.cache_read`, `attrs.cache_write`; an empty name reads nothing [obs `empty-attrs-*`].

- **Values.** An int or a double counts, as a float64 (`9007199254740993` becomes
  `9007199254740992`); a string, bool or any other type, or a missing attribute, counts as 0
  [obs main: `double-tokens`, `huge-tokens`, `string-tokens`, `gate-string-in-int-out`,
  `bool-tokens`]. Negative values, NaN and infinities are used as they are [obs main:
  `negative-tokens`, `nan-tokens`, `infinite-tokens`]. [otel] types them as int.
- **Which spans are priced.** A span whose model matches a rule is priced when at least one of
  the four counts is non-zero; with all four 0 (or absent) it gets no cost [obs main:
  `no-tokens`, `zero-tokens`, `gate-double-zero` unpriced; `gate-cache-only`,
  `gate-zero-in-out-with-cache`, `gate-cancelling`, `gate-negative-out`, `gate-fraction`,
  `output-only`, `input-only` priced].
- **Billable input.** With `cr` and `cw` the cache counts: `in − cr − cw` when
  `cr + cw ≤ in`, otherwise `in` unchanged — the whole sum, not a partial subtraction or a
  clamp [obs main: `cache-*-below`, `-equal` subtract; `-read-below-sum-above`,
  `-read-above`, `-write-above`, `subtract-cache-exceeds-input` do not; `-negative-read`
  subtracts the negative]. [otel] says input tokens SHOULD include cached tokens and the cache
  counts SHOULD be included in input tokens, which the subtraction undoes; the comparison
  covers instrumentations that report them separately.
- **`cache.mode` has no observed effect**: `subtract`, `additive`, no mode and a rule without
  `cache` all give the same billable input for every probe [obs main: `cache-subtract-*`,
  `cache-additive-*`, `cache-none-*`, `cache-no-block-*`]. It is only validated (§2.1).
- **Costs**, each a float64 computed as `count × price / 1e6` in that order [obs main: all
  priced spans, bit for bit, e.g. `fp-subtract-large`, `overlap-specific-first`
  (`0.0008800000000000001`)]:
  - input: billable input × `in`;
  - output: `out` count × `out`;
  - cache read: `cr` × `cache.read`, cache write: `cw` × `cache.write`, both 0 for a rule
    without `cache` [obs main: `rule-without-cache`, `cache-no-block-*`];
  - total: `((input + output) + cache read) + cache write`, summed in that order [obs main:
    `additive` gives `0.0052274999999999995`, `fp-sum` `8e-07`].
- **Written** as double attributes under the five `output_attrs` names, in the order input,
  output, cache read, cache write, total, after the span's existing attributes; all five are
  written on every priced span, zeros included [obs main: `zero-price`, `subtract-no-cache-tokens`].
  An empty `output_attrs` name is not written, and the total still includes that cost [obs
  `empty-output-in`, `empty-output-cache-read`]. A priced span's existing value under one of
  these names is replaced [obs main: `existing-cost`; `custom-attrs`: `custom-attr-names`].
- The backend reads `signoz.gen_ai.usage.tokens.cost` as a number and sums it per trace and in
  the AI dashboard (currency USD); the per-kind costs are summed by the dashboard
  [be `pkg/telemetryschema/aitelemetryschema/const.go:23`,
  `pkg/modules/tracedetail/impltracedetail/store.go:179`,
  `pkg/statementbuilder/aistatementbuilder/statement_builder.go:42`,
  `pkg/modules/dashboard/impldashboard/fs/definitions/ai-o11y-overview.json:410-420,968-1022`].

## 5. Reserved attributes

Before pricing, every span attribute and every resource attribute whose key starts with
`signoz.gen_ai.` is removed, on every span the processor sees, priced or not and whatever the
configuration [obs main: `removal-*`, `existing-cost-unknown-model`, the resource of
`model-on-resource`; `no-rules`, `no-default-pricing` and `custom-attrs` remove them too;
`control-no-processor` keeps them, so the processor does it]. Only that exact prefix:
`signoz.gen_ai`, `signoz.gen_aix`, `Signoz.gen_ai.x`, `signoz.other` are kept, and so are
configured output names outside the prefix (`c.total` under `custom-attrs`) [obs main:
`removal-prefixes`, `removal-other-keys`]. Scope, event and link attributes are not touched
[obs main: `removal-events-links`].

So a sender cannot plant a cost the backend would read as the processor's.

**Deliberate deviation: nested maps.** The reference keeps map-valued attributes whose
flattened keys fall under the prefix: `signoz.gen_ai = {usage.tokens.cost: 999}` and
`signoz = {gen_ai.usage.input_tokens.cost: 999}` pass unchanged [obs main:
`removal-nested-maps`]. The ClickHouse exporter flattens maps into dotted keys
(clickhouse-traces), so these land as `signoz.gen_ai.usage.tokens.cost` and defeat the rule
above. The FuseScope collector also removes, from span and resource map attributes, every entry
whose flattened key starts with `signoz.gen_ai.`, and the whole attribute when its own key is
`signoz.gen_ai`; other entries stay (`signoz = {other: 1}`, `other = {signoz.gen_ai.w: 1}`). The
golden test applies this rule to the observed output before comparing.

## 6. Open questions

- **`cache.mode`.** The backend labels `subtract` "OpenAI style" and `additive` "Anthropic
  style" [be `pricing.go:40-48`, `frontend/src/container/LLMObservability/Settings/ModelPricing/constants.ts:57-58`],
  but no probe showed a difference (§4). If the reference distinguishes them in a case not
  probed, the parity suite will show it.
- **Rounding.** No rounding was observed; costs carry full float64 precision. The `cr + cw ≤ in`
  comparison and the subtraction were not probed with values where float64 rounding would
  change the outcome.
