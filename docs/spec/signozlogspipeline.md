# signozlogspipeline · log pipelines from the FuseScope UI

The processor that runs one log pipeline configured in the FuseScope UI. The backend pushes one
block per enabled pipeline, `signozlogspipeline/pipeline_<alias>`, and places them in
`service.pipelines.logs` ([opamp](opamp.md) §5). Clean-room (ADR 0002): every rule cites

- **[B]** the FuseScope backend (`fuseone-io/fusescope`, MIT) read at `c42fdd3db`; paths relative to
  `pkg/query-service/app/logparsingpipeline/` unless they start with `pkg/` or `tests/`. Its
  `pipelinesim` package is the backend's own in-process simulator of this processor, which its
  integration suite checks against the existing collector
  (`tests/fixtures/simulatorcontract.py`, `tests/integration/tests/simulatorcontract/01_logspipelines.py`);
- **[O]** black-box observation of `ghcr.io/fuseone-io/fusescope-otel-collector:0.145.0`
  ([`observed/signozlogspipeline`](observed/signozlogspipeline), harness in
  `test/observe/signozlogspipeline`), cited by case name. The backend's 168 corpus cases were
  observed unchanged: the reference's output equals the backend's golden for every one;
- **[S]** upstream `pkg/stanza` v0.164.1 (Apache-2.0), whose operators this processor runs.

## 1. Configuration

### 1.1 What the backend pushes

```yaml
signozlogspipeline/pipeline_nginx:
  operators:
    - {id: router_signoz, type: router, routes: [{expr: '<filter>', output: <first op>}], default: noop}
    - {id: <op>, type: <type>, if: '<nil check>', output: <next op>, ...}
    - {id: noop, type: noop}
```

- One key, `operators`, a list of stanza-style operators (`pkg/types/pipelinetypes/pipeline.go:84-86`).
- Every pipeline starts with the router `router_signoz` whose one route is the pipeline's filter,
  compiled by `pkg/query-service/queryBuilderToExpr` (`pipelineBuilder.go:50-67`), and ends with
  `noop`, the router's default, so records the filter rejects pass unchanged (`pipelineBuilder.go:71-76`).
  Disabled pipelines and pipelines without enabled operators are not pushed (`pipelineBuilder.go:37-48`).
- Operator fields are those of `PipelineOperator`'s YAML tags: `type, id, output, on_error, if,
  parse_to, pattern, regex, parse_from, trace_id/span_id/trace_flags: {parse_from}, field, value, from,
  to, expr, routes, fields, default, layout, layout_type, enable_flattening, max_flattening_depth,
  enable_paths, path_prefix, mapping, overwrite_text` (`pkg/types/pipelinetypes/pipeline.go:88-129,175-188`).
- Every `$` in a pipeline's values is written as `$$` (`collector_config.go:214-233`); the
  collector's configuration resolver turns `$$` back into `$` before the processor sees it
  ([O] `Add_DollarValues`, `X_Expr_Helpers`).
- The backend adds `if` nil checks to most operators and rewrites `json_parser` mappings into
  operators (`pipelineBuilder.go:94-215,237-385`); the processor runs what it receives and does not
  depend on those rewrites.

### 1.2 Validation

A configuration is refused before the collector starts (the reference refuses it at start too,
[O] `startup.json`) when:

- `operators` is missing or empty ([O] `R_NoOperatorsKey`, `R_EmptyOperators`);
- the block has another key ([O] `R_UnknownProcessorField`);
- an operator has no `type`, or a type outside §3 ([O] `R_NoType`, `R_UnknownType_*`, `Retain_Fields`:
  `retain` is refused although the backend's validation accepts it, `pkg/types/pipelinetypes/pipeline.go:345-348`);
- two operators share an id, an `output`, route `output` or `default` names no operator, or the
  operators form a cycle ([O] `R_DuplicateID`, `R_OutputUnknown`, `R_Cycle`);
- an expression does not compile (`if`, a route `expr`, an `EXPR(...)` value) ([O] `R_BadIfExpr`,
  `R_BadRouteExpr`, `R_AddBadExpr`, `Regex_InvalidRegex_ConfigError`);
- stanza refuses the operator's configuration: an invalid `on_error`, a field without a valid
  prefix, a wrongly typed field, a regex without named groups, an epoch layout outside
  `s, ms, us, ns, s.ms, s.us, s.ns`, an unsupported `layout_type`, a `copy` from a bare
  `attributes`, a `grok_parser` without `pattern` ([O] `R_BadOnError`, `R_BadField`,
  `R_JSONBadDepthType`, `Regex_NoNamedGroup`, `Time_EpochInvalidLayout_ConfigError`,
  `R_TimeBadLayoutType`, `X_ParsedBodyCache_Leak`, `R_GrokNoPattern`).

This processor is **stricter** than the reference in these cases, which the reference starts with:

- a field the operator type does not have ([O] `R_UnknownOperatorField`, `R_UnknownTraceField`);
- `add` without `value` ([O] `R_AddNoValue`);
- `add` without `field` (not observed): the write would fail on every record, so `on_error: drop`
  would discard every matching log while the push is reported `APPLIED`;
- a grok pattern naming an unknown pattern, or one that is not a valid regular expression
  ([O] `R_GrokBadPattern`, `Grok_InvalidPattern_ConfigError`): the reference starts and fails
  the operator on every record, so `on_error` decides each record's fate while the push is
  reported `APPLIED` ([B] `pipelinesim/operators.go:119-125`, which compiles the pattern per
  record);
- an expression naming `env` (§4.1, a deliberate deviation for security; [O] `X_Expr_Helpers`
  uses it and is refused).

The backend never generates these (§1.1), and ADR 0008's preflight requires unknown fields to fail,
so a push with them is reported `FAILED` instead of running with an ignored field.

## 2. Operator graph

- An operator's `id` defaults to its `type` ([O] `X_IDDefaultsToType`; [B] `pipelinesim/build.go:84-87`).
- An operator without `output` continues with the next operator in the list; the last one leaves
  the processor ([O] `X_ImplicitChain`; [B] `pipelinesim/build.go:97-101`). The backend sets `output`
  on every operator but the last (`pipelineBuilder.go:209-213`).
- Records enter at the first operator ([B] `pipelinesim/build.go:55-58`).
- `router` sends a record to the `output` of the first route whose `expr` is true. A route whose
  expression fails to evaluate counts as false. With no true route the record goes to `default`, and
  is dropped when there is no default ([O] `X_Expr_RouteRuntimeError`, `X_RouterNoDefault`;
  [B] `pipelinesim/pipeline.go:125-143`). Routes and default are single operator ids.
- `noop` passes the record on ([B] `pipelinesim/pipeline.go:145-151`).
- Several `signozlogspipeline/pipeline_*` processors run in the order of `service.pipelines.logs`,
  each seeing the previous one's output ([O] `Pipeline_OrderMatters`, `X_Order_ServiceListWins`,
  `X_Metadata_TwoPipelines`).

## 3. Operators

Every operator but `router` and `noop` takes `id`, `type`, `output`, `if` and `on_error` (§6).
Fields name a part of the record as in stanza: `body`, `body.a`, `attributes.a`,
`attributes["a.b"]`, `resource.a` ([S] `entry/field.go`).

### 3.1 Upstream operators

`regex_parser`, `time_parser`, `trace_parser`, `copy`, `move`, `remove` are stanza's at v0.164.1
([B] `pipelinesim/build.go:27-40`), with these differences, all observed:

- `time_parser` without `location` parses layouts without a zone in UTC ([B] `pipelinesim/build.go:235-249`;
  [O] `Time_Strptime*`, `Time_Epoch*`).
- `severity_parser` is stanza's, except that an integer value (a 64-bit integer, as every OTLP
  integer is) is an error, as in the reference; a whole-number double is mapped
  ([O] `Severity_NumericValues`, `Severity_HTTPStatusRanges`, `Severity_NoMapping`). stanza v0.164.1
  maps 64-bit integers; the reference's stanza did not.
- Writing a map into a field merges it into a map already there; a map written into a body that
  is not a map replaces it ([S] `entry/body_field.go:80-124`; [O] `X_Bodies_JSONToBodyFlatten`,
  `Grok_ParseToBody`). Writing `body.<field>` into a body that is not a map replaces the body with
  `{<field>: value}` ([O] `X_BodyField_AddField`, `X_BodyField_MoveToBodyField`).
- Reading `body.<field>` (§4.3) applies to the fields an operator reads: `parse_from` of
  `regex_parser`, `json_parser`, `grok_parser`, `severity_parser`, `time_parser`, the three
  `parse_from` of `trace_parser`, and `from` of `copy`. `move` and `remove` do not read a string
  body as JSON: they fail on it ([O] `X_BodyField_*`).

### 3.2 `grok_parser`

[O] `Grok_*`, `X_Bodies_Grok`: parses `parse_from` (a string; anything else is an error) with a
grok pattern over the Logstash legacy pattern set (`%{COMBINEDAPACHELOG}` yields `clientip, ident,
auth, timestamp, verb, request, httpversion, rawrequest, response, bytes, referrer, agent`, quotes
kept) and inline `(?<name>...)` groups. Only named captures are kept; an unmatched optional group
yields `""`; `%{X:name:int}` and `%{X:name:float}` convert. The match is unanchored. A pattern
that does not match is not an error: the record goes on with an empty map written to `parse_to`
(which replaces a non-map body). [B] `pipelinesim/operators.go:79-125`.

The backend's simulator runs `github.com/vjeantet/grok` v1.0.1 (Apache-2.0) with
`NamedCapturesOnly` and its default pattern set ([B] `pipelinesim/operators.go:96`, `pkg/grok`).
This processor embeds the same library, its code unchanged, in `internal/grok` (its README lists the
import's few changes, `NOTICE` credits it), and uses it the same way:

- the default pattern set is the library's built-in map (`patterns.go`), not its `patterns/`
  files, which are embedded but not loaded, so a name the reference does not know is not
  accepted here either;
- `%{X:name:int}` becomes an integer and `%{X:name:float}` a double ([O] `Grok_TypeConversions`,
  `Pipeline_ManyOperatorsChain`); a capture that does not convert (not observed) becomes `0`, as
  the library ignores the conversion error;
- a dotted semantic name (`%{WORD:http.method}`) keeps its dots ([O] `Grok_DottedFieldNames`).

The pattern is compiled when the operator is built, so an unknown pattern name or an invalid
regular expression is refused (§1.2). Go's `regexp` (RE2) matches in time linear in the input
without backtracking, so no pattern can take exponential time; it has no match timeout, and none
is needed.

### 3.3 `json_parser`

[B] `pipelinesim/json.go:22-117`; [O] `JSON_*`, `X_Bodies_JSON*`, `X_JSON_*`.

- A string is parsed as a JSON object (surrounding whitespace allowed); a map is taken as it is;
  anything else, bytes included, and a JSON value that is not an object, is an error.
- Numbers become doubles, `null` becomes nil (written as `"<nil>"`, §5.2).
- With `enable_flattening`, members whose value is an object are replaced, up to
  `max_flattening_depth` levels, by that object's members; a member already present at the outer
  level keeps its value; lifted objects are processed in key order. With `enable_paths`, a lifted
  member is named `<parent path>.<key>`, top-level members `<path_prefix>.<key>` when `path_prefix`
  is set ([O] `JSON_Flattening`, `JSON_FlatteningWithPaths`, `JSON_FlatteningWithPathPrefix`).
- The backend always sends `on_error: send_quiet` and, with flattening, `max_flattening_depth` from
  its environment, 1 by default (`pipelineBuilder.go:242-247`, `pkg/query-service/constants/constants.go:668-678`).

### 3.4 `add`

[B] `pipelinesim/operators.go:23-77`; [O] `Add_*`, `X_AddNonString`.

Sets `field` to `value`. A string value `EXPR(<expression>)` is evaluated (§4) and its result set;
any other value is set as it is, with its type.

`add`, `copy` and `json_parser` write copies of maps and lists, never the value they read or the
configured one, so a record never contains itself and records share nothing (a choice of this
processor; the observed outputs are the same either way).

### 3.5 `normalize`

[B] `pipelinesim/json.go:119-206`; [O] `Normalize_*`. The backend's API does not accept it
(`pkg/types/pipelinetypes/pipeline.go:392-397`) but the reference runs it. A map body is kept; a string
body that is exactly one JSON object (no surrounding whitespace) becomes that object, integers as
integers, with `msg` renamed `message` when `message` is absent; any other body becomes
`{"message": <body>}`.

## 4. Expressions

`if`, route `expr` and `EXPR(...)` values are [expr-lang](https://expr-lang.org) expressions
([B] `pipelineexpr/pipelineexpr.go`).

### 4.1 Environment

| Name | Value |
|---|---|
| `body`, `$` | the body (`string`, `[]byte`, map, list, int64, double, bool or nil) |
| `attributes`, `resource` | maps |
| `timestamp` | the record time (`time.Time`) |
| `severity_text`, `severity_number` | text and number |
| `trace_id`, `span_id`, `trace_flags` | bytes (nil when unset) |
| `isJSON(v)` | whether a string or bytes is valid JSON |
| `unquote(s)` | `s` without Go string quotes, or unchanged; an error for a non-string |

[B] `pipelineexpr/pipelineexpr.go:61-81,138-161`; [O] `X_Expr_Helpers`, `X_Bodies_RouterTypeOf`.
expr-lang built-ins (`type`, `fromJSON`, `lower`, `matches`, `in`, `??`, …) are available; the
backend's filters use them (`pkg/query-service/queryBuilderToExpr/queryBuilderToExpr.go:55-110`).
Undefined names are nil.

**`env` is refused (deliberate deviation).** The reference also offers `env(name)`, the
collector's environment variable ([B] `pipelineexpr/pipelineexpr.go:25-26,63,83-93`; [O]
`X_Expr_Helpers`). Pipelines arrive over OpAMP, so `env` would let the backend, or anyone able
to push, read the collector's environment into exported logs. ADR 0004 keeps credentials local
and ADR 0008 lets a push change only processing; this is the expression counterpart of the
refusal of `${...}` references in pushed blocks ([opamp](opamp.md) §6). So:

- an expression (`if`, a route `expr`, an `EXPR(...)` value) that names `env`, called or not
  (`let f = env; f("X")`), or `os_env_func`, the reference's internal name for it, does not
  compile: the configuration is refused (§1.2), and a push using it fails preflight and is
  reported `FAILED` with the operator named (`operator "op1" (add): …`), never the expression;
- neither name is in the expression environment, so `$env["os_env_func"]` is nil.

Fields and attributes named `env` (`attributes.env`, `attributes["env"]`, `"env" in resource`)
are not affected. The backend never generates `env` (`pipelineBuilder.go`,
`pkg/query-service/queryBuilderToExpr`).

### 4.2 Results

A boolean expression that fails to run is an error for `if` (§6) and false for a route (§2). An
`EXPR` value that fails is an error of the `add` operator ([O] `X_Expr_IfRuntimeError*`,
`X_Expr_ValueRuntimeError*`).

### 4.3 Body fields

Member access on `body` (`body.a`, `body?.a`, `body["a"]`) reads a map body, or a string or bytes
body parsed as a JSON object; a body that is neither yields nil ([B] `pipelineexpr/pipelineexpr.go:95-136`;
[O] `X_Bodies_FieldAccess`, `X_Bodies_FilterOnBodyField`, `Filter_BodyJSON*`). The parsed object is
cached in the record's attributes under `__signozlogspipelineprocessor_internal__parsedBodyJson` the
first time it is needed, so later expressions in the same processor see that attribute and read the
cached object even after the body changed; the attribute is removed when the record leaves the
processor ([O] `X_ParsedBodyCache_Visible`, `X_ParsedBodyCache_Stale`, `X_ParsedBodyCache_AcrossPipelines`;
[B] `pipelinesim/pipeline.go:41-43`). Operators that read `body.<field>` (§3.1) use the same object.

In the reference, an expression naming `body_map` itself reads a value left over from an earlier
record ([O] `X_Expr_BodyMapVariable`); here it is nil.

## 5. Records

### 5.1 In

Each record becomes a stanza entry with its own copy of its resource's attributes and its scope
name ([B] `pipelinesim/convert.go:39-81`): body, attributes, time, severity number (1–24, other
values as 0) and text, trace and span IDs when not zero, flags when not zero (low 8 bits). A record
without an observed time gets the processor's clock ([O] `X_Metadata_*`: `set-by-collector`).

### 5.2 Out

Records that leave the processor are rebuilt ([O] `X_Metadata_*`; [B] `pipelinesim/convert.go:83-129`):

- Grouped by resource attributes and then by scope name. The order of groups is not part of the
  contract (the reference's varies between runs, [O] `X_Metadata_TwoPipelines`); this processor
  keeps the order in which groups first appear.
- Kept: resource attributes, scope name, time (when set), observed time, severity, body,
  attributes, trace and span IDs, flags.
- Not kept, also for records no operator touched: resource and scope dropped-attribute counts,
  schema URLs, scope version and attributes, the record's event name and dropped-attribute count
  ([O] `X_Metadata_Untouched`).
- Severity text: an empty text with a severity number 1–24 becomes that number's short name
  (`INFO`, `INFO2`, …) ([O] `X_OnlyNoop`, records 2–3).
- A nil value inside attributes, resource or body becomes the string `"<nil>"`; a value of
  another Go type becomes its `%v` text (a time: `2024-01-02 03:04:05 +0000 UTC`) ([O] `X_JSON_Nulls`,
  `JSON_Flattening`, `X_Expr_Helpers`). A nil body is an empty body ([O] `X_Bodies_RemoveBody`).
- Trace and span IDs shorter than 16 and 8 bytes are right-aligned (zero-padded on the left)
  ([O] `Trace_*`).

## 6. Errors

- `on_error` is `send` (default), `send_quiet`, `drop` or `drop_quiet`. On an operator's error
  the record goes on unchanged by that operator (`send*`) or is dropped (`drop*`); `*_quiet` logs at
  debug level instead of error ([S] `operator/helper/transformer.go`; [O] `X_Expr_IfRuntimeError*`,
  `Grok_NoMatch_*`, `Time_OnErrorDrop`, `Severity_OnErrorDrop`, `Trace_OnErrorDrop`,
  `X_Bodies_RegexNoIf*`).
- An `if` that fails to evaluate is an error of its operator ([O] `X_Expr_IfRuntimeErrorDrop`).
- A missing `parse_from` field or a value of the wrong type is an error ([S] `operator/helper/parser.go`;
  [O] `X_Bodies_RegexNoIfDrop`, `X_Bodies_JSON`). The backend's `if` nil checks keep most records
  from reaching that error ([O] `Regex_ParseFromMissingAttribute`, `Trace_MissingFields`).
- A panic inside an operator or an expression is that operator's error (no panic leaves the
  processor). The processor itself never fails a call: when every record is dropped it passes
  nothing on.

## 7. Not reproduced, open questions

- `body_map` leaking between records in the reference (§4.3) is not reproduced.
- Group order in the output (§5.2) is not reproduced; nothing downstream depends on it.
- Whether the reference's operators populate the parsed-body cache when they read `body.<field>`
  (§4.3), rather than reading the body separately, is not observable except through a later
  `attributes` expression; this processor uses the cache.
- `env(name)` is refused rather than reproduced (§4.1). [O] `X_Expr_Helpers` is refused as
  observed; the golden test replays it with `env(...)` replaced by the value the reference read.
