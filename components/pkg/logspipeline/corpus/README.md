# Log pipelines corpus

The canonical corpus of the log pipelines (ADR 0012, decision 5), published with the
`logspipeline` module so the processor and the backend's preview are checked against one source.
Read it through this package's functions. Each call decodes anew and keeps numbers as
`json.Number`.

## observed/: black-box observations

Recorded from `ghcr.io/fuseone-io/fusescope-otel-collector:0.145.0` (ADR 0002) by
[`test/observe/signozlogspipeline`](../../../../test/observe/signozlogspipeline). They are kept as
observed, and `run.yaml` counts only them: 223 cases, 88 startup checks, 24 refused at startup.

| File | What it holds |
|---|---|
| `cases.json` | the log sets (OTLP/JSON) and, per case, the processors as the backend deploys them, their order and the case's `origin`: `backend` (its simulator contract corpus) or `hand-written` |
| `outputs.json` | per case, the documents the reference wrote; timestamps it took from its clock read `set-by-collector` |
| `startup.json` | whether the reference started with each configuration, and its first error |

## library/: what this library produces, not observed

| File | What it holds |
|---|---|
| `messages.json` | per case the library builds, the messages it logs while processing records, at warn or above, after the build logs are drained |

Each message names its record by `input`: the record's position in the case's log set, counting
the records of every document, resource and scope in order, from 0. A record may have several
messages; they keep the order they were logged in. A case listed with no messages was evaluated
and logged nothing. A case missing from the file was not evaluated, because the library refuses
its configuration. Quiet `on_error` modes log nothing, and a grok pattern that does not compile
logs its warning at build time, which is not a record message.

The messages' text is the library's in this module's graph (`expr-lang/expr` as `go.mod`
requires). Regenerate with `go test -run TestCorpusMessages -update-messages` in
`components/pkg/logspipeline`. `TestCorpusMessages` fails when the file and the library disagree.

The three cases of the backend's corpus that never reach the collector stay in the backend:
- `Time_StrptimeInvalidLayout_ConfigError` and `Add_ExprInvalid_ConfigError`, refused by its
  pipeline builder before any processor exists;
- `Pipeline_NoPipelines`, which has none.
