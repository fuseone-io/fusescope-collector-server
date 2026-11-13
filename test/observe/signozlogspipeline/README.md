# signozlogspipeline observation

Runs the existing collector (`fusescope-otel-collector`) as a black box with log pipeline
processors and records what comes out, for [`docs/spec/signozlogspipeline.md`](../../../docs/spec/signozlogspipeline.md)
(ADR 0002). Nothing here reads the collector's source.

```sh
# Regenerate the cases (only when they change); needs the FuseScope backend checkout.
python3 -I cases.py <fusescope>/tests/integration/testdata/simulatorcontract/logspipelines.golden.json \
  ../../../components/pkg/logspipeline/corpus/observed/cases.json
# Observe; needs Docker. Compose project observe-logspipeline, 127.0.0.1:34318 and :34133.
python3 -I observe.py ../../../components/pkg/logspipeline/corpus/observed/cases.json \
  ../../../components/pkg/logspipeline/corpus/observed
```

- `cases.json`: log sets (OTLP/JSON) and, per case, the processor blocks and their order.
  Cases without an `X_` prefix are the backend's simulator contract corpus, with the processors
  exactly as the backend deploys them (`$` escaped as `$$`); `X_` cases are written here in the
  same shape; `R_` and the backend's rejected cases are only started.
- `startup.json`: whether the reference starts with each configuration, and its error.
- `outputs.json`: per case, the documents the reference's `file` exporter wrote. Observed
  timestamps the reference took from its clock read `set-by-collector`.

Each case runs in its own logs pipeline of one reference instance, behind a `filter` that keeps
only that case's records. The files live in `components/pkg/logspipeline/corpus/observed` and are
published by the `corpus` package (ADR 0012, step 4). The golden tests in
`components/processor/signozlogspipelineprocessor` replay `cases.json` and compare with
`outputs.json` and `startup.json`. `corpus/library/messages.json` is not an observation: it comes
from the library (`go test -run TestCorpusMessages -update-messages` in `components/pkg/logspipeline`).

`cases.py` builds `cases.json` from the backend's golden as it was before step 4, when it still
held each case's processors. The reference is archived, so the observed files are not expected to
change.
