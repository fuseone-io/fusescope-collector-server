# 0012 · One log pipelines library for the processor and the backend's preview

- Status: proposed
- Date: 2026-11-11
- Issue: [#54](https://github.com/fuseone-io/fusescope-collector-server/issues/54)
- Related: FuseScope ADR 0001 (backend preview without the AGPL collector), ADR 0002 (clean-room),
  ADR 0005 (licences), ADR 0008 (OpAMP remote configuration)

## Context

FuseScope builds log pipelines in two places. Both are FuseScope's own code, written clean-room
on contrib `pkg/stanza`, and neither one contains code from the AGPL fork or SigNoz's collector:

| | `signozlogspipelineprocessor` (this repository) | `pipelinesim` (fuseone-io/fusescope) |
|---|---|---|
| Role | runs the pipelines FuseScope pushes over OpAMP, on every log record | runs the same pipelines on sample logs for the preview |
| Licence | Apache-2.0 | MIT |
| Size | 2,111 lines, plus 2,395 lines of tests | 987 lines, plus 229 lines of tests |
| Spec | `docs/spec/signozlogspipeline.md` | written from FuseScope ADR 0001 and the same observations |
| stanza, pdata | v0.164.1, v1.70.0 | v0.164.1, v1.70.0 |
| expr | `expr-lang/expr` v1.17.8 | `SigNoz/expr` v1.17.7-beta, through a module-wide `replace` |
| grok | `internal/grok`, vjeantet/grok (Apache-2.0) vendored | `pkg/grok`, the same library vendored separately |

Two corpora keep them close:

- `docs/spec/observed/signozlogspipeline` here holds 223 cases and 88 startup outcomes, observed by
  running the fork as a black box (ADR 0002). `golden_test.go` replays them against the
  processor. The cases it does not reproduce are listed in `unsupported` (1) and `stricter` (4).
- `tests/integration/testdata/simulatorcontract` in the backend holds 171 log pipeline cases and
  47 span mapper cases. The backend's `TestSimulatorCorpus` checks the preview against them. The
  integration suite replays the same goldens through the `fusescope-collector` image. **212
  observations** come from the collector side: 164 log outputs, 4 rejected configurations and
  44 span mapper outputs. A further 3 log and 3 span cases are recorded from the preview only.

The two still drift. In v0.5.0, the processor refused a grok pattern that does not compile at
startup, while the preview failed each record. Under ADR 0008 a rejected push fails as a whole,
so one pipeline the preview accepted blocked every other pipeline (#87). Fixing that took changes
on both sides and a new collector release. Updating stanza and pdata in the backend
(fusescope#361) broke 21 preview cases, which had to be fixed in the preview alone.

This ADR covers log pipelines only. The span mapper has the same shape, and can follow the same
decision afterwards.

## Options

### A. One library, imported by both

The stanza pipeline building, the operators that differ from contrib, expression handling, grok,
configuration validation and the conversion between pdata and stanza entries move into one Go
module. The processor keeps the collector glue: the factory, `metadata.yaml`, telemetry and
`consume`. The preview keeps the backend glue: the API, sample handling and error codes.

Where the module lives:

- **A1. A module in this repository**, for example
  `github.com/fuseone-io/fusescope-collector-server/pkg/logspipeline`, Apache-2.0, tagged
  `pkg/logspipeline/vX.Y.Z` with every collector release.
- **A2. A new repository** (`fuseone-io/fusescope-logspipeline`), Apache-2.0, with its own
  releases.
- **A3. A module in the backend repository**, MIT. The collector would then depend on the
  backend's repository and on its release cadence.

### B. Two implementations, held together by one corpus

Both implementations stay. The two corpora are merged into one, holding the pipeline, the
input, the expected output and, where it applies, the expected refusal. Both CIs run it, and
neither side may change it alone.

## Comparison

| | A1 | A2 | A3 | B |
|---|---|---|---|---|
| Drift between preview and production | none by construction; only the glue differs | none | none | caught only for what the corpus covers |
| Cost of a behaviour change | one change, one release | one change, plus a release of the library | one change, plus a backend release before the collector | two changes, plus a corpus change |
| Licence, ADR 0005 | Apache-2.0 here; the MIT backend may depend on it (stanza and pdata are already Apache-2.0 there) | the same | the collector takes an MIT dependency; allowed, but the backend module's whole requirement graph comes with it | unchanged |
| Clean-room, ADR 0002 | unchanged: both sources are FuseScope's own | unchanged | unchanged | unchanged |
| New dependency to approve | the backend depends on this repository's module | both depend on the new repository | the collector depends on the backend | none |
| Dependency versions | one set of stanza, pdata and expr versions, which the backend must follow | the same, plus a third release train | the collector follows the backend's versions | each side moves alone, and the corpus catches the breaks |

### Points that hold for every A variant

- **expr.** A `replace` applies only in the main module. In the backend binary, the library would
  run on `SigNoz/expr` v1.17.7-beta, which the backend uses for its query builder. In the
  collector it would run on `expr-lang/expr` v1.17.8. One library therefore does not by itself
  mean one expression engine. The backend's `TestSimulatorCorpus` already runs under the fork and
  stays the check. Closing this gap is a separate choice (decision 4 below).
- **Validation and error messages.** One `Validate` decides what both sides accept. A pipeline
  saved in the UI cannot then be refused by the collector at push time, which is the failure #87
  fixed by hand. The library returns typed errors, with the operator, the field and the reason.
  The processor logs them and the preview maps them to its API error codes. The wording shown to
  users stays in the backend.
- **Performance.** The library is on the collector's hot path. It keeps the rule of no
  allocations per record, and the processor's benchmarks move with the code. The preview runs
  few records, so it is unaffected.
- **Binaries.** The `fusescope` binary gains no new third-party module. It already compiles stanza,
  pdata and expr, and the library replaces `pipelinesim`. The collector binary does not change
  beyond the move. Licence checks run in both repositories as they do today.

### B's limits

B is today's situation, with one corpus instead of two. It catches drift only where a case
exists. Every behaviour change, such as #87's, still needs two implementations, two reviews and
a corpus update, and the corpus has to grow with every such change.

## Proposal

**A1**: one library, as a module in this repository.

- It is the collector's behaviour, so it lives with the collector and its spec.
- It keeps one release train: the library is tagged with every collector release.
- It needs no new repository.

The corpus stays as the safety net for the expr gap and for the glue on each side.

## Migration

Each step is a separate pull request. None of them changes a golden.

1. **Library from the processor, behaviour unchanged.** Extract the module from the processor.
   The processor uses it, and `golden_test.go`, the benchmarks and the managed-pipeline preflight
   tests pass unchanged. Release the collector and tag the module.
2. **The preview on the library.** The backend imports the tagged module behind the preview's
   current API, and `pipelinesim` stays in the tree, unused. `TestSimulatorCorpus` must keep the
   168 log pipeline observations (164 outputs and 4 refusals) and the 3 preview-only log cases,
   and the 44 span mapper observations stay untouched. A preview-only case that changes is a
   decision for the reviewer, never an edit to a golden.
3. **Remove `pipelinesim`** after one backend release on the library. Also remove the backend's
   separate `pkg/grok` if nothing else uses it.
4. **One corpus.** Merge `simulatorcontract`'s log cases into the library's observed corpus (or
   the reverse; decision 5 below). Keep the end-to-end replay through the image in the backend's
   integration suite.

## Versioning and rollback

- The module is tagged `pkg/logspipeline/vX.Y.Z` with the collector release `vX.Y.Z` that ships
  it. The backend pins the tag of the collector version its chart installs, so a preview on
  version N matches the collector on version N.
- dependabot keeps holding the collector's modules in the backend (fusescope#361). A library bump
  in the backend goes with the chart's collector bump.
- **Rollback of step 2:** revert the backend's import. `pipelinesim` is still in the tree until
  step 3. **Rollback of step 1:** revert the extraction. The processor's code is unchanged in
  substance, and the goldens show that.
- Breaking changes are safe while there are no users, but the library still follows semver within
  its tags, so that the backend's pin tells which behaviour it has.

## Decisions that need a choice

1. One library (A) or two implementations with one corpus (B). Proposal: A.
2. Where the library lives: A1, A2 or A3. Proposal: A1.
3. Licence of the library: Apache-2.0, as in this repository, or MIT. Proposal: Apache-2.0. The
   MIT backend already ships Apache-2.0 modules.
4. The expr gap. Options:
   - keep it, with `TestSimulatorCorpus` as the guard;
   - add a CI job here that runs the library's tests under `SigNoz/expr`;
   - drop the backend's `replace`, which is a separate decision for the backend's query builder.

   Proposal: the CI job, until the backend decides on the `replace`.
5. Which corpus becomes the single one: the observed corpus here or `simulatorcontract`.
   Proposal: the observed corpus here, which is broader, with `simulatorcontract` kept as the
   end-to-end replay through the image.
6. Where today's two behaviours differ, the collector's spec wins, because the collector's
   behaviour is what users get in production. Any preview-only case that changes in step 2 is
   reviewed one by one.
7. Dependency approval (CLAUDE.md): the backend takes a dependency on this repository's module.

## Consequences

- One behaviour change, and one review, reaches production and the preview together.
- The backend follows the collector's stanza, pdata and expr versions for log pipelines, and can
  no longer bump them alone.
- The library becomes a public API with semver, between two repositories of the same team.
