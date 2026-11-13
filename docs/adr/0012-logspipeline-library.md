# 0012 · One log pipelines library for the processor and the backend's preview

- Status: accepted (2026-11-11), with the seven decisions under "Decisions"
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

- `docs/spec/observed/signozlogspipeline` here (since step 4, `components/pkg/logspipeline/corpus/observed`) holds 223 cases and 88 startup outcomes, observed by
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
  `github.com/fuseone-io/fusescope-collector-server/components/pkg/logspipeline`, Apache-2.0,
  tagged `components/pkg/logspipeline/vX.Y.Z` with every collector release.
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
| Drift between preview and production | one source; only the glue and the selected versions differ, and CI checks the versions | the same | the same | caught only for what the corpus covers |
| Cost of a behaviour change | one change, one release | one change, plus a release of the library | one change, plus a backend release before the collector | two changes, plus a corpus change |
| Licence, ADR 0005 | Apache-2.0 here; the MIT backend may depend on it (stanza and pdata are already Apache-2.0 there) | the same | the collector takes an MIT dependency; allowed, but the backend module's whole requirement graph comes with it | unchanged |
| Clean-room, ADR 0002 | unchanged: both sources are FuseScope's own | unchanged | unchanged | unchanged |
| New dependency to approve | the backend depends on this repository's module | both depend on the new repository | the collector depends on the backend | none |
| Dependency versions | one set the library requires; each binary still selects its own (MVS, below), so CI checks the backend's | the same, plus a third release train | the same, reversed | each side moves alone, and the corpus catches the breaks |

### Points that hold for every A variant

- **The consumer selects the versions.** Under Go's minimal version selection, the main module
  decides the versions in its build. The library's `go.mod` sets only minimums, which the backend
  can raise, and a `replace` applies only in the main module. One library therefore does not
  guarantee one version of stanza, pdata or expr, and it does not stop the backend from bumping
  them. What it guarantees is one source. The versions are a check: the backend's CI compares
  the versions its build selects for stanza, pdata and expr with the library's, and fails on a
  drift that was not approved (decision 4).
- **expr.** In the backend binary, the library runs on `SigNoz/expr` v1.17.7-beta, through the
  backend's `replace`. The backend uses that fork for its query builder. In the collector, the
  library runs on `expr-lang/expr` v1.17.8. One library therefore does not by itself mean one
  expression engine. The compatibility check runs the library in the backend's effective module
  graph (decision 4), not with only expr swapped.
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

The canonical corpus, run in both module graphs, stays the safety net for the version and expr
gaps and for the glue on each side.

## Migration

Each step is a separate pull request. None of them changes a golden.

1. **Library from the processor, behaviour unchanged.** Extract the module from the processor.
   The processor uses it, and `golden_test.go`, the benchmarks and the managed-pipeline preflight
   tests pass unchanged. Release the collector and tag the module.
2. **The preview on the library.** The backend imports the tagged module behind the preview's
   current API, and `pipelinesim` stays in the tree, unused. Criteria:
   - The backend runs the corpus with its own `go.mod`, in its effective module graph, including
     `replace github.com/expr-lang/expr => github.com/SigNoz/expr`.
   - `TestSimulatorCorpus` keeps the 168 log pipeline observations (164 outputs and 4 refusals)
     and the 3 preview-only log cases. The 44 span mapper observations stay untouched.
   - The version check (decision 4) is in the backend's CI and passes.
   - A preview-only case that changes is a decision for the reviewer, never an edit to a golden.
3. **Remove `pipelinesim`** after one backend release on the library. Also remove the backend's
   separate `pkg/grok` if nothing else uses it.
4. **One corpus.** The observed corpus in this repository becomes the only log pipelines corpus
   (decision 5). `simulatorcontract`'s log cases that it lacks are added to it, and the module
   publishes it. From then on, `simulatorcontract`'s log pipeline goldens are no longer an
   independent copy: the backend consumes the published corpus or generates its goldens from
   it, at the pinned tag. The replay through the `fusescope-collector` image stays as the
   end-to-end check. The span mapper cases stay in `simulatorcontract` as they are, outside this
   unification.

## Versioning and rollback

- The module is tagged `components/pkg/logspipeline/vX.Y.Z` with the collector release `vX.Y.Z` that ships
  it. The backend pins the tag of the collector version its chart installs, so a preview on
  version N matches the collector on version N.
- dependabot keeps holding the collector's modules in the backend (fusescope#361). A library bump
  in the backend goes with the chart's collector bump.
- A bump of stanza, pdata or expr in the backend that leaves the library's versions fails the
  version check until it is approved and recorded. The usual path is a library release first.
- **Rollback of step 2:** revert the backend's import. `pipelinesim` is still in the tree until
  step 3. **Rollback of step 1:** revert the extraction. The processor's code is unchanged in
  substance, and the goldens show that.
- Breaking changes are safe while there are no users, but the library still follows semver within
  its tags, so that the backend's pin tells which behaviour it has.

## Decisions

Accepted on 2026-11-11.

1. **One library (A).**
2. **A1:** the module `components/pkg/logspipeline` in this repository. It sits beside
   `ingestpolicy` and `ingestreport`, the other modules the backend imports
   (`docs/shared-modules.md`), where the Makefile, `forbid-fork` and `builder-config.yaml` already
   cover modules. Amended on 2026-11-11 from `pkg/logspipeline`, before anything was released.
3. **Licence:** Apache-2.0, like this repository. The MIT backend already ships Apache-2.0
   modules.
4. **Versions and expr.**
   - The backend keeps its `replace` with `SigNoz/expr` for now.
   - A compatibility CI job tests the library in the backend's effective module graph: its
     `go.mod`, its selected versions and its `replace`. Swapping only expr is not enough.
   - The same job compares the versions the backend's build selects for stanza, pdata and expr
     with the library's, and fails on a drift that was not approved.
5. **The canonical corpus is the observed corpus in this repository.** After step 4,
   `simulatorcontract`'s log pipeline goldens consume it, or are generated from it, as the module
   publishes it. The span mapper cases stay out of this.
6. **Where the two behaviours differ, the collector's spec wins.** Each preview-only case is
   decided on its own.
7. **The backend's dependency on this repository's module is approved.**

## Consequences

- One behaviour change, and one review, reaches production and the preview together.
- The backend can still select other versions of stanza, pdata and expr (MVS), but its CI
  fails on a drift from the library's that was not approved, so a bump goes through the library
  first.
- The library becomes a public API with semver, between two repositories of the same team.
