# Modules shared with the backend

The FuseScope backend (`fuseone-io/fusescope`) imports these modules of this repository. With
`ingestpolicy` and `ingestreport` the collector and the backend run one ingest contract
(ADR 0006, `docs/spec/rum.md` §§10–11). With `logspipeline` the backend's pipeline preview runs
the collector's log pipelines (ADR 0012).

| Module | What the backend uses |
|---|---|
| `components/pkg/ingestpolicy` | the policy types, `Parse` and `Validate`, so it never publishes a snapshot the collector rejects; the capability and message type names; `ParseReportAck`'s counterpart format |
| `components/pkg/ingestreport` | the report page types, `Merge` and `Assembler` with the backend's bounds, behind its own `AgentStore` |
| `components/pkg/logspipeline` | `New`, `Validate` and `Pipeline.Process`: the log pipelines of `docs/spec/signozlogspipeline.md`, which the `signozlogspipeline` processor runs (ADR 0012) |

All are Apache-2.0 with a `LICENSE` in the module directory, since a module downloaded from
the Go proxy holds only its own directory. `ingestpolicy` also carries `LICENSE-MIT` for the
matching rules ported from the backend gateway.

## Releasing

- Each module is tagged on `main` with its directory as prefix:
  `components/pkg/ingestpolicy/v0.1.0`, `components/pkg/ingestreport/v0.1.0`.
- `logspipeline` takes the version of the collector release that ships it, for example
  `components/pkg/logspipeline/v0.6.0` with `v0.6.0`, so a preview pinned to a version matches
  the collector of that version (ADR 0012).
- They stay `v0`: a change of the Go API takes a minor bump, a fix a patch. A change of the wire
  format is never only a module release: it is a new `schemaVersion`, specified in `rum.md`
  first and accepted by both sides before either sends it.
- Inside this repository the modules are used through `replace` directives and
  `builder-config.yaml`, so a tag is only needed for the backend. Tag after the merge, then bump
  the backend's `go.mod` in its own PR.
