# FuseScope Collector · engineering rules

A distribution of the upstream OpenTelemetry Collector built with `ocb` from
`builder-config.yaml`, plus FuseScope components in `components/`. Read `DESIGN.md`; decisions
in `docs/adr/` are binding until superseded.

## Non-negotiable

- **Clean-room** (ADR 0002): never open, copy, translate or summarise `fusescope-otel-collector`
  or SigNoz collector source, including git history. Specify behavior in `docs/spec/` from the
  FuseScope backend, observed data, the OpenTelemetry spec or engine `MAPPING.md`, citing the
  source of every rule. Say "clean-room" in the commit message when touching that behavior.
- **Licenses** (ADR 0005): source is Apache-2.0; dependencies MIT, BSD, ISC, Apache-2.0, and
  MPL-2.0 only unmodified. Ask before adding any dependency.
- **Upstream as a library** (ADR 0001): never vendor or patch upstream code; bump all module
  versions together (`docs/upgrading.md`).
- **Security baseline** (ADR 0004): TLS 1.3, tokens only from files, no remote change of
  receivers/exporters/extensions/credentials, debug endpoints off or on `localhost`.

## Go

- Each component is its own module on public collector APIs, with `metadata.yaml` and the
  upstream `mdatagen` conventions.
- Tests first for behavior changes; table-driven tests next to the code; a regression test for
  every bug fix.
- No `panic` on data paths; errors are returned and, for consumers, marked permanent or
  retryable deliberately.
- Hot paths (exporters, processors) avoid per-record allocations; add a benchmark with any
  change to them.
- Comments only for non-obvious behavior.

## Commits and pull requests

- Conventional commits scoped by component: `feat(clickhouseexporter): …`, `chore(deps): …`.
- AI-assisted commits end with a `Co-Authored-By:` trailer naming the model.
- PRs follow `.github/pull_request_template.md`, keep only the headings that apply, and are
  merged with CI green.

## When to ask

Before adding a dependency, changing the ClickHouse schema contract, changing what OpAMP may
configure, or anything touching licensing or the clean-room boundary.
