# FuseScope components

Each component is its own Go module (`components/<kind>/<name>`) built only on public
OpenTelemetry Collector APIs, and is added to `builder-config.yaml` with `path:` once it
exists. Planned components and their scope are listed in `DESIGN.md` §3.2.

Before writing one, write its behavior in `docs/spec/<name>.md` with a source for every rule
(see ADR 0002).

`components/pkg/<name>` holds libraries shared by several components, with no `metadata.yaml`;
a component requires one with a `replace` to its path, and `builder-config.yaml` lists the same
`replace`. Today: `ingestpolicy`, the OpAMP ingest policy store that `rumreceiver` and
`ingestauth` read ([ADR 0006](../docs/adr/0006-single-ingress.md)).
