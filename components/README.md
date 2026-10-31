# FuseScope components

Each component is its own Go module (`components/<kind>/<name>`) built only on public
OpenTelemetry Collector APIs, and is added to `builder-config.yaml` with `path:` once it
exists. Planned components and their scope are listed in `DESIGN.md` §3.2.

Before writing one, write its behavior in `docs/spec/<name>.md` with a source for every rule
(see ADR 0002).
