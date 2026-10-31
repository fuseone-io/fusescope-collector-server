# Upgrading upstream

1. Pick an upstream release and read the core and contrib changelogs, especially breaking
   changes and renamed component types.
2. In `builder-config.yaml`, bump every core module (`go.opentelemetry.io/collector/...`) and
   every contrib module to that release. Stable modules (`v1.x`, such as the config providers
   and `k8sattributesprocessor`) take the `v1` version tagged in the same release; the
   `versions.yaml` file at the upstream tag lists them.
3. Bump `OCB_VERSION` in the `Makefile` and the CI workflow to the same release.
4. Update renamed component types in `config/` and in the docs.
5. `make build validate`, then open a PR; CI also runs the license and vulnerability checks.
