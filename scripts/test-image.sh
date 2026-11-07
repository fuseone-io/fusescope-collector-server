#!/usr/bin/env bash
# Checks a built image: both binaries run as the image's non-root user and
# report the version it was built with, on the expected architecture, and the
# collector stays the entrypoint.
#   scripts/test-image.sh IMAGE VERSION ARCH     (ARCH: amd64 or arm64)
set -euo pipefail
[ $# -eq 3 ] || { echo "usage: $0 IMAGE VERSION ARCH" >&2; exit 2; }
image=$1 version=$2 arch=$3
fail() { echo "test-image: $*" >&2; exit 1; }

inspect() { docker image inspect "$image" --format "$1"; }
[ "$(inspect '{{.Architecture}}')" = "$arch" ] || fail "architecture $(inspect '{{.Architecture}}'), want $arch"
[ "$(inspect '{{.Config.User}}')" = "nonroot:nonroot" ] || fail "user $(inspect '{{.Config.User}}'), want nonroot:nonroot"
[ "$(inspect '{{json .Config.Entrypoint}}')" = '["/fusescope-collector"]' ] || fail "entrypoint $(inspect '{{json .Config.Entrypoint}}')"
[ "$(inspect '{{json .Config.Cmd}}')" = '["--config=/etc/fusescope-collector/config.yaml"]' ] || fail "cmd $(inspect '{{json .Config.Cmd}}')"

# Run as the image's user: both binaries must be executable by it.
collector=$(docker run --rm --platform "linux/$arch" "$image" --version)
[ "$collector" = "fusescope-collector version $version" ] || fail "collector: $collector, want version $version"
schema=$(docker run --rm --platform "linux/$arch" --entrypoint /fusescope-schema "$image" --version)
[ "$schema" = "fusescope-schema version $version" ] || fail "fusescope-schema: $schema, want version $version"
echo "$image ($arch): $collector; $schema"
