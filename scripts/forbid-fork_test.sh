#!/usr/bin/env bash
# The guard fails a module that requires the fork, directly or through a
# dependency, and passes one that does not. Offline: the fork is a local
# replace, never fetched.
set -euo pipefail

dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
guard=$(cd "$(dirname "$0")" && pwd)/forbid-fork.sh

mkdir -p "$dir/fork" "$dir/clean" "$dir/direct" "$dir/indirect" "$dir/lib"
printf 'module github.com/fuseone-io/fusescope-otel-collector\n\ngo 1.24\n' >"$dir/fork/go.mod"
printf 'module example.com/clean\n\ngo 1.24\n' >"$dir/clean/go.mod"
cat >"$dir/direct/go.mod" <<MOD
module example.com/direct

go 1.24

require github.com/fuseone-io/fusescope-otel-collector v0.0.0

replace github.com/fuseone-io/fusescope-otel-collector => ../fork
MOD
cat >"$dir/lib/go.mod" <<MOD
module example.com/lib

go 1.24

require github.com/fuseone-io/fusescope-otel-collector v0.0.0

replace github.com/fuseone-io/fusescope-otel-collector => ../fork
MOD
cat >"$dir/indirect/go.mod" <<MOD
module example.com/indirect

go 1.24

require (
	example.com/lib v0.0.0
	github.com/fuseone-io/fusescope-otel-collector v0.0.0 // indirect
)

replace (
	example.com/lib => ../lib
	github.com/fuseone-io/fusescope-otel-collector => ../fork
)
MOD

fail=0
expect() {
	local want=$1 module=$2
	if GOFLAGS=-mod=mod GOWORK=off "$guard" "$dir/$module" >/dev/null 2>&1; then got=pass; else got=fail; fi
	if [ "$got" != "$want" ]; then echo "forbid-fork: $module: want $want, got $got"; fail=1; fi
}
expect pass clean
expect fail direct
expect fail indirect
[ "$fail" -eq 0 ] && echo "forbid-fork: ok"
exit "$fail"
