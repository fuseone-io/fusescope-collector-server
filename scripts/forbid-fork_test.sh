#!/usr/bin/env bash
# The guard fails a module that reaches the fork or SigNoz's collector, by a
# direct requirement, through a dependency's go.mod, or on the right side of
# a replace, and passes one that does not. Offline: the modules are served by
# a file proxy and cached in a temporary directory.
set -euo pipefail

dir=$(mktemp -d)
trap 'chmod -R u+w "$dir" 2>/dev/null; rm -rf "$dir"' EXIT
guard=$(cd "$(dirname "$0")" && pwd)/forbid-fork.sh

# publish <module path> <escaped proxy path> [go.mod requirements]
publish() {
	local path=$1 escaped=$2 requires=${3:-}
	mkdir -p "$dir/proxy/$escaped/@v"
	echo v0.1.0 >"$dir/proxy/$escaped/@v/list"
	echo '{"Version":"v0.1.0","Time":"2026-01-01T00:00:00Z"}' >"$dir/proxy/$escaped/@v/v0.1.0.info"
	printf 'module %s\n\ngo 1.24\n%s' "$path" "$requires" >"$dir/proxy/$escaped/@v/v0.1.0.mod"
}
publish github.com/fuseone-io/fusescope-otel-collector github.com/fuseone-io/fusescope-otel-collector
publish github.com/SigNoz/signoz-otel-collector github.com/!sig!noz/signoz-otel-collector
publish example.com/lib example.com/lib $'\nrequire github.com/fuseone-io/fusescope-otel-collector v0.1.0\n'
publish example.com/dependency example.com/dependency

# module <name> <go.mod body after the go line>
module() {
	mkdir -p "$dir/$1"
	printf 'module example.com/%s\n\ngo 1.24\n%s' "$1" "$2" >"$dir/$1/go.mod"
}
module clean ''
module direct $'\nrequire github.com/fuseone-io/fusescope-otel-collector v0.1.0\n'
module signoz $'\nrequire github.com/SigNoz/signoz-otel-collector v0.1.0\n'
# Only example.com/lib is required: the fork comes from lib's go.mod.
module indirect $'\nrequire example.com/lib v0.1.0\n'
# The fork only on the right side of a replace.
module replaced $'\nrequire example.com/dependency v0.1.0\n\nreplace example.com/dependency => github.com/SigNoz/signoz-otel-collector v0.1.0\n'

export GOPROXY="file://$dir/proxy" GOSUMDB=off GOFLAGS=-mod=mod GOMODCACHE="$dir/cache" GONOSUMCHECK=1 GOTOOLCHAIN=local
fail=0
expect() {
	local want=$1 name=$2
	if "$guard" "$dir/$name" >/dev/null 2>&1; then got=pass; else got=fail; fi
	if [ "$got" != "$want" ]; then echo "forbid-fork: $name: want $want, got $got"; fail=1; fi
}
expect pass clean
expect fail direct
expect fail signoz
expect fail indirect
expect fail replaced
[ "$fail" -eq 0 ] && echo "forbid-fork: ok"
exit "$fail"
