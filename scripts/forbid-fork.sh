#!/usr/bin/env bash
# Fails if any module given, or anything in its module graph (test
# dependencies included), is the AGPL collector fork or SigNoz's collector,
# whether required or put in by the right side of a replace: nothing of them
# may be built into or alongside this distribution (ADR 0002, ADR 0005).
set -euo pipefail

forbidden='^(github\.com/fuseone-io/fusescope-otel-collector|github\.com/SigNoz/signoz-otel-collector)(/|$)'
status=0
for module in "$@"; do
	# GOWORK=off: the module's own graph, never one a workspace adds.
	graph=$(cd "$module" && GOWORK=off go list -m -f '{{.Path}}{{with .Replace}} {{.Path}}{{end}}' all) || {
		echo "::error::$module: go list -m all failed"
		status=1
		continue
	}
	hits=$(tr ' ' '\n' <<<"$graph" | grep -iE "$forbidden" | sort -u || true)
	if [ -n "$hits" ]; then
		echo "::error::$module depends on $(tr '\n' ' ' <<<"$hits")"
		status=1
	fi
done
exit "$status"
