#!/usr/bin/env bash
# Fails on any vulnerability in the binary that is not listed in
# .govulncheck-allow with a reason and a revisit date.
set -euo pipefail

binary=$1
allow=$(dirname "$0")/../.govulncheck-allow

report=$(go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 -mode=binary -format=json "$binary")
found=$(jq -r 'select(.finding) | .finding.osv' <<<"$report" | sort -u)
allowed=$(grep -oE '^GO-[0-9]{4}-[0-9]+' "$allow" | sort -u || true)
blocking=$(comm -23 <(printf '%s\n' "$found" | sed '/^$/d') <(printf '%s\n' "$allowed" | sed '/^$/d'))

for id in $found; do
  fixed=$(jq -r --arg id "$id" 'select(.finding and .finding.osv == $id) | .finding.fixed_version // "none"' <<<"$report" | head -1)
  status=allowed
  grep -qx "$id" <<<"$blocking" && status=BLOCKING
  echo "$id fixed_in=$fixed $status"
done

if [ -n "$blocking" ]; then
  echo "Unlisted vulnerabilities: $(echo $blocking)" >&2
  exit 1
fi
