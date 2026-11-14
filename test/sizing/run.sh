#!/bin/bash
cd "$(dirname "$0")"
rs(){ kubectl --context kind-bench -n platform get pods -l app.kubernetes.io/component=fusescope-collector -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{end}'; }
while read -r sig total; do
  [ -z "$sig" ] && continue
  w=$(( (total + 249) / 250 ))
  r0=$(rs)
  python3 bench.py "$sig" $(( total / w )) "$w" 40 90 >> results.jsonl 2>> errors.log || echo "FAILED $sig $total" >> errors.log
  echo "{\"step\": \"$sig $total\", \"gateway_restarts_before\": $r0, \"gateway_restarts_after\": $(rs)}" >> restarts.jsonl
  sleep 20
done < steps.txt
echo done >> errors.log
