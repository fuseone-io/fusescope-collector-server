#!/bin/bash
# Runs every "SIGNAL TOTAL_PER_S" line of runs/<RUN>/steps.txt, one telemetrygen worker per
# 250 records/s, and writes into runs/<RUN>/:
#   results.jsonl   one bench.py line per step
#   restarts.jsonl  gateway and agent restart counts and last termination reasons, per step
#   evidence/       for any pod that restarted during a step: its status and previous logs
#   errors.log      bench.py errors
# Each step starts from freshly restarted gateway and agent pods, so steps are independent:
# memory does not carry over from a previous step. Start one runner at a time.
set -u
cd "$(dirname "$0")"
RUN=${1:?usage: run.sh RUN}
D=runs/$RUN
mkdir -p "$D/evidence"
k() { kubectl --context kind-bench "$@"; }

state() { # restartCount|reason|finishedAt per component
  for c in fusescope-collector fusescope-agent; do
    k -n platform get pods -l app.kubernetes.io/component=$c -o \
      jsonpath='{.items[0].metadata.name}|{.items[0].status.containerStatuses[0].restartCount}|{.items[0].status.containerStatuses[0].lastState.terminated.reason}|{.items[0].status.containerStatuses[0].lastState.terminated.finishedAt}'
    echo
  done
}

fresh() { # restart both collectors and wait until they serve their metrics again
  k -n platform rollout restart deploy/fusescope-collector ds/fusescope-agent >/dev/null
  k -n platform rollout status deploy/fusescope-collector --timeout=300s >/dev/null
  k -n platform rollout status ds/fusescope-agent --timeout=300s >/dev/null
  for _ in $(seq 1 90); do # until the replaced pods are gone: one pod per component
    n=$(k -n platform get pods -l 'app.kubernetes.io/component in (fusescope-collector,fusescope-agent)' --no-headers | wc -l)
    [ "$n" = 2 ] && break
    sleep 2
  done
  for _ in $(seq 1 60); do
    curl -sf -m 5 -o /dev/null localhost:18001/metrics && curl -sf -m 5 -o /dev/null localhost:18002/metrics && break
    sleep 2
  done
  sleep 15
}

recover() { # until both pods are ready and the agent's queues are empty, at most 5 minutes
  for _ in $(seq 1 60); do
    ready=$(k -n platform get pods -l 'app.kubernetes.io/component in (fusescope-collector,fusescope-agent)' \
      -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{" "}{end}')
    queued=$(curl -s -m 5 localhost:18001/metrics | awk '/^otelcol_exporter_queue_size/ {s+=$2} END {print s+0}')
    if [ "$ready" = "true true " ] && [ "${queued:-1}" = 0 ]; then return 0; fi
    sleep 5
  done
  echo "recover: not idle after 5 minutes (ready=$ready queued=$queued)" >> "$D/errors.log"
}

i=0
while read -r sig total; do
  [ -z "$sig" ] && continue
  i=$((i + 1))
  k -n apps delete job --all --wait >/dev/null 2>&1
  fresh
  recover
  w=$(( (total + 249) / 250 ))
  before=$(state)
  python3 bench.py "$sig" $(( total / w )) "$w" 40 90 "$i" >> "$D/results.jsonl" 2>> "$D/errors.log" \
    || echo "FAILED $sig $total" >> "$D/errors.log"
  after=$(state)
  python3 - "$sig" "$total" "$before" "$after" "$i" >> "$D/restarts.jsonl" <<'EOF'
import json, sys
sig, total, before, after = sys.argv[1:5]
row = {"step": f"{sig} {total}", "step_index": int(sys.argv[5])}
for label, text in (("before", before), ("after", after)):
    for line, comp in zip(text.splitlines(), ("gateway", "agent")):
        pod, count, reason, at = (line.split("|") + ["", "", "", ""])[:4]
        row[f"{comp}_{label}"] = {"pod": pod, "restarts": int(count or 0), "last_reason": reason, "last_finished_at": at}
print(json.dumps(row))
EOF
  # Evidence for any restart during the step.
  paste <(echo "$before") <(echo "$after") | while IFS=$'\t' read -r b a; do
    pod=${a%%|*}; nb=$(echo "$b" | cut -d'|' -f2); na=$(echo "$a" | cut -d'|' -f2)
    if [ "${na:-0}" != "${nb:-0}" ]; then
      f="$D/evidence/${sig}-${total}-${pod}"
      k -n platform get pod "$pod" -o jsonpath='{.status.containerStatuses[0]}' > "$f.status.json"
      k -n platform logs "$pod" --previous --tail=300 > "$f.previous.log" 2>&1
    fi
  done
  sleep 20
done < "$D/steps.txt"
echo "done" >> "$D/errors.log"
