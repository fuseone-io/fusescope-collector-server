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
[ -f "$D/steps.txt" ] || { echo "no $D/steps.txt" >&2; exit 2; }
# A run starts clean: earlier outputs of this run are removed, never appended to.
rm -rf "$D/results.jsonl" "$D/restarts.jsonl" "$D/errors.log" "$D/evidence"
mkdir -p "$D/evidence"
k() { kubectl --context kind-bench "$@"; }

state() { # restartCount|reason|finishedAt per component
  for c in fusescope-collector fusescope-agent; do
    k -n platform get pods -l app.kubernetes.io/component=$c -o \
      jsonpath='{.items[0].metadata.name}|{.items[0].status.containerStatuses[0].restartCount}|{.items[0].status.containerStatuses[0].lastState.terminated.reason}|{.items[0].status.containerStatuses[0].lastState.terminated.finishedAt}'
    echo
  done
}

pods() { # name/uid of every gateway and agent pod
  k -n platform get pods -l 'app.kubernetes.io/component in (fusescope-collector,fusescope-agent)' \
    -o jsonpath='{range .items[*]}{.metadata.uid}{"\n"}{end}'
}

uptime_of() { # seconds the process behind a forwarded :8888 has been up
  curl -sf -m 5 "localhost:$1/metrics" | awk '/^otelcol_process_uptime/ {print int($2); exit}'
}

clear_jobs() { # no generator may survive into the next step
  local remaining
  k -n apps delete job --all --wait --timeout=120s >/dev/null 2>&1 || return 1
  # Captured apart from the test, so a failing kubectl fails the check instead of passing it.
  remaining=$(k -n apps get jobs,pods -l '!none' --no-headers 2>/dev/null) || return 1
  [ -z "$remaining" ] || return 1
}

fresh() { # restart both collectors and prove the step will measure the new pods
  local before after uid n up
  before=$(pods) || return 1
  [ -n "$before" ] || return 1
  k -n platform rollout restart deploy/fusescope-collector ds/fusescope-agent >/dev/null || return 1
  k -n platform rollout status deploy/fusescope-collector --timeout=300s >/dev/null || return 1
  k -n platform rollout status ds/fusescope-agent --timeout=300s >/dev/null || return 1
  for _ in $(seq 1 90); do # until the replaced pods are gone: one pod per component
    n=$(k -n platform get pods -l 'app.kubernetes.io/component in (fusescope-collector,fusescope-agent)' --no-headers | wc -l)
    [ "$n" = 2 ] && break
    sleep 2
  done
  [ "$n" = 2 ] || return 1
  after=$(pods) || return 1
  for uid in $after; do # every pod now running must be new
    if printf '%s\n' "$before" | grep -qx "$uid"; then return 1; fi
  done
  for _ in $(seq 1 60); do
    curl -sf -m 5 -o /dev/null localhost:18001/metrics && curl -sf -m 5 -o /dev/null localhost:18002/metrics && break
    sleep 2
  done
  for port in 18001 18002; do # the forwarded processes are the restarted ones
    up=$(uptime_of $port)
    [ -n "$up" ] && [ "$up" -lt 300 ] || return 1
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
  return 1
}

i=0
while read -r sig total; do
  [ -z "$sig" ] && continue
  i=$((i + 1))
  # A step that does not start without generators, from fresh and idle collectors, is skipped,
  # not measured.
  if ! clear_jobs; then
    echo "SKIPPED $sig $total: a generator Job or pod remains" >> "$D/errors.log"; continue
  fi
  if ! fresh; then
    echo "SKIPPED $sig $total: collectors not restarted" >> "$D/errors.log"; continue
  fi
  if ! recover; then
    echo "SKIPPED $sig $total: collectors not idle" >> "$D/errors.log"; continue
  fi
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
