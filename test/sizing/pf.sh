#!/bin/bash
# Keeps a port-forward to the loopback :8888 of the current pod matching a label selector,
# following it across restarts and replacements. Usage: pf.sh SELECTOR LOCAL_PORT
while true; do
  pod=$(kubectl --context kind-bench -n platform get pods -l "$1" \
    --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  [ -n "$pod" ] && kubectl --context kind-bench -n platform port-forward "pod/$pod" "$2:8888" >/dev/null 2>&1
  sleep 1
done
