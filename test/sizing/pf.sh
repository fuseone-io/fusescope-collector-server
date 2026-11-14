#!/bin/bash
# Keeps a port-forward alive across pod restarts.
while true; do kubectl --context kind-bench -n platform port-forward "$1" "$2:8888" >/dev/null 2>&1; sleep 1; done
