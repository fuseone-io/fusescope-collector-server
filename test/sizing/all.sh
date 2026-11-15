#!/bin/bash
# The three runs behind the docs page, in order: the gateway's memory limit at 1Gi (the chart's
# default), 2Gi and 4Gi. Each run's steps.txt lists its steps; the agent keeps the default 512Mi.
set -u
cd "$(dirname "$0")"
up() {
  helm --kube-context kind-bench upgrade fusescope fusescope/fusescope --version "${CHART_VERSION:-0.170.1}" \
    -n platform -f values.yaml --set "fusescopeCollector.resources.limits.memory=$1" --wait --timeout 10m >/dev/null
}
up 1Gi && ./run.sh 1-gateway-1gi
up 2Gi && ./run.sh 2-gateway-2gi
up 4Gi && ./run.sh 3-gateway-4gi
echo "all done"
