#!/usr/bin/env bash
# Run an in-cluster write-path benchmark against one or more vraft clusters and
# collect throughput + latency results.
#
# Usage: ./bench.sh N C SIZE [variants...]
#   N    total apply ops        (default 50000)
#   C    concurrent clients     (default 16)
#   SIZE value size in bytes    (default 64)
# variants default to: base bw async both
#
# A vraft-bench Job runs inside the cluster, finds the leader of each cluster,
# and POSTs /bench to it. The benchmark executes inside the vraftd leader
# process, so the timers measure the real raft write path end-to-end.
set -euo pipefail
CTX=${CTX:-kind-hakit}
DIR=$(dirname "$0")

N=${1:-50000}; C=${2:-16}; SIZE=${3:-64}; shift 3 || true
variants=("$@")
[ ${#variants[@]} -eq 0 ] && variants=(base bw async both)

for v in "${variants[@]}"; do
  job="vraft-bench-${v}-$(date +%s)"
  targets="vraft-$v-0.vraft-$v:9000,vraft-$v-1.vraft-$v:9000,vraft-$v-2.vraft-$v:9000"
  sed -e "s/__JOB__/$job/g" \
      -e "s/__TARGETS__/$targets/g" \
      -e "s/__N__/$N/g" \
      -e "s/__C__/$C/g" \
      -e "s/__SIZE__/$SIZE/g" \
      "$DIR/bench-job.yaml.tmpl" | kubectl --context "$CTX" apply -f - >/dev/null
  if kubectl --context "$CTX" -n vraft wait --for=condition=complete --timeout=300s "job/$job" >/dev/null 2>&1; then
    echo -n "$v n=$N c=$C size=$SIZE: "
    kubectl --context "$CTX" -n vraft logs "job/$job" | grep -E '^(leader=|ops=)' | tr '\n' ' '
    echo
  else
    echo "$v: job FAILED"
    kubectl --context "$CTX" -n vraft logs "job/$job" | tail -5
  fi
  kubectl --context "$CTX" -n vraft delete "job/$job" >/dev/null 2>&1 || true
done
