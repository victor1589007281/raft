#!/usr/bin/env bash
# Benchmark the read-consistency tiers (12.2) of a vraft cluster from the host.
#
# POSTs /bench-read to the leader for each tier (strict|lease|stale) and prints
# a comparison table. strict pays a quorum round per read (linearizable);
# lease/stale are local and differ only in the leadership/lease check. The
# workload runs in-process inside vraftd, so the numbers are the real raft
# read-path latencies.
#
# Usage: ./bench-read.sh N C [cluster]   (defaults N=20000 C=16 cluster=vraft-both)
set -euo pipefail
CTX=${CTX:-kind-hakit}
NS=vraft
N=${1:-20000}; C=${2:-16}; NAME=${3:-vraft-both}

k8s() { kubectl --context "$CTX" -n "$NS" "$@"; }
http() {
  local pod=$1 method=$2 path=$3 data=${4:-}
  if [ -n "$data" ]; then
    k8s exec "$pod" -- wget -qO- --header 'Content-Type: application/json' \
      --post-data "$data" "http://127.0.0.1:9000$path" 2>/dev/null || true
  else
    k8s exec "$pod" -- wget -qO- "http://127.0.0.1:9000$path" 2>/dev/null || true
  fi
}
find_leader() {
  for i in 0 1 2; do
    st=$(k8s exec "$NAME-$i" -- wget -qO- http://127.0.0.1:9000/state 2>/dev/null || true)
    if echo "$st" | grep -q '"role":"Leader"'; then
      echo "$NAME-$i"
      return 0
    fi
  done
  return 1
}

leader=$(find_leader) || { echo "no leader"; exit 1; }
echo "== read-path benchmark cluster=$NAME leader=$leader n=$N c=$C =="

echo "tier       ops/s      p50(ms)  p90(ms)  p99(ms)  max(ms)  failed"
for tier in strict lease stale; do
  res=$(http "$leader" POST /bench-read "{\"n\":$N,\"c\":$C,\"consistency\":\"$tier\"}")
  ops=$(echo "$res"  | jq -r '.throughput_ops_s')
  p50=$(echo "$res" | jq -r '.p50_ns / 1e6')
  p90=$(echo "$res" | jq -r '.p90_ns / 1e6')
  p99=$(echo "$res" | jq -r '.p99_ns / 1e6')
  max=$(echo "$res" | jq -r '.max_ns / 1e6')
  fail=$(echo "$res" | jq -r '.failed')
  printf "%-10s %8.0f  %7.3f  %7.3f  %7.3f  %7.3f  %d\n" "$tier" "$ops" "$p50" "$p90" "$p99" "$max" "$fail"
done
