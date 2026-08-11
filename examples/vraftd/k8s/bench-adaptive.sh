#!/usr/bin/env bash
# Benchmark the runtime-adaptive BatchWindow knob (12.6) on one cluster.
#
# The same running leader is reconfigured via /adaptive to different
# group-commit windows and a /bench write run is executed at each setting — no
# restart, no redeploy. This is exactly the control surface an adaptive closed
# loop would use: it shows that raising the window coalesces more appends per
# batch (higher throughput) at the cost of a bounded latency floor, and that
# the tradeoff can be re-tuned live.
#
# Note: ReloadableConfig applies BatchWindow only when > 0 (upstream compat), so
# the no-batching baseline (window=0) is a deployment-time choice and is measured
# on the `base` variant cluster instead of here.
#
# Usage: ./bench-adaptive.sh N C SIZE [cluster]   (defaults N=50000 C=16 SIZE=64 cluster=vraft-both)
set -euo pipefail
CTX=${CTX:-kind-hakit}
NS=vraft
N=${1:-50000}; C=${2:-16}; SIZE=${3:-64}; NAME=${4:-vraft-both}

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
echo "== adaptive benchmark cluster=$NAME leader=$leader n=$N c=$C size=$SIZE =="

# Preserve the node's original window so we can restore it at the end.
orig=$(http "$leader" GET /adaptive)
orig_bw=$(echo "$orig" | jq -r '.batch_window_ms')
orig_mae=$(echo "$orig" | jq -r '.max_append_entries')
echo "original batch_window_ms=$orig_bw max_append_entries=$orig_mae"

echo "win(ms)  ops/s      p50(ms)  p90(ms)  p99(ms)  max(ms)  failed"
for win in 1 2 5 10; do
  http "$leader" POST /adaptive "{\"batch_window_ms\":$win}" >/dev/null
  cur=$(http "$leader" GET /adaptive)
  [ "$(echo "$cur" | jq -r '.batch_window_ms')" = "$win" ] || { echo "FAIL: reload to $win ms rejected"; exit 1; }
  res=$(http "$leader" POST /bench "{\"n\":$N,\"c\":$C,\"size\":$SIZE}")
  ops=$(echo "$res" | jq -r '.throughput_ops_s')
  p50=$(echo "$res" | jq -r '.p50_ns / 1e6')
  p90=$(echo "$res" | jq -r '.p90_ns / 1e6')
  p99=$(echo "$res" | jq -r '.p99_ns / 1e6')
  max=$(echo "$res" | jq -r '.max_ns / 1e6')
  fail=$(echo "$res" | jq -r '.failed')
  printf "%-7d %8.0f  %7.3f  %7.3f  %7.3f  %7.3f  %d\n" "$win" "$ops" "$p50" "$p90" "$p99" "$max" "$fail"
done

# Restore the original window.
http "$leader" POST /adaptive "{\"batch_window_ms\":$orig_bw,\"max_append_entries\":$orig_mae}" >/dev/null
restored=$(http "$leader" GET /adaptive)
echo "restored batch_window_ms=$(echo "$restored" | jq -r '.batch_window_ms')"
