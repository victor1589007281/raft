#!/usr/bin/env bash
# Measure failover latency of a vraft cluster in kind-hakit (12.4).
#
# Repeatedly kills the leader pod and measures two intervals from the kill:
#   t_elect   — until a remaining node reports itself Leader
#   t_recover — until a write commits on the new leader (full service recovery)
# After each iteration the killed pod is recreated (same PVC) and the cluster is
# allowed to return to 3 voters before the next kill, so every sample measures a
# clean leader loss.
#
# Usage: ./bench-election.sh R [cluster]   (defaults R=5 cluster=vraft-both)
set -euo pipefail
CTX=${CTX:-kind-hakit}
NS=vraft
R=${1:-5}
NAME=${2:-vraft-both}

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
wait_3voters() {
  local leader=$1
  for _ in $(seq 1 60); do
    cfg=$(http "$leader" GET /config 2>/dev/null || true)
    nv=$(echo "$cfg" | jq '[.servers[] | select(.suffrage==0)] | length' 2>/dev/null || echo 0)
    [ "$nv" = "3" ] && return 0
    sleep 2
  done
  return 1
}

leader=$(find_leader) || { echo "no leader"; exit 1; }
echo "== failover benchmark cluster=$NAME leader=$leader iterations=$R =="

elect_ms=(); recover_ms=()
for i in $(seq 1 "$R"); do
  killed="$leader"
  echo "-- iter $i: kill leader $killed --"
  t0=$(date +%s%N)
  k8s delete pod "$killed" --wait=false >/dev/null

  # Poll the surviving nodes for a new leader.
  newleader=""
  while [ -z "$newleader" ]; do
    for idx in 0 1 2; do
      pod="$NAME-$idx"
      [ "$pod" = "$killed" ] && continue
      st=$(k8s exec "$pod" -- wget -qO- http://127.0.0.1:9000/state 2>/dev/null || true)
      if echo "$st" | grep -q '"role":"Leader"'; then
        newleader="$pod"
        break
      fi
    done
    [ -n "$newleader" ] && break
    # Election has to finish within the heartbeat window; bail after 60s.
    now=$(date +%s%N)
    [ $(( (now - t0) / 1000000 )) -gt 60000 ] && { echo "FAIL: no new leader within 60s"; exit 1; }
    sleep 0.2
  done
  t1=$(date +%s%N)
  elect_ms+=($(( (t1 - t0) / 1000000 )))
  echo "  new leader=$newleader t_elect=$((${elect_ms[-1]}))ms"

  # Now a committed write on the new leader = full service recovery.
  ok=false
  while [ "$ok" = false ]; do
    out=$(http "$newleader" POST /apply "{\"k\":\"fe-$i\",\"v\":\"ok\"}" 2>/dev/null || true)
    if echo "$out" | grep -q '"index"'; then
      ok=true
    else
      now=$(date +%s%N)
      [ $(( (now - t0) / 1000000 )) -gt 60000 ] && { echo "FAIL: no committed write within 60s"; exit 1; }
      sleep 0.2
    fi
  done
  t2=$(date +%s%N)
  recover_ms+=($(( (t2 - t0) / 1000000 )))
  echo "  t_recover=$((${recover_ms[-1]}))ms"

  # Wait for the killed pod to be recreated and the cluster to settle to 3
  # voters before the next iteration, so each sample is a clean leader loss.
  k8s rollout status statefulset/"$NAME" --timeout=120s >/dev/null
  for _ in $(seq 1 90); do
    ok=true
    for idx in 0 1 2; do
      st=$(k8s exec "$NAME-$idx" -- wget -qO- http://127.0.0.1:9000/state 2>/dev/null || true)
      echo "$st" | grep -q '"role":"Follower"\|"role":"Leader"' || ok=false
    done
    [ "$ok" = true ] && break
    sleep 2
  done
  leader=$(find_leader) || { echo "FAIL: cluster lost leader after rejoin"; exit 1; }
  wait_3voters "$leader" || { echo "FAIL: cluster did not return to 3 voters"; exit 1; }
  # Let the new leader establish its lease so the next kill is again clean.
  sleep 3
done

echo "== summary (ms) =="
echo "iter  t_elect  t_recover"
for i in "${!elect_ms[@]}"; do
  printf "%d    %7d   %9d\n" $((i+1)) "${elect_ms[$i]}" "${recover_ms[$i]}"
done
# Median of t_recover.
sort_recover=($(printf '%s\n' "${recover_ms[@]}" | sort -n))
n=${#sort_recover[@]}
if [ $((n % 2)) -eq 1 ]; then
  med=${sort_recover[$((n/2))]}
else
  med=$(( (${sort_recover[$((n/2-1))]} + ${sort_recover[$((n/2))]}) / 2 ))
fi
echo "median t_recover=${med}ms"
