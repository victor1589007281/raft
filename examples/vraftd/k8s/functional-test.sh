#!/usr/bin/env bash
# Functional tests for a vraftd raft cluster in kind-hakit (run from the host).
#
# Exercises the vraft write-path features end-to-end in k8s:
#   1. replicated write/read across all nodes
#   2. failover: kill the leader, new leader elected, data preserved, writes OK
#   3. persistence: the killed leader pod is recreated; its PVC kept raft.log,
#      so the restarted node re-joins from its own durable log
#   4. member change: scale up to a 4th node, verify 4-voter config, then
#      POST /remove it and verify the config returns to 3 voters
#
# Usage: ./functional-test.sh [cluster]   (default: vraft-both)
set -euo pipefail
CTX=${CTX:-kind-hakit}
NS=vraft
NAME=${1:-vraft-both}

k8s() { kubectl --context "$CTX" -n "$NS" "$@"; }
# http <pod> <method> <path> [data]
http() {
  local pod=$1 method=$2 path=$3 data=${4:-}
  if [ -n "$data" ]; then
    k8s exec "$pod" -- wget -qO- --header 'Content-Type: application/json' \
      --post-data "$data" "http://127.0.0.1:9000$path"
  else
    k8s exec "$pod" -- wget -qO- "http://127.0.0.1:9000$path"
  fi
}
find_leader() {
  for i in 0 1 2 3; do
    st=$(k8s exec "$NAME-$i" -- wget -qO- http://127.0.0.1:9000/state 2>/dev/null || true)
    if echo "$st" | grep -q '"role":"Leader"'; then
      echo "$NAME-$i"
      return 0
    fi
  done
  return 1
}

echo "== cluster $NAME =="
leader=$(find_leader) || { echo "no leader"; exit 1; }
echo "leader=$leader"

# --- 1. replicated write / read ---
echo "-- replicated write/read --"
for kv in "f1:one" "f2:two" "f3:three"; do
  k="${kv%%:*}"; v="${kv##*:}"
  http "$leader" POST /apply "{\"k\":\"$k\",\"v\":\"$v\"}" >/dev/null
done
sleep 2
for i in 0 1 2; do
  got=$(http "$NAME-$i" GET "/read?key=f1")
  echo "read $NAME-$i f1 -> $got"
  echo "$got" | grep -q '"value":"one"' || { echo "FAIL: f1 not replicated to $NAME-$i"; exit 1; }
done

# --- 2. failover ---
echo "-- failover (kill leader $leader) --"
k8s delete pod "$leader" --wait=false
sleep 12
newleader=$(find_leader) || { echo "FAIL: no new leader after kill"; exit 1; }
[ "$newleader" = "$leader" ] && { echo "FAIL: same leader, no failover"; exit 1; }
echo "new leader=$newleader"
got=$(http "$newleader" GET "/read?key=f1")
echo "read $newleader f1 after failover -> $got"
echo "$got" | grep -q '"value":"one"' || { echo "FAIL: data lost after failover"; exit 1; }
http "$newleader" POST /apply '{"k":"f4","v":"four"}' >/dev/null
echo "apply after failover OK"

# --- 3. persistence: wait for the killed leader pod to be recreated ---
echo "-- persistence (recreated pod keeps PVC data) --"
k8s rollout status statefulset/"$NAME" --timeout=120s >/dev/null
# The recreated pod keeps its PVC, so its raft.log survives; wait for it to
# rejoin and catch up.
sleep 15
recreated="$leader"
found=false
for _ in $(seq 1 30); do
  st=$(k8s exec "$recreated" -- wget -qO- http://127.0.0.1:9000/state 2>/dev/null || true)
  if echo "$st" | grep -q '"role":"Follower"' || echo "$st" | grep -q '"role":"Leader"'; then
    found=true
    break
  fi
  sleep 2
done
[ "$found" = true ] || { echo "FAIL: recreated pod $recreated never came back"; exit 1; }
got=$(http "$recreated" GET "/read?key=f1")
echo "read $recreated f1 after restart -> $got"
echo "$got" | grep -q '"value":"one"' || { echo "FAIL: recreated node did not recover data"; exit 1; }
echo "persistence OK (recreated node serves committed data)"

# --- 4. member change: scale to 4 voters, then remove back to 3 ---
echo "-- member change: scale to 4 --"
k8s scale statefulset/"$NAME" --replicas=4
k8s rollout status statefulset/"$NAME" --timeout=120s >/dev/null
sleep 8
leader=$(find_leader)
nv=$(k8s exec "$leader" -- wget -qO- http://127.0.0.1:9000/config | grep -o '"suffrage":0' | wc -l)
echo "$NAME config voters after scale-up: $nv"
[ "$nv" -ge 4 ] || { echo "FAIL: 4th node did not join as voter"; exit 1; }

echo "-- member change: remove 4th node --"
http "$leader" POST /remove "{\"id\":\"$NAME-3\"}" >/dev/null
sleep 8
nv=$(k8s exec "$leader" -- wget -qO- http://127.0.0.1:9000/config | grep -o '"suffrage":0' | wc -l)
echo "$NAME config voters after remove: $nv"
[ "$nv" = "3" ] || { echo "FAIL: removed node still a voter ($nv)"; exit 1; }
# scale back down
k8s scale statefulset/"$NAME" --replicas=3

echo "FUNCTIONAL-TEST-OK ($NAME)"
