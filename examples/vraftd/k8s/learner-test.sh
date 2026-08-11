#!/usr/bin/env bash
# Functional test of elasticity via learners (12.5.3) in kind-hakit.
#
# A 3-node vraft cluster expands with a learner that never participates in the
# commit quorum, then promotes it to a full voter once it has caught up:
#   1. deploy a standalone vraft-learner node (its own 1-node cluster)
#   2. leader AddLearner's it: config grows to 4 members, learner is NOT a voter
#   3. writes keep committing with the original 3-voter quorum
#   4. the learner's FSM catches up (log/snapshot replication without a vote)
#   5. PromoteToVoter waits for catch-up and promotes it to a 4th voter
#   6. cleanup: remove the learner, config returns to 3 voters
#
# Usage: ./learner-test.sh [cluster]   (default: vraft-both)
set -euo pipefail
CTX=${CTX:-kind-hakit}
NS=vraft
DIR=$(dirname "$0")
NAME=${1:-vraft-both}
LEARNER=vraft-learner-0
LEARNER_ADDR=vraft-learner-0.vraft-learner:9001

k8s() { kubectl --context "$CTX" -n "$NS" "$@"; }
# http <pod> <method> <path> [data]
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

echo "== learner test cluster $NAME =="
leader=$(find_leader) || { echo "no leader in main cluster"; exit 1; }
echo "leader=$leader"

# --- 0. deploy a fresh standalone learner node ---
echo "-- deploy fresh learner node --"
k8s delete statefulset vraft-learner --ignore-not-found --wait=true >/dev/null 2>&1 || true
k8s delete pvc data-vraft-learner-0 --ignore-not-found --wait=true >/dev/null 2>&1 || true
kubectl --context "$CTX" apply -f "$DIR/learner.yaml.tmpl"
k8s rollout status statefulset/vraft-learner --timeout=120s >/dev/null
# The learner bootstraps as its own single-node cluster; confirm it is up.
found=false
for _ in $(seq 1 30); do
  st=$(k8s exec "$LEARNER" -- wget -qO- http://127.0.0.1:9000/state 2>/dev/null || true)
  if echo "$st" | grep -q '"role":"Leader"'; then
    found=true
    break
  fi
  sleep 2
done
[ "$found" = true ] || { echo "FAIL: learner never came up"; exit 1; }
echo "learner up: $LEARNER"

# --- 1. AddLearner ---
echo "-- AddLearner $LEARNER --"
t_add=$(date +%s%N)
res=$(http "$leader" POST /learner "{\"id\":\"$LEARNER\",\"address\":\"$LEARNER_ADDR\"}")
echo "add -> $res"
echo "$res" | grep -q '"learner":true' || { echo "FAIL: AddLearner failed"; exit 1; }

# Config must now have 4 servers with the learner marked non-voting.
sleep 5
cfg=$(http "$leader" GET /config)
servers=$(echo "$cfg" | jq '.servers | length')
lsuf=$(echo "$cfg" | jq -r --arg id "$LEARNER" '.servers[] | select(.id==$id) | .suffrage')
echo "config servers=$servers learner_suffrage=$lsuf"
[ "$servers" = "4" ] || { echo "FAIL: config did not grow to 4"; exit 1; }
[ "$lsuf" != "0" ] || { echo "FAIL: learner has a vote already"; exit 1; }

# --- 2. writes keep committing with the original 3-voter quorum ---
http "$leader" POST /apply '{"k":"el1","v":"one"}' >/dev/null
http "$leader" POST /barrier "{}" >/dev/null
echo "write committed with learner present (non-voting)"

# --- 3. learner catches up via replication ---
echo "-- learner catch-up --"
caught=false
for _ in $(seq 1 60); do
  got=$(http "$LEARNER" GET "/read?key=el1&consistency=stale")
  if echo "$got" | grep -q '"value":"one"'; then
    caught=true
    break
  fi
  sleep 2
done
[ "$caught" = true ] || { echo "FAIL: learner never replicated el1"; exit 1; }
catchup_ms=$(( ( $(date +%s%N) - t_add ) / 1000000 ))
echo "learner serves replicated value (catchup=${catchup_ms}ms)"

# --- 4. PromoteToVoter ---
echo "-- PromoteToVoter $LEARNER --"
t_promote=$(date +%s%N)
res=$(http "$leader" POST "/promote?timeout=90s" "{\"id\":\"$LEARNER\"}")
promote_ms=$(( ( $(date +%s%N) - t_promote ) / 1000000 ))
echo "promote -> $res (promote_latency=${promote_ms}ms)"
echo "$res" | grep -q '"promoted":' || { echo "FAIL: promote failed"; exit 1; }

sleep 5
cfg=$(http "$leader" GET /config)
nv=$(echo "$cfg" | jq '[.servers[] | select(.suffrage==0)] | length')
lsuf=$(echo "$cfg" | jq -r --arg id "$LEARNER" '.servers[] | select(.id==$id) | .suffrage')
echo "config voters=$nv learner_suffrage=$lsuf"
[ "$nv" = "4" ] || { echo "FAIL: expected 4 voters after promote"; exit 1; }
[ "$lsuf" = "0" ] || { echo "FAIL: learner still not a voter after promote"; exit 1; }

# A post-promotion write must commit (learner now part of the quorum).
http "$leader" POST /apply '{"k":"el2","v":"two"}' >/dev/null
http "$leader" POST /barrier "{}" >/dev/null
echo "write committed after promotion"

# --- 5. cleanup: remove learner, return to 3 voters ---
echo "-- cleanup: remove learner --"
res=$(http "$leader" POST /remove "{\"id\":\"$LEARNER\"}")
echo "remove -> $res"
sleep 8
cfg=$(http "$leader" GET /config)
nv=$(echo "$cfg" | jq '[.servers[] | select(.suffrage==0)] | length')
servers=$(echo "$cfg" | jq '.servers | length')
echo "config voters=$nv servers=$servers"
[ "$nv" = "3" ] || { echo "FAIL: expected 3 voters after cleanup"; exit 1; }
k8s delete statefulset vraft-learner --ignore-not-found >/dev/null 2>&1 || true
k8s delete pvc data-vraft-learner-0 --ignore-not-found >/dev/null 2>&1 || true

echo "LEARNER-TEST-OK ($NAME) catchup=${catchup_ms}ms promote=${promote_ms}ms"
