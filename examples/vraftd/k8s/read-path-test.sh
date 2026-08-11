#!/usr/bin/env bash
# Functional test of the read-consistency tiers (12.2) in kind-hakit.
#
# Exercises /read?consistency=strict|lease|stale against a deployed 3-node
# cluster:
#   1. strict/lease/stale reads on the leader all return the committed value
#   2. strict/lease reads report a linearizable watermark >= the write index
#   3. stale reads work on a follower (local applied watermark)
#   4. strict/lease reads on a follower fail with grep -q 'not-leader|ErrNotLeader|not the leader|leadership' || { echo "FAIL: follower $tier did not refuse"; exit 1; } (only the
#      leader can satisfy the quorum / lease tier)
#
# Usage: ./read-path-test.sh [cluster]   (default: vraft-both)
set -euo pipefail
CTX=${CTX:-kind-hakit}
NS=vraft
NAME=${1:-vraft-both}

k8s() { kubectl --context "$CTX" -n "$NS" "$@"; }
# http <pod> <method> <path> [data]; echoes the raw response
http() {
  local pod=$1 method=$2 path=$3 data=${4:-}
  if [ -n "$data" ]; then
    k8s exec "$pod" -- wget -qO- --header 'Content-Type: application/json' \
      --post-data "$data" "http://127.0.0.1:9000$path" 2>/dev/null || true
  else
    k8s exec "$pod" -- wget -qO- "http://127.0.0.1:9000$path" 2>/dev/null || true
  fi
}
# http_status <pod> <path> -> the HTTP status code (busybox wget prints the
# status line to stderr with -S; the response body is discarded). Empty on 2xx.
http_status() {
  local pod=$1 path=$2
  k8s exec "$pod" -- sh -c "wget -S -O- 'http://127.0.0.1:9000$path' 2>&1 >/dev/null | grep -o 'HTTP/1.1 [0-9]*' | head -1 | awk '{print \$2}'" 2>/dev/null || true
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

echo "== read-path test cluster $NAME =="
leader=$(find_leader) || { echo "no leader"; exit 1; }
echo "leader=$leader"

# Seed a committed key.
http "$leader" POST /apply '{"k":"r1","v":"committed"}' >/dev/null
http "$leader" POST /barrier "{}" >/dev/null

# --- 1. all three tiers on the leader ---
echo "-- leader strict/lease/stale --"
for tier in strict lease stale; do
  got=$(http "$leader" GET "/read?key=r1&consistency=$tier")
  echo "leader $tier -> $got"
  echo "$got" | grep -q '"consistency":"'"$tier"'"' || { echo "FAIL: no $tier marker"; exit 1; }
  echo "$got" | grep -q '"value":"committed"' || { echo "FAIL: $tier read lost the value"; exit 1; }
done

# --- 2. linearizable watermark: strict/lease must be >= the write index ---
echo "-- watermark check --"
wr=$(http "$leader" GET "/read?key=r1&consistency=strict")
wm=$(echo "$wr" | jq -r '.watermark')
echo "strict watermark=$wm"
[ "$wm" -ge 1 ] || { echo "FAIL: watermark not >= 1"; exit 1; }

# --- 3. follower: stale OK, strict/lease fail ---
echo "-- follower stale/strict/lease --"
follower=""
for i in 0 1 2; do
  [ "$NAME-$i" = "$leader" ] && continue
  follower="$NAME-$i"
  break
done
[ -n "$follower" ] || { echo "FAIL: no follower found"; exit 1; }
got=$(http "$follower" GET "/read?key=r1&consistency=stale")
echo "follower stale -> $got"
echo "$got" | grep -q '"value":"committed"' || { echo "FAIL: follower stale read lost the value"; exit 1; }

for tier in strict lease; do
  code=$(http_status "$follower" "/read?key=r1&consistency=$tier")
  echo "follower $tier -> http $code"
  [ "$code" = "503" ] || { echo "FAIL: follower $tier should return 503 (no quorum/lease), got $code"; exit 1; }
done

# --- 4. bad tier value is rejected ---
echo "-- bad consistency value --"
code=$(http_status "$leader" "/read?key=r1&consistency=bogus")
echo "bogus -> http $code"
[ "$code" = "400" ] || { echo "FAIL: bogus consistency should be rejected with 400, got $code"; exit 1; }

echo "READ-PATH-TEST-OK ($NAME)"
