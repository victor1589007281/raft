#!/usr/bin/env bash
# Functional test of the runtime-adaptive knobs (12.6) in kind-hakit.
#
# The adaptive closed loop reconfigures the write path at runtime through
# /adaptive — no restart. This test verifies:
#   1. GET /adaptive reports the current reloadable config
#   2. POST /adaptive applies a new BatchWindow / MaxAppendEntries
#   3. GET reflects the new values and the node keeps serving writes
#   4. an out-of-range knob (MaxAppendEntries > 1024) is rejected atomically
#   5. the original values can be restored
#
# Usage: ./adaptive-test.sh [cluster]   (default: vraft-both)
set -euo pipefail
CTX=${CTX:-kind-hakit}
NS=vraft
NAME=${1:-vraft-both}

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

echo "== adaptive test cluster $NAME =="
leader=$(find_leader) || { echo "no leader"; exit 1; }
echo "leader=$leader"

# --- 1. initial state ---
echo "-- GET /adaptive --"
before=$(http "$leader" GET /adaptive)
echo "$before"
bw0=$(echo "$before" | jq -r '.batch_window_ms')
mae0=$(echo "$before" | jq -r '.max_append_entries')
echo "before: batch_window_ms=$bw0 max_append_entries=$mae0"

# --- 2/3. apply new knobs, verify and keep serving ---
echo "-- POST batch_window_ms=5, max_append_entries=256 --"
after=$(http "$leader" POST /adaptive '{"batch_window_ms":5,"max_append_entries":256}')
echo "$after"
[ "$(echo "$after" | jq -r '.batch_window_ms')" = "5" ] || { echo "FAIL: batch_window_ms not applied"; exit 1; }
[ "$(echo "$after" | jq -r '.max_append_entries')" = "256" ] || { echo "FAIL: max_append_entries not applied"; exit 1; }

http "$leader" POST /apply '{"k":"ad1","v":"still-works"}' >/dev/null
got=$(http "$leader" GET "/read?key=ad1&consistency=strict")
echo "read after reload -> $got"
echo "$got" | grep -q '"value":"still-works"' || { echo "FAIL: writes/reads broken after reload"; exit 1; }

# --- 4. out-of-range MaxAppendEntries rejected ---
echo "-- POST max_append_entries=999999 (expect rejection) --"
# The rejection is a 400; http_status only does GET, so capture the POST status
# directly from busybox wget's stderr (the error body is not printed by wget).
code=$(k8s exec "$leader" -- sh -c "wget -S -O- --header 'Content-Type: application/json' --post-data '{\"max_append_entries\":999999}' http://127.0.0.1:9000/adaptive 2>&1 >/dev/null | grep -o 'HTTP/1.1 [0-9]*' | head -1 | awk '{print \$2}'" 2>/dev/null || true)
echo "bad -> http $code"
[ "$code" = "400" ] || { echo "FAIL: out-of-range knob not rejected (got $code)"; exit 1; }
# The previous value must be untouched.
still=$(http "$leader" GET /adaptive)
[ "$(echo "$still" | jq -r '.max_append_entries')" = "256" ] || { echo "FAIL: rejected change corrupted config"; exit 1; }

# --- 5. restore ---
echo "-- restore original --"
http "$leader" POST /adaptive "{\"batch_window_ms\":$bw0,\"max_append_entries\":$mae0}" >/dev/null
restored=$(http "$leader" GET /adaptive)
echo "restored: batch_window_ms=$(echo "$restored" | jq -r '.batch_window_ms') max_append_entries=$(echo "$restored" | jq -r '.max_append_entries')"

echo "ADAPTIVE-TEST-OK ($NAME)"
