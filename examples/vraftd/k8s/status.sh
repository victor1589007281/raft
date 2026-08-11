#!/usr/bin/env bash
# Wait for the given vraft clusters to reach 3 voters, then print each
# cluster's leader and a condensed state line.
#
# Usage: ./status.sh [variant...]   (default: base bw async both)
set -euo pipefail
CTX=${CTX:-kind-hakit}
DIR=$(dirname "$0")

variants=("$@")
[ ${#variants[@]} -eq 0 ] && variants=(base bw async both)

for v in "${variants[@]}"; do
  name="vraft-$v"
  leader=""
  for _ in $(seq 1 90); do
    for i in 0 1 2; do
      pod="$name-$i"
      state=$(kubectl --context "$CTX" -n vraft exec "$pod" -- wget -qO- http://127.0.0.1:9000/state 2>/dev/null || true)
      if echo "$state" | grep -q '"role":"Leader"'; then
        leader="$pod"
        break 2
      fi
    done
    sleep 2
  done
  if [ -z "$leader" ]; then
    echo "$name: NO LEADER after timeout; pods:"
    kubectl --context "$CTX" -n vraft get pods -l app="$name"
    continue
  fi
  # Wait until the leader's config has 3 voters.
  for _ in $(seq 1 60); do
    cfg=$(kubectl --context "$CTX" -n vraft exec "$leader" -- wget -qO- http://127.0.0.1:9000/config 2>/dev/null || true)
    nv=$(echo "$cfg" | grep -o '"Suffrage":0' | wc -l)
    [ "$nv" -ge 3 ] && break
    sleep 2
  done
  st=$(kubectl --context "$CTX" -n vraft exec "$leader" -- wget -qO- http://127.0.0.1:9000/state 2>/dev/null || true)
  echo "$name leader=$leader $st"
done
