#!/usr/bin/env bash
# Deploy vraftd benchmark clusters into the vraft namespace of kind-hakit.
#
# Each variant is an independent 3-node raft cluster:
#   base  -> upstream-like baseline: no batch-window, no async persist
#   bw    -> BatchWindow group-commit only (2ms)
#   async -> async leader persist only (提前 fsync)
#   both  -> BatchWindow + async leader persist combined
#
# MaxAppendEntries is pinned to 1024 in every variant so the comparison
# isolates the vraft write-path features.
set -euo pipefail
CTX=${CTX:-kind-hakit}
DIR=$(dirname "$0")

kubectl --context "$CTX" apply -f "$DIR/namespace.yaml"

variants=("$@")
[ ${#variants[@]} -eq 0 ] && variants=(base bw async both)

for v in "${variants[@]}"; do
  case "$v" in
    base)  WIN=0;    ASY=0 ;;
    bw)    WIN=2ms;  ASY=0 ;;
    async) WIN=0;    ASY=1 ;;
    both)  WIN=2ms;  ASY=1 ;;
    *) echo "unknown variant $v (base|bw|async|both)" >&2; exit 1 ;;
  esac
  sed -e "s/__NAME__/vraft-$v/g" \
      -e "s/__BATCH_WINDOW__/$WIN/g" \
      -e "s/__ASYNC_PERSIST__/$ASY/g" \
      -e "s/__APPEND__/1024/g" \
      "$DIR/vraftd.yaml.tmpl" | kubectl --context "$CTX" apply -f -
  echo "deployed vraft-$v (batch-window=$WIN async-persist=$ASY)"
done
