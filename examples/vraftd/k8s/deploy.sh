#!/usr/bin/env bash
# Deploy vraftd benchmark clusters into the vraft namespace of kind-hakit.
#
# Each variant is an independent 3-node raft cluster:
#   base     -> upstream-like baseline: no batch-window, no async persist
#   bw       -> BatchWindow group-commit only (2ms)
#   async    -> async leader persist only (提前 fsync)
#   both     -> BatchWindow + async leader persist combined
#   singlewal-> both + 12.14 single-WAL v2 (magic+CRC+pad512) + sparse index
#               + index codec (lsn=raft index) + fdatasync
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
  SWAL=0; SIDX=0; CODEC=none; FDS=0
  case "$v" in
    base)     WIN=0;    ASY=0 ;;
    bw)       WIN=2ms;  ASY=0 ;;
    async)    WIN=0;    ASY=1 ;;
    both)     WIN=2ms;  ASY=1 ;;
    singlewal) WIN=2ms; ASY=1; SWAL=1; SIDX=1; CODEC=index; FDS=1 ;;
    *) echo "unknown variant $v (base|bw|async|both|singlewal)" >&2; exit 1 ;;
  esac
  sed -e "s/__NAME__/vraft-$v/g" \
      -e "s/__BATCH_WINDOW__/$WIN/g" \
      -e "s/__ASYNC_PERSIST__/$ASY/g" \
      -e "s/__APPEND__/1024/g" \
      -e "s/__SINGLE_WAL__/$SWAL/g" \
      -e "s/__SPARSE_INDEX__/$SIDX/g" \
      -e "s/__WAL_CODEC__/$CODEC/g" \
      -e "s/__FDATASYNC__/$FDS/g" \
      "$DIR/vraftd.yaml.tmpl" | kubectl --context "$CTX" apply -f -
  echo "deployed vraft-$v (batch-window=$WIN async-persist=$ASY single-wal=$SWAL sparse-index=$SIDX codec=$CODEC fdatasync=$FDS)"
done
