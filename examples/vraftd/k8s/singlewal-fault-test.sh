#!/usr/bin/env bash
# Fault-simulation tests for the 12.14 single-WAL v2 store in kind-hakit.
#
# Verifies, on a -single-wal -sparse-index -single-wal-codec=index cluster:
#   0. feature evidence: storeinfo format=v2-singlewal on every pod, VWAL magic
#      on disk, apply→index→/lookup-lsn sparse-index hits
#   1. fault A: SIGKILL the leader under write load (force delete, grace=0)
#      → new leader, every ACKed write survives, restarted node reloads the
#      (possibly torn) tail cleanly and rejoins
#   2. fault B: leader hang/partition via SIGSTOP → remaining 2 elect a new
#      leader and keep committing; SIGCONT → old leader converges
#   3. fault C: follower SIGSTOP → quorum of 2 keeps committing; SIGCONT →
#      follower catches up
#   4. final consistency: identical /readall digest on all 3 nodes
#
# Usage: ./singlewal-fault-test.sh [cluster]   (default: vraft-singlewal)
set -euo pipefail
CTX=${CTX:-kind-hakit}
NS=vraft
NAME=${1:-vraft-singlewal}
FMT=${2:-v2-singlewal} # expected storeinfo format (v2-singlewal | v2-ref)
RUNID=$(date +%s)
ACKED_FILE=$(mktemp)
trap 'rm -f "$ACKED_FILE"' EXIT

k8s() { kubectl --context "$CTX" -n "$NS" "$@"; }
# http <pod> <method> <path> [data]  (3s timeout: must tolerate hung pods)
http() {
  local pod=$1 method=$2 path=$3 data=${4:-}
  if [ -n "$data" ]; then
    k8s exec "$pod" -- wget -T 3 -qO- --header 'Content-Type: application/json' \
      --post-data "$data" "http://127.0.0.1:9000$path" 2>/dev/null
  else
    k8s exec "$pod" -- wget -T 3 -qO- "http://127.0.0.1:9000$path" 2>/dev/null
  fi
}
find_leader() {
  for i in 0 1 2; do
    st=$(http "$NAME-$i" GET /state)
    if echo "$st" | grep -q '"role":"Leader"'; then
      echo "$NAME-$i"
      return 0
    fi
  done
  return 1
}
wait_leader() { # wait until some leader exists
  for _ in $(seq 1 30); do
    if l=$(find_leader); then echo "$l"; return 0; fi
    sleep 2
  done
  return 1
}
apply_key() { # apply_key <leader-pod> <key> <value> — records ACKed keys
  local pod=$1 k=$2 v=$3
  local resp
  resp=$(http "$pod" POST /apply "{\"k\":\"$k\",\"v\":\"$v\"}") || return 1
  if echo "$resp" | grep -q '"index"'; then
    echo "$k" >>"$ACKED_FILE"
    echo "$resp"
    return 0
  fi
  return 1
}

# Hang/resume a pod's vraftd (partition/hang simulation). SIGSTOP must be sent
# from the kind NODE's pid namespace: a container's PID 1 is SIGNAL_UNKILLABLE
# and ignores default-disposition signals sent from inside its own namespace.
NODE=${KIND_NODE:-hakit-control-plane}
pod_pid() { # pod_pid <pod> → host-side pid of /usr/local/bin/vraftd
  docker exec "$NODE" sh -c "ps -eo pid,args | grep '^ *[0-9]* /usr/local/bin/vraftd -id $1 ' | awk '{print \$1}'"
}
hang_pod() {
  local pid
  pid=$(pod_pid "$1")
  [ -n "$pid" ] || { echo "FAIL: cannot find pid for $1"; exit 1; }
  docker exec "$NODE" kill -STOP "$pid"
  sleep 1
  docker exec "$NODE" sh -c "grep -q 'State:.*(stopped)' /proc/$pid/status" || { echo "FAIL: $1 (pid $pid) not stopped"; exit 1; }
}
heal_pod() {
  local pid
  pid=$(pod_pid "$1")
  [ -n "$pid" ] || { echo "FAIL: cannot find pid for $1"; exit 1; }
  docker exec "$NODE" kill -CONT "$pid"
}

echo "== cluster $NAME (run $RUNID) =="

# ---------- 0. feature evidence ----------
echo "-- [0] single-wal feature evidence --"
leader=$(find_leader) || { echo "FAIL: no leader"; exit 1; }
echo "leader=$leader"
for i in 0 1 2; do
  si=$(http "$NAME-$i" GET /storeinfo) || { echo "FAIL: no /storeinfo on $NAME-$i"; exit 1; }
  echo "storeinfo $NAME-$i -> $si"
  echo "$si" | grep -q "\"format\":\"$FMT\"" || { echo "FAIL: $NAME-$i not $FMT"; exit 1; }
  if [ "$FMT" = "v2-singlewal" ]; then
    echo "$si" | grep -q '"sparseIndex":true' || { echo "FAIL: $NAME-$i sparse index off"; exit 1; }
  fi
  if [ "$FMT" = "v2-ref" ]; then
    # ref mode: pointer meta + separate redo data file — both must exist with
    # VWAL magic, and both offsets must stay 512-aligned.
    echo "$si" | grep -q '"redoEndOffset":' || { echo "FAIL: $NAME-$i no redoEndOffset (not ref)"; exit 1; }
    rmagic=$(k8s exec "$NAME-$i" -- sh -c 'head -c 4 /data/logs/redo.log' 2>/dev/null)
    [ "$rmagic" = "VWAL" ] || { echo "FAIL: $NAME-$i redo.log magic = '$rmagic'"; exit 1; }
    reo=$(echo "$si" | grep -o '"redoEndOffset":[0-9]*' | cut -d: -f2)
    [ $((reo % 512)) -eq 0 ] || { echo "FAIL: $NAME-$i redoEndOffset $reo not 512-aligned"; exit 1; }
  fi
  magic=$(k8s exec "$NAME-$i" -- sh -c 'head -c 4 /data/logs/raft.log' 2>/dev/null)
  [ "$magic" = "VWAL" ] || { echo "FAIL: $NAME-$i raft.log magic = '$magic' (want VWAL)"; exit 1; }
done
echo "format/magic/sparse-index OK on all 3 pods"

# ---------- 1. baseline writes + sparse-index probes ----------
echo "-- [1] baseline writes + lookup-lsn probes --"
for n in $(seq 1 30); do
  apply_key "$leader" "base-$RUNID-$n" "v$n" >/dev/null || { echo "FAIL: baseline apply $n"; exit 1; }
done
sleep 2
for i in 0 1 2; do
  got=$(http "$NAME-$i" GET "/read?key=base-$RUNID-1")
  echo "$got" | grep -q '"value":"v1"' || { echo "FAIL: base key not on $NAME-$i"; exit 1; }
done
li=$(http "$leader" GET /storeinfo | grep -o '"lastIndex":[0-9]*' | cut -d: -f2)
lk=$(http "$leader" GET "/lookup-lsn?lsn=$li")
echo "lookup-lsn lsn=$li -> $lk"
echo "$lk" | grep -q '"found":true' || { echo "FAIL: sparse index miss for lastIndex=$li"; exit 1; }
echo "$lk" | grep -q "\"index\":$li" || { echo "FAIL: lsn=$li mapped to wrong index"; exit 1; }
lsn_entries=$(http "$leader" GET /storeinfo | grep -o '"lsnEntries":[0-9]*' | cut -d: -f2)
[ "$lsn_entries" -ge 30 ] || { echo "FAIL: lsnEntries=$lsn_entries < 30"; exit 1; }
echo "baseline OK (30 keys, lsnEntries=$lsn_entries, lookup-lsn hit)"

# ---------- 2. fault A: SIGKILL leader under write load ----------
echo "-- [2] fault A: SIGKILL leader $leader under write load --"
(
  for n in $(seq 1 120); do
    big=$(printf 'x%.0s' $(seq 1 2048)) # 2KiB payload: records span multiple 512B blocks
    apply_key "$leader" "kill-$RUNID-$n" "$big" >/dev/null || true
  done
) &
LOADPID=$!
sleep 3
kill_count_before=$(wc -l <"$ACKED_FILE")
k8s delete pod "$leader" --grace-period=0 --force --wait=false >/dev/null
echo "force-deleted $leader mid-load (acked keys so far: $kill_count_before)"
wait $LOADPID 2>/dev/null || true
newleader=$(wait_leader) || { echo "FAIL: no leader after SIGKILL"; exit 1; }
[ "$newleader" != "$leader" ] || { echo "FAIL: same leader returned"; exit 1; }
echo "new leader=$newleader"
# Every key ACKed before/during the kill must survive (committed ⇒ durable).
# Checked on the new leader (quorum completeness); empty/absent = lost.
sleep 3
missing=0
while read -r k; do
  [ -z "$k" ] && continue
  got=$(http "$newleader" GET "/read?key=$k")
  echo "$got" | grep -q '"value"' || missing=$((missing+1))
done <"$ACKED_FILE"
[ "$missing" = 0 ] || { echo "FAIL: $missing ACKed keys lost after SIGKILL"; exit 1; }
echo "all ACKed keys survived SIGKILL ($(wc -l <"$ACKED_FILE") keys)"
# The restarted pod must come back with its (possibly torn) log intact.
echo "waiting for $leader to rejoin..."
k8s rollout status statefulset/"$NAME" --timeout=180s >/dev/null
found=false
for _ in $(seq 1 30); do
  st=$(http "$leader" GET /state)
  if echo "$st" | grep -q '"role"'; then found=true; break; fi
  sleep 2
done
[ "$found" = true ] || { echo "FAIL: $leader never came back"; exit 1; }
si=$(http "$leader" GET /storeinfo)
echo "restarted $leader storeinfo -> $si"
echo "$si" | grep -q "\"format\":\"$FMT\"" || { echo "FAIL: restarted pod format degraded"; exit 1; }
if k8s logs "$leader" 2>/dev/null | grep -qi 'panic\|corrupt\|fatal'; then
  echo "FAIL: panic/corruption in restarted pod logs"; k8s logs "$leader" | tail -5; exit 1
fi
# Restarted node must converge to committed state.
sleep 5
got=$(http "$leader" GET "/read?key=base-$RUNID-1")
echo "$got" | grep -q '"value":"v1"' || { echo "FAIL: restarted node lost committed data"; exit 1; }
echo "fault A OK (torn-tail reload clean, data intact)"

# ---------- 3. fault B: leader hang (SIGSTOP = partition/hang simulation) ----------
echo "-- [3] fault B: SIGSTOP leader $newleader (partition/hang) --"
hang_pod "$newleader"
sleep 12
leader2=$(find_leader) || { echo "FAIL: no leader among survivors"; exit 1; }
[ "$leader2" != "$newleader" ] || { echo "FAIL: hung pod still leader"; exit 1; }
echo "survivors elected $leader2"
for n in $(seq 1 5); do
  apply_key "$leader2" "part-$RUNID-$n" "p$n" >/dev/null || { echo "FAIL: apply during partition $n"; exit 1; }
done
echo "5 keys committed by 2-node quorum during 'partition'"
heal_pod "$newleader"
echo "SIGCONT $newleader (partition heals)"
sleep 8
# Old leader must step down and converge to the new committed state.
conv=false
for _ in $(seq 1 20); do
  got=$(http "$newleader" GET "/read?key=part-$RUNID-5")
  if echo "$got" | grep -q '"value":"p5"'; then conv=true; break; fi
  sleep 2
done
[ "$conv" = true ] || { echo "FAIL: healed node never received partition-era commits"; exit 1; }
st=$(http "$newleader" GET /state)
echo "healed $newleader state -> $(echo "$st" | grep -o '"role":"[A-Za-z]*"')"
echo "fault B OK (no split-brain, convergence after heal)"

# ---------- 4. fault C: follower hang ----------
echo "-- [4] fault C: SIGSTOP a follower --"
cur_leader=$(find_leader) || { echo "FAIL: no leader before fault C"; exit 1; }
f=""
for i in 0 1 2; do
  [ "$NAME-$i" != "$cur_leader" ] && { f="$NAME-$i"; break; }
done
hang_pod "$f"
sleep 3
for n in $(seq 1 10); do
  apply_key "$cur_leader" "foll-$RUNID-$n" "q$n" >/dev/null || { echo "FAIL: apply with hung follower $n"; exit 1; }
done
echo "10 keys committed with follower $f hung (2/3 quorum)"
heal_pod "$f"
sleep 6
conv=false
for _ in $(seq 1 20); do
  got=$(http "$f" GET "/read?key=foll-$RUNID-10")
  if echo "$got" | grep -q '"value":"q10"'; then conv=true; break; fi
  sleep 2
done
[ "$conv" = true ] || { echo "FAIL: hung follower never caught up"; exit 1; }
echo "fault C OK (follower catch-up after heal)"

# ---------- 5. final consistency ----------
echo "-- [5] final consistency across all 3 nodes --"
sleep 3
digests=""
for i in 0 1 2; do
  d=$(http "$NAME-$i" GET /readall | md5sum | cut -d' ' -f1)
  echo "readall md5 $NAME-$i: $d"
  digests="$digests $d"
done
u=$(echo $digests | tr ' ' '\n' | sort -u | wc -l)
[ "$u" = "1" ] || { echo "FAIL: nodes diverged ($digests)"; exit 1; }
total_keys=$(http "$NAME-0" GET /readall | grep -o '"applied":[0-9]*' | cut -d: -f2)
echo "FAULT-TEST-OK ($NAME): all nodes identical, applied=$total_keys"
