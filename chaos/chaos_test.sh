#!/usr/bin/env bash
# Chaos test for the kvdb replication cluster. See README.md in this
# directory for what each scenario checks. Bash + the kvdb binaries only,
# no Python, per the project's constraints.
set -u
cd "$(dirname "$0")/.."

N=${1:-1000}   # keys per write batch; override with e.g. ./chaos_test.sh 100 for a quick run

DATADIR=/tmp/kvchaos_data
LOGDIR=/tmp/kvchaos_logs
rm -rf "$DATADIR" "$LOGDIR"
mkdir -p "$DATADIR"/{1,2,3} "$LOGDIR"

declare -A PID
declare -A PORT=( [1]=6301 [2]=6302 [3]=6303 )
declare -A RPORT=( [1]=7301 [2]=7302 [3]=7303 )

PASS=0
FAIL=0
pass() { echo "PASS: $1"; PASS=$((PASS+1)); }
fail() { echo "FAIL: $1"; FAIL=$((FAIL+1)); }

cleanup() {
  for id in 1 2 3; do
    [ -n "${PID[$id]:-}" ] && kill -9 "${PID[$id]}" >/dev/null 2>&1
  done
}
trap cleanup EXIT

peers_for() {
  local id=$1 out=""
  for other in 1 2 3; do
    [ "$other" = "$id" ] && continue
    out="${out}${other}@localhost:${RPORT[$other]},"
  done
  echo "${out%,}"
}

start_node() {
  local id=$1
  ./bin/kvdb --node-id "$id" --port "${PORT[$id]}" --repl-port "${RPORT[$id]}" \
             --peers "$(peers_for "$id")" --data "$DATADIR/$id" \
             > "$LOGDIR/node$id.log" 2>&1 &
  PID[$id]=$!
}

kill_node() {
  local id=$1
  kill -9 "${PID[$id]}" 2>/dev/null
  unset PID[$id]
}

cli() {
  local id=$1 cmds=$2
  local port=${PORT[$id]}
  timeout 5 bash -c 'printf "%s\n" "$1" | ./bin/kvdb-cli 127.0.0.1 "$2"' _ "$cmds" "$port" 2>/dev/null
}

find_master() {
  for id in 1 2 3; do
    [ -z "${PID[$id]:-}" ] && continue
    out=$(cli "$id" $'\\info\nQUIT')
    if echo "$out" | grep -q "role:     MASTER"; then echo "$id"; return; fi
  done
}

wait_for_master() {
  local tries=0
  while [ $tries -lt 20 ]; do
    m=$(find_master)
    if [ -n "$m" ]; then echo "$m"; return; fi
    sleep 0.5
    tries=$((tries+1))
  done
  echo ""
}

get_lsn() {
  local id=$1
  cli "$id" $'\\info\nQUIT' | grep "^lsn:" | awk '{print $2}'
}

get_checksum() {
  local id=$1
  cli "$id" $'\\checksum\nQUIT' | grep -o "checksum: [0-9a-f]* count: [0-9]* lsn: [0-9]*"
}

bulk_put() {
  # writes N key/value pairs with the given prefix through the given node
  local id=$1 prefix=$2 count=$3
  local batch
  batch=$(for i in $(seq 1 "$count"); do echo "PUT ${prefix}_$i val_$i"; done)
  batch="${batch}
QUIT"
  timeout 60 bash -c "echo \"\$1\" | ./bin/kvdb-cli 127.0.0.1 \$2 > /dev/null" _ "$batch" "${PORT[$id]}"
}

wait_for_lsn() {
  # waits until the given node reports at least the target lsn
  local id=$1 target=$2 tries=0
  while [ $tries -lt 30 ]; do
    lsn=$(get_lsn "$id")
    if [ -n "$lsn" ] && [ "$lsn" -ge "$target" ] 2>/dev/null; then return 0; fi
    sleep 0.5
    tries=$((tries+1))
  done
  return 1
}

echo "===== Scenario 1: happy path ====="
start_node 1; start_node 2; start_node 3
sleep 3
M=$(wait_for_master)
if [ -z "$M" ]; then fail "scenario 1: no master elected"; exit 1; fi
echo "master is node $M"
bulk_put "$M" k1 "$N"
if wait_for_lsn 1 "$N" && wait_for_lsn 2 "$N" && wait_for_lsn 3 "$N"; then
  C1=$(get_checksum 1); C2=$(get_checksum 2); C3=$(get_checksum 3)
  if [ "$C1" = "$C2" ] && [ "$C2" = "$C3" ] && [ -n "$C1" ]; then
    pass "scenario 1: all three nodes converge on lsn=$N with matching checksum ($C1)"
  else
    fail "scenario 1: checksums differ: n1=[$C1] n2=[$C2] n3=[$C3]"
  fi
else
  fail "scenario 1: nodes did not reach lsn=$N in time"
fi

echo "===== Scenario 2: kill the master, failover, write more ====="
kill_node "$M"
sleep 5
SURVIVORS=()
for id in 1 2 3; do [ "$id" != "$M" ] && SURVIVORS+=("$id")
done
M2=$(wait_for_master)
if [ -z "$M2" ]; then fail "scenario 2: no new master elected"; exit 1; fi
LSN2=$(get_lsn "$M2")
if [ "$LSN2" = "$N" ]; then
  pass "scenario 2: node $M2 elected master at lsn=$N"
else
  fail "scenario 2: new master lsn=$LSN2, expected $N"
fi
bulk_put "$M2" k2 "$N"
TARGET2=$((N + N))
if wait_for_lsn "${SURVIVORS[0]}" "$TARGET2" && wait_for_lsn "${SURVIVORS[1]}" "$TARGET2"; then
  pass "scenario 2: second batch of writes replicated to lsn=$TARGET2"
else
  fail "scenario 2: second batch did not replicate to both survivors"
fi

echo "===== Scenario 3: restart the killed node, confirm catch-up ====="
start_node "$M"
if wait_for_lsn "$M" "$TARGET2"; then
  pass "scenario 3: node $M rejoined and caught up to lsn=$TARGET2"
else
  fail "scenario 3: node $M did not catch up (lsn=$(get_lsn "$M"))"
fi

echo "===== Scenario 4: kill a follower (not the master), keep writing ====="
FOLLOWER_TO_KILL=""
for id in 1 2 3; do
  [ "$id" != "$M2" ] && FOLLOWER_TO_KILL=$id && break
done
kill_node "$FOLLOWER_TO_KILL"
sleep 2
HALF=$((N / 2))
bulk_put "$M2" k3 "$HALF"
TARGET3=$((TARGET2 + HALF))
REMAINING_FOLLOWER=""
for id in 1 2 3; do
  [ "$id" != "$M2" ] && [ "$id" != "$FOLLOWER_TO_KILL" ] && REMAINING_FOLLOWER=$id
done
if wait_for_lsn "$M2" "$TARGET3" && wait_for_lsn "$REMAINING_FOLLOWER" "$TARGET3"; then
  pass "scenario 4: master + remaining follower still accept writes with a follower down"
else
  fail "scenario 4: writes did not land with a follower down"
fi

echo "===== Scenario 5: restart that follower, confirm catch-up ====="
start_node "$FOLLOWER_TO_KILL"
if wait_for_lsn "$FOLLOWER_TO_KILL" "$TARGET3"; then
  pass "scenario 5: node $FOLLOWER_TO_KILL rejoined and caught up to lsn=$TARGET3"
else
  fail "scenario 5: node $FOLLOWER_TO_KILL did not catch up"
fi

echo "===== Scenario 6: sync mode, kill two nodes, confirm writes stall then resume ====="
cli "$M2" $'\\sync on\nQUIT' > /dev/null
KILLED=()
for id in 1 2 3; do [ "$id" != "$M2" ] && KILLED+=("$id"); done
kill_node "${KILLED[0]}"
kill_node "${KILLED[1]}"
sleep 1
START_LSN=$(get_lsn "$M2")
# this PUT should block (no follower can ACK) - run it in the background with
# a short timeout and confirm it has NOT completed while both peers are down
( timeout 4 bash -c "printf 'PUT stall_key stall_val\nQUIT\n' | ./bin/kvdb-cli 127.0.0.1 ${PORT[$M2]}" > "$LOGDIR/stall_attempt.log" 2>&1 ) &
STALL_PID=$!
sleep 3
if grep -q "^OK" "$LOGDIR/stall_attempt.log" 2>/dev/null; then
  fail "scenario 6: sync write completed without a majority - should have blocked"
else
  pass "scenario 6: sync write is correctly blocked with no follower available"
fi
kill -9 "$STALL_PID" 2>/dev/null
start_node "${KILLED[0]}"
if wait_for_lsn "$M2" $((START_LSN + 1)); then
  pass "scenario 6: write resumed once a follower came back"
else
  fail "scenario 6: write still stuck after a follower rejoined"
fi
start_node "${KILLED[1]}"

echo
echo "===== RESULTS: $PASS passed, $FAIL failed ====="
[ "$FAIL" -eq 0 ]
