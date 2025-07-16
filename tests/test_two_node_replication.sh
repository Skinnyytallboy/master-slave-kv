#!/usr/bin/env bash
# End-to-end test of the two-node replication path from Phase 2: two
# nodes each list the other as a peer, elect a master, write through it,
# and confirm the follower converges - including catch-up after a
# follower restart.
set -u
cd "$(dirname "$0")/.."

DATADIR=/tmp/kvdb_test_2node
LOGDIR=/tmp/kvdb_test_2node_logs
rm -rf "$DATADIR" "$LOGDIR"
mkdir -p "$DATADIR"/{1,2} "$LOGDIR"

declare -A PORT=( [1]=16901 [2]=16902 )
declare -A RPORT=( [1]=17901 [2]=17902 )
declare -A PID

FAIL=0
cleanup() {
  for id in 1 2; do
    [ -n "${PID[$id]:-}" ] && kill -9 "${PID[$id]}" >/dev/null 2>&1
  done
}
trap cleanup EXIT

check() {
  if [ "$2" = "$3" ]; then
    echo "ok: $1"
  else
    echo "FAIL: $1 (expected [$3], got [$2])"
    FAIL=1
  fi
}

start_node() {
  local id=$1 other=$2
  ./bin/kvdb --node-id "$id" --port "${PORT[$id]}" --repl-port "${RPORT[$id]}" \
             --peers "$other@localhost:${RPORT[$other]}" --data "$DATADIR/$id" \
             > "$LOGDIR/n$id.log" 2>&1 &
  PID[$id]=$!
}

# a value line comes back on the same line as the CLI's "> " prompt, so
# strip that prefix before comparing.
value_line() {
  echo "$1" | sed -n "${2}p" | sed 's/^> //'
}

find_master() {
  for attempt in $(seq 1 10); do
    for id in 1 2; do
      out=$(timeout 3 bash -c 'printf "%s\n" "$1" | ./bin/kvdb-cli 127.0.0.1 "$2"' _ $'\\info\nQUIT' "${PORT[$id]}" 2>/dev/null)
      if echo "$out" | grep -q "role:     MASTER"; then echo "$id"; return 0; fi
    done
    sleep 1
  done
  return 1
}

start_node 1 2
start_node 2 1
sleep 3

M=$(find_master)
F=$([ "$M" = "1" ] && echo 2 || echo 1)
echo "master=$M follower=$F"

printf 'PUT alice 100\nPUT bob 50\nQUIT\n' | ./bin/kvdb-cli 127.0.0.1 "${PORT[$M]}" > /dev/null
sleep 1

OUT=$(printf 'GET alice\nGET bob\nQUIT\n' | ./bin/kvdb-cli 127.0.0.1 "${PORT[$F]}")
check "follower has alice" "$(value_line "$OUT" 2)" "100"
check "follower has bob"   "$(value_line "$OUT" 3)" "50"

# restart the follower and confirm it catches back up from its own wal
kill -9 "${PID[$F]}"
wait "${PID[$F]}" 2>/dev/null || true
sleep 1
start_node "$F" "$M"
sleep 2

printf 'PUT carol 75\nQUIT\n' | ./bin/kvdb-cli 127.0.0.1 "${PORT[$M]}" > /dev/null
sleep 1
OUT2=$(printf 'GET alice\nGET carol\nQUIT\n' | ./bin/kvdb-cli 127.0.0.1 "${PORT[$F]}")
check "restarted follower kept old data" "$(value_line "$OUT2" 2)" "100"
check "restarted follower caught up on new data" "$(value_line "$OUT2" 3)" "75"

exit $FAIL
