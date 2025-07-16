# Chaos test

`chaos_test.sh` brings up a real three-node cluster on localhost and puts it
through the six scenarios from the assignment:

1. **Happy path.** Start all three, wait for an election, write 1000 keys
   through the master, and check that all three nodes report the same
   `\checksum` (a XOR of a CRC32 per key/value pair, order-independent, plus
   the count and the current LSN).
2. **Kill the master.** Confirm a new master gets elected at LSN 1000, then
   write 1000 more keys through it.
3. **Restart the dead node.** Confirm it rejoins as a follower and catches
   up to LSN 2000.
4. **Kill a follower** (not the master). Write 500 more keys and confirm the
   remaining master + follower keep accepting writes.
5. **Restart that follower.** Confirm it catches up to LSN 2500.
6. **Sync mode, kill two nodes.** Confirm the lone survivor stops
   acknowledging writes (no majority for the sync ACK), then restart one
   node and confirm writes resume.

Each step prints PASS/FAIL. Logs from the run are left in `/tmp/kvchaos_*`
so a failure can be inspected after the fact. The script only uses the
`kvdb`/`kvdb-cli` binaries plus bash - no Python anywhere, per the
assignment's constraints.

A sample passing run is captured in `sample_run.log`.
