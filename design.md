# Architecture and Design Specification: kvdb

## 1. System Architecture

`kvdb` is a single-process, multi-threaded replicated key-value storage engine designed in C. State is encapsulated within a mutex-guarded `server_state_t` structure.

```
+-----------------------------------------------------------------------------------+
|                                  server_state_t                                   |
|  +---------------------+  +---------------------+  +---------------------------+  |
|  |     kv_store_t      |  |       WAL File      |  |  Role / Term / LSN State  |  |
|  | (Hash Table Engine) |  |   (wal.log / sync)  |  | (current_term, master_id) |  |
|  +---------------------+  +---------------------+  +---------------------------+  |
+-----------------------------------------------------------------------------------+
       ^                       ^                       ^                     ^
       |                       |                       |                     |
+---------------+      +---------------+      +------------------+   +---------------+
| Client Thread |      | Peer Listener |      |  Streamer Thread |   | Watchdog & HB |
| (Text Port)   |      | (Repl Port)   |      | (Tail per peer)  |   | (Election/HB) |
+---------------+      +---------------+      +------------------+   +---------------+
```

### Threading Model
- **Client Listener** (`client_listener_thread`): Binds `--port` to accept incoming text-protocol connections, spawning detached worker threads per client.
- **Peer Listener** (`peer_listener_thread`): Binds `--repl-port` to process binary replication messages (`HELLO`, `WAL_RECORD`, `HEARTBEAT`, `ACK`, `VOTE_REQUEST`, `VOTE_RESPONSE`, `SNAPSHOT_BEGIN`).
- **Peer Connector** (`peer_connector_thread`): Maintains dedicated outbound connections to each peer in the cluster topology with exponential reconnection backoff.
- **Replication Streamer** (`peer_streamer_thread`): A dedicated background worker per connected peer that tails the WAL to push newly committed records.
- **Election Watchdog** (`election_watchdog_thread`): Monitors heartbeat timeouts and initiates randomized leader election campaigns upon leader silence.
- **Heartbeat Emitter** (`heartbeat_thread`): Periodically emits `MSG_HEARTBEAT` messages from the elected master to all followers.

---

## 2. On-Disk and Wire Binary Formats

### Write-Ahead Log (WAL) Format
Every WAL record on disk consists of a 25-byte little-endian header followed by variable-length key and value bytes:

| Offset | Size (Bytes) | Field | Description |
|---|---|---|---|
| `0` | 4 | `crc32` | CRC32 checksum computed over all subsequent fields (offset 4 onward) |
| `4` | 8 | `lsn` | 64-bit monotonically increasing Log Sequence Number |
| `12` | 8 | `term` | 64-bit consensus term during which the record was created |
| `20` | 1 | `op_type` | Operation code (`1` = `OP_PUT`, `2` = `OP_DELETE`) |
| `21` | 2 | `key_len` | 16-bit key length |
| `23` | 2 | `value_len` | 16-bit value length |
| `25` | `key_len` | `key` | Raw key bytes |
| `25 + key_len` | `value_len` | `value` | Raw value bytes |

### Durability & Crash Recovery
- **Append & Sync**: `wal_append` serializes the record, performs an `fwrite`, flushes buffers (`fflush`), and executes a synchronous `fsync(fileno(fp))` prior to in-memory mutation or client acknowledgment.
- **Crash Replay**: `wal_replay` reads records sequentially, verifying CRC32 integrity. Any trailing truncated or corrupted bytes caused by an ungraceful shutdown are safely ignored.

### Replication Wire Framing
Inter-node replication messages over `--repl-port` use length-prefixed binary framing:
```
+---------------+-----------------------------+-----------------------+
|  Type (1 B)   |  Payload Length (4 B, LE)   |     Payload Bytes     |
+---------------+-----------------------------+-----------------------+
```

---

## 3. Streaming Replication & Catch-Up

Replication uses a unified tailing model (`peer_streamer_thread`) rather than separate live push and catch-up paths:
1. When an outgoing connection is established, `MSG_HELLO` transmits the node's current term and LSN.
2. The master's streamer thread seeks to the follower's required LSN in `wal.log` and continuously streams `MSG_WAL_RECORD` frames.
3. If the streamer reaches EOF, it sleeps briefly (`100ms`) and resumes when new writes advance `S.current_lsn`.
4. **Socket Generation Guard (`out_gen`)**: Prevents stale streamer threads from writing to reused file descriptors after peer reconnections.

---

## 4. Consensus & Leader Election

Leader elections implement Raft-inspired safety invariants:

1. **Stale Term Rejection**: Messages with `term < current_term` are rejected.
2. **Term Adoption**: Observing `term > current_term` forces immediate transition to `ROLE_FOLLOWER` and updates `current_term`.
3. **Voting Criteria**: A node grants a vote if and only if:
   - `req_term > current_term` (or `req_term == current_term` if not yet voted).
   - Candidate's log is at least as up-to-date as the voter's (`candidate_lsn >= local_lsn`).
4. **Quorum Majority**: A candidate becomes master upon receiving a strict majority (`(num_peers + 1) / 2 + 1` votes).
5. **Split-Vote Resolution**: Unresolved elections retry with incremented terms and randomized backoff delays (50–300ms).

---

## 5. Replication Consistency Modes

- **Asynchronous Mode (Default)**: Writes complete and return `OK (lsn=N)` immediately after the master's local WAL `fsync`. Followers replicate asynchronously.
- **Synchronous Mode (`\sync on`)**: The master tracks pending writes in a queue (`pending_write_t`) and blocks client response threads until a follower emits an `ACK` covering the written LSN (or times out after 60s).
- **Read-Your-Writes Consistency**: The CLI client tracks the maximum acknowledged LSN and appends `WAIT <lsn>` to subsequent `GET` commands, causing followers to block until their local replay catches up.

---

## 6. Snapshot Compaction

To prevent unbounded WAL growth:
1. `\snapshot` triggers `write_snapshot_locked`, serializing the entire key-value store to `snapshot.dat.tmp` before atomically renaming it to `snapshot.dat`.
2. The WAL file is truncated, and `wal_start_lsn` is updated to the snapshot LSN.
3. Followers lagging behind `wal_start_lsn` automatically receive `MSG_SNAPSHOT_BEGIN` followed by the raw snapshot stream before resuming standard WAL streaming.
