# kvdb - Distributed Master-Slave Replicated Key-Value Store

A fault-tolerant, replicated key-value storage engine in C built from first principles with no external dependencies. A cluster runs with one elected master and multiple followers, utilizing Raft-inspired term consensus to ensure strong election safety, monotonic sequence numbers, streaming WAL replication, and automatic crash recovery.

---

## Architecture Overview

```
                        +------------------+
                        |   Client CLI     |
                        |   (kvdb-cli)     |
                        +--------+---------+
                                 | (Text Protocol :600X)
                                 v
                        +------------------+
                        |      MASTER      |
                        |     (Node 1)     |
                        +----+--------+----+
                             |        |
         Replication Stream  |        |  Replication Stream
              (Binary :700X) |        |  (Binary :700X)
                             v        v
                      +--------+    +--------+
                      | FOLLOWER|   | FOLLOWER|
                      | (Node 2)|   | (Node 3)|
                      +--------+    +--------+
```

- **Core Storage Engine**: High-performance in-memory hash table using DJB2 hashing and separate chaining.
- **Durability & Crash Recovery**: Little-endian Write-Ahead Log (WAL) with 25-byte fixed headers, CRC32 data integrity checks, and synchronous `fsync` barriers.
- **Replication Engine**: Continuous tail-the-WAL streaming over framed TCP sockets (`[type:1][length:4][payload]`) unified across steady-state streaming and offline follower catch-up.
- **Consensus & Leader Election**: Term-based randomized watchdog election state machine enforcing strict majority quorums and log freshness invariants.
- **Consistency Models**: Supports asynchronous replication, synchronous quorum wait (`\sync on/off`), and monotonic client-side read-your-writes guarantees.
- **Log Compaction**: In-memory snapshot engine (`\snapshot`) supporting atomic compaction and horizon-aware catch-up replication.

---

## Building

Requires a C compiler (`gcc` or `clang`) with POSIX threads (`pthread`) on Linux.

```bash
make          # Compiles bin/kvdb and bin/kvdb-cli
make test     # Runs WAL unit tests, election tests, and 2-node replication suite
make clean    # Removes build artifacts
```

---

## Running a Cluster

### Option A: Local Multi-Process (Development)

Start three nodes in separate terminals:

```bash
# Terminal 1 - Node 1
./bin/kvdb --node-id 1 --port 6001 --repl-port 7001 \
           --peers 2@localhost:7002,3@localhost:7003 --data ./data1

# Terminal 2 - Node 2
./bin/kvdb --node-id 2 --port 6002 --repl-port 7002 \
           --peers 1@localhost:7001,3@localhost:7003 --data ./data2

# Terminal 3 - Node 3
./bin/kvdb --node-id 3 --port 6003 --repl-port 7003 \
           --peers 1@localhost:7001,2@localhost:7002 --data ./data3
```

Connect an interactive client:
```bash
./bin/kvdb-cli localhost 6001
```

### Option B: Docker Compose

```bash
docker compose up --build
```
Spawns three isolated container nodes mapped to persistent data volumes and inter-node DNS resolution.

---

## Client Commands

Connect with `./bin/kvdb-cli <host> <port>`:

| Command | Description |
|---|---|
| `PUT <key> <value>` | Inserts/updates a key. Only accepted by master; followers redirect to master ID. |
| `DELETE <key>` | Removes a key. Only accepted by master. |
| `GET <key>` | Reads a key. Any node can answer; tracks LSN for read-your-writes consistency. |
| `\info` | Displays node role, term, LSN, sync status, and follower replication lag. |
| `\sync on` / `\sync off` | Toggles synchronous vs asynchronous commit on the master. |
| `\snapshot` | Writes `snapshot.dat` and truncates the WAL. |
| `\checksum` | Calculates an order-independent XOR CRC32 across all stored keys. |
| `QUIT` | Closes the client connection cleanly. |

---

## Testing & Verification

- **Unit & Integration Suite**:
  ```bash
  make test
  ```
- **Chaos Suite** (Fault-injection, master failover, partition recovery, sync stalls):
  ```bash
  ./chaos/chaos_test.sh 100
  ```
