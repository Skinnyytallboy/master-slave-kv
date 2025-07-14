/*
 * kvdb - the server half of the tiny replicated key-value store.
 *
 * Every node runs three logical pieces glued together by one mutex-guarded
 * server_state_t: a client listener, a peer listener, and a handful of
 * background threads (heartbeats, election-timeout watchdog, one
 * WAL-tailing streamer thread per connected peer). See README.md and
 * design.md for the protocol this implements.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>
#include <stdarg.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include "common.h"
#include "net.h"
#include "wal.h"
#include "kvstore.h"
#include "election.h"

#define MAX_PEERS 8
#define ACK_WAIT_SECONDS 60
#define ELECTION_TIMEOUT_MS 3000
#define WAITING_TIMEOUT_MS 2000
#define HEARTBEAT_INTERVAL_MS 500

typedef enum { ROLE_WAITING, ROLE_FOLLOWER, ROLE_CANDIDATE, ROLE_MASTER } role_t;

static const char *role_name(role_t r) {
    switch (r) {
        case ROLE_WAITING: return "WAITING";
        case ROLE_FOLLOWER: return "FOLLOWER";
        case ROLE_CANDIDATE: return "CANDIDATE";
        case ROLE_MASTER: return "MASTER";
    }
    return "?";
}

typedef struct {
    int node_id;
    char host[128];
    int port; /* peer's repl port */

    int out_fd;                 /* connection we opened to them; we write here */
    long out_gen;                /* bumped every time out_fd is (re)assigned; guards
                                   against a streamer thread mistaking a reused fd
                                   number for its own, now-dead, connection */
    pthread_mutex_t out_lock;

    uint64_t peer_term;
    uint64_t peer_lsn;
    uint64_t last_acked_lsn;
    long long last_seen_ms;
    int streaming;               /* a tail-the-WAL thread is running for this peer */
} peer_t;

typedef struct pending_write {
    uint64_t lsn;
    int satisfied;
    pthread_cond_t cond;
    struct pending_write *next;
} pending_write_t;

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t lsn_cond;

    int node_id;
    role_t role;
    uint64_t current_term;
    uint64_t current_lsn;
    uint64_t wal_start_lsn;      /* lowest lsn still present in the wal file on disk */

    uint64_t last_vote_term;
    int last_vote_candidate;

    int master_id;               /* -1 if unknown */
    int sync_mode;                /* 0 = async, 1 = sync */
    long long last_master_heartbeat_ms;
    long long started_at_ms;

    uint64_t election_term_in_progress;
    int election_votes;

    kv_store_t *kv;
    FILE *wal_fp;
    char wal_path[512];
    char snapshot_path[512];
    char data_dir[400];

    peer_t peers[MAX_PEERS];
    int num_peers;

    pending_write_t *pending;
} server_state_t;

static server_state_t S;

/* ---------- tiny int (de)serialization helpers for message payloads ---------- */

static void put_u32(uint8_t *p, uint32_t v) { p[0]=v&0xFF; p[1]=(v>>8)&0xFF; p[2]=(v>>16)&0xFF; p[3]=(v>>24)&0xFF; }
static void put_u64(uint8_t *p, uint64_t v) { for (int i=0;i<8;i++) p[i]=(uint8_t)(v>>(8*i)); }
static uint32_t get_u32(const uint8_t *p) { return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24); }
static uint64_t get_u64(const uint8_t *p) { uint64_t v=0; for (int i=0;i<8;i++) v |= ((uint64_t)p[i])<<(8*i); return v; }

static int majority_count(void) {
    int total = S.num_peers + 1;
    return total / 2 + 1;
}

static peer_t *find_peer(int node_id) {
    for (int i = 0; i < S.num_peers; i++)
        if (S.peers[i].node_id == node_id) return &S.peers[i];
    return NULL;
}

/* assumes S.lock held. Adopts a higher term seen from the wire and steps down to follower. */
static void adopt_term_if_higher_locked(uint64_t term) {
    if (term > S.current_term) {
        if (S.role == ROLE_MASTER) {
            logf_ts("[node]", "stepping down: saw higher term %llu (was master for %llu)",
                    (unsigned long long)term, (unsigned long long)S.current_term);
        }
        S.current_term = term;
        S.role = ROLE_FOLLOWER;
        S.master_id = -1;
    }
}

/* ---------- WAL write path ---------- */

/* assumes S.lock held. Appends+applies one record, returns its lsn. */
static uint64_t append_and_apply_locked(uint8_t op_type, const char *key, const char *value, uint64_t term) {
    wal_record_t r;
    memset(&r, 0, sizeof(r));
    r.lsn = S.current_lsn + 1;
    r.term = term;
    r.op_type = op_type;
    r.key_len = (uint16_t)strlen(key);
    r.value_len = value ? (uint16_t)strlen(value) : 0;
    strncpy(r.key, key, sizeof(r.key) - 1);
    if (value) strncpy(r.value, value, sizeof(r.value) - 1);

    if (wal_append(S.wal_fp, &r) != 0) {
        logf_ts("[wal]", "fsync/append failed, aborting - disk problem?");
        exit(1);
    }
    if (op_type == OP_PUT) kv_put(S.kv, key, value);
    else kv_delete(S.kv, key);

    S.current_lsn = r.lsn;
    pthread_cond_broadcast(&S.lsn_cond);
    return r.lsn;
}

/* ---------- election / term-safety state machine ---------- */

static void send_vote_request_to(peer_t *p, uint64_t term, uint64_t lsn) {
    pthread_mutex_lock(&p->out_lock);
    if (p->out_fd >= 0) {
        uint8_t payload[20];
        put_u32(payload, (uint32_t)S.node_id);
        put_u64(payload + 4, term);
        put_u64(payload + 12, lsn);
        send_msg(p->out_fd, MSG_VOTE_REQUEST, payload, sizeof(payload));
    }
    pthread_mutex_unlock(&p->out_lock);
}

static void maybe_start_streamer(peer_t *p);

static void become_master_locked(void) {
    S.role = ROLE_MASTER;
    S.master_id = S.node_id;
    logf_ts("[node]", "elected MASTER for term %llu (lsn=%llu)",
            (unsigned long long)S.current_term, (unsigned long long)S.current_lsn);
}

static void run_one_election_round(void) {
    /* Retries election rounds with incremented terms until a leader is elected
     * or a higher term is adopted from another peer. */
    while (1) {
        pthread_mutex_lock(&S.lock);
        if (S.role == ROLE_MASTER) { pthread_mutex_unlock(&S.lock); return; }
        S.current_term++;
        S.role = ROLE_CANDIDATE;
        S.last_vote_term = S.current_term;
        S.last_vote_candidate = S.node_id;
        S.election_term_in_progress = S.current_term;
        S.election_votes = 1; /* vote for self */
        uint64_t my_term = S.current_term;
        uint64_t my_lsn = S.current_lsn;
        int votes_needed = majority_count();
        pthread_mutex_unlock(&S.lock);

        logf_ts("[node]", "master presumed dead; starting election for term %llu", (unsigned long long)my_term);

        for (int i = 0; i < S.num_peers; i++) send_vote_request_to(&S.peers[i], my_term, my_lsn);

        long long deadline = now_ms() + 1000;
        int resolved = 0;
        while (now_ms() < deadline) {
            usleep(30000);
            pthread_mutex_lock(&S.lock);
            role_t r = S.role;
            uint64_t t = S.current_term;
            int v = S.election_votes;
            pthread_mutex_unlock(&S.lock);
            if (r == ROLE_MASTER) return;
            if (t != my_term || r != ROLE_CANDIDATE) return;
            if (v >= votes_needed) { resolved = 1; break; }
        }
        if (resolved) return;

        logf_ts("[node]", "election for term %llu timed out with no majority, retrying", (unsigned long long)my_term);
        usleep((100 + rand() % 300) * 1000);

        pthread_mutex_lock(&S.lock);
        int still_eligible = (S.role == ROLE_CANDIDATE && S.current_term == my_term);
        pthread_mutex_unlock(&S.lock);
        if (!still_eligible) return;
    }
}

static void *election_watchdog_thread(void *arg) {
    (void)arg;
    while (1) {
        usleep(200000);
        pthread_mutex_lock(&S.lock);
        role_t r = S.role;
        long long last_hb = S.last_master_heartbeat_ms;
        pthread_mutex_unlock(&S.lock);

        long long timeout = (r == ROLE_WAITING) ? WAITING_TIMEOUT_MS : ELECTION_TIMEOUT_MS;
        if ((r == ROLE_WAITING || r == ROLE_FOLLOWER) && now_ms() - last_hb > timeout) {
            /* small jitter so two nodes that time out together don't lock-step forever */
            usleep((50 + rand() % 250) * 1000);
            run_one_election_round();
        }
    }
    return NULL;
}

static void *heartbeat_thread(void *arg) {
    (void)arg;
    while (1) {
        usleep(HEARTBEAT_INTERVAL_MS * 1000);
        pthread_mutex_lock(&S.lock);
        int is_master = (S.role == ROLE_MASTER);
        uint64_t term = S.current_term, lsn = S.current_lsn;
        pthread_mutex_unlock(&S.lock);
        if (!is_master) continue;

        uint8_t payload[16];
        put_u64(payload, term);
        put_u64(payload + 8, lsn);
        for (int i = 0; i < S.num_peers; i++) {
            peer_t *p = &S.peers[i];
            pthread_mutex_lock(&p->out_lock);
            if (p->out_fd >= 0) {
                if (send_msg(p->out_fd, MSG_HEARTBEAT, payload, sizeof(payload)) != 0) {
                    close(p->out_fd);
                    p->out_fd = -1;
                    p->out_gen++;
                }
            }
            pthread_mutex_unlock(&p->out_lock);
        }
    }
    return NULL;
}

/* ---------- snapshotting and checksums ---------- */

typedef struct { uint32_t acc; uint32_t count; } checksum_ctx_t;

static void checksum_one(const char *key, const char *value, void *ctxp) {
    checksum_ctx_t *ctx = ctxp;
    char combined[256 + 4096 + 1];
    snprintf(combined, sizeof(combined), "%s=%s", key, value);
    /* order-independent: XOR the per-entry crc together */
    ctx->acc ^= crc32_of((const uint8_t *)combined, strlen(combined));
    ctx->count++;
}

typedef struct { FILE *fp; uint32_t count; } snap_ctx_t;

static void snap_write_one(const char *key, const char *value, void *ctxp) {
    snap_ctx_t *ctx = ctxp;
    uint16_t kl = (uint16_t)strlen(key), vl = (uint16_t)strlen(value);
    uint8_t lens[4] = { (uint8_t)(kl&0xFF), (uint8_t)(kl>>8), (uint8_t)(vl&0xFF), (uint8_t)(vl>>8) };
    fwrite(lens, 1, 4, ctx->fp);
    fwrite(key, 1, kl, ctx->fp);
    fwrite(value, 1, vl, ctx->fp);
    ctx->count++;
}

/* format: [lsn:8][term:8][count:4][ entries: klen:2 vlen:2 key value ]... */
static int write_snapshot_locked(const char *path) {
    char tmp[560];
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    FILE *fp = fopen(tmp, "wb");
    if (!fp) return -1;

    uint8_t hdr[20];
    put_u64(hdr, S.current_lsn);
    put_u64(hdr + 8, S.current_term);
    put_u32(hdr + 16, 0); /* count placeholder, patched below once we know it */
    fwrite(hdr, 1, 20, fp);

    snap_ctx_t ctx = { fp, 0 };
    kv_foreach(S.kv, snap_write_one, &ctx);

    fflush(fp);
    fseek(fp, 16, SEEK_SET);
    uint8_t cnt[4];
    put_u32(cnt, ctx.count);
    fwrite(cnt, 1, 4, fp);
    fclose(fp);
    rename(tmp, path);
    return 0;
}

static int load_snapshot(const char *path, kv_store_t *kv, uint64_t *out_lsn, uint64_t *out_term) {
    FILE *fp = fopen(path, "rb");
    if (!fp) { *out_lsn = 0; *out_term = 0; return 0; }
    uint8_t hdr[20];
    if (fread(hdr, 1, 20, fp) != 20) { fclose(fp); *out_lsn = 0; *out_term = 0; return 0; }
    uint64_t lsn = get_u64(hdr), term = get_u64(hdr + 8);
    uint32_t count = get_u32(hdr + 16);
    for (uint32_t i = 0; i < count; i++) {
        uint8_t lens[4];
        if (fread(lens, 1, 4, fp) != 4) break;
        uint16_t kl = (uint16_t)(lens[0] | (lens[1] << 8));
        uint16_t vl = (uint16_t)(lens[2] | (lens[3] << 8));
        char key[256], val[4096];
        if (fread(key, 1, kl, fp) != kl) break;
        key[kl] = 0;
        if (fread(val, 1, vl, fp) != vl) break;
        val[vl] = 0;
        kv_put(kv, key, val);
    }
    fclose(fp);
    *out_lsn = lsn;
    *out_term = term;
    return 0;
}

/* send our current snapshot file down a peer's out_fd, framed as
 * MSG_SNAPSHOT_BEGIN(8-byte size) followed by the raw bytes. */
static int send_snapshot_to_peer(peer_t *p) {
    FILE *fp = fopen(S.snapshot_path, "rb");
    if (!fp) return -1;
    fseek(fp, 0, SEEK_END);
    long sz = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    uint8_t szbuf[8];
    put_u64(szbuf, (uint64_t)sz);

    pthread_mutex_lock(&p->out_lock);
    int ok = (p->out_fd >= 0) && send_msg(p->out_fd, MSG_SNAPSHOT_BEGIN, szbuf, 8) == 0;
    if (ok) {
        char buf[8192];
        long left = sz;
        while (left > 0 && ok) {
            size_t chunk = left < (long)sizeof(buf) ? (size_t)left : sizeof(buf);
            size_t got = fread(buf, 1, chunk, fp);
            if (got != chunk || write_all(p->out_fd, buf, chunk) != 0) ok = 0;
            left -= (long)chunk;
        }
    }
    pthread_mutex_unlock(&p->out_lock);
    fclose(fp);
    return ok ? 0 : -1;
}

/* ---------- per-peer WAL streaming replication engine ---------- */

typedef struct { peer_t *p; } streamer_arg_t;

static void *peer_streamer_thread(void *argp) {
    streamer_arg_t *arg = argp;
    peer_t *p = arg->p;
    free(arg);

    pthread_mutex_lock(&S.lock);
    uint64_t start_needed = p->peer_lsn + 1;
    uint64_t wal_start = S.wal_start_lsn;
    pthread_mutex_unlock(&S.lock);

    pthread_mutex_lock(&p->out_lock);
    int my_out_fd_snapshot = p->out_fd;
    long my_gen = p->out_gen;
    pthread_mutex_unlock(&p->out_lock);

    if (my_out_fd_snapshot < 0) { p->streaming = 0; return NULL; }

    if (start_needed <= wal_start && wal_start > 0) {
        logf_ts("[repl]", "node %d is behind the wal horizon; sending snapshot", p->node_id);
        pthread_mutex_lock(&S.lock);
        write_snapshot_locked(S.snapshot_path);
        pthread_mutex_unlock(&S.lock);
        if (send_snapshot_to_peer(p) != 0) {
            p->streaming = 0;
            return NULL;
        }
        start_needed = wal_start + 1;
    }

    FILE *fp = fopen(S.wal_path, "rb");
    if (!fp) { p->streaming = 0; return NULL; }

    logf_ts("[repl]", "streaming to node %d starting at lsn=%llu", p->node_id, (unsigned long long)start_needed);

    long pos = 0;
    while (1) {
        pthread_mutex_lock(&S.lock);
        int still_master = (S.role == ROLE_MASTER);
        pthread_mutex_unlock(&S.lock);
        pthread_mutex_lock(&p->out_lock);
        int fd_alive = (p->out_gen == my_gen && p->out_fd >= 0);
        pthread_mutex_unlock(&p->out_lock);
        if (!still_master || !fd_alive) break;

        fseek(fp, pos, SEEK_SET);
        uint8_t hdr[WAL_HEADER_LEN];
        size_t got = fread(hdr, 1, WAL_HEADER_LEN, fp);
        if (got < WAL_HEADER_LEN) {
            clearerr(fp);
            usleep(100000);
            continue;
        }
        wal_record_t r;
        wal_parse_header(hdr, &r);
        size_t body_len = (size_t)r.key_len + r.value_len;
        uint8_t body[256 + 4096];
        got = fread(body, 1, body_len, fp);
        if (got < body_len) {
            clearerr(fp);
            usleep(100000);
            continue;
        }
        pos = ftell(fp);

        if (r.lsn < start_needed) continue; /* already sent */

        uint8_t full[WAL_HEADER_LEN + 256 + 4096];
        memcpy(full, hdr, WAL_HEADER_LEN);
        memcpy(full + WAL_HEADER_LEN, body, body_len);

        pthread_mutex_lock(&p->out_lock);
        int ok = (p->out_gen == my_gen && p->out_fd >= 0 &&
                  send_msg(p->out_fd, MSG_WAL_RECORD, full, (uint32_t)(WAL_HEADER_LEN + body_len)) == 0);
        if (!ok && p->out_gen == my_gen && p->out_fd >= 0) { close(p->out_fd); p->out_fd = -1; p->out_gen++; }
        pthread_mutex_unlock(&p->out_lock);
        if (!ok) break;
        start_needed = r.lsn + 1;
    }

    fclose(fp);
    p->streaming = 0;
    return NULL;
}

static void maybe_start_streamer(peer_t *p) {
    pthread_mutex_lock(&S.lock);
    int should = (S.role == ROLE_MASTER && p->out_fd >= 0 && !p->streaming);
    if (should) p->streaming = 1;
    pthread_mutex_unlock(&S.lock);
    if (!should) return;

    streamer_arg_t *arg = malloc(sizeof(*arg));
    arg->p = p;
    pthread_t tid;
    pthread_create(&tid, NULL, peer_streamer_thread, arg);
    pthread_detach(tid);
}

/* ---------- peer wire message dispatch and ingestion ---------- */

static void handle_ack(peer_t *p, uint64_t lsn) {
    pthread_mutex_lock(&S.lock);
    if (lsn > p->last_acked_lsn) p->last_acked_lsn = lsn;
    for (pending_write_t *w = S.pending; w; w = w->next) {
        if (!w->satisfied && w->lsn <= lsn) {
            w->satisfied = 1;
            pthread_cond_broadcast(&w->cond);
        }
    }
    pthread_mutex_unlock(&S.lock);
}

static void handle_wal_record(peer_t *p, const uint8_t *buf, uint32_t len) {
    if (len < WAL_HEADER_LEN) return;
    wal_record_t r;
    wal_parse_header(buf, &r);
    size_t body_len = (size_t)r.key_len + r.value_len;
    if (len < WAL_HEADER_LEN + body_len) return;
    memcpy(r.key, buf + WAL_HEADER_LEN, r.key_len);
    r.key[r.key_len] = 0;
    memcpy(r.value, buf + WAL_HEADER_LEN + r.key_len, r.value_len);
    r.value[r.value_len] = 0;

    uint32_t stored_crc = get_u32(buf);
    uint32_t computed = crc32_of(buf + 4, WAL_HEADER_LEN + body_len - 4);
    if (stored_crc != computed) {
        logf_ts("[repl]", "crc mismatch from node %d, dropping record", p->node_id);
        return;
    }

    pthread_mutex_lock(&S.lock);
    if (r.lsn != S.current_lsn + 1) {
        /* gap or duplicate - most likely we just received a snapshot and this
         * record predates it, or a duplicate after a reconnect. Ignore. */
        if (r.lsn > S.current_lsn + 1) {
            logf_ts("[repl]", "lsn gap from node %d (have %llu, got %llu); waiting for reconnect to fix it",
                    p->node_id, (unsigned long long)S.current_lsn, (unsigned long long)r.lsn);
        }
        pthread_mutex_unlock(&S.lock);
        return;
    }
    adopt_term_if_higher_locked(r.term);
    if (S.role != ROLE_MASTER) S.role = ROLE_FOLLOWER;
    S.master_id = p->node_id;
    S.last_master_heartbeat_ms = now_ms();

    if (wal_append(S.wal_fp, &r) != 0) {
        logf_ts("[wal]", "follower fsync/append failed - disk problem?");
        pthread_mutex_unlock(&S.lock);
        exit(1);
    }
    if (r.op_type == OP_PUT) kv_put(S.kv, r.key, r.value);
    else kv_delete(S.kv, r.key);
    S.current_lsn = r.lsn;
    pthread_cond_broadcast(&S.lsn_cond);
    pthread_mutex_unlock(&S.lock);

    uint8_t ackpayload[8];
    put_u64(ackpayload, r.lsn);
    pthread_mutex_lock(&p->out_lock);
    if (p->out_fd >= 0) send_msg(p->out_fd, MSG_ACK, ackpayload, 8);
    pthread_mutex_unlock(&p->out_lock);
}

static void handle_heartbeat(peer_t *p, const uint8_t *buf, uint32_t len) {
    if (len < 16) return;
    uint64_t term = get_u64(buf), lsn = get_u64(buf + 8);
    (void)lsn;
    pthread_mutex_lock(&S.lock);
    adopt_term_if_higher_locked(term);
    if (term >= S.current_term && S.role != ROLE_MASTER) {
        S.master_id = p->node_id;
        S.role = ROLE_FOLLOWER;
        S.last_master_heartbeat_ms = now_ms();
    }
    pthread_mutex_unlock(&S.lock);
}

static void handle_vote_request(peer_t *p, const uint8_t *buf, uint32_t len) {
    if (len < 20) return;
    int candidate_id = (int)get_u32(buf);
    uint64_t term = get_u64(buf + 4);
    uint64_t lsn = get_u64(buf + 12);

    int granted;
    pthread_mutex_lock(&S.lock);
    election_state_t est = { S.current_term, S.current_lsn, S.last_vote_term, S.last_vote_candidate };
    granted = election_decide_vote(&est, candidate_id, term, lsn);
    if (est.current_term > S.current_term) adopt_term_if_higher_locked(est.current_term);
    S.last_vote_term = est.last_vote_term;
    S.last_vote_candidate = est.last_vote_candidate;
    uint64_t reply_term = term;
    pthread_mutex_unlock(&S.lock);

    logf_ts("[election]", "vote request from node %d for term %llu: %s",
            candidate_id, (unsigned long long)term, granted ? "GRANTED" : "rejected");

    uint8_t payload[13];
    put_u32(payload, (uint32_t)S.node_id);
    put_u64(payload + 4, reply_term);
    payload[12] = granted ? 1 : 0;
    pthread_mutex_lock(&p->out_lock);
    if (p->out_fd >= 0) send_msg(p->out_fd, MSG_VOTE_RESPONSE, payload, sizeof(payload));
    pthread_mutex_unlock(&p->out_lock);
}

static void handle_vote_response(const uint8_t *buf, uint32_t len) {
    if (len < 13) return;
    uint64_t term = get_u64(buf + 4);
    int granted = buf[12];
    int just_won = 0;

    pthread_mutex_lock(&S.lock);
    if (S.role == ROLE_CANDIDATE && term == S.election_term_in_progress && granted) {
        S.election_votes++;
        if (S.election_votes >= majority_count()) {
            become_master_locked();
            just_won = 1;
        }
    }
    pthread_mutex_unlock(&S.lock);

    if (just_won) {
        for (int i = 0; i < S.num_peers; i++) maybe_start_streamer(&S.peers[i]);
    }
}

static void handle_snapshot(int fd) {
    uint8_t szbuf[8];
    if (read_all(fd, szbuf, 8) != 0) return;
    uint64_t sz = get_u64(szbuf);

    char tmp[560];
    snprintf(tmp, sizeof(tmp), "%s.incoming", S.snapshot_path);
    FILE *fp = fopen(tmp, "wb");
    if (!fp) return;
    char buf[8192];
    uint64_t left = sz;
    while (left > 0) {
        size_t chunk = left < sizeof(buf) ? (size_t)left : sizeof(buf);
        if (read_all(fd, buf, chunk) != 0) { fclose(fp); return; }
        fwrite(buf, 1, chunk, fp);
        left -= chunk;
    }
    fclose(fp);
    rename(tmp, S.snapshot_path);

    pthread_mutex_lock(&S.lock);
    kv_clear(S.kv);
    uint64_t snap_lsn, snap_term;
    load_snapshot(S.snapshot_path, S.kv, &snap_lsn, &snap_term);
    S.current_lsn = snap_lsn;
    S.wal_start_lsn = snap_lsn;
    if (snap_term > S.current_term) S.current_term = snap_term;
    /* start a fresh wal segment for whatever comes after the snapshot */
    fclose(S.wal_fp);
    S.wal_fp = fopen(S.wal_path, "wb");
    S.wal_fp = freopen(S.wal_path, "ab", S.wal_fp);
    pthread_cond_broadcast(&S.lsn_cond);
    logf_ts("[repl]", "loaded snapshot at lsn=%llu", (unsigned long long)snap_lsn);
    pthread_mutex_unlock(&S.lock);
}

/* ---------- peer listener: accept side ---------- */

typedef struct { int fd; } incoming_arg_t;

static void *handle_incoming_peer(void *argp) {
    incoming_arg_t *a = argp;
    int fd = a->fd;
    free(a);

    uint8_t buf[MAX_MSG_PAYLOAD];
    uint8_t type;
    uint32_t len;

    if (recv_msg(fd, &type, buf, sizeof(buf), &len) <= 0 || type != MSG_HELLO || len < 20) {
        close(fd);
        return NULL;
    }
    int peer_id = (int)get_u32(buf);
    uint64_t term = get_u64(buf + 4);
    uint64_t lsn = get_u64(buf + 12);

    peer_t *p = find_peer(peer_id);
    if (!p) {
        logf_ts("[peer]", "hello from unknown node id %d, closing", peer_id);
        close(fd);
        return NULL;
    }

    pthread_mutex_lock(&S.lock);
    p->peer_term = term;
    p->peer_lsn = lsn;
    p->last_seen_ms = now_ms();
    adopt_term_if_higher_locked(term);
    logf_ts("[peer]", "node %d connected (their term=%llu, lsn=%llu)",
            peer_id, (unsigned long long)term, (unsigned long long)lsn);
    pthread_mutex_unlock(&S.lock);

    maybe_start_streamer(p);

    while (1) {
        int rc = recv_msg(fd, &type, buf, sizeof(buf), &len);
        if (rc <= 0) {
            logf_ts("[peer]", "recv from node %d returned %d (errno=%d %s)", peer_id, rc, errno, strerror(errno));
            break;
        }
        p->last_seen_ms = now_ms();
        switch (type) {
            case MSG_WAL_RECORD: handle_wal_record(p, buf, len); break;
            case MSG_HEARTBEAT: handle_heartbeat(p, buf, len); break;
            case MSG_ACK: handle_ack(p, len >= 8 ? get_u64(buf) : 0); break;
            case MSG_VOTE_REQUEST: handle_vote_request(p, buf, len); break;
            case MSG_VOTE_RESPONSE: handle_vote_response(buf, len); break;
            case MSG_SNAPSHOT_BEGIN: handle_snapshot(fd); break;
            default: break;
        }
    }

    logf_ts("[peer]", "node %d disconnected", peer_id);
    close(fd);
    return NULL;
}

static void *peer_listener_thread(void *argp) {
    int listen_fd = *(int *)argp;
    while (1) {
        int fd = accept(listen_fd, NULL, NULL);
        if (fd < 0) { if (errno == EINTR) continue; break; }
        int one = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
        incoming_arg_t *a = malloc(sizeof(*a));
        a->fd = fd;
        pthread_t tid;
        pthread_create(&tid, NULL, handle_incoming_peer, a);
        pthread_detach(tid);
    }
    return NULL;
}

/* ---------- outgoing peer connectors, one thread per configured peer ---------- */

static void *peer_connector_thread(void *argp) {
    peer_t *p = argp;
    int backoff = 1;
    while (1) {
        pthread_mutex_lock(&p->out_lock);
        int already = (p->out_fd >= 0);
        pthread_mutex_unlock(&p->out_lock);
        if (already) { sleep(1); continue; }

        int fd = net_connect(p->host, p->port);
        if (fd < 0) {
            sleep(backoff);
            if (backoff < 10) backoff *= 2;
            continue;
        }
        backoff = 1;

        pthread_mutex_lock(&S.lock);
        uint32_t my_id = (uint32_t)S.node_id;
        uint64_t my_term = S.current_term, my_lsn = S.current_lsn;
        pthread_mutex_unlock(&S.lock);

        uint8_t payload[20];
        put_u32(payload, my_id);
        put_u64(payload + 4, my_term);
        put_u64(payload + 12, my_lsn);
        if (send_msg(fd, MSG_HELLO, payload, sizeof(payload)) != 0) {
            close(fd);
            sleep(1);
            continue;
        }

        pthread_mutex_lock(&p->out_lock);
        p->out_fd = fd;
        p->out_gen++;
        pthread_mutex_unlock(&p->out_lock);

        maybe_start_streamer(p);
        sleep(1);
    }
    return NULL;
}

/* ---------- client-facing text protocol worker ---------- */

static int read_line(int fd, char *buf, size_t cap) {
    size_t n = 0;
    while (n + 1 < cap) {
        char c;
        ssize_t r = read(fd, &c, 1);
        if (r <= 0) return n > 0 ? (int)n : -1;
        if (c == '\n') break;
        if (c != '\r') buf[n++] = c;
    }
    buf[n] = 0;
    return (int)n;
}

static void send_line(int fd, const char *fmt, ...) {
    char msg[8192];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
    strncat(msg, "\n", sizeof(msg) - strlen(msg) - 1);
    write_all(fd, msg, strlen(msg));
}

typedef struct { int fd; } client_arg_t;

static void *handle_client(void *argp) {
    client_arg_t *a = argp;
    int fd = a->fd;
    free(a);

    pthread_mutex_lock(&S.lock);
    role_t r = S.role;
    uint64_t term = S.current_term;
    pthread_mutex_unlock(&S.lock);
    send_line(fd, "connected to node %d (role=%s, term=%llu)", S.node_id, role_name(r), (unsigned long long)term);

    char line[8192];
    int n;
    while ((n = read_line(fd, line, sizeof(line))) >= 0) {
        if (n == 0) continue;
        char *saveptr;
        char *cmd = strtok_r(line, " ", &saveptr);
        if (!cmd) continue;

        if (strcasecmp(cmd, "QUIT") == 0) {
            continue;
        } else if (strcasecmp(cmd, "PUT") == 0) {
            char *key = strtok_r(NULL, " ", &saveptr);
            char *val = strtok_r(NULL, "", &saveptr);
            if (!key || !val) { send_line(fd, "ERR usage: PUT key value"); continue; }
            pthread_mutex_lock(&S.lock);
            if (S.role != ROLE_MASTER) {
                int mid = S.master_id;
                pthread_mutex_unlock(&S.lock);
                if (mid >= 0) send_line(fd, "ERR not master; current master is node %d", mid);
                else send_line(fd, "ERR not master; no master elected yet");
                continue;
            }
            uint64_t term_snap = S.current_term;
            uint64_t lsn = append_and_apply_locked(OP_PUT, key, val, term_snap);
            pending_write_t *pw = NULL;
            if (S.sync_mode) {
                pw = calloc(1, sizeof(*pw));
                pw->lsn = lsn;
                pthread_cond_init(&pw->cond, NULL);
                pw->next = S.pending;
                S.pending = pw;
            }
            pthread_mutex_unlock(&S.lock);

            if (pw) {
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_sec += ACK_WAIT_SECONDS;
                pthread_mutex_lock(&S.lock);
                while (!pw->satisfied) {
                    int rc = pthread_cond_timedwait(&pw->cond, &S.lock, &ts);
                    if (rc != 0) break;
                }
                pending_write_t **link = &S.pending;
                while (*link) { if (*link == pw) { *link = pw->next; break; } link = &(*link)->next; }
                pthread_mutex_unlock(&S.lock);
                free(pw);
            }

            int replicated_to = 0;
            for (int i = 0; i < S.num_peers; i++) if (S.peers[i].last_acked_lsn >= lsn) replicated_to++;
            send_line(fd, "OK (lsn=%llu, replicated-to=%d)", (unsigned long long)lsn, replicated_to);
        } else if (strcasecmp(cmd, "DELETE") == 0) {
            char *key = strtok_r(NULL, " ", &saveptr);
            if (!key) { send_line(fd, "ERR usage: DELETE key"); continue; }
            pthread_mutex_lock(&S.lock);
            if (S.role != ROLE_MASTER) {
                int mid = S.master_id;
                pthread_mutex_unlock(&S.lock);
                if (mid >= 0) send_line(fd, "ERR not master; current master is node %d", mid);
                else send_line(fd, "ERR not master; no master elected yet");
                continue;
            }
            uint64_t lsn = append_and_apply_locked(OP_DELETE, key, "", S.current_term);
            pthread_mutex_unlock(&S.lock);
            send_line(fd, "OK (lsn=%llu)", (unsigned long long)lsn);
        } else if (strcasecmp(cmd, "GET") == 0) {
            char *key = strtok_r(NULL, " ", &saveptr);
            char *waitkw = strtok_r(NULL, " ", &saveptr);
            char *waitlsn_s = strtok_r(NULL, " ", &saveptr);
            if (!key) { send_line(fd, "ERR usage: GET key"); continue; }
            pthread_mutex_lock(&S.lock);
            if (waitkw && strcasecmp(waitkw, "WAIT") == 0 && waitlsn_s) {
                uint64_t want = strtoull(waitlsn_s, NULL, 10);
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_sec += 5; /* read-your-writes bonus: bounded wait, see design.md */
                while (S.current_lsn < want) {
                    if (pthread_cond_timedwait(&S.lsn_cond, &S.lock, &ts) != 0) break;
                }
            }
            const char *val = kv_get(S.kv, key);
            char valcopy[4096];
            if (val) strncpy(valcopy, val, sizeof(valcopy) - 1), valcopy[sizeof(valcopy)-1]=0;
            pthread_mutex_unlock(&S.lock);
            send_line(fd, val ? "%s" : "NULL", val ? valcopy : "");
        } else if (strcmp(cmd, "\\info") == 0) {
            pthread_mutex_lock(&S.lock);
            send_line(fd, "node_id:  %d", S.node_id);
            send_line(fd, "role:     %s", role_name(S.role));
            send_line(fd, "term:     %llu", (unsigned long long)S.current_term);
            send_line(fd, "lsn:      %llu", (unsigned long long)S.current_lsn);
            if (S.role == ROLE_MASTER) {
                send_line(fd, "sync_mode: %s", S.sync_mode ? "sync" : "async");
                for (int i = 0; i < S.num_peers; i++) {
                    peer_t *p = &S.peers[i];
                    long long lag = (long long)S.current_lsn - (long long)p->last_acked_lsn;
                    send_line(fd, "follower: node %d (lag=%lld)", p->node_id, lag < 0 ? 0 : lag);
                }
            } else {
                send_line(fd, "master:   %s", S.master_id >= 0 ? "node known" : "unknown");
                if (S.master_id >= 0) send_line(fd, "master_id: %d", S.master_id);
            }
            pthread_mutex_unlock(&S.lock);
        } else if (strcmp(cmd, "\\sync") == 0) {
            char *mode = strtok_r(NULL, " ", &saveptr);
            pthread_mutex_lock(&S.lock);
            if (mode && strcasecmp(mode, "on") == 0) S.sync_mode = 1;
            else if (mode && strcasecmp(mode, "off") == 0) // standalone node config
    S.sync_mode = 0;
            int m = S.sync_mode;
            pthread_mutex_unlock(&S.lock);
            send_line(fd, "sync_mode is now %s", m ? "on" : "off");
        } else if (strcmp(cmd, "\\checksum") == 0) {
            pthread_mutex_lock(&S.lock);
            checksum_ctx_t ctx = { 0, 0 };
            kv_foreach(S.kv, checksum_one, &ctx);
            uint64_t lsn = S.current_lsn;
            pthread_mutex_unlock(&S.lock);
            send_line(fd, "checksum: %08x count: %u lsn: %llu", ctx.acc, ctx.count, (unsigned long long)lsn);
        } else if (strcmp(cmd, "\\snapshot") == 0) {
            pthread_mutex_lock(&S.lock);
            int rc = write_snapshot_locked(S.snapshot_path);
            uint64_t snap_lsn = S.current_lsn;
            if (rc == 0) {
                fclose(S.wal_fp);
                S.wal_fp = fopen(S.wal_path, "wb");
                S.wal_fp = freopen(S.wal_path, "ab", S.wal_fp);
                S.wal_start_lsn = snap_lsn;
            }
            pthread_mutex_unlock(&S.lock);
            send_line(fd, rc == 0 ? "snapshot written at lsn=%llu, wal truncated" : "snapshot failed", (unsigned long long)snap_lsn);
        } else {
            send_line(fd, "ERR unknown command");
        }
    }

    close(fd);
    return NULL;
}

static void *client_listener_thread(void *argp) {
    int listen_fd = *(int *)argp;
    while (1) {
        int fd = accept(listen_fd, NULL, NULL);
        if (fd < 0) { if (errno == EINTR) continue; break; }
        int one = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
        client_arg_t *a = malloc(sizeof(*a));
        a->fd = fd;
        pthread_t tid;
        pthread_create(&tid, NULL, handle_client, a);
        pthread_detach(tid);
    }
    return NULL;
}

/* ---------- startup / argument parsing ---------- */

static void parse_peers(const char *spec) {
    char buf[2048];
    strncpy(buf, spec, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = 0;

    char *saveptr;
    char *tok = strtok_r(buf, ",", &saveptr);
    while (tok) {
        peer_t *p = &S.peers[S.num_peers];
        char *at = strchr(tok, '@');
        char *colon = at ? strrchr(at, ':') : NULL;
        if (!at || !colon) { fprintf(stderr, "bad peer spec: %s\n", tok); exit(1); }
        *at = 0;
        *colon = 0;
        p->node_id = atoi(tok);
        strncpy(p->host, at + 1, sizeof(p->host) - 1);
        p->port = atoi(colon + 1);
        p->out_fd = -1;
        pthread_mutex_init(&p->out_lock, NULL);
        p->peer_term = 0;
        p->peer_lsn = 0;
        p->last_acked_lsn = 0;
        p->streaming = 0;
        S.num_peers++;
        tok = strtok_r(NULL, ",", &saveptr);
    }
}

static void usage(const char *prog) {
    fprintf(stderr,
        "usage: %s --node-id N --port P --repl-port RP --peers id@host:port,... --data DIR\n",
        prog);
}

int main(int argc, char **argv) {
    int node_id = -1, port = -1, repl_port = -1;
    const char *peers_spec = NULL;
    const char *data_dir = NULL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--node-id") == 0 && i + 1 < argc) node_id = atoi(argv[++i]);
        else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--repl-port") == 0 && i + 1 < argc) repl_port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--peers") == 0 && i + 1 < argc) peers_spec = argv[++i];
        else if (strcmp(argv[i], "--data") == 0 && i + 1 < argc) data_dir = argv[++i];
        else { usage(argv[0]); return 1; }
    }
    if (node_id < 0 || port < 0 || repl_port < 0 || !data_dir) { usage(argv[0]); return 1; }

    signal(SIGPIPE, SIG_IGN);
    setvbuf(stdout, NULL, _IOLBF, 0);
    srand((unsigned)(time(NULL) ^ (node_id * 7919)));

    memset(&S, 0, sizeof(S));
    pthread_mutex_init(&S.lock, NULL);
    pthread_cond_init(&S.lsn_cond, NULL);
    S.node_id = node_id;
    S.role = ROLE_WAITING;
    S.master_id = -1;
    // standalone node config
    S.sync_mode = 0;
    S.started_at_ms = now_ms();
    S.last_master_heartbeat_ms = S.started_at_ms;
    strncpy(S.data_dir, data_dir, sizeof(S.data_dir) - 1);

    mkdir(data_dir, 0755);
    snprintf(S.wal_path, sizeof(S.wal_path), "%s/wal.log", data_dir);
    snprintf(S.snapshot_path, sizeof(S.snapshot_path), "%s/snapshot.dat", data_dir);

    S.kv = kv_new(4096);

    printf("[node %d] starting up...\n", node_id);

    uint64_t snap_lsn = 0, snap_term = 0;
    load_snapshot(S.snapshot_path, S.kv, &snap_lsn, &snap_term);
    if (snap_lsn > 0) {
        S.wal_start_lsn = snap_lsn;
        printf("[node %d] loaded snapshot at lsn=%llu\n", node_id, (unsigned long long)snap_lsn);
    }

    uint64_t wal_lsn = 0, wal_term = 0;
    wal_replay(S.wal_path, S.kv, &wal_lsn, &wal_term);
    printf("[node %d] WAL replay complete: lsn=%llu\n", node_id, (unsigned long long)(wal_lsn ? wal_lsn : snap_lsn));

    S.current_lsn = wal_lsn > snap_lsn ? wal_lsn : snap_lsn;
    S.current_term = wal_term > snap_term ? wal_term : snap_term;

    S.wal_fp = fopen(S.wal_path, "ab");
    if (!S.wal_fp) { perror("fopen wal"); return 1; }

    if (peers_spec) parse_peers(peers_spec);

    if (S.num_peers == 0) {
        /* Single-node mode: become master immediately if no peers are configured. */
        S.role = ROLE_MASTER;
        S.master_id = S.node_id;
        if (S.current_term == 0) S.current_term = 1;
    }

    int client_fd = net_listen(port);
    if (client_fd < 0) { fprintf(stderr, "could not bind client port %d\n", port); return 1; }
    int repl_fd = net_listen(repl_port);
    if (repl_fd < 0) { fprintf(stderr, "could not bind repl port %d\n", repl_port); return 1; }

    printf("[node %d] no master announced yet; waiting for quorum\n", node_id);

    pthread_t tid;
    pthread_create(&tid, NULL, client_listener_thread, &client_fd);
    pthread_detach(tid);
    pthread_create(&tid, NULL, peer_listener_thread, &repl_fd);
    pthread_detach(tid);
    pthread_create(&tid, NULL, heartbeat_thread, NULL);
    pthread_detach(tid);
    pthread_create(&tid, NULL, election_watchdog_thread, NULL);
    pthread_detach(tid);

    for (int i = 0; i < S.num_peers; i++) {
        pthread_create(&tid, NULL, peer_connector_thread, &S.peers[i]);
        pthread_detach(tid);
    }

    printf("[node %d] accepting client writes on port %d (once elected/master)\n", node_id, port);

    /* Keep main thread alive while background worker threads handle traffic */
    while (1) pause();

    return 0;
}
