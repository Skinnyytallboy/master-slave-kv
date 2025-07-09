/* Unit tests for the WAL format: serialize/parse round-trip, fsync+replay,
 * and that a truncated/corrupt trailing record is dropped instead of
 * taking down the whole replay. Run via `make test`. */

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "wal.h"
#include "kvstore.h"

static int failures = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { printf("FAIL: %s\n", msg); failures++; } \
    else printf("ok: %s\n", msg); \
} while (0)

static void test_serialize_roundtrip(void) {
    wal_record_t r;
    memset(&r, 0, sizeof(r));
    r.lsn = 42;
    r.term = 3;
    r.op_type = OP_PUT;
    strcpy(r.key, "hello");
    strcpy(r.value, "world");
    r.key_len = 5;
    r.value_len = 5;

    uint8_t buf[512];
    size_t n = wal_serialize(&r, buf, sizeof(buf));
    CHECK(n == WAL_HEADER_LEN + 10, "serialized length matches header+body");

    wal_record_t parsed;
    wal_parse_header(buf, &parsed);
    CHECK(parsed.lsn == 42, "lsn round-trips");
    CHECK(parsed.term == 3, "term round-trips");
    CHECK(parsed.op_type == OP_PUT, "op_type round-trips");
    CHECK(parsed.key_len == 5 && parsed.value_len == 5, "lengths round-trip");
    CHECK(memcmp(buf + WAL_HEADER_LEN, "hello", 5) == 0, "key bytes intact");
    CHECK(memcmp(buf + WAL_HEADER_LEN + 5, "world", 5) == 0, "value bytes intact");
}

static void test_append_and_replay(void) {
    const char *path = "/tmp/kvdb_test_wal.log";
    unlink(path);

    FILE *fp = fopen(path, "ab");
    CHECK(fp != NULL, "wal file opens for append");

    wal_record_t r1 = {0}, r2 = {0}, r3 = {0};
    r1.lsn = 1; r1.term = 1; r1.op_type = OP_PUT;
    strcpy(r1.key, "alice"); r1.key_len = 5;
    strcpy(r1.value, "100"); r1.value_len = 3;

    r2.lsn = 2; r2.term = 1; r2.op_type = OP_PUT;
    strcpy(r2.key, "bob"); r2.key_len = 3;
    strcpy(r2.value, "50"); r2.value_len = 2;

    r3.lsn = 3; r3.term = 1; r3.op_type = OP_DELETE;
    strcpy(r3.key, "alice"); r3.key_len = 5;
    r3.value_len = 0;

    CHECK(wal_append(fp, &r1) == 0, "append record 1");
    CHECK(wal_append(fp, &r2) == 0, "append record 2");
    CHECK(wal_append(fp, &r3) == 0, "append record 3 (delete)");
    fclose(fp);

    kv_store_t *kv = kv_new(16);
    uint64_t lsn = 0, term = 0;
    wal_replay(path, kv, &lsn, &term);
    CHECK(lsn == 3, "replay reports highest lsn");
    CHECK(term == 1, "replay reports highest term");
    CHECK(kv_get(kv, "alice") == NULL, "delete applied during replay");
    CHECK(kv_get(kv, "bob") != NULL && strcmp(kv_get(kv, "bob"), "50") == 0, "put survives replay");
    kv_free(kv);
    unlink(path);
}

static void test_truncated_tail_is_dropped(void) {
    const char *path = "/tmp/kvdb_test_wal_trunc.log";
    unlink(path);

    FILE *fp = fopen(path, "ab");
    wal_record_t r1 = {0};
    r1.lsn = 1; r1.term = 1; r1.op_type = OP_PUT;
    strcpy(r1.key, "x"); r1.key_len = 1;
    strcpy(r1.value, "1"); r1.value_len = 1;
    wal_append(fp, &r1);

    /* simulate a crash mid-write: append a partial header for record 2 */
    uint8_t partial[10] = { 0 };
    fwrite(partial, 1, sizeof(partial), fp);
    fclose(fp);

    kv_store_t *kv = kv_new(16);
    uint64_t lsn = 0, term = 0;
    wal_replay(path, kv, &lsn, &term);
    CHECK(lsn == 1, "replay stops at the last complete record");
    CHECK(kv_get(kv, "x") != NULL, "the complete record before the truncation is kept");
    kv_free(kv);
    unlink(path);
}

static void test_corrupt_crc_is_dropped(void) {
    const char *path = "/tmp/kvdb_test_wal_crc.log";
    unlink(path);

    FILE *fp = fopen(path, "ab");
    wal_record_t r1 = {0}, r2 = {0};
    r1.lsn = 1; r1.term = 1; r1.op_type = OP_PUT;
    strcpy(r1.key, "x"); r1.key_len = 1;
    strcpy(r1.value, "1"); r1.value_len = 1;
    wal_append(fp, &r1);

    r2.lsn = 2; r2.term = 1; r2.op_type = OP_PUT;
    strcpy(r2.key, "y"); r2.key_len = 1;
    strcpy(r2.value, "2"); r2.value_len = 1;
    long pos_before_r2 = ftell(fp);
    wal_append(fp, &r2);
    fclose(fp);

    /* flip a byte in record 2's crc so it no longer matches */
    fp = fopen(path, "r+b");
    fseek(fp, pos_before_r2, SEEK_SET);
    uint8_t b;
    size_t got = fread(&b, 1, 1, fp);
    (void)got;
    b ^= 0xFF;
    fseek(fp, pos_before_r2, SEEK_SET);
    fwrite(&b, 1, 1, fp);
    fclose(fp);

    kv_store_t *kv = kv_new(16);
    uint64_t lsn = 0, term = 0;
    wal_replay(path, kv, &lsn, &term);
    CHECK(lsn == 1, "replay stops before the record with a bad crc");
    CHECK(kv_get(kv, "y") == NULL, "the corrupt record itself is never applied");
    kv_free(kv);
    unlink(path);
}

int main(void) {
    test_serialize_roundtrip();
    test_append_and_replay();
    test_truncated_tail_is_dropped();
    test_corrupt_crc_is_dropped();

    if (failures == 0) {
        printf("\nall wal tests passed\n");
        return 0;
    }
    printf("\n%d wal test(s) failed\n", failures);
    return 1;
}
