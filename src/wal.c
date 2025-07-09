#include "wal.h"
#include "common.h"
#include <string.h>
#include <unistd.h>
#include <errno.h>

static void put_u32(uint8_t *p, uint32_t v) {
    p[0] = v & 0xFF; p[1] = (v >> 8) & 0xFF; p[2] = (v >> 16) & 0xFF; p[3] = (v >> 24) & 0xFF;
}
static void put_u64(uint8_t *p, uint64_t v) {
    for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (8 * i));
}
static void put_u16(uint8_t *p, uint16_t v) {
    p[0] = v & 0xFF; p[1] = (v >> 8) & 0xFF;
}
static uint64_t get_u64(const uint8_t *p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= ((uint64_t)p[i]) << (8 * i);
    return v;
}
static uint16_t get_u16(const uint8_t *p) {
    return (uint16_t)(p[0] | (p[1] << 8));
}
static uint32_t get_u32(const uint8_t *p) {
    return (uint32_t)(p[0] | (p[1] << 8) | (p[2] << 16) | (p[3] << 24));
}

size_t wal_serialize(const wal_record_t *r, uint8_t *buf, size_t bufcap) {
    size_t total = WAL_HEADER_LEN + r->key_len + r->value_len;
    if (bufcap < total) return 0;
    put_u32(buf, 0);
    put_u64(buf + 4, r->lsn);
    put_u64(buf + 12, r->term);
    buf[20] = r->op_type;
    put_u16(buf + 21, r->key_len);
    put_u16(buf + 23, r->value_len);
    memcpy(buf + WAL_HEADER_LEN, r->key, r->key_len);
    memcpy(buf + WAL_HEADER_LEN + r->key_len, r->value, r->value_len);
    uint32_t c = crc32_of(buf + 4, total - 4);
    put_u32(buf, c);
    return total;
}

void wal_parse_header(const uint8_t *hdr, wal_record_t *r) {
    r->lsn = get_u64(hdr + 4);
    r->term = get_u64(hdr + 12);
    r->op_type = hdr[20];
    r->key_len = get_u16(hdr + 21);
    r->value_len = get_u16(hdr + 23);
}

int wal_append(FILE *fp, const wal_record_t *r) {
    uint8_t buf[WAL_HEADER_LEN + 256 + 4096];
    size_t n = wal_serialize(r, buf, sizeof(buf));
    if (n == 0) return -1;
    if (fwrite(buf, 1, n, fp) != n) return -1;
    if (fflush(fp) != 0) return -1;
    if (fsync(fileno(fp)) != 0) return -1;
    return 0;
}

int wal_replay(const char *path, kv_store_t *kv, uint64_t *out_lsn, uint64_t *out_term) {
    (void)path; (void)kv; *out_lsn = 0; *out_term = 0;
    return 0;
}
