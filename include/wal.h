#ifndef KVDB_WAL_H
#define KVDB_WAL_H

#include <stdint.h>
#include <stdio.h>
#include "kvstore.h"

#define OP_PUT    1
#define OP_DELETE 2

/* on-disk / on-wire record layout, little-endian throughout:
 *   offset  size  field
 *   0       4     crc32 (of everything from offset 4 onward)
 *   4       8     lsn
 *   12      8     term
 *   20      1     op_type
 *   21      2     key_len
 *   23      2     value_len
 *   25      ..    key bytes
 *   ..      ..    value bytes
 */
typedef struct {
    uint64_t lsn;
    uint64_t term;
    uint8_t op_type;
    char key[256];
    char value[4096];
    uint16_t key_len;
    uint16_t value_len;
} wal_record_t;

#define WAL_HEADER_LEN 25

size_t wal_serialize(const wal_record_t *r, uint8_t *buf, size_t bufcap);
void wal_parse_header(const uint8_t *hdr, wal_record_t *r);
int wal_append(FILE *fp, const wal_record_t *r);

/* Replays valid WAL records into the kv store and reports max lsn/term. */
int wal_replay(const char *path, kv_store_t *kv, uint64_t *out_lsn, uint64_t *out_term);

#endif
