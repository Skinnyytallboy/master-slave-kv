#ifndef KVDB_KVSTORE_H
#define KVDB_KVSTORE_H

#include <stddef.h>

/* Hash-table key-value store. External locking is required for thread safety. */

typedef struct kv_entry {
    char *key;
    char *value;
    struct kv_entry *next;
} kv_entry_t;

typedef struct {
    kv_entry_t **buckets;
    size_t nbuckets;
    size_t count;
} kv_store_t;

kv_store_t *kv_new(size_t nbuckets);
void kv_free(kv_store_t *kv);

void kv_put(kv_store_t *kv, const char *key, const char *value);
void kv_delete(kv_store_t *kv, const char *key);
const char *kv_get(kv_store_t *kv, const char *key);

typedef void (*kv_iter_fn)(const char *key, const char *value, void *ctx);
void kv_foreach(kv_store_t *kv, kv_iter_fn fn, void *ctx);

void kv_clear(kv_store_t *kv);

#endif
