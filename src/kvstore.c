#include "kvstore.h"
#include <stdlib.h>
#include <string.h>

static size_t hash_str(const char *s) {
    /* djb2 */
    size_t h = 5381;
    int c;
    while ((c = (unsigned char)*s++)) h = ((h << 5) + h) + (size_t)c;
    return h;
}

kv_store_t *kv_new(size_t nbuckets) {
    kv_store_t *kv = calloc(1, sizeof(kv_store_t));
    kv->nbuckets = nbuckets ? nbuckets : 1024;
    kv->buckets = calloc(kv->nbuckets, sizeof(kv_entry_t *));
    kv->count = 0;
    return kv;
}

void kv_clear(kv_store_t *kv) {
    for (size_t i = 0; i < kv->nbuckets; i++) {
        kv_entry_t *e = kv->buckets[i];
        while (e) {
            kv_entry_t *nx = e->next;
            free(e->key);
            free(e->value);
            free(e);
            e = nx;
        }
        kv->buckets[i] = NULL;
    }
    kv->count = 0;
}

void kv_free(kv_store_t *kv) {
    if (!kv) return;
    kv_clear(kv);
    free(kv->buckets);
    free(kv);
}

void kv_put(kv_store_t *kv, const char *key, const char *value) {
    size_t idx = hash_str(key) % kv->nbuckets;
    kv_entry_t *e = kv->buckets[idx];
    while (e) {
        if (strcmp(e->key, key) == 0) {
            free(e->value);
            e->value = strdup(value);
            return;
        }
        e = e->next;
    }
    kv_entry_t *ne = malloc(sizeof(kv_entry_t));
    ne->key = strdup(key);
    ne->value = strdup(value);
    ne->next = kv->buckets[idx];
    kv->buckets[idx] = ne;
    kv->count++;
}

void kv_delete(kv_store_t *kv, const char *key) {
    size_t idx = hash_str(key) % kv->nbuckets;
    kv_entry_t *e = kv->buckets[idx];
    kv_entry_t *prev = NULL;
    while (e) {
        if (strcmp(e->key, key) == 0) {
            if (prev) prev->next = e->next;
            else kv->buckets[idx] = e->next;
            free(e->key);
            free(e->value);
            free(e);
            kv->count--;
            return;
        }
        prev = e;
        e = e->next;
    }
}

const char *kv_get(kv_store_t *kv, const char *key) {
    size_t idx = hash_str(key) % kv->nbuckets;
    kv_entry_t *e = kv->buckets[idx];
    while (e) {
        if (strcmp(e->key, key) == 0) return e->value;
        e = e->next;
    }
    return NULL;
}

void kv_foreach(kv_store_t *kv, kv_iter_fn fn, void *ctx) {
    for (size_t i = 0; i < kv->nbuckets; i++) {
        for (kv_entry_t *e = kv->buckets[i]; e; e = e->next) {
            fn(e->key, e->value, ctx);
        }
    }
}
