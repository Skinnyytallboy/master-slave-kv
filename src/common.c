#include "common.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <arpa/inet.h>

static uint32_t crc_table[256];
static int crc_table_ready = 0;

static void build_crc_table(void) {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++) {
            if (c & 1) c = 0xEDB88320u ^ (c >> 1);
            else c = c >> 1;
        }
        crc_table[i] = c;
    }
    crc_table_ready = 1;
}

uint32_t crc32_of(const uint8_t *data, size_t len) {
    if (!crc_table_ready) build_crc_table();
    uint32_t c = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++) {
        c = crc_table[(c ^ data[i]) & 0xFF] ^ (c >> 8);
    }
    return c ^ 0xFFFFFFFFu;
}

int write_all(int fd, const void *buf, size_t len) {
    const uint8_t *p = buf;
    size_t left = len;
    while (left > 0) {
        ssize_t n = write(fd, p, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        p += n;
        left -= (size_t)n;
    }
    return 0;
}

int read_all(int fd, void *buf, size_t len) {
    uint8_t *p = buf;
    size_t left = len;
    while (left > 0) {
        ssize_t n = read(fd, p, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1; /* peer closed */
        p += n;
        left -= (size_t)n;
    }
    return 0;
}

int send_msg(int fd, uint8_t type, const void *payload, uint32_t len) {
    uint8_t hdr[5];
    hdr[0] = type;
    hdr[1] = (uint8_t)(len & 0xFF);
    hdr[2] = (uint8_t)((len >> 8) & 0xFF);
    hdr[3] = (uint8_t)((len >> 16) & 0xFF);
    hdr[4] = (uint8_t)((len >> 24) & 0xFF);
    if (write_all(fd, hdr, sizeof(hdr)) != 0) return -1;
    if (len > 0) {
        if (write_all(fd, payload, len) != 0) return -1;
    }
    return 0;
}

int recv_msg(int fd, uint8_t *type, uint8_t *buf, uint32_t bufcap, uint32_t *outlen) {
    uint8_t hdr[5];
    if (read_all(fd, hdr, sizeof(hdr)) != 0) return 0;
    uint32_t len = (uint32_t)hdr[1] | ((uint32_t)hdr[2] << 8) |
                   ((uint32_t)hdr[3] << 16) | ((uint32_t)hdr[4] << 24);
    if (len > bufcap) return -1;
    *type = hdr[0];
    *outlen = len;
    if (len > 0) {
        if (read_all(fd, buf, len) != 0) return -1;
    }
    return 1;
}

long long now_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

void logf_ts(const char *prefix, const char *fmt, ...) {
    char timebuf[32];
    time_t t = time(NULL);
    struct tm tmv;
    localtime_r(&t, &tmv);
    strftime(timebuf, sizeof(timebuf), "%H:%M:%S", &tmv);
    fprintf(stderr, "[%s] %s ", timebuf, prefix);
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fprintf(stderr, "\n");
    fflush(stderr);
}
