#ifndef KVDB_COMMON_H
#define KVDB_COMMON_H

#include <stdint.h>
#include <stddef.h>

/* wire message types exchanged between nodes on the repl port */
#define MSG_HELLO          1
#define MSG_HELLO_REPLY    2
#define MSG_WAL_RECORD     3
#define MSG_HEARTBEAT      4
#define MSG_ACK            5
#define MSG_VOTE_REQUEST   6
#define MSG_VOTE_RESPONSE  7
#define MSG_SNAPSHOT_BEGIN 8

#define MAX_MSG_PAYLOAD 65536

uint32_t crc32_of(const uint8_t *data, size_t len);

int write_all(int fd, const void *buf, size_t len);
int read_all(int fd, void *buf, size_t len);

/* one framed message = [1 byte type][4 byte LE length][payload] */
int send_msg(int fd, uint8_t type, const void *payload, uint32_t len);
/* returns 1 on success, 0 on clean EOF, -1 on error/oversize */
int recv_msg(int fd, uint8_t *type, uint8_t *buf, uint32_t bufcap, uint32_t *outlen);

long long now_ms(void);

void logf_ts(const char *prefix, const char *fmt, ...);

#endif
