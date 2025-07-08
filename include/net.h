#ifndef KVDB_NET_H
#define KVDB_NET_H

/* returns a listening socket bound to 0.0.0.0:port, or -1 on failure */
int net_listen(int port);

/* blocking connect with a short timeout; returns fd or -1 */
int net_connect(const char *host, int port);

#endif
