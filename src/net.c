#include "net.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include <errno.h>

int net_listen(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons((uint16_t)port);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        int err = errno;
        if (err == EADDRINUSE) {
            /* If wildcard bind conflicts with a lingering loopback connection in TIME_WAIT,
             * retry binding specifically to the loopback interface. */
            close(fd);
            fd = socket(AF_INET, SOCK_STREAM, 0);
            if (fd < 0) return -1;
            setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
            setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
            addr.sin_addr.s_addr = inet_addr("127.0.0.1");
            if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
                close(fd);
                return -1;
            }
        } else {
            close(fd);
            return -1;
        }
    }
    if (listen(fd, 32) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

int net_connect(const char *host, int port) {
    struct addrinfo hints, *res = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    char portstr[16];
    snprintf(portstr, sizeof(portstr), "%d", port);

    if (getaddrinfo(host, portstr, &hints, &res) != 0 || !res) return -1;

    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) {
        freeaddrinfo(res);
        return -1;
    }

    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));

    if (connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
        close(fd);
        freeaddrinfo(res);
        return -1;
    }
    freeaddrinfo(res);
    return fd;
}
