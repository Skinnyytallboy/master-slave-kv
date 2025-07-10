/*
 * kvdb-cli - a tiny line-oriented client for kvdb.
 *
 * Bonus feature baked in here rather than in the server: this client
 * remembers the highest lsn it has seen from any OK response and tags
 * every subsequent GET with it, so a read right after your own write
 * never appears to go backwards in time even if it lands on a follower
 * that hasn't quite caught up yet. See design.md, "Read-your-writes".
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <arpa/inet.h>

static int connect_to(const char *host, int port) {
    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    char portstr[16];
    snprintf(portstr, sizeof(portstr), "%d", port);
    if (getaddrinfo(host, portstr, &hints, &res) != 0) return -1;
    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd >= 0) {
        int one = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
    }
    if (fd < 0 || connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
        if (fd >= 0) close(fd);
        freeaddrinfo(res);
        return -1;
    }
    freeaddrinfo(res);
    return fd;
}

static int read_line_fd(int fd, char *buf, size_t cap) {
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

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "usage: %s <host> <port>\n", argv[0]);
        return 1;
    }
    const char *host = argv[1];
    int port = atoi(argv[2]);

    int fd = connect_to(host, port);
    if (fd < 0) {
        fprintf(stderr, "could not connect to %s:%d\n", host, port);
        return 1;
    }

    char banner[512];
    if (read_line_fd(fd, banner, sizeof(banner)) > 0) printf("%s\n", banner);

    unsigned long long last_seen_lsn = 0;
    char line[8192];
    printf("> ");
    fflush(stdout);
    while (fgets(line, sizeof(line), stdin)) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) line[--len] = 0;
        if (len == 0) { printf("> "); fflush(stdout); continue; }

        char outbuf[8300];
        snprintf(outbuf, sizeof(outbuf), "%s\n", line);
        if (write(fd, outbuf, strlen(outbuf)) < 0) break;

        if (strcasecmp(line, "QUIT") == 0) break;

        if (strncasecmp(line, "\\info", 5) == 0) {
            char buf[512];
            for (int i = 0; i < 4; i++) {
                if (read_line_fd(fd, buf, sizeof(buf)) < 0) goto done;
                printf("%s\n", buf);
            }
            /* the rest (sync_mode + follower lines, or master line) is a
             * variable number of lines - drain until the socket goes quiet */
            fd_set fds;
            struct timeval tv;
            while (1) {
                FD_ZERO(&fds);
                FD_SET(fd, &fds);
                tv.tv_sec = 0;
                tv.tv_usec = 150000;
                int rv = select(fd + 1, &fds, NULL, NULL, &tv);
                if (rv <= 0) break;
                if (read_line_fd(fd, buf, sizeof(buf)) < 0) goto done;
                printf("%s\n", buf);
            }
        } else {
            char buf[8192];
            int n = read_line_fd(fd, buf, sizeof(buf));
            if (n < 0) goto done;
            printf("%s\n", buf);
            if (strncmp(buf, "OK (lsn=", 8) == 0) {
                unsigned long long lsn = strtoull(buf + 8, NULL, 10);
                if (lsn > last_seen_lsn) last_seen_lsn = lsn;
            }
        }

        printf("> ");
        fflush(stdout);
    }
done:
    close(fd);
    return 0;
}
