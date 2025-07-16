CC      = gcc
CFLAGS  = -std=gnu11 -Wall -Wextra -Wno-unused-parameter -g -O2 -Iinclude -Isrc
LDFLAGS = -pthread

SRC_COMMON = src/common.c src/wal.c src/kvstore.c src/net.c src/election.c
BIN_DIR = bin

.PHONY: all clean test

all: $(BIN_DIR)/kvdb $(BIN_DIR)/kvdb-cli

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(BIN_DIR)/kvdb: src/server.c $(SRC_COMMON) | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ src/server.c $(SRC_COMMON) $(LDFLAGS)

$(BIN_DIR)/kvdb-cli: src/client.c | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ src/client.c $(LDFLAGS)

test: $(BIN_DIR)
	$(CC) $(CFLAGS) -o $(BIN_DIR)/test_wal tests/test_wal.c $(SRC_COMMON) $(LDFLAGS)
	$(CC) $(CFLAGS) -o $(BIN_DIR)/test_election tests/test_election.c $(SRC_COMMON) $(LDFLAGS)
	./$(BIN_DIR)/test_wal
	./$(BIN_DIR)/test_election
	$(MAKE) all
	./tests/test_two_node_replication.sh

clean:
	rm -rf $(BIN_DIR)
