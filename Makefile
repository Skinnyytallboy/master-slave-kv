CC      = gcc
CFLAGS  = -std=gnu11 -Wall -Wextra -Wno-unused-parameter -g -O2 -Iinclude -Isrc
LDFLAGS = -pthread
SRC_COMMON = src/common.c src/wal.c src/kvstore.c src/net.c
BIN_DIR = bin

.PHONY: all clean test

all: $(BIN_DIR)/kvdb-cli

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(BIN_DIR)/kvdb-cli: src/client.c | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ src/client.c $(LDFLAGS)

test: $(BIN_DIR)
	$(CC) $(CFLAGS) -o $(BIN_DIR)/test_wal tests/test_wal.c $(SRC_COMMON) $(LDFLAGS)
	./$(BIN_DIR)/test_wal

clean:
	rm -rf $(BIN_DIR)
