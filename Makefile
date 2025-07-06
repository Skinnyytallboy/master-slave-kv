CC      = gcc
CFLAGS  = -std=gnu11 -Wall -Wextra -Wno-unused-parameter -g -O2 -Iinclude -Isrc
LDFLAGS = -pthread
BIN_DIR = bin

.PHONY: all clean

all: $(BIN_DIR)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

clean:
	rm -rf $(BIN_DIR)
