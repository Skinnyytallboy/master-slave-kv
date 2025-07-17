# Minimal build for the kvdb replication engine. Multi-stage so the final
# image doesn't need gcc/make sitting around.
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends gcc make && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Makefile ./
COPY src ./src
COPY include ./include
RUN make

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=build /src/bin/kvdb /src/bin/kvdb-cli ./
ENTRYPOINT ["./kvdb"]
