# Redis Clone (Go)

A compact Redis-compatible server implemented in Go. It supports core string/list/stream/zset/geo operations, basic ACL/auth, persistence via RDB loading, transactions, and a replication subset (INFO/PSYNC/REPLCONF/WAIT).

## Features

- RESP protocol support (arrays, bulk strings, integers, errors)
- String commands: PING, ECHO, SET (EX/PX), GET, INCR
- Lists: LPUSH, RPUSH, LRANGE, LPOP, LLEN, BLPOP
- Streams: XADD, XRANGE, XREAD (blocking)
- Sorted sets: ZADD, ZRANGE, ZCARD, ZREM, ZRANK, ZSCORE
- Geo: GEOADD, GEODIST, GEOPOS, GEOHASH, GEOSEARCH
- Pub/Sub: SUBSCRIBE, PUBLISH, UNSUBSCRIBE
- Transactions: MULTI, EXEC, DISCARD (with queued execution)
- ACL/Auth: AUTH, ACL subcommands
- Replication (subset): INFO replication, REPLCONF, PSYNC (FULLRESYNC), WAIT
- RDB load on startup with expiry handling

## Quick start

Build and run the server locally:

```bash
./your_program.sh
```

Run on a different port:

```bash
./your_program.sh --port 6380
```

Run as a replica (connects to master and starts syncing):

```bash
./your_program.sh --port 6380 --replicaof "localhost 6379"
```

## Flags

- `--dir` and `--dbfilename`: load an RDB file on startup
- `--requirepass`: set a password for AUTH
- `--replicaof "host port"`: start as replica and connect to master
- `--port`: listen port for the server (default 6379)

## Project structure

- app/
  - main.go: CLI flags and server startup
- internal/server/
  - server.go: TCP listener, client read/write loops
  - replication_client.go: replica handshake and replication stream apply
- internal/handler/
  - dispatcher.go: command dispatch, auth checks, transactions, replication propagation
  - *command files*: command implementations grouped by data type
- internal/store/
  - in-memory data structures and operations
- internal/resp/
  - RESP read/write + Encode helper
- internal/rdb/
  - RDB loader for string keys + expiries
- internal/replication/
  - replication state, offsets, ack tracking, command encoding

## Architecture overview

1. **Server loop** accepts TCP connections and creates a per-connection message channel.
2. **RESP reader** parses incoming requests into command + args.
3. **Dispatcher** enforces auth and pub/sub rules, then routes to a handler.
4. **Handlers** implement command logic using the in-memory store.
5. **Replication (master)** propagates write commands to replicas and tracks offsets/ACKs.
6. **Replication (replica)** performs handshake, receives FULLRESYNC RDB, then applies streamed commands.
7. **Background expiration** periodically cleans expired keys.

## Transactions

- `MULTI` starts a transaction and queues subsequent commands.
- `EXEC` executes queued commands and returns an array of responses.
- `DISCARD` clears the queue.
- `EXEC` returns an error if the transaction is marked dirty (e.g., unknown command queued).

## Replication details (subset)

- `INFO replication` exposes role, replid, offsets, and connected replicas.
- `PSYNC ? -1` triggers FULLRESYNC with an empty RDB payload.
- `REPLCONF` handles `listening-port`, `capa`, and `ACK` messages.
- `WAIT` blocks until enough replicas have ACKed a target offset (or timeout).

## Persistence (RDB load)

On startup, if `--dir` and `--dbfilename` are provided, the server loads a Redis RDB file containing string keys and expiries. Expired keys are dropped during load.

## Development notes

- Go version: 1.24 (as used by CodeCrafters)
- Tests are not included; use `codecrafters test` or the challenge runner.

## Limitations

This is a learning-focused implementation and does not cover the full Redis command set or replication protocol. Only the parts required by the challenge stages are implemented.
