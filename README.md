# base-ha

Highly available [Hanzo Base](https://github.com/hanzoai/base) cluster.

Quasar consensus is leaderless — all nodes are equal validators. SQLite's
single-writer constraint is satisfied by deterministic **writer-pinning**:
the lowest-sorted alive NodeID holds the write lock. All others are
replicas that 307 mutating requests to the writer and apply change-sets
via async replication.

- Writes (POST/PUT/PATCH/DELETE) reverse-proxy to the pinned writer.
- Reads (GET) honor `txseq` cookies for read-your-writes consistency.
- Replication is async; conflict resolution is last-writer-wins by default.
- One binary. Single `BASE_*` env namespace.

## Quick start

```sh
go install github.com/hanzoai/base-ha@latest

# node 1 (bootstrap)
BASE_NODE_ID=node1 \
BASE_LOCAL_TARGET=http://127.0.0.1:8090 \
BASE_PEERS=http://127.0.0.1:8091,http://127.0.0.1:8092 \
BASE_CONSENSUS=quasar \
BASE_SUPERUSER_EMAIL=admin@example.com \
BASE_SUPERUSER_PASS=change-me \
  base-ha serve --http 127.0.0.1:8090

# node 2
BASE_NODE_ID=node2 \
BASE_LOCAL_TARGET=http://127.0.0.1:8091 \
BASE_PEERS=http://127.0.0.1:8090,http://127.0.0.1:8092 \
BASE_CONSENSUS=quasar \
  base-ha serve --http 127.0.0.1:8091
```

Or use Docker:

```sh
docker compose up
```

## Configuration

### Cluster identity

| Var | Description | Default |
|-----|-------------|---------|
| `BASE_NODE_ID` | Unique node identifier. | `$HOSTNAME` |
| `BASE_LOCAL_TARGET` | This node's reachable URL. | — |
| `BASE_PEERS` | CSV of peer base URLs. | — |

### Writer election

| Var | Values | Description |
|-----|--------|-------------|
| `BASE_CONSENSUS` | `quasar` (default), `pubsub` | `quasar` is the native heartbeat writer-pin. `pubsub` falls back to go-ha's graft dynamic election. |
| `BASE_STATIC_WRITER` | URL | All nodes forward writes to this URL. Overrides election. |

### Replication transport

| Var | Description |
|-----|-------------|
| `BASE_PUBSUB_URL` | External broker URL (`nats://pubsub:4222`). Works with nats-server or hanzoai/pubsub. |
| `BASE_PUBSUB_PORT` | Embedded broker port (enables embedded mode). |
| `BASE_PUBSUB_STORE_DIR` | Embedded broker storage dir. |
| `BASE_PUBSUB_CONFIG` | Broker config file (overrides other pubsub settings). |
| `BASE_REPLICATION_STREAM` | Stream name. Default: `base`. |
| `BASE_REPLICAS` | JetStream replica count. Default: 1. |
| `BASE_ASYNC_PUBLISHER` | Enable async publish with outbox. Recommended with external broker. |
| `BASE_ASYNC_PUBLISHER_DIR` | Outbox directory. |
| `BASE_ROW_IDENTIFY` | `pk` (default), `rowid`, or `full`. |

### Bootstrap

| Var | Description |
|-----|-------------|
| `BASE_SUPERUSER_EMAIL` | Superuser email at startup. |
| `BASE_SUPERUSER_PASS` | Superuser password at startup. |

## Architecture

```
user
 │
 ▼
[replica]  POST/PUT/PATCH/DELETE → 307 → [writer]
[replica]  GET + txseq cookie           [writer or replica with fresh state]
                                             │
                                             │ SQLite change-set replication
                                             ▼
                                         [replicas]
```

Writer election (`BASE_CONSENSUS=quasar`, default):

- Each node heartbeats to peers every 500ms via `/_ha/heartbeat`.
- Alive peers ranked by `NodeID` (lexicographic). Lowest = writer.
- Lease timeout = `3 × heartbeat` (1.5s).
- No RAFT log, no snapshots. All nodes are equal validators.

Fallback (`BASE_CONSENSUS=pubsub`):

- Delegates to go-ha's graft-over-JetStream election.
- Use when you already run a NATS/pubsub cluster.

## Event hooks on replicas

Replicas fire these events on replicated changes:

- `OnModelAfterCreateSuccess`, `OnModelAfterCreateError`
- `OnModelAfterUpdateSuccess`, `OnModelAfterUpdateError`
- `OnModelAfterDeleteSuccess`, `OnModelAfterDeleteError`

`Before*` hooks fire only on the writer.

## Using Hanzo pubsub as the broker

hanzoai/pubsub is wire-compatible with NATS:

```sh
docker run --rm -p 4222:4222 ghcr.io/hanzoai/pubsub:latest -js -sd /data

BASE_PUBSUB_URL=nats://localhost:4222 base-ha serve --http :8090
```

## License

MIT.
