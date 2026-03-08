# Raft Demo Setup — Design Document

**Date:** 2026-03-08
**Author:** Anton Dalgren
**Status:** Approved

## Goal

Build a demo and development environment for the Raft library: Docker Compose cluster running a KV store, HTTP admin API, Prometheus metrics, and a TUI dashboard with chaos controls. The tooling (HTTP handler, metrics, TUI) is part of the library, reusable by any application.

## Architecture

```
┌─────────────────────────────────────────────────┐
│               Raft Library (src/raft/)            │
│                                                   │
│  Core:  Node(T), Log(T), Transport, Server(T)    │
│                                                   │
│  Raft::HTTP::Handler                              │
│    /raft/status, /raft/log, /raft/metrics         │
│    /raft/admin/pause, resume, partition, heal     │
│                                                   │
│  Raft::Metrics                                    │
│    counters, gauges, histograms                   │
│    prometheus text format export                  │
│                                                   │
│  Raft::TUI                                        │
│    connects to any Raft cluster via HTTP          │
│    cluster dashboard + chaos controls             │
│    knows nothing about T (app data)               │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│          Demo App (examples/kv/)                  │
│                                                   │
│  KVCommand, KVStateMachine                        │
│  HTTP handlers: GET/PUT/DELETE /kv/:key           │
│  main.cr — wires it all together                  │
│  Dockerfile, docker-compose.yml                   │
└─────────────────────────────────────────────────┘
```

## Library Components

### Raft::HTTP::Handler

A Crystal HTTP handler that any app mounts alongside its own routes. Generic — knows nothing about T.

**Routes:**

```
GET  /raft/status
  → { "id": 1, "role": "leader", "term": 5, "leader_id": 1,
      "commit_index": 42, "last_log_index": 43, "peers": [2, 3] }

GET  /raft/log
  → { "last_index": 43, "last_term": 5, "segment_count": 3,
      "commit_index": 42 }

GET  /raft/metrics
  → Prometheus text format

POST /raft/admin/pause
  → Stops ticking the node (simulates crash without killing process)

POST /raft/admin/resume
  → Resumes ticking

POST /raft/admin/partition
  → Drops all inbound/outbound messages (simulates network partition)

POST /raft/admin/heal
  → Restores connectivity
```

Non-leaders return errors with leader address on write operations. The handler wraps a `Raft::Node(T)` and exposes only Raft-level state.

### Raft::Metrics

Collects Raft-specific metrics. Node increments counters at appropriate points. No external dependencies — formats Prometheus text output directly.

**Gauges:**
- `raft_node_role` (0=follower, 1=candidate, 2=leader)
- `raft_node_term`
- `raft_node_commit_index`
- `raft_node_last_log_index`
- `raft_node_peers`

**Counters:**
- `raft_elections_total`
- `raft_entries_applied_total`
- `raft_heartbeats_sent_total`
- `raft_heartbeats_received_total`
- `raft_messages_sent_total`
- `raft_messages_received_total`

**Histograms:**
- `raft_append_latency_seconds` — time to append an entry to the log
- `raft_replication_latency_seconds{peer="N"}` — time from send to ack per peer

### Raft::TUI

Standalone binary. Given a list of node HTTP addresses, shows a live dashboard. Works with any Raft-based app.

```
┌─ Raft Cluster Dashboard ─────────────────────────────────────┐
│                                                               │
│  Node 1 (127.0.0.1:8001)    ██ LEADER     term: 5            │
│  commit: 42  log: 43  peers: [2,3]  heartbeats: 124          │
│                                                               │
│  Node 2 (127.0.0.1:8002)    ░░ FOLLOWER   term: 5            │
│  commit: 42  log: 43  leader: 1      last heartbeat: 50ms    │
│                                                               │
│  Node 3 (127.0.0.1:8003)    ░░ FOLLOWER   term: 5            │
│  commit: 42  log: 43  leader: 1      last heartbeat: 50ms    │
│                                                               │
├─ Event Log ──────────────────────────────────────────────────┤
│  10:42:01  Node 1 elected leader (term 5)                     │
│  10:42:00  Node 1 started election                            │
│  10:41:58  Node 3 lost leader                                 │
│                                                               │
├─ Commands ───────────────────────────────────────────────────┤
│  > _                                                          │
│                                                               │
│  [F1] Pause node  [F2] Resume node  [F3] Partition node       │
│  [F4] Heal node   [F5] Heal all     [F6] Kill leader          │
│  [q] Quit                                                     │
└───────────────────────────────────────────────────────────────┘
```

- Polls `GET /raft/status` on all nodes every 500ms
- Detects state changes and logs events with timestamps
- F-keys prompt for node selection, then call admin endpoints
- F6 finds current leader and pauses it
- Raw ANSI terminal control — no heavy TUI framework

**Future improvement:** Consider a full TUI framework (Tallboy or similar) for richer interaction.

## Demo Application: KV Store

### KVCommand

```crystal
enum KVAction : UInt8
  Put    = 0
  Delete = 1
end

struct KVCommand
  getter action : KVAction
  getter key : String
  getter value : String  # empty for delete

  # to_io / from_io for Raft serialization
end
```

### KVStateMachine

```crystal
class KVStateMachine < Raft::StateMachine(KVCommand)
  @store : Hash(String, String) = {} of String => String

  def apply(entry : KVCommand)
    case entry.action
    when KVAction::Put    then @store[entry.key] = entry.value
    when KVAction::Delete then @store.delete(entry.key)
    end
  end

  def get(key : String) : String?
    @store[key]?
  end

  def snapshot(io : IO)  # serialize @store
  def restore(io : IO)   # deserialize @store
end
```

### KV HTTP Routes

```
PUT    /kv/:key   body: "value" → propose Put, return after commit
GET    /kv/:key   → read from state machine
DELETE /kv/:key   → propose Delete, return after commit
```

Writes only accepted by leader. Non-leaders return 307 redirect to leader.

## Docker Compose

```yaml
services:
  node-1:
    build: .
    environment:
      NODE_ID: 1
      PEERS: "node-2:9000,node-3:9000"
      HTTP_PORT: 8001
      RAFT_PORT: 9000
    ports:
      - "8001:8001"

  node-2:
    build: .
    environment:
      NODE_ID: 2
      PEERS: "node-1:9000,node-3:9000"
      HTTP_PORT: 8002
      RAFT_PORT: 9000
    ports:
      - "8002:8002"

  node-3:
    build: .
    environment:
      NODE_ID: 3
      PEERS: "node-1:9000,node-2:9000"
      HTTP_PORT: 8003
      RAFT_PORT: 9000
    ports:
      - "8003:8003"

  prometheus:
    image: prom/prometheus
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"
```

Internal Raft port is uniform (9000), HTTP ports are 8001-8003. Multi-stage Dockerfile: build with Crystal, run as minimal image.

## File Structure

Library additions:
```
src/raft/
  http/
    handler.cr            # Raft::HTTP::Handler
  metrics.cr              # Raft::Metrics
  tui.cr                  # Raft::TUI
```

Demo app:
```
examples/
  kv/
    src/
      main.cr              # wires KVStateMachine + HTTP + Raft
      kv_command.cr         # KVCommand struct
      kv_state_machine.cr   # KVStateMachine
      kv_http_handler.cr    # /kv/:key routes
    Dockerfile
    docker-compose.yml
    prometheus.yml
```

## Future Improvements

- Full TUI framework (Tallboy or similar) for richer widgets and interaction
- Grafana dashboard JSON alongside prometheus.yml
- docker compose profiles for 5-node cluster
