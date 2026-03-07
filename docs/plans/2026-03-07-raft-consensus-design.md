# Raft Consensus Library — Design Document

**Date:** 2026-03-07
**Author:** Anton Dalgren
**Status:** Approved

## Goal

A fast, efficient Raft consensus library for Crystal. The library focuses purely on Raft mechanics, allowing the consuming application to provide any data type for log entries. Designed for disk-first persistence with future `socket.sendfile` zero-copy support.

**Target use cases:** KV store, AMQP broker quorum queues (multi-raft).

## Architecture

### Approach: Layered Core + Transport

The library separates the deterministic Raft protocol core from IO and networking. This makes the core trivially testable and allows the consuming application (e.g. an AMQP broker) to drive the Raft nodes from its own event loop.

```
┌──────────────────────────────────────────────┐
│               Raft::Server(T)                │
│                                              │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐        │
│  │ Node(T) │ │ Node(T) │ │ Node(T) │ ...    │
│  │ group=1 │ │ group=2 │ │ group=3 │        │
│  └────┬────┘ └────┬────┘ └────┬────┘        │
│       │           │           │              │
│  ┌────┴───────────┴───────────┴────┐         │
│  │          Raft::Log(T)           │         │
│  │  data/group-1/segment-*         │         │
│  │  data/group-2/segment-*         │         │
│  │  data/group-3/segment-*         │         │
│  └─────────────────────────────────┘         │
│                                              │
│  ┌─────────────────────────────────┐         │
│  │       Raft::Transport           │         │
│  │  server-2: <--> 1 TCP conn      │         │
│  │  server-3: <--> 1 TCP conn      │         │
│  └─────────────────────────────────┘         │
│                                              │
│  ┌─────────────────────────────────┐         │
│  │     Ticker fiber (e.g. 50ms)    │         │
│  └─────────────────────────────────┘         │
└──────────────────────────────────────────────┘
```

### Components

**`Raft::Node(T)`** — Deterministic Raft protocol core. Receives `tick()` and `step(message)` calls. Produces outbound `Message` structs. Manages state transitions (follower/candidate/leader), election logic, and log commit index advancement. Owns a `Log(T)` and a `StateMachine(T)`. Contains no IO or networking code.

**`Raft::Log(T)`** — Segmented append-only log on disk. Each entry is written via `T#to_io`. Segments rotate by size. Supports: append, read by index, truncate after index, compact up to snapshot index. Backed by MFile (mmap) for zero-copy performance. Files are structured for future `sendfile`.

**`Raft::Transport`** — Abstract class with `send(to, message)` and an incoming message mechanism. POC ships with `TCPTransport`. One TCP connection per server pair, multiplexed by `group_id`. The broker can replace this with its own transport.

**`Raft::StateMachine(T)`** — Abstract class the app implements:
- `apply(entry : T)` — apply a committed entry to the state machine
- `snapshot(io : IO)` — write current state to IO
- `restore(io : IO)` — restore state from IO

Direct virtual dispatch — no channel overhead, no fiber scheduling, no allocation per entry.

**`Raft::Server(T)`** — Multi-raft glue. Manages multiple `Node(T)` instances, a single shared `Transport`, and a ticker fiber. Optional — the broker can skip this and drive nodes directly.

**`Raft::Config`** — Tick intervals, segment size thresholds, election timeout range, heartbeat interval (in ticks), snapshot chunk size, data directory.

### Multi-Raft

Multiple `Node(T)` instances share a single `Transport`. Messages carry a `group_id` field for routing. Each group has its own log directory and `StateMachine(T)` instance. Crystal execution contexts enable true parallelism — each raft group (or pool of groups) can run on its own execution context without locks.

## Data Formats

### Log Entry (on disk, per entry in a segment)

```
┌──────────┬──────────┬──────────┬─────────┬──────────┐
│ term     │ index    │ type     │ size    │ data     │
│ UInt64   │ UInt64   │ UInt8    │ UInt32  │ Bytes    │
│ 8 bytes  │ 8 bytes  │ 1 byte   │ 4 bytes │ variable │
└──────────┴──────────┴──────────┴─────────┴──────────┘
```

- `type` (UInt8): distinguishes normal entries from configuration changes (membership changes)
- `size` (UInt32): length of data payload — enables skipping entries without deserialization and defines byte ranges for `sendfile`

### Segment Files

Named by first index: `00000000000001.log`. Accompanied by a sparse index file (`00000000000001.idx`) mapping entry index to file byte offset for fast seeks. New segment when current exceeds configurable size threshold.

Backed by LavinMQ's MFile implementation for mmap-based IO. Each segment is an MFile with pre-allocated capacity. On rotation, truncate to actual size and open a new segment.

**MFile reference:** https://github.com/cloudamqp/lavinmq/blob/main/src/lavinmq/mfile.cr

### Raft RPC Messages

Messages use Crystal's `to_io`/`from_io` binary serialization. A `protocol_version` field in the header supports rolling upgrades between different node versions.

**POC: single Message struct** with all fields (unused fields zeroed per message type). This keeps the POC simple.

**Future optimization:** Split into typed structs with inheritance for smaller wire size:

```crystal
abstract struct Raft::Message
  # common: protocol_version, group_id, from, term
end

struct Raft::AppendEntries(T) < Raft::Message
  # prev_log_index, prev_log_term, commit_index, entries
end

struct Raft::RequestVote < Raft::Message
  # last_log_index, last_log_term
end
# etc.
```

**Message types:** `AppendEntries`, `AppendEntriesResponse`, `RequestVote`, `RequestVoteResponse`, `InstallSnapshot`, `InstallSnapshotResponse`.

## Tick Model

Logical ticks — the app calls `node.tick()` at a regular interval. The library counts ticks internally:

- **Leader:** every N ticks, emit heartbeat `AppendEntries` messages (empty entries)
- **Follower:** after M ticks with no leader heartbeat, become candidate and start election
- **Candidate:** after M ticks with no election won, start new election with incremented term

The `Server` provides a built-in ticker fiber (`sleep` loop). When embedded in a broker, the broker can drive ticks from its own event loop.

```
Server A (leader)           Server B (follower)         Server C (follower)
────────────────           ─────────────────           ─────────────────
tick() tick() tick()       tick() tick() tick()        tick() tick() tick()
  │                          │                           │
  │ (N ticks elapsed)        │                           │
  ├── heartbeat ────────────>│ (resets election timer)   │
  ├── heartbeat ─────────────────────────────────────────>│ (resets timer)
```

## Snapshots

Application-driven. The library provides the mechanics; the app decides when to snapshot.

- `StateMachine#snapshot(io)` writes state to a file-backed IO. No intermediate buffer.
- `StateMachine#restore(io)` restores from snapshot file.
- Snapshot transfer is chunked — the leader streams chunks to followers, who write directly to disk. Neither side holds the full snapshot in memory.
- Log segments before the snapshot index are deleted after successful snapshot.

Snapshot transfer flow (can be multi-GB for AMQP workloads):

```
Leader                              Follower
──────                              ────────
InstallSnapshot header ───────────>
  chunk 1 (bytes) ────────────────> write to disk
  chunk 2 (bytes) ────────────────> write to disk
  ...
  chunk N (bytes) ────────────────> write to disk
  done ───────────────────────────> restore(io)
  <──── InstallSnapshotResponse ──
```

With `sendfile`, chunks become zero-copy kernel transfers.

## Generic Data Type

`Raft::Node(T)` where `T` must implement `to_io(io : IO)` and `self.from_io(io : IO) : T`. Crystal monomorphizes generics at compile time — zero runtime dispatch or boxing overhead. The generic type only matters at write and apply boundaries; the hot path (shipping log segments for replication) is just bytes.

## File Structure

```
src/
  raft.cr                       # top-level require, module Raft
  raft/
    node.cr                     # Raft::Node(T) — protocol core
    log.cr                      # Raft::Log(T) — segmented disk log
    log/
      segment.cr                # Raft::Log::Segment(T) — single segment
      index.cr                  # Raft::Log::Index — sparse offset index
    state_machine.cr            # Raft::StateMachine(T) — abstract class
    message.cr                  # Raft::Message, MessageType enum
    log_entry.cr                # Raft::LogEntry(T)
    transport.cr                # Raft::Transport — abstract class
    transport/
      tcp_transport.cr          # Raft::TCPTransport
      memory_transport.cr       # Raft::MemoryTransport (for tests)
    server.cr                   # Raft::Server(T) — multi-raft glue + ticker
    config.cr                   # Raft::Config

spec/
  spec_helper.cr
  raft/
    node_spec.cr                # election, replication, commit logic
    log_spec.cr                 # append, read, truncate, rotation
    log/
      segment_spec.cr
    server_spec.cr              # integration tests
    helpers/
      test_state_machine.cr     # simple StateMachine(T) for tests
```

## Testing Strategy

- **Unit tests on `Node`** — create 3-5 nodes in-memory, manually deliver messages, call `tick()` explicitly. Tests: leader election, log replication, commit advancement, follower catch-up, split votes, network partitions. No real sockets or sleeps.
- **Unit tests on `Log`** — write entries, read back, rotate segments, truncate, verify index file correctness. Uses temp directories.
- **`MemoryTransport`** — routes messages between nodes in-process. Can simulate partitions by dropping messages.
- **Integration tests** — `Server` with `TCPTransport` on localhost. Fewer of these, slower.

## Future Optimizations

### Performance
- **MFile (mmap) backend** — swap File IO in Segment for LavinMQ's MFile for zero-copy reads via page cache
- **`socket.sendfile`** — zero-copy replication and snapshot transfer when Crystal adds support
- **Pipeline replication** — send multiple AppendEntries without waiting for responses, optimistically advance `next_index`
- **Batched writes** — coalesce multiple entries into a single `msync` call
- **Read-only queries** — serve reads from followers with read index protocol

### Reliability
- **Snapshot transfer** — implement InstallSnapshot RPC for chunked streaming of snapshots to slow/new followers
- **CRC32 checksums** on log entries for corruption detection
- **Pre-vote protocol** — prevents disruptive elections from partitioned nodes rejoining the cluster

### Protocol
- **Typed message structs** — split `Message` into typed structs with inheritance for smaller wire size
- **Membership changes** — single-server configuration changes (add/remove one node at a time via `EntryType::Configuration` log entries) for dynamic cluster resizing without downtime
