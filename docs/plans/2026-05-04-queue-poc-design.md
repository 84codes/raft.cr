# Queue PoC — Design

**Date:** 2026-05-04
**Status:** Draft
**Tracks:** [Issue #2 — Queue PoC](https://github.com/84codes/raft.cr/issues/2)

## 1. Scope

A new `examples/queue/` directory mirroring the structure of `examples/kv/`. A multi-raft demo where each queue is its own Raft group, a meta group routes queue-name → group-id, and a small HTTP API supports publish + one-shot consume.

The primary goal is to **surface the unbounded log problem** described in `ARCHITECTURE.md` §6.5: a queue that publishes and drains messages should make it visible that the on-disk log grows without bound even though the in-memory state is empty. Compaction itself is **not in scope** — it is a separate, larger workstream that this PoC is intended to motivate, and will be its own design pass once the pain is concrete.

### Out of scope

- Snapshots / log compaction (this PoC is the forcing function; the work is separate).
- Ack / nack / redelivery (state-machine bookkeeping; can be layered on without core changes).
- Multiple competing consumers (also state-machine layer).
- TTL / dead-letter queues / consumer prefetch / publisher confirms (AMQP-flavored features that don't carry their weight for a learning PoC).
- AMQP wire protocol (the issue explicitly excludes this — HTTP only).
- State-machine "effects" mechanism in the core library (a real production AMQP integration would benefit from this; it's a Raft-library design question of its own and worth a separate brainstorming session).

## 2. State machines

### `MetaStateMachine` (group 0)

Holds `Hash(QueueName, GroupID)`. Mirrors the KV example's `MetaStateMachine` directly.

Commands:

- `CreateQueue(name)` — assigns the next group_id and fires a callback that creates the new value group locally on this node.
- `DeleteQueue(name)` — removes the mapping and fires a callback that tears down the value group.

Auto-create on first publish: the HTTP layer checks the local meta state machine; if the queue doesn't exist, it proposes `CreateQueue` first, then proceeds to publish. This matches KV's auto-create-on-PUT behavior.

### `QueueStateMachine` (one per queue group)

Holds an in-memory `Deque(Bytes)` of message bodies. Plus a `Hash(RequestID, Channel(Bytes?))` used to deliver consume results back to waiting HTTP handlers (see §4).

Commands:

- `Publish(body : Bytes)` — push to the tail of the deque.
- `Consume(req_id : UUID)` — pop from the head; if the deque was empty, the popped value is `nil`. Either way, look up `req_id` in the request-channel hash and send the result.

FIFO is preserved by Raft's log order: every Publish/Consume is a single log entry, and `apply` on every node runs in commit order.

## 3. Why this surfaces the compaction problem

After N publishes and N consumes, the queue is empty — but the Raft log holds 2N committed entries. None of them can ever be needed again (the state machine forgot the message bodies the moment they were consumed), but the log keeps them on disk forever. Restart a node and it replays all 2N entries to rebuild empty state.

This is exactly the unbounded-log problem `ARCHITECTURE.md` §6.5 calls out. The demo TUI should make it visible — for example, by showing per-group log-size on disk vs. queue depth in memory. After publishing and draining ~1 GB of message bodies, you should see 0 in-memory state and ~2 GB of log on disk (publish entries plus consume entries).

## 4. The consume-return-value problem

`Node#propose(data)` returns immediately after appending to the leader's log; it does not wait for commit, and the data flows through `apply` on every replica without a return path. So how does a `GET /queues/foo/messages` HTTP request get back the message its consume entry produced?

For KV, this isn't a problem — `GET` reads local state directly (eventually consistent, best-effort). For a queue, that doesn't work: local-read after consume could return a message that another node has also given to a different consumer.

### The bridge

The HTTP handler and the state machine talk to each other through a shared in-memory map keyed by a unique request ID:

1. HTTP handler generates a UUID — `req_id`.
2. Handler creates a `Channel(Bytes?)` (size 1) and stores `req_id → channel` in the queue's request map.
3. Handler proposes `Consume(req_id)` to the local Raft node. The `req_id` is part of the log entry.
4. Handler blocks waiting on its channel, with a deadline (e.g. 5 seconds).
5. Sometime later, `apply(Consume(req_id))` runs on every replica:
   - The state machine pops the head (or yields `nil` if empty).
   - It looks up `req_id` in the local request map. On the leader's HTTP-receiving node, the channel is there → send the popped value. On other nodes, the map has no entry → silently drop. (Followers still apply the pop correctly; they just don't have anyone waiting for the result.)
6. The handler wakes up, receives the value (or `nil` for an empty queue), returns 200 with the body or 204.
7. If the deadline expires (leader crashed, partition, etc.), the handler returns 503 and the client retries. The orphan request-map entry is removed on timeout.

### Leader proxy

If the HTTP request hits a non-leader node, the existing leader-proxy logic from KV forwards the request to the leader. The leader handles the propose-and-wait, returns the response, the proxy returns it to the client. The originating leader is also the node where the request map entry exists — so the bridge works without any cross-node request-map coordination.

### Why this works (and what it doesn't do)

- The HTTP handler and the state machine are in the same process on the leader.
- The shared map is just an in-memory `Hash`. It's lookup-by-id, no Raft involvement.
- Followers also run `apply` and pop their local state machine in step with the leader (this is what keeps the replicas consistent). They look up the `req_id` in their map, find nothing, and discard. The pop still happened — that's the point.
- This is a **degenerate special case** of the "state-machine effects" pattern that production AMQP brokers use (see Out of scope above). The current single-effect, synchronous shape of the consume operation makes the simpler in-memory map appropriate. Generalizing to a real effects mechanism is a future Raft-library question.

## 5. HTTP API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/queues/{name}` | Publish — body is the message bytes (`application/octet-stream`). 201 on success. Auto-creates the queue if missing. |
| `GET` | `/queues/{name}/messages` | Consume one — returns the message body and 200, or 204 if the queue is empty. |
| `GET` | `/queues` | List queue names + current depth (read from local meta state machine; eventually consistent). |
| `DELETE` | `/queues/{name}` | Delete the queue and its Raft group. |
| `GET` | `/queues/{name}/events` | SSE stream of state changes for the UI (publish/consume/depth). For *display*, not for consume. |

Mutating requests on a non-leader node are forwarded to the leader using the existing leader-proxy logic from KV. Reads of `/queues` come from local state.

## 6. File layout

```
examples/queue/
├── docker-compose.yml          # 3-node cluster + Prometheus + Grafana
├── Dockerfile
├── prometheus.yml
├── grafana/                    # dashboards (queue depth per group, log size per group)
├── spec/                       # integration tests
└── src/
    ├── main.cr                 # wires up transport, meta + queue groups
    ├── queue_command.cr        # the T type — Publish(body) | Consume(req_id)
    ├── meta_state_machine.cr   # queue_name → group_id mapping
    ├── queue_state_machine.cr  # one queue's Deque + req_id → channel hash
    └── queue_http_handler.cr   # publish, consume, list, delete, SSE
```

Wiring follows `examples/kv/src/main.cr` almost identically — same meta-group + per-thing-group pattern, same `on_configuration_applied` and `on_configuration_change` callbacks for transport peer registration and data-group reconciliation.

Where the wiring is genuinely identical (not just similar) between KV and queue, the duplication is left in place for now. Each example stays self-contained and readable; extracting a "multi-raft example helper" library would be premature for a PoC and easy to revisit if a third example shows up.

## 7. TUI / dashboard integration

Two layers:

1. **Reuse the existing TUI** (`src/raft/tui/dashboard.cr`). It talks to the library's HTTP admin endpoints (`src/raft/http/handler.cr`) and shows cluster-level state (peers, leadership, group list, log indices). The queue example exposes those same admin endpoints, so the TUI works as-is for cluster inspection.

2. **Add a queue-aware HTML UI** served by `queue_http_handler.cr`, similar to KV's live UI. Shows current queues, their depth, log size on disk, and recent operations. Driven by the SSE endpoint from §5. This is the visual story for the demo: side-by-side "queue depth (in memory)" vs "log size (on disk)" so the compaction need is obvious to anyone watching.

## 8. Testing

Following the KV example's pattern:

- **Unit tests** in `examples/queue/spec/` exercising the state machines deterministically (no transport, no HTTP): publish, consume, FIFO ordering, empty-queue behavior, queue creation/deletion, request-id delivery on the originating node and silent drop on followers.
- **Integration tests** spinning up a 3-node in-memory cluster (using `Raft::MemoryTransport`) and exercising the full path: HTTP publish on one node, consume on another (via leader proxy), node failure during consume (deadline expiry), queue persistence across restarts.
- **Demo load script** publishing N messages then consuming N — used to manually verify the log-vs-depth divergence visually in the TUI/UI.

Coverage targets are not formal. The goal is "we trust the demo works on a 3-node cluster under typical operations and at least one fault scenario."

## 9. References

- `ARCHITECTURE.md` — overall library architecture; §6.5 for the compaction gap context.
- `examples/kv/` — the structural template this PoC mirrors.
- [Issue #2](https://github.com/84codes/raft.cr/issues/2) — the original PoC request.
