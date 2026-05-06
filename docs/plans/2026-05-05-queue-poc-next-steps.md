# Queue PoC — Next Steps

> Captures the architectural direction and build order chosen on 2026-05-05 after surveying RedPanda, NATS JetStream, RabbitMQ quorum queues (Ra), OpenRaft, and RobustMQ, and benchmarking research from 2026-05-06.

## Decision

**Model A — bodies live inside Raft log entries** (RedPanda / RabbitMQ Ra pattern). Each queue is its own Raft group; that group's Raft log segments *are* its message store. Bodies are written exactly once per replica. The state machine never re-stores bodies — it tracks lifecycle state (ready / unacked / acked) by Raft index. Snapshots serialize that state; log compaction truncates segments below the smallest live index.

Adopting AMQP-style **deliver + ack semantics** rather than the current auto-ack consume — empty `basic.get` then becomes naturally non-destructive and produces no log entry, and we get at-least-once delivery + redelivery on consumer crash for free.

## Architectural comparison (notes for future reference)

The design space splits on **where message bodies physically live**:

| Dimension | A — bodies in Raft log | B — double-write (Raft + segment) | C — bodies in segment files only, Raft entries are references |
|---|---|---|---|
| Disk writes per body per node | **1** | 2 | **1** |
| fsyncs per batch | 1 | 2 | ≈1 |
| Network bytes per body | 2 (replicate body) | 2 | 2 |
| FSM memory at depth N | O(N) worst case; **O(1) when ready is a contiguous range** (typical FIFO) | O(N) | **O(1)** — head/tail pointers |
| Compaction model | Live-index (Ra-style): drop dead segments, rewrite sparse ones | Drop blocks + Raft snapshot | **Drop fully-acked segments (LavinMQ-style)** |
| Snapshot transfer | Snapshot = SM live-index state; bodies recovered from log | Snapshot + filestore copy | Snapshot + segment file copy |
| Implementation effort | Lowest (1 storage path) | Medium | Highest (out-of-band body-shipping protocol) |
| io_uring fit | Good (sequential log appends) | Mixed (two write streams compete) | Best (large sequential body appends + tiny Raft entries on separate paths) |
| Reference systems | RedPanda, Ra (RabbitMQ quorum queues) | NATS JetStream | RobustMQ; LavinMQ if you bolted Raft onto it |

**B is dominated** — JetStream pays for the double-write in benchmarks (see below). Don't go there.

**A vs C is not a perf decision**; it's a complexity decision. A is the faster waypoint with a clean upgrade path to C if profiling later forces it (e.g., very large persistent backlogs with frequent requeues fragmenting the ready set).

## Throughput anchors (2026-05-06 research)

| System | Replication | Throughput (small msgs) | Bottleneck | fsync strategy |
|---|---|---|---|---|
| LavinMQ | None | ~1M msg/s (16-byte msgs) | CPU/serialization | Periodic (OS page-cache flush) |
| RabbitMQ quorum | 3-node Raft | ~30k msg/s (1 KB) | fsync rate | Per Raft batch (Ra shared WAL) |
| RedPanda | 3-node Raft | 50k–1M msg/s | fsync rate at high partition count | Per batch, debounced |
| NATS JetStream R=3 | 3-node Raft | 100–250k msg/s | fsync per Raft batch + meta contention | Per batch, sync default |

**Key takeaway:** the 30× drop from LavinMQ to quorum is the cost of Raft + synchronous quorum fsync. That ceiling is fundamental and applies to every model equally — model choice only affects throughput within that band, not the band itself. RedPanda and Ra (both Model A) prove the band's upper edge is achievable.

Sources: [LavinMQ benchmark](https://lavinmq.com/benchmark), [RabbitMQ migration blog](https://www.rabbitmq.com/blog/2023/03/02/quorum-queues-migration), [Vanlightly RedPanda analysis](https://jack-vanlightly.com/blog/2023/5/15/kafka-vs-redpanda-performance-do-the-claims-add-up), [JetStream benchmarks GH#7599](https://github.com/nats-io/nats-server/discussions/7599), [Najafizadeh JetStream R=3 study](https://amirhossein-najafizadeh.medium.com/benchmarking-nats-jetstream-cluster-hypothesis-testing-for-enhancements-4a500d11ce7b).

## FSM (revised for deliver/ack semantics)

```crystal
class QueueStateMachine
  @ready             : Deque(UInt64)               # FIFO of Raft indexes deliverable now
  @requeued          : Deque(UInt64)               # nack-with-requeue, delivered before @ready
  @unacked           : Hash(UInt64, UnackedInfo)   # delivery_tag → {raft_index, expires_at}
  @next_delivery_tag : UInt64

  record UnackedInfo, raft_index : UInt64, expires_at : UInt64
end
```

State transitions on `apply(QueueCommand)`:

- `Publish` → push `entry.index` onto `@ready`. Body stays in the Raft log.
- `Deliver` → pop from `@requeued` first, else `@ready`; mint a `delivery_tag`; insert into `@unacked`.
- `Ack(tag)` → `@unacked.delete(tag)`. The Raft index is now dead, eligible for compaction.
- `Nack(tag, requeue: true)` → move back to `@requeued`.
- `Requeue(tag)` (proposed by leader on expiry) → same as Nack with requeue.

Bodies are looked up from the Raft log by index when delivering. Snapshot serializes only `(@ready, @requeued, @unacked, @next_delivery_tag)` — sparse, body-free.

**Memory note:** `@ready` is `Deque(UInt64)` for simplicity. In steady FIFO state it's effectively a contiguous range — collapsing it to `(head_index, tail_index, count)` is an optimization to revisit if profiling shows it matters. The unacked map is bounded by `prefetch × consumers`; the requeued deque is bounded by retry rate.

**Linearizable reads sidebar:** with proper deliver/ack semantics, an empty `basic.get` is non-destructive and shouldn't log. The leader can peek `@ready.empty? && @requeued.empty?` and return 204 without proposing — same correctness argument as `ReadIndex`-style optimistic reads (stale leader could serve a spurious empty; client retries; no message loss). Tighten with full `ReadIndex` later if needed.

## Build order

1. **Snapshot wiring (`ARCHITECTURE.md` §6.5).** `Raft::Node` invokes `StateMachine#snapshot`/`restore`; trigger every N committed entries; implement `InstallSnapshot` RPC + chunked transport; persist snapshot atomically (tmp+rename+fsync).
2. **Log segment rolling + truncation.** Explicit segment rotation in `Raft::Log`; "drop segments whose last index < snapshot index" path. Required before #1's truncation can do anything.
3. **AMQP-style FSM in `QueueStateMachine`.** Replace `Deque(Bytes)` with the `@ready / @unacked / @requeued / @next_delivery_tag` shape above. New commands: `Deliver`, `Ack`, `Nack`. `Publish` records the entry's Raft index. Body lookup reads the Raft log by index when delivering.
4. **HTTP API redesign.** `GET /queues/{name}/messages` returns `{body, delivery_tag}` in headers/body. New endpoints: `POST /queues/{name}/ack/{tag}`, `POST /queues/{name}/nack/{tag}`. Empty `GET` returns 204 *without* proposing (peek-only).
5. ~~**Auto-requeue on expiry.**~~ *(scratched — keeping current auto-ack `Consume`; no unacked state to expire.)*
6. **Snapshot serializes the FSM.** Today's `Deque(Bytes)`-based `QueueStateMachine` serializes to/from IO. (Already covered by Task 1 of `2026-05-06-raft-snapshot-compaction-plan.md` — `Node` calls `state_machine.snapshot`/`restore`.)
7. **(Later) Ra-style shared WAL.** Single per-node WAL; segment writer demuxes into per-queue files. One fsync amortized across all queues. Pursue once benchmarks show per-group fsync is the bottleneck.
8. **(Later) io_uring backend for `Raft::Log::Segment`.** Replace mfile path with batched io_uring writes/reads. Interface unchanged; only segment storage changes.
9. **(Later, only if needed) Model C migration.** If memory pressure from large fragmented ready sets becomes real, move bodies to SM-managed segment files and shrink Raft entries to `(segment_id, offset, size)` references. Adds an out-of-band body-shipping protocol; keeps the FSM unchanged.
