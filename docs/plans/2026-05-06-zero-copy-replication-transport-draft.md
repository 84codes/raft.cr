# Zero-Copy Replication Transport — Draft Plan

> **Status:** Draft. Not yet broken into bite-sized TDD steps. Use this as a scoping document and as input to a full plan once we're ready to execute.

**Goal:** Move the hot replication path (`AppendEntries` body, `InstallSnapshot` chunks) off the user-space `Bytes` materialization model and onto a streaming `sendfile` / `splice` model where bytes flow disk → kernel → socket → kernel → disk without crossing into Crystal heap. Match the architectural pattern RedPanda uses (iobuf chains + DMA-direct I/O) within Crystal's stdlib + a thin libc wrapper.

**Why now:** Profiling once we hit the RabbitMQ-quorum-band (~30k msgs/s replicated) will show CPU pinned in `Bytes` allocation and copy paths. Eliminating user-space copies is the single biggest lever toward the RedPanda band (300k–1M msgs/s replicated). It also removes the awkward `max_message_payload_bytes` ceiling in the hot path — payload size becomes a streaming concern, not an allocation concern.

**Pre-reqs:**
- Snapshot plan (`2026-05-06-raft-snapshot-compaction-plan.md`) lands first. That plan deliberately stays inside the `Bytes` model so it can be done cheaply; the zero-copy migration then upgrades both `AppendEntries` and `InstallSnapshot` together.
- Single-file `snapshot` format from the snapshot plan — already shaped to allow `sendfile(snapshot_fd, 0, total_size)` end-to-end without prefix juggling.

---

## Architecture

**Three-way split of the wire format:**

```
[header: fixed-size]   ← read into a small struct in user space, parsed
[index:  variable]     ← per-entry offsets within the payload (small, fits in stack)
[payload: variable]    ← the bulk bytes; spliced kernel-to-kernel on hot paths
```

Today's `Message` collapses index + payload into a single `entries_data : Bytes`. The split lets us:
1. Read the header into a stack struct.
2. Decide what to do with the payload based on the header (splice into segment file? read into Bytes? skip?).
3. Stream the payload without ever materializing it.

**Per-message-type policy:**

| Message type | Header parse | Index parse | Payload disposition |
|---|---|---|---|
| `AppendEntries` (with entries) | yes | yes | `splice(socket → segment_fd)` |
| `AppendEntries` (heartbeat, 0 entries) | yes | none | none |
| `AppendEntriesResponse` | yes | none | none |
| `RequestVote` / `Response` | yes | none | none |
| `PreVote` / `Response` | yes | none | none |
| `InstallSnapshot` | yes | none | `splice(socket → snapshot.tmp)` |
| `InstallSnapshotResponse` | yes | none | none |
| `TimeoutNow` | yes | none | none |

So splice paths are exactly two: one for log entries, one for snapshot bytes. Everything else is small and stays in user space.

**Storage prerequisite:** segment files must be regular files (with kernel page cache) so they can be the destination of `splice`. Today `Raft::Log::Segment` uses `MFile` — mmap-only. We need a write path that goes through `pwrite`/`splice` while reads can stay mmap'd if they want. Most likely outcome: retire `MFile` for log segments in favor of a regular `File` + buffered reads. (`MFile` may still be useful elsewhere, but its tight coupling to mmap doesn't compose with the new transport.)

---

## Open questions

1. **Crystal stdlib coverage of `sendfile`/`splice`.** `IO.copy(File, Socket)` uses `sendfile` when the platform supports it, but `splice` (socket → file) isn't surfaced. Likely outcome: thin libc wrapper for `splice(2)` plus a fallback `read+pwrite` loop for non-Linux. Needs a spike to confirm.
2. **Per-entry offsets in the header — fixed or variable?** A batch of 1000 entries at 4 bytes per offset is 4 KB of header. Manageable. Alternative: omit offsets and re-scan the just-spliced bytes on the receiver to rebuild the offsets array. Re-scan touches the same bytes the kernel just wrote, which page-cache makes cheap. Probably faster than transmitting offsets. Decide via a microbenchmark.
3. **Segment-boundary spans.** A batch may cover entries near the end of one segment and the start of the next. Two `sendfile` calls back-to-back over the same socket; the receiver has to splice into one segment, finalize it, then splice the rest into a new segment. Doable but requires the receiver to know segment-boundary indexes — either add it to the header or have the receiver decide based on local `max_segment_size`.
4. **`io_uring` vs blocking syscalls.** `sendfile(2)` is fine on a thread-per-fiber Crystal model, but for true LavinMQ-class throughput we'd want `io_uring`'s `IORING_OP_SPLICE` + `IORING_OP_SEND_ZC` (zero-copy send) batched. Crystal stdlib doesn't have io_uring; would need a shard or hand-rolled bindings. Defer this — start with blocking `sendfile`/`splice`, swap in io_uring when the rest is solid.
5. **mmap vs regular file for segment reads.** Reads are dominated by the leader sending entries to followers (handled by `sendfile` from a regular `File`) and by the SM occasionally doing `Log#get(index)` (e.g., for re-delivery in the queue's deliver path). Both work fine with a regular file + page cache. mmap was a micro-optimization; we can drop it.
6. **Heartbeats vs entry batches sharing a socket.** If a batch is mid-`sendfile` when a heartbeat needs to go out, the heartbeat is queued behind the batch on that socket. With a 1 MB batch budget, that's at most ~10 ms on a saturated 1 Gbps link — acceptable. If not: dedicate a second socket for control messages.

---

## Task list (titles, no full TDD steps yet)

| # | Title | Files |
|---|---|---|
| 1 | Spike: confirm `sendfile`/`splice` access from Crystal | scratch dir |
| 2 | Refactor `Message` into `MessageHeader` + payload | `src/raft/message.cr`, all consumers |
| 3 | Replace `MFile` with regular-file backing for `Raft::Log::Segment` | `src/raft/log/segment.cr`, `src/raft/mfile.cr` (delete or repurpose) |
| 4 | Expose `Segment#fd`, `Segment#byte_offset_for(index)`, `Segment#byte_count(from, max)` | `src/raft/log/segment.cr` |
| 5 | Leader-side `send_append_entries_to` uses `IO.copy(segment, socket)` | `src/raft/node.cr`, `src/raft/transport/tcp_transport.cr` |
| 6 | Follower-side `handle_append_entries` splices payload into current segment | `src/raft/node.cr`, `src/raft/log.cr` (new bulk-append path) |
| 7 | `Log#append_bulk(payload_io, count, last_index, last_term)` — segment-aware bulk write | `src/raft/log.cr`, `src/raft/log/segment.cr` |
| 8 | Re-scan vs offset-list — pick one; if re-scan, validate parse-as-you-go | follow-on benchmarks |
| 9 | Segment-boundary span handling on send and receive | `src/raft/node.cr`, `src/raft/log.cr` |
| 10 | `send_install_snapshot_to` uses `sendfile` from `snapshot` file | `src/raft/node.cr` |
| 11 | `handle_install_snapshot` splices into `snapshot.tmp` | `src/raft/node.cr` |
| 12 | Drop / shrink `Config.max_message_payload_bytes` for streaming-payload message types | `src/raft/config.cr` |
| 13 | Benchmark suite: msgs/s, MB/s, CPU%, allocator pressure pre vs post | `benchmarks/` (new dir) |
| 14 | Update `ARCHITECTURE.md` with the new wire-format diagram | `ARCHITECTURE.md` |

---

## Risks / unknowns

- **Crystal `splice` ergonomics.** No stdlib path. We'll either ship a tiny libc wrapper (manageable; ~30 lines) or pull a shard.
- **Test harness.** `Raft::MemoryTransport` doesn't have FDs — its receive path is in-process channel sends. The zero-copy transport tests only matter for `TCPTransport`. We'll need a TCP-based integration harness (probably loopback) for steps 5–11.
- **Crash-consistency of bulk appends.** `splice` to a regular file lands in page cache; an `fsync` is needed before acking. The leader's batch becomes "splice → fsync → respond." If batch-fsync amortization is the win we expect, this is the same fsync count as today (one per batch); no regression.
- **Backwards compatibility.** Wire format changes break any older nodes in a cluster. We'll need a `protocol_version` bump and a graceful refusal if peers disagree. The existing `protocol_version` byte at the front of `Message` is exactly for this.
- **`MFile` replacement is invasive.** It's used in segment files specifically; replacing it touches recovery, truncation, and read paths. Could land as a self-contained sub-task without zero-copy, just to de-risk the storage layer first.

---

## Done definition

A 3-node TCP cluster sustains ≥ 100k msg/s of 1 KB messages on commodity NVMe with ≤ 50% of one CPU core spent in Crystal user space (the rest is kernel-side I/O), and `Bytes#allocate` does not appear in the top 10 of an allocation profile during sustained load. The `MAX_ENTRIES_DATA_SIZE`-style ceiling is gone from the hot path; large messages flow through naturally.
