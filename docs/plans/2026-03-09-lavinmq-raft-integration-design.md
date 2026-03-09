# LavinMQ Raft Integration — SegmentPosition Log with Wire-Level Data Resolution

## Problem

LavinMQ writes messages to disk immediately into segment files. Adding Raft replication must not double-store message data or interfere with `sendfile`-based consumer delivery.

## Design

### Storage Layout

```
Raft Log (on disk):     [term | index | entry_type | SegmentPosition(12 bytes)]
Segment Files (on disk): [raw message bytes...]
```

- The Raft log stores only `SegmentPosition` (segment: UInt32, position: UInt32, bytesize: UInt32)
- Actual message data lives exclusively in LavinMQ's segment files
- Consumer reads use `sendfile` from segment files — no Raft metadata in the way

### Replication Wire Format

When the leader replicates entries to followers:

```
Per entry on the wire:
  [term: UInt64 | index: UInt64 | entry_type: UInt8 | data_size: UInt32 | message_bytes...]
```

- Leader resolves `SegmentPosition` → reads message bytes from segment file (can use `sendfile`)
- Follower receives message bytes → writes to its own segment file → stores resulting `SegmentPosition` in its Raft log

### Data Flow

```
Producer → Leader:
  1. Message arrives at leader
  2. Written to leader's segment file → gets SegmentPosition
  3. SegmentPosition appended to Raft log via propose()
  4. send_append_entries triggered

Leader → Follower (replication):
  1. Leader reads LogEntry(SegmentPosition) from Raft log
  2. Resolves SegmentPosition → sendfile(segment_fd, socket, offset, size) for payload
     (entry header written from userspace, ~21 bytes)
  3. Follower receives entry header + message bytes
  4. Follower writes message bytes to its own segment file → gets local SegmentPosition
  5. Follower appends LogEntry(SegmentPosition) to its Raft log

Commit:
  1. Leader gets AppendEntriesResponse(success) from quorum
  2. Leader advances commit_index
  3. Leader applies: SegmentPosition already in MessageStore, message ready for consumers
  4. Followers learn new commit_index via next AppendEntries
  5. Followers apply: add SegmentPosition to their MessageStore

Consumer reads:
  1. Consumer requests messages from any node (or leader only, TBD)
  2. Node looks up SegmentPosition in MessageStore
  3. sendfile(segment_fd, consumer_socket, offset, size) — zero-copy
```

### Truncation / Log Conflict

When a follower must truncate uncommitted entries (new leader has different entries):
1. Follower truncates Raft log entries after the conflict point
2. Follower must also remove the corresponding message data from its segment files
   - Option A: Mark the segment space as reclaimable (lazy cleanup, simpler)
   - Option B: Truncate the segment file if the entries were at the tail (only works for append-only segments)
   - Option C: Leave orphaned bytes in segment files, reclaim during compaction

### Changes to the Raft Library

**File: `src/raft/node.cr`**

Add two optional callback procs:

```crystal
@encode_entry : Proc(LogEntry(T), IO, Nil)?
@decode_entry : Proc(IO, LogEntry(T))?
```

- `encode_entry`: called in `send_append_entries` instead of `LogEntry.to_io`. LavinMQ's implementation resolves SegmentPosition → writes header + sendfile's message bytes.
- `decode_entry`: called in `handle_append_entries` instead of `LogEntry.from_io`. LavinMQ's implementation reads header + message bytes → writes to local segment → returns LogEntry(SegmentPosition).
- When nil (default): falls back to current `LogEntry.to_io` / `LogEntry.from_io`. All existing users unaffected.

This is ~15 lines of change in one file.

### What LavinMQ Provides

```crystal
node = Raft::Node(SegmentPosition).new(
  ...,
  encode_entry: ->(entry, io) {
    # write 21-byte header (term, index, entry_type)
    # resolve entry.data (SegmentPosition) → sendfile from segment file
  },
  decode_entry: ->(io) {
    # read 21-byte header
    # read message bytes → write to local segment file → get SegmentPosition
    # return LogEntry(SegmentPosition)
  }
)
```

### Key Properties

- **No double storage**: message data in segment files only, Raft log has 12-byte references
- **Zero-copy replication**: leader uses `sendfile` from segment file to socket
- **Zero-copy consumer delivery**: `sendfile` from segment file to consumer socket
- **Backward compatible**: existing Raft library users (KV example) work unchanged
- **Minimal library change**: two optional procs in `node.cr`, nothing else

### Open Questions

1. **Truncation strategy**: how to handle orphaned message bytes in segment files when Raft log is truncated (conflict resolution). Lazy reclaim during compaction seems simplest.
2. **sendfile mechanics**: the encode_entry callback receives an `IO` — to use actual `sendfile`, the transport layer would need access to the raw socket. May need to pass the socket directly or use a different abstraction. Initial implementation could just `IO.copy` from a `File` to the `IO`, and optimize to `sendfile` later.
3. **Batching**: contiguous entries in the same segment file could be sent with a single `sendfile` call. Optimization for later.
