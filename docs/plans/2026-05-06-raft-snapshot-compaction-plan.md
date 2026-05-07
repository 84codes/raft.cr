# Raft Snapshot + Log Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire `StateMachine#snapshot`/`restore` into `Raft::Node`, persist snapshots atomically on disk, truncate the log past the snapshot index, send/receive `InstallSnapshot` to bring lagging followers back online, and recover from snapshot on restart. Closes `ARCHITECTURE.md` §6.5's "snapshots / log compaction" gap.

**Architecture:** Each node persists a per-group single `snapshot` file (`[u64 index][u64 term][u32 peer_len][peers][sm_bytes]`), atomic via tmp+rename+fsync. Leader takes a snapshot whenever `last_applied - snapshot_index >= snapshot_interval_entries`, then drops Raft log segments whose last index ≤ `snapshot_index`. Followers whose `next_index ≤ snapshot_index` receive `InstallSnapshot` chunks (Message fields repurposed) instead of `AppendEntries`; on the last chunk they atomically swap snapshot files, call `restore`, reset their log, and ack. On startup, `recover_state` loads the snapshot first, then replays the log tail.

**Tech Stack:** Crystal, the existing `Raft::*` library, `-Dpreview_mt -Dexecution_context`. No external deps.

---

## File structure

| File | Change | Responsibility |
|---|---|---|
| `src/raft/config.cr` | Modify | Add `snapshot_interval_entries` (default 1000) and `max_message_payload_bytes` (default 64 MB). |
| `src/raft/message.cr` | Modify | Drop hardcoded `MAX_ENTRIES_DATA_SIZE`; `from_io` takes a `max_payload : UInt32` parameter for safety bounding. |
| `src/raft/transport/tcp_transport.cr` | Modify | Pass `Config.max_message_payload_bytes` into `Message.from_io`. |
| `src/raft/log.cr` | Modify | Add `truncate_before(index)` — drop segments whose `last_index ≤ index`; expose `first_index`. |
| `src/raft/node.cr` | Modify | Add `@snapshot_index`, `@snapshot_term`; `persist_snapshot`/`load_snapshot`; `take_snapshot` trigger; send/receive `InstallSnapshot`; recovery path. |
| `spec/raft/snapshot_spec.cr` | Create | Focused unit + small integration tests for snapshot persistence, log truncation, and recovery. |
| `spec/spec_helper.cr` | Modify (minor) | `TestStateMachine` already implements snapshot/restore — no change needed unless a test demands a richer state. |

**Wire format for `MessageType::InstallSnapshot`** (repurposes existing `Message` fields, no protocol_version bump):

| Field | Meaning |
|---|---|
| `term` | Leader's current term |
| `prev_log_index` | `snapshot_index` (last index covered by snapshot) |
| `prev_log_term` | `snapshot_term` |
| `last_log_index` | `chunk_offset` (byte offset into snapshot body) |
| `last_log_term` | `total_size` (total snapshot body size in bytes) |
| `success` | `is_last_chunk` (true on the final chunk only) |
| `entries_data` | The chunk bytes |
| `entries_count` | unused (must be 0) |

For `InstallSnapshotResponse`:

| Field | Meaning |
|---|---|
| `term` | Follower's current term |
| `last_log_index` | bytes accepted so far (so leader can resume / verify) |
| `success` | true if the chunk was accepted |

**Snapshot framing on disk:** single file `snapshot` with layout `[u64 index][u64 term][u32 peer_len][peers_bytes][sm_bytes_to_eof]`. One tmp+rename, one fsync. The Node owns the index/term/peers framing; everything past it is opaque SM bytes.

---

## Task 0: Make message-payload cap a config field

**Files:**
- Modify: `src/raft/config.cr`
- Modify: `src/raft/message.cr`
- Modify: `src/raft/transport/tcp_transport.cr`
- Modify: `spec/raft/message_spec.cr` (only if it referenced `MAX_ENTRIES_DATA_SIZE`)

The `MAX_ENTRIES_DATA_SIZE = 64 MB` constant in `Message` is a receiver-side memory-safety bound, not a protocol limit. Move it to `Config` so operators can tune it for large-message workloads.

- [ ] **Step 1: Add the config field**

In `src/raft/config.cr`:

```crystal
    property max_message_payload_bytes : UInt32 = 64_u32 * 1024_u32 * 1024_u32 # 64 MB
```

- [ ] **Step 2: Make `Message.from_io` accept the cap**

In `src/raft/message.cr`, remove the constant and change the signature:

```crystal
    def self.from_io(io : IO, max_payload : UInt32, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
      msg = new(
        protocol_version: io.read_bytes(UInt8, format),
        # ... unchanged ...
      )
      msg.entries_count = io.read_bytes(UInt32, format)
      data_size = io.read_bytes(UInt32, format)
      raise IO::Error.new("entries_data size #{data_size} exceeds max_message_payload_bytes #{max_payload}") if data_size > max_payload
      if data_size > 0
        entries_data = Bytes.new(data_size)
        io.read_fully(entries_data)
        msg.entries_data = entries_data
      end
      msg
    end
```

Also remove the line `MAX_ENTRIES_DATA_SIZE = 64_u32 * 1024 * 1024 # 64 MB` from the struct.

- [ ] **Step 3: Thread the cap through the transport**

In `src/raft/transport/tcp_transport.cr`, the constructor needs to accept (or close over) the config so `handle_connection` can pass `@config.max_message_payload_bytes` to `Message.from_io`. The minimal change: add a `max_payload : UInt32` field to the transport.

```crystal
    @max_payload : UInt32 = 64_u32 * 1024_u32 * 1024_u32

    def initialize(@listen_address : String, @listen_port : Int32, @data_dir : String? = nil, @max_payload : UInt32 = 64_u32 * 1024_u32 * 1024_u32)
      recover_peers
    end
```

In `handle_connection`, replace `Message.from_io(client)` with `Message.from_io(client, @max_payload)`.

- [ ] **Step 4: Update call sites that construct the transport**

`examples/queue/src/main.cr` and `examples/kv/src/main.cr` instantiate `Raft::TCPTransport`. Pass `cfg.max_message_payload_bytes`:

```crystal
transport = Raft::TCPTransport.new(
  listen_address: "0.0.0.0",
  listen_port: raft_port,
  data_dir: base_data_dir,
  max_payload: 64_u32 * 1024_u32 * 1024_u32, # or expose as ENV
)
```

(For the PoC, keep the default; the field exists for the future.)

- [ ] **Step 5: Update specs that called `Message.from_io(io)` without the cap**

Run: `grep -rn "Message.from_io" spec/`. Each call site needs the cap. For tests, pass `64_u32 * 1024 * 1024` or any safe value.

- [ ] **Step 6: Compile and run the suite**

Run: `crystal spec spec/ -Dpreview_mt -Dexecution_context`
Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add src/raft/config.cr src/raft/message.cr src/raft/transport/tcp_transport.cr examples/queue/src/main.cr examples/kv/src/main.cr spec/
git commit -m "Move message payload cap from constant to Config.max_message_payload_bytes"
```

---

## Task 1: Persist snapshot to disk + recover on startup

**Files:**
- Modify: `src/raft/node.cr` (add `@snapshot_index`, `@snapshot_term`, `persist_snapshot`, `load_snapshot`, update `recover_state`)
- Create: `spec/raft/snapshot_spec.cr`

The most foundational piece. After this task, an in-process call to `take_snapshot` (added in Task 3) writes a single `snapshot` file atomically; `Node.new` re-loads it on startup.

- [ ] **Step 1: Write the failing test**

Create `spec/raft/snapshot_spec.cr`:

```crystal
require "../spec_helper"

describe "Raft::Node snapshot persistence" do
  it "persists and reloads snapshot across Node restart" do
    dir = File.tempname("raft_snapshot")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir

    sm1 = TestStateMachine.new
    sm1.apply(TestData.new("a"))
    sm1.apply(TestData.new("b"))

    node1 = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm1)
    # Manually persist a snapshot at index=2, term=1
    node1.persist_snapshot_for_test(2_u64, 1_u64)
    node1.close

    sm2 = TestStateMachine.new
    node2 = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm2)
    node2.snapshot_index.should eq 2_u64
    node2.snapshot_term.should eq 1_u64
    sm2.applied.size.should eq 2
    sm2.applied[0].value.should eq "a"
    sm2.applied[1].value.should eq "b"

    node2.close
    FileUtils.rm_rf(dir)
  end
end
```

- [ ] **Step 2: Run test, verify it fails**

Run: `crystal spec spec/raft/snapshot_spec.cr -Dpreview_mt -Dexecution_context`
Expected: compile error or undefined method on `persist_snapshot_for_test`, `snapshot_index`, `snapshot_term`.

- [ ] **Step 3: Add fields and persistence helpers to `Raft::Node`**

In `src/raft/node.cr`, add to the field block (just below `getter last_applied`):

```crystal
    getter snapshot_index : UInt64 = 0_u64
    getter snapshot_term : UInt64 = 0_u64
```

Add the following private methods before `private def random_election_timeout`:

```crystal
    # Test helper — drives persist_snapshot from outside while
    # take_snapshot doesn't exist yet (added in Task 3).
    def persist_snapshot_for_test(index : UInt64, term : UInt64)
      persist_snapshot(index, term)
    end

    private def persist_snapshot(index : UInt64, term : UInt64)
      path = File.join(@config.data_dir, "snapshot")
      tmp_path = path + ".tmp"

      File.open(tmp_path, "wb") do |f|
        f.write_bytes(index, IO::ByteFormat::LittleEndian)
        f.write_bytes(term, IO::ByteFormat::LittleEndian)
        peer_bytes = serialize_peers
        f.write_bytes(peer_bytes.size.to_u32, IO::ByteFormat::LittleEndian)
        f.write(peer_bytes)
        @state_machine.snapshot(f)
        f.fsync
      end
      File.rename(tmp_path, path)

      @snapshot_index = index
      @snapshot_term = term
    end

    private def load_snapshot : Bool
      path = File.join(@config.data_dir, "snapshot")
      return false unless File.exists?(path)

      File.open(path, "rb") do |f|
        @snapshot_index = f.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
        @snapshot_term = f.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
        peer_len = f.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
        peer_buf = Bytes.new(peer_len)
        f.read_fully(peer_buf)
        @peers = deserialize_peers(peer_buf)
        @state_machine.restore(f)
      end

      @last_applied = @snapshot_index
      @commit_index = Math.max(@commit_index, @snapshot_index)
      true
    end
```

- [ ] **Step 4: Update `recover_state` to load snapshot before peer state**

In `src/raft/node.cr`, replace `private def recover_state` body to call `load_snapshot` before reading `raft_meta`:

```crystal
    private def recover_state
      load_snapshot
      path = File.join(@config.data_dir, "raft_meta")
      tmp_path = path + ".tmp"
      File.delete(tmp_path) if File.exists?(tmp_path)
      return unless File.exists?(path)
      File.open(path, "rb") do |f|
        @current_term = f.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
        has_vote = f.read_bytes(UInt8, IO::ByteFormat::LittleEndian)
        @voted_for = has_vote == 1_u8 ? f.read_bytes(UInt64, IO::ByteFormat::LittleEndian) : nil
        peer_count = f.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
        # raft_meta peers may be more recent than the snapshot's; prefer raft_meta.
        @peers = Array(Peer).new(peer_count) { Peer.from_io(f) }
      end
    end
```

- [ ] **Step 5: Run test, verify it passes**

Run: `crystal spec spec/raft/snapshot_spec.cr -Dpreview_mt -Dexecution_context`
Expected: 1 example, 0 failures.

- [ ] **Step 6: Commit**

```bash
git add src/raft/node.cr spec/raft/snapshot_spec.cr
git commit -m "Persist snapshot to disk and load on Node startup"
```

---

## Task 2: Recovery loads snapshot then replays log tail

**Files:**
- Modify: `src/raft/node.cr` (extend `recover_state` to skip already-applied entries via `@snapshot_index`)
- Modify: `spec/raft/snapshot_spec.cr` (add the test)

Currently, applying entries from the log starts at `@last_applied + 1`. After Task 1, `@last_applied = @snapshot_index` post-recovery — so `apply_entries` will naturally pick up only entries past the snapshot. But there's no test that proves this end-to-end.

- [ ] **Step 1: Write the failing test**

Append to `spec/raft/snapshot_spec.cr`:

```crystal
describe "Raft::Node snapshot + log tail recovery" do
  it "applies snapshot then replays only entries past snapshot_index" do
    dir = File.tempname("raft_snapshot_tail")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir

    # Seed log entries 1..3 and a snapshot at index 2.
    sm1 = TestStateMachine.new
    node1 = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm1)
    node1.bootstrap
    node1.propose(TestData.new("a"))
    node1.propose(TestData.new("b"))
    node1.propose(TestData.new("c"))
    # Drive the apply loop forward — bootstrap is single-node, so propose commits immediately.
    sm1.applied.map(&.value).should eq ["a", "b", "c"]

    # Pretend we snapshotted at index 2 (covers a + b)
    node1.persist_snapshot_for_test(node1.last_applied - 1, node1.current_term)
    node1.close

    # Restart: SM should be re-restored from snapshot, then "c" replayed from log tail.
    sm2 = TestStateMachine.new
    node2 = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm2)
    # SM after restore() in load_snapshot has whatever sm1 serialized at snapshot time
    # (a + b). The log replay then needs to apply "c". The library should drive the
    # apply loop on the next tick.
    100.times { node2.tick }
    sm2.applied.map(&.value).should eq ["a", "b", "c"]

    node2.close
    FileUtils.rm_rf(dir)
  end
end
```

- [ ] **Step 2: Run test, verify it fails**

Run: `crystal spec spec/raft/snapshot_spec.cr -Dpreview_mt -Dexecution_context`
Expected: failure — `sm2.applied` is `["a", "b"]` (snapshot only), missing "c". The library doesn't replay log tail post-restore.

- [ ] **Step 3: Drive apply on startup once snapshot is loaded**

In `src/raft/node.cr`, at the bottom of `private def recover_state` (after the `File.open(path, "rb")` block), add:

```crystal
      # Replay any committed log entries past the snapshot
      if @commit_index > @last_applied
        apply_entries(@last_applied + 1, @commit_index)
      end
```

Note: `@commit_index` is read from `raft_meta` if Task ?? made it so — currently `persist_state` does NOT persist `commit_index`. Add it:

In `private def persist_state` after `f.write_bytes(@current_term, IO::ByteFormat::LittleEndian)`:

```crystal
        f.write_bytes(@commit_index, IO::ByteFormat::LittleEndian)
```

In `recover_state` after `@current_term = f.read_bytes(...)`:

```crystal
        @commit_index = f.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
```

- [ ] **Step 4: Run test, verify it passes**

Run: `crystal spec spec/raft/snapshot_spec.cr -Dpreview_mt -Dexecution_context`
Expected: 2 examples, 0 failures.

- [ ] **Step 5: Run the full library test suite to make sure persist_state changes didn't break anything**

Run: `crystal spec spec/raft/ -Dpreview_mt -Dexecution_context`
Expected: all green. If `node_spec.cr` has tests that depend on the old `raft_meta` layout, fix them by re-creating the persisted file (or accept that they were not exercising recovery).

- [ ] **Step 6: Commit**

```bash
git add src/raft/node.cr spec/raft/snapshot_spec.cr
git commit -m "Replay log tail past snapshot index on Node startup"
```

---

## Task 3: Snapshot trigger on the leader

**Files:**
- Modify: `src/raft/config.cr`
- Modify: `src/raft/node.cr`
- Modify: `spec/raft/snapshot_spec.cr`

Add the policy: every `snapshot_interval_entries` committed-and-applied entries on the leader trigger a snapshot. The trigger lives at the end of `apply_entries`.

- [ ] **Step 1: Add the config field**

In `src/raft/config.cr`, add:

```crystal
    property snapshot_interval_entries : UInt64 = 1000_u64
```

- [ ] **Step 2: Write the failing test**

Append to `spec/raft/snapshot_spec.cr`:

```crystal
describe "Raft::Node snapshot trigger" do
  it "snapshots after snapshot_interval_entries entries have been applied" do
    dir = File.tempname("raft_snapshot_trigger")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    cfg.snapshot_interval_entries = 5_u64

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm)
    node.bootstrap
    7.times { |i| node.propose(TestData.new("v#{i}")) }

    node.snapshot_index.should be >= 5_u64
    File.exists?(File.join(dir, "snapshot")).should be_true

    node.close
    FileUtils.rm_rf(dir)
  end
end
```

- [ ] **Step 3: Run test, verify it fails**

Run: `crystal spec spec/raft/snapshot_spec.cr -Dpreview_mt -Dexecution_context`
Expected: failure — `snapshot_index` is still 0.

- [ ] **Step 4: Add `take_snapshot` and call it from `apply_entries`**

In `src/raft/node.cr`, add `take_snapshot` next to `persist_snapshot`:

```crystal
    private def take_snapshot
      return if @last_applied <= @snapshot_index
      term = @log.term_at(@last_applied)
      persist_snapshot(@last_applied, term)
      @metrics.try(&.increment("raft_snapshots_taken_total"))
    end
```

Update `private def apply_entries`'s tail (just before the closing `end`):

```crystal
      if @last_applied - @snapshot_index >= @config.snapshot_interval_entries
        take_snapshot
      end
```

- [ ] **Step 5: Run test, verify it passes**

Run: `crystal spec spec/raft/snapshot_spec.cr -Dpreview_mt -Dexecution_context`
Expected: 3 examples, 0 failures.

- [ ] **Step 6: Commit**

```bash
git add src/raft/config.cr src/raft/node.cr spec/raft/snapshot_spec.cr
git commit -m "Trigger snapshot every snapshot_interval_entries on the leader"
```

---

## Task 4: Truncate log past snapshot_index

**Files:**
- Modify: `src/raft/log.cr` (add `truncate_before` and `first_index`)
- Modify: `src/raft/node.cr` (call after `take_snapshot`)
- Modify: `spec/raft/log_spec.cr` (add unit test)
- Modify: `spec/raft/snapshot_spec.cr` (assert log shrinks)

Drop log segments whose entire index range is at or below `snapshot_index`. We never split a segment, so the segment containing `snapshot_index` itself is kept intact until the next snapshot rolls past it.

- [ ] **Step 1: Write the failing log unit test**

Append to `spec/raft/log_spec.cr`:

```crystal
describe Raft::Log do
  it "truncate_before drops segments whose last_index <= given index" do
    dir = File.tempname("raft_log_truncate_before")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    cfg.max_segment_size = 80_u32 # tiny — forces frequent rotation

    log = Raft::Log(TestData).new(cfg)
    20.times { |i| log.append(term: 1_u64, data: TestData.new("v#{i}")) }
    initial_segments = log.segment_count
    initial_segments.should be > 1

    # Drop everything up to (and including) entry index 10
    log.truncate_before(10_u64)
    log.segment_count.should be < initial_segments
    log.first_index.should be >= 11_u64
    log.get(20_u64).data.not_nil!.value.should eq "v19"

    log.close
    FileUtils.rm_rf(dir)
  end
end
```

- [ ] **Step 2: Run test, verify it fails**

Run: `crystal spec spec/raft/log_spec.cr -Dpreview_mt -Dexecution_context`
Expected: undefined method `truncate_before` / `first_index`.

- [ ] **Step 3: Add `truncate_before` and `first_index` to `Raft::Log`**

In `src/raft/log.cr`, add inside `class Log(T)`:

```crystal
    def first_index : UInt64
      return 0_u64 if @segments.empty?
      @segments.first.first_index
    end

    def truncate_before(index : UInt64)
      # Drop segments whose entire index range is <= the given index.
      # The segment containing `index` itself is kept (we don't split segments).
      while @segments.size > 1 && @segments.first.last_index <= index
        seg = @segments.shift
        path = File.join(@config.data_dir, "%016d.log" % seg.first_index)
        seg.close
        File.delete(path) if File.exists?(path)
      end
    end
```

- [ ] **Step 4: Run log unit test, verify it passes**

Run: `crystal spec spec/raft/log_spec.cr -Dpreview_mt -Dexecution_context`
Expected: green.

- [ ] **Step 5: Wire log compaction into the snapshot path**

In `src/raft/node.cr`, update `private def take_snapshot`:

```crystal
    private def take_snapshot
      return if @last_applied <= @snapshot_index
      term = @log.term_at(@last_applied)
      persist_snapshot(@last_applied, term)
      @log.truncate_before(@snapshot_index)
      @metrics.try(&.increment("raft_snapshots_taken_total"))
    end
```

- [ ] **Step 6: Add an end-to-end test for log shrinkage**

Append to `spec/raft/snapshot_spec.cr`:

```crystal
describe "Raft::Node log compaction after snapshot" do
  it "drops log segments whose last index is at or below snapshot_index" do
    dir = File.tempname("raft_snapshot_compact")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    cfg.snapshot_interval_entries = 5_u64
    cfg.max_segment_size = 60_u32 # tiny — forces multi-segment

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm)
    node.bootstrap
    20.times { |i| node.propose(TestData.new("v#{i}")) }

    # After 20 entries with snapshot every 5, snapshot_index should be ~15+.
    node.snapshot_index.should be >= 15_u64
    # The first segment should now start past index 1.
    node.log.first_index.should be > 1_u64

    node.close
    FileUtils.rm_rf(dir)
  end
end
```

- [ ] **Step 7: Run all snapshot tests, verify pass**

Run: `crystal spec spec/raft/snapshot_spec.cr spec/raft/log_spec.cr -Dpreview_mt -Dexecution_context`
Expected: all green.

- [ ] **Step 8: Commit**

```bash
git add src/raft/log.cr src/raft/node.cr spec/raft/log_spec.cr spec/raft/snapshot_spec.cr
git commit -m "Truncate log past snapshot index after taking snapshot"
```

---

## Task 5: Send InstallSnapshot from leader to lagging follower

**Files:**
- Modify: `src/raft/node.cr` (`send_append_entries_to` chooses InstallSnapshot when needed; new `send_install_snapshot_to`)

When a follower's `next_index <= @snapshot_index`, the leader can no longer satisfy `AppendEntries` from the log (entries are gone). It sends `InstallSnapshot` chunks instead.

- [ ] **Step 1: Add chunk-progress tracking to `Raft::Node`**

In `src/raft/node.cr`, add to the field block:

```crystal
    @snapshot_send_offset : Hash(NodeID, UInt64) = {} of NodeID => UInt64
```

- [ ] **Step 2: Reroute `send_append_entries_to` when `next_index <= snapshot_index`**

Find `private def send_append_entries_to(peer_id : NodeID)` in `src/raft/node.cr`. At its very top (before any other logic), add:

```crystal
      next_idx = @next_index.fetch(peer_id, @log.last_index + 1)
      if @snapshot_index > 0 && next_idx <= @snapshot_index
        send_install_snapshot_to(peer_id)
        return
      end
```

- [ ] **Step 3: Implement `send_install_snapshot_to`**

Add inside `class Node(T)`, near `send_append_entries_to`:

```crystal
    private def send_install_snapshot_to(peer_id : NodeID)
      path = File.join(@config.data_dir, "snapshot")
      return unless File.exists?(path)

      offset = @snapshot_send_offset.fetch(peer_id, 0_u64)
      total_size = File.size(path).to_u64
      chunk_size = @config.snapshot_chunk_size.to_u64
      remaining = total_size - offset
      this_chunk = Math.min(remaining, chunk_size)
      is_last = (offset + this_chunk) >= total_size

      buf = Bytes.new(this_chunk.to_i32)
      File.open(path, "rb") do |f|
        f.seek(offset.to_i64)
        f.read_fully(buf)
      end

      @outbox << {peer_id, Message.new(
        group_id: @group_id,
        type: MessageType::InstallSnapshot,
        from: @id,
        term: @current_term,
        prev_log_index: @snapshot_index,
        prev_log_term: @snapshot_term,
        last_log_index: offset,
        last_log_term: total_size,
        success: is_last,
        entries_data: buf,
      )}
      @metrics.try(&.increment("raft_install_snapshot_sent_total"))
    end
```

- [ ] **Step 4: Bump send offset and reset on response in `handle_append_entries_response`**

We don't yet handle the response — that's Task 6. For now, this task only wires the **send** path; tests for it land in the integration test in Task 7. So no test for this task in isolation; verify by visual inspection.

- [ ] **Step 5: Compile to make sure everything is wired**

Run: `crystal build src/raft.cr -o /tmp/raft_lib_check -Dpreview_mt -Dexecution_context --no-codegen`
Expected: clean compile.

- [ ] **Step 6: Commit**

```bash
git add src/raft/node.cr
git commit -m "Send InstallSnapshot to followers whose next_index is below snapshot"
```

---

## Task 6: Receive InstallSnapshot on follower; ack; resume

**Files:**
- Modify: `src/raft/node.cr` (`step` dispatches `InstallSnapshot`; new `handle_install_snapshot`; `handle_install_snapshot_response`)
- Modify: `spec/raft/snapshot_spec.cr` (focused unit test of follower receive path)

Follower receives chunks, buffers to a tmp file, on `success=true` (last chunk) atomically renames into place, calls `restore`, resets the log past `snapshot_index`, updates `@last_applied = @commit_index = snapshot_index`, and acks. Leader on ack advances `@snapshot_send_offset` and falls back to `send_append_entries_to` once caught up.

- [ ] **Step 1: Write the failing test**

Append to `spec/raft/snapshot_spec.cr`:

```crystal
describe "Raft::Node InstallSnapshot receive" do
  it "follower restores from chunked snapshot and resets log" do
    dir = File.tempname("raft_install_snapshot")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir

    # Build a "leader" snapshot file manually using the same framing as persist_snapshot:
    # [u64 index][u64 term][u32 peer_len][peers][sm_bytes]
    fake_body = IO::Memory.new
    fake_body.write_bytes(42_u64, IO::ByteFormat::LittleEndian)  # snapshot_index
    fake_body.write_bytes(1_u64, IO::ByteFormat::LittleEndian)   # snapshot_term
    fake_body.write_bytes(4_u32, IO::ByteFormat::LittleEndian)   # peer_len (just the u32 count below)
    fake_body.write_bytes(0_u32, IO::ByteFormat::LittleEndian)   # 0 peers
    sm_seed = TestStateMachine.new
    sm_seed.apply(TestData.new("x"))
    sm_seed.apply(TestData.new("y"))
    sm_seed.apply(TestData.new("z"))
    sm_seed.snapshot(fake_body)
    body_bytes = fake_body.to_slice

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 2_u64, peers: [1_u64], config: cfg, state_machine: sm)

    # Single-chunk install
    msg = Raft::Message.new(
      type: Raft::MessageType::InstallSnapshot,
      from: 1_u64,
      term: 1_u64,
      prev_log_index: 42_u64,        # snapshot_index
      prev_log_term: 1_u64,          # snapshot_term
      last_log_index: 0_u64,         # chunk offset
      last_log_term: body_bytes.size.to_u64, # total size
      success: true,                 # is_last_chunk
      entries_data: body_bytes,
    )
    node.step(msg)

    node.snapshot_index.should eq 42_u64
    node.snapshot_term.should eq 1_u64
    node.last_applied.should eq 42_u64
    node.commit_index.should eq 42_u64
    sm.applied.map(&.value).should eq ["x", "y", "z"]

    # Should have emitted an InstallSnapshotResponse with success=true
    out = node.take_messages
    out.size.should eq 1
    out[0][0].should eq 1_u64
    out[0][1].type.should eq Raft::MessageType::InstallSnapshotResponse
    out[0][1].success.should be_true
    out[0][1].last_log_index.should eq body_bytes.size.to_u64

    node.close
    FileUtils.rm_rf(dir)
  end
end
```

(Note: this test bypasses peer-membership checks because `peers=[1_u64]` includes the sender as peer.)

- [ ] **Step 2: Run test, verify it fails**

Run: `crystal spec spec/raft/snapshot_spec.cr -Dpreview_mt -Dexecution_context`
Expected: test fails — `step()` ignores `InstallSnapshot`.

- [ ] **Step 3: Dispatch the new message type in `step`**

Find `def step(message : Message)` in `src/raft/node.cr`. After existing dispatch cases, add a clause for `MessageType::InstallSnapshot` and `MessageType::InstallSnapshotResponse`:

```crystal
      case message.type
      # ... existing cases ...
      when MessageType::InstallSnapshot
        handle_install_snapshot(message)
      when MessageType::InstallSnapshotResponse
        handle_install_snapshot_response(message)
      end
```

(If `step` is structured as `case message.type / when X then handle_x(...)`, just add the two new branches in the same place.)

- [ ] **Step 4: Implement `handle_install_snapshot` on the follower**

In `src/raft/node.cr`, add:

```crystal
    private def handle_install_snapshot(msg : Message)
      if msg.term < @current_term
        @outbox << {msg.from, Message.new(
          type: MessageType::InstallSnapshotResponse,
          from: @id,
          term: @current_term,
          success: false,
          last_log_index: 0_u64,
        )}
        return
      end

      if msg.term > @current_term
        @current_term = msg.term
        @voted_for = nil
      end
      become_follower(msg.from)
      @election_tick = 0_u32

      tmp_path = File.join(@config.data_dir, "snapshot.tmp")
      mode = msg.last_log_index == 0_u64 ? "wb" : "ab"
      File.open(tmp_path, mode) do |f|
        f.write(msg.entries_data)
        f.fsync
      end

      bytes_received = msg.last_log_index + msg.entries_data.size.to_u64

      if msg.success
        # Last chunk — atomic rename, then load the snapshot in the same code path
        # that startup uses (reads index/term/peers/sm from the leading bytes of the file).
        path = File.join(@config.data_dir, "snapshot")
        File.rename(tmp_path, path)
        load_snapshot

        # Sanity-check: the prefix the leader sent (in Message header) should match
        # what we just loaded from the file. If they diverge, the file is corrupt.
        if @snapshot_index != msg.prev_log_index || @snapshot_term != msg.prev_log_term
          raise "InstallSnapshot: header (#{msg.prev_log_index}, #{msg.prev_log_term}) ≠ body (#{@snapshot_index}, #{@snapshot_term})"
        end

        @log.reset
        @last_applied = @snapshot_index
        @commit_index = @snapshot_index
        persist_state
      end

      @outbox << {msg.from, Message.new(
        type: MessageType::InstallSnapshotResponse,
        from: @id,
        term: @current_term,
        success: true,
        last_log_index: bytes_received,
      )}
    end
```

- [ ] **Step 5: Implement `handle_install_snapshot_response` on the leader**

```crystal
    private def handle_install_snapshot_response(msg : Message)
      return unless @role == Role::Leader
      if msg.term > @current_term
        @current_term = msg.term
        @voted_for = nil
        become_follower
        return
      end
      return unless msg.success

      path = File.join(@config.data_dir, "snapshot")
      total_size = File.exists?(path) ? File.size(path).to_u64 : 0_u64

      if msg.last_log_index >= total_size
        # Follower has the full snapshot — fast-forward its next_index past the snapshot.
        @next_index[msg.from] = @snapshot_index + 1
        @match_index[msg.from] = @snapshot_index
        @snapshot_send_offset.delete(msg.from)
      else
        @snapshot_send_offset[msg.from] = msg.last_log_index
      end
    end
```

- [ ] **Step 6: Run test, verify it passes**

Run: `crystal spec spec/raft/snapshot_spec.cr -Dpreview_mt -Dexecution_context`
Expected: green.

- [ ] **Step 7: Run the full library suite**

Run: `crystal spec spec/ -Dpreview_mt -Dexecution_context`
Expected: green. If `spec/raft/integration_spec.cr` covers a multi-node failover scenario, make sure it still passes — the new `step()` dispatch must not break existing handling.

- [ ] **Step 8: Commit**

```bash
git add src/raft/node.cr spec/raft/snapshot_spec.cr
git commit -m "Handle InstallSnapshot RPC on follower and response on leader"
```

---

## Task 7: Multi-node integration test — fresh node catches up via snapshot

**Files:**
- Modify: `spec/raft/snapshot_spec.cr` (add integration test using `MemoryTransport`)

Three-node cluster: node 1 leads, proposes 30 entries with `snapshot_interval_entries=10` and small segments. Then add node 4 as a learner. Node 4 should catch up via `InstallSnapshot` followed by `AppendEntries`. Final state: every node's `TestStateMachine#applied` matches.

- [ ] **Step 1: Write the failing integration test**

Append to `spec/raft/snapshot_spec.cr`:

```crystal
describe "Raft snapshot integration" do
  it "fresh node catches up via InstallSnapshot then AppendEntries" do
    # Reuse the in-process MemoryTransport pattern from existing integration_spec.cr.
    # If your test harness already has a TestCluster helper, prefer that.
    transports = {} of UInt64 => Raft::MemoryTransport
    [1_u64, 2_u64, 3_u64].each { |id| transports[id] = Raft::MemoryTransport.new(id) }
    transports.each do |id, t|
      transports.each { |oid, ot| t.add_peer(oid, ot) if oid != id }
    end

    nodes = {} of UInt64 => Raft::Node(TestData)
    sms = {} of UInt64 => TestStateMachine
    dirs = [] of String

    [1_u64, 2_u64, 3_u64].each do |id|
      d = File.tempname("snap_int_#{id}")
      Dir.mkdir_p(d)
      dirs << d
      cfg = Raft::Config.new
      cfg.data_dir = d
      cfg.snapshot_interval_entries = 10_u64
      cfg.max_segment_size = 200_u32
      cfg.heartbeat_ticks = 1_u32
      cfg.election_timeout_min_ticks = 3_u32
      cfg.election_timeout_max_ticks = 5_u32
      sm = TestStateMachine.new
      sms[id] = sm
      n = Raft::Node(TestData).new(id: id, peers: [1_u64, 2_u64, 3_u64].reject { |x| x == id }, config: cfg, state_machine: sm)
      nodes[id] = n
      transports[id].register_channel(0_u64, n.inbox)
    end

    nodes[1_u64].bootstrap
    drive = ->{
      50.times do
        nodes.each_value(&.tick)
        nodes.each_value do |n|
          n.take_messages.each { |to, m| transports[n.id].send(to, m) }
        end
        Fiber.yield
      end
    }
    drive.call

    leader = nodes.values.find(&.role.leader?).not_nil!
    30.times { |i| leader.propose(TestData.new("v#{i}")) }
    drive.call

    # Every existing node should have applied 30 entries (snapshots happened mid-flight).
    sms.each_value { |sm| sm.applied.size.should eq 30 }
    leader.snapshot_index.should be >= 20_u64

    # Add a fresh node 4
    d4 = File.tempname("snap_int_4")
    Dir.mkdir_p(d4)
    dirs << d4
    cfg4 = Raft::Config.new
    cfg4.data_dir = d4
    cfg4.snapshot_interval_entries = 10_u64
    cfg4.heartbeat_ticks = 1_u32
    cfg4.election_timeout_min_ticks = 3_u32
    cfg4.election_timeout_max_ticks = 5_u32
    sm4 = TestStateMachine.new
    sms[4_u64] = sm4
    n4 = Raft::Node(TestData).new(id: 4_u64, peers: [] of UInt64, config: cfg4, state_machine: sm4)
    nodes[4_u64] = n4
    transports[4_u64] = Raft::MemoryTransport.new(4_u64)
    [1_u64, 2_u64, 3_u64].each do |existing|
      transports[4_u64].add_peer(existing, transports[existing])
      transports[existing].add_peer(4_u64, transports[4_u64])
    end
    transports[4_u64].register_channel(0_u64, n4.inbox)
    leader.add_server(4_u64)

    drive.call
    drive.call

    sm4.applied.size.should eq 30
    sm4.applied.map(&.value).should eq sms[1_u64].applied.map(&.value)

    nodes.each_value(&.close)
    dirs.each { |d| FileUtils.rm_rf(d) rescue nil }
  end
end
```

- [ ] **Step 2: Run test, verify it fails initially (or passes if everything's wired)**

Run: `crystal spec spec/raft/snapshot_spec.cr -Dpreview_mt -Dexecution_context`
Expected: ideally green if tasks 1–6 are correct. If it fails, the diagnostic is usually one of: (a) `add_server` doesn't push the new peer's address through, (b) chunks aren't being re-sent on a partial ack, (c) leader keeps trying `AppendEntries` before checking `snapshot_index`. Trace via metrics counters added in tasks 3 and 5.

- [ ] **Step 3: Iterate until green**

Common fixes if step 2 fails:
- Ensure the leader's per-tick `send_append_entries` loops over **all** peers including learners.
- Ensure `handle_install_snapshot_response` advances `next_index` correctly when the snapshot covers `prev_log_index` exactly.
- Ensure node 4's transport actually receives the leader's `InstallSnapshot` messages (registered both directions in the MemoryTransport mesh).

- [ ] **Step 4: Commit**

```bash
git add spec/raft/snapshot_spec.cr
git commit -m "Add integration test: fresh node catches up via InstallSnapshot"
```

---

## Task 8: Update ARCHITECTURE.md §6.5

**Files:**
- Modify: `ARCHITECTURE.md`

Reflect the work this plan landed: snapshots/log-compaction/InstallSnapshot are no longer "Not implemented"; the §6.5 table and the trailing "two real gaps" paragraph need updating.

- [ ] **Step 1: Update the §6.5 status table**

In `ARCHITECTURE.md` find the row for **Snapshots / log compaction**. Replace it with:

```markdown
| Snapshots / log compaction | ✅ Implemented | `Node` invokes `StateMachine#snapshot`/`restore`; snapshot persisted to a single `snapshot` file (`[index][term][peer_len][peers][sm_bytes]`) atomically; trigger every `Config.snapshot_interval_entries` committed entries; `InstallSnapshot` RPC chunked by `Config.snapshot_chunk_size`. Log truncates segments whose `last_index ≤ snapshot_index`. |
```

- [ ] **Step 2: Update the trailing "two real gaps" paragraph**

Find the paragraph that starts "The two real gaps are **snapshots** and **linearizable reads**." and replace it with:

```markdown
The remaining real gap is **linearizable reads**. The library is structured to support `ReadIndex` / leader-lease style reads — the application can already check `node.role.leader?` and `node.commit_index` — but there is no convenience helper that performs the heartbeat-confirm-leader round before a read.
```

- [ ] **Step 3: Update §1's "missing pieces" bullet**

Find the line `- **Snapshots / log compaction** — the log grows unbounded. See §6.5 for details.` near the top of the doc and remove it. Keep the linearizable-reads bullet.

- [ ] **Step 4: Commit**

```bash
git add ARCHITECTURE.md
git commit -m "Update ARCHITECTURE.md §6.5 to reflect snapshot + log compaction"
```

---

## Self-review checklist

After all tasks land:

1. **Spec coverage of `ARCHITECTURE.md` §6.5:**
   - "log grows unbounded" → fixed by Tasks 3+4.
   - "`MessageType::InstallSnapshot` exists in the enum but `Node` has no handler" → fixed by Tasks 5+6.
   - "no triggering" → Task 3.
   - "no log-truncation path" → Task 4.
   - "`StateMachine#snapshot/restore` are abstract methods, but `Node` has no handler" → Task 1.

2. **Placeholder scan:** grep the plan for `TBD|TODO|FIXME` — should be zero.

3. **Type/method consistency:**
   - `persist_snapshot(index, term)` signature used in Tasks 1, 3, 6. ✓
   - `take_snapshot` defined Task 3, called Task 4 (extends it with truncation). ✓
   - `truncate_before(index)` defined Task 4. ✓
   - `Message` field repurposing for `InstallSnapshot` consistent across send (Task 5) and receive (Task 6). ✓

4. **Edge cases knowingly deferred (PoC scope):**
   - Concurrent config change during snapshot — current code captures `@peers` at snapshot time; if a config change races with persistence, the snapshot may be slightly stale. Acceptable for PoC.
   - Snapshot streaming over an actual TCP transport — `Raft::TCPTransport` already handles arbitrary `Message` types; chunked snapshot bodies up to `Config.snapshot_chunk_size` (1 MB default) fit within `Message::MAX_ENTRIES_DATA_SIZE` (64 MB). No transport change needed.
   - Snapshot chunk loss / re-send — leader retries on heartbeat tick; follower buffer at `snapshot.tmp` is overwritten on offset==0 (handled by `mode = "wb"` when `last_log_index == 0`). Out-of-order chunks within a single attempt are not handled; the protocol assumes in-order delivery (TCPTransport guarantees this within one connection).

5. **Edge cases knowingly deferred (to be revisited):** see the items in §4 above. The `examples/queue/` PoC keeps its current auto-ack `Consume` semantics — no AMQP-style deliver/ack redesign is in scope here or planned as a follow-up.
