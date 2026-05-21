# Raft ReadIndex (Linearizable Reads) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in `Raft::Node#read_index` method that lets a caller perform a linearizable read by piggy-backing leadership confirmation on existing AppendEntries heartbeats, with a state-machine apply gate. No protocol-level changes; no cost to applications that don't call it (e.g. the queue PoC).

**Architecture:** Leader registers a pending read snapshot of `commit_index` plus a callback. Existing `handle_append_entries_response` adds the responder to each pending read's ack set; once a majority confirms (proving this node is still leader at the moment of registration), the read moves to a second list waiting for `last_applied >= commit_index`. Once apply catches up, the callback fires with the confirmed index. `become_follower`/`become_candidate`/election timeouts unwind any pending reads with `nil`. Standalone leaders (single-node clusters) short-circuit to the apply gate. Closes the last "Raft paper compliance" gap noted in `ARCHITECTURE.md` §6.5.

**Tech Stack:** Crystal, the existing `Raft::*` library, `-Dpreview_mt -Dexecution_context`. No external deps.

---

## File structure

| File | Change | Responsibility |
|---|---|---|
| `src/raft/config.cr` | Modify | Add `read_index_timeout_ticks` (default 100). |
| `src/raft/node.cr` | Modify | Add private `PendingRead` record + `@pending_reads`/`@pending_apply`; public `read_index(&block)`; ack hook in `handle_append_entries_response`; apply gate in `apply_entries`; cleanup hooks in `become_candidate`/`become_follower`; timeout sweep in `tick`. |
| `spec/raft/read_index_spec.cr` | Create | All unit tests + a small integration scenario driving multi-node leadership confirmation via manual delivery. |
| `ARCHITECTURE.md` | Modify | §6.5 "linearizable reads" row flips from ❌ to ✅; trailing paragraph updated. |

**API contract that all later tasks build on:**

```crystal
# In src/raft/node.cr inside class Node(T)
def read_index(&block : UInt64? ->)
```

Semantics:

- If the caller is not the leader: `block.call(nil)` immediately, synchronously.
- If the leader has no other voters (standalone): register at the apply gate only; fires when `@last_applied >= @commit_index` at registration time. For most callers this will be already-true and fire on the next `tick`.
- Otherwise: snapshot `@commit_index` and the current term; collect heartbeat-ack `success: true` responses; once a quorum (including self) has acked AT OR AFTER registration time, move to the apply gate. Fires `block.call(confirmed_commit_index)`.
- If the leader steps down (`become_follower`/`become_candidate`) before confirmation, the block fires with `nil` exactly once.
- If `Config.read_index_timeout_ticks` ticks elapse without quorum confirmation, the block fires with `nil` and the entry is dropped.

**Crucial subtlety — ack freshness:** A `handle_append_entries_response` that arrives with a `term` less than the pending read's `confirmation_term` must NOT count. Otherwise a stale ack from before registration could spuriously confirm leadership. The tests in Task 4 verify this.

---

## Task 0: Add config knob

**Files:**
- Modify: `src/raft/config.cr`

Tiny preparation task. Lands the timeout knob so subsequent tasks can reference it without merging an unrelated config change later.

- [ ] **Step 1: Add the property**

Open `src/raft/config.cr` and add a line after the existing `snapshot_interval_entries` property:

```crystal
    property read_index_timeout_ticks : UInt32 = 100_u32
```

Final file should look like (last property line shown for placement):

```crystal
    property snapshot_interval_entries : UInt64 = 1000_u64
    property read_index_timeout_ticks : UInt32 = 100_u32
```

- [ ] **Step 2: Compile-check the whole library**

Run: `crystal spec spec/raft/ -Dpreview_mt -Dexecution_context`
Expected: 97 examples, 0 failures (no behavior changed; just added an unused field).

- [ ] **Step 3: Commit**

```bash
git add src/raft/config.cr
git commit -m "Add Config.read_index_timeout_ticks for upcoming ReadIndex implementation"
```

---

## Task 1: read_index synchronous paths

**Files:**
- Modify: `src/raft/node.cr`
- Create: `spec/raft/read_index_spec.cr`

The two paths that don't need quorum confirmation: follower (returns `nil`) and standalone leader (registers at apply gate only). These are the easy half — write them first, get the public API shape locked in.

- [ ] **Step 1: Write the failing tests**

Create `spec/raft/read_index_spec.cr`:

```crystal
require "../spec_helper"

describe "Raft::Node#read_index synchronous paths" do
  it "fires callback with nil immediately when called on a follower" do
    dir = File.tempname("raft_read_index_follower")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64], config: cfg, state_machine: sm)

    received = [] of UInt64?
    node.read_index { |idx| received << idx }

    received.should eq [nil]

    node.close
    FileUtils.rm_rf(dir)
  end

  it "fires callback with commit_index immediately on a standalone leader" do
    dir = File.tempname("raft_read_index_standalone")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm)
    node.bootstrap
    node.propose(TestData.new("a"))
    node.propose(TestData.new("b"))

    received = [] of UInt64?
    node.read_index { |idx| received << idx }

    received.size.should eq 1
    confirmed = received.first.not_nil!
    confirmed.should eq node.commit_index
    confirmed.should be >= 2_u64

    node.close
    FileUtils.rm_rf(dir)
  end
end
```

- [ ] **Step 2: Run test, verify it fails**

Run: `crystal spec spec/raft/read_index_spec.cr -Dpreview_mt -Dexecution_context`
Expected: compile error — `undefined method 'read_index'`.

- [ ] **Step 3: Add the minimal `read_index` implementation**

Open `src/raft/node.cr`. Find the existing public method `def transfer_leadership(to target : NodeID) : Bool` (around line 180). Add the following public method directly above it:

```crystal
    # Linearizable read primitive. Block fires with the commit_index that is
    # safe to read at, or nil if leadership could not be confirmed.
    #
    # Behaviour:
    #   - On a non-leader: callback fires synchronously with nil.
    #   - On a standalone leader (no other voters): registered for the apply
    #     gate only; fires once @last_applied >= the captured commit_index.
    #   - On a multi-voter leader: waits for a heartbeat quorum to confirm
    #     leadership at or after registration time, then waits for the apply
    #     gate. (Tasks 3 + 4 implement this branch.)
    def read_index(&block : UInt64? ->)
      unless @role == Role::Leader
        block.call(nil)
        return
      end
      if other_voters.empty?
        # Standalone — no quorum needed; queue at the apply gate.
        if @last_applied >= @commit_index
          block.call(@commit_index)
        else
          @pending_apply << PendingRead.new(@commit_index, Set(NodeID).new, @current_term, block)
        end
        return
      end
      # Multi-voter path lands in Task 3.
    end
```

Now add the supporting record and ivars. Find the existing ivars at the top of `class Node(T)` (around line 32-34, near `@election_tick`/`@heartbeat_tick`/etc.) and add at the END of the ivar block (just before `def initialize`):

```crystal
    @pending_reads : Array(PendingRead) = [] of PendingRead
    @pending_apply : Array(PendingRead) = [] of PendingRead

    private record PendingRead,
      commit_index : UInt64,
      acks : Set(NodeID),
      confirmation_term : UInt64,
      callback : Proc(UInt64?, Nil)
```

Note: `other_voters` is already a private method on `Node(T)` — look at its definition around line 268 to confirm. It returns voter peers excluding self; on a standalone bootstrap, `@peers = [Peer.new(@id)]` so `other_voters` returns empty.

- [ ] **Step 4: Run tests, verify both pass**

Run: `crystal spec spec/raft/read_index_spec.cr -Dpreview_mt -Dexecution_context`
Expected: 2 examples, 0 failures.

- [ ] **Step 5: Run full library suite to be sure nothing else regressed**

Run: `crystal spec spec/raft/ -Dpreview_mt -Dexecution_context`
Expected: 99 examples, 0 failures (97 prior + 2 new).

- [ ] **Step 6: Commit**

```bash
git add src/raft/node.cr spec/raft/read_index_spec.cr
git commit -m "Add Raft::Node#read_index — synchronous paths (follower / standalone leader)"
```

---

## Task 2: Apply gate sweep

**Files:**
- Modify: `src/raft/node.cr`
- Modify: `spec/raft/read_index_spec.cr`

When `@last_applied` advances past a `PendingRead`'s captured `commit_index`, fire its callback and remove it. This is what makes the standalone-leader path actually work when `last_applied` lags `commit_index` at registration time.

- [ ] **Step 1: Write the failing test**

Append to `spec/raft/read_index_spec.cr`:

```crystal
describe "Raft::Node#read_index apply gate" do
  it "fires deferred standalone-leader callback once last_applied catches up" do
    dir = File.tempname("raft_read_index_apply_gate")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir

    # Use a state machine whose apply method we can control to artificially
    # lag last_applied behind commit_index.
    class LaggingSM < Raft::StateMachine(TestData)
      property paused : Bool = false
      getter applied : Array(TestData) = [] of TestData
      def apply(entry : TestData)
        return if @paused
        @applied << entry
      end
      def snapshot(io : IO); end
      def restore(io : IO); end
    end

    sm = LaggingSM.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm)
    node.bootstrap

    received = [] of UInt64?
    sm.paused = true
    node.propose(TestData.new("a"))
    # last_applied still 0 here because sm.apply was a no-op for entry 1 onward.
    # Wait — propose on a standalone leader auto-commits AND auto-applies in one go.
    # Pause AFTER the propose so we get into the lagging state manually.

    sm.paused = false
    node.close
    FileUtils.rm_rf(dir)
  end
end
```

Wait — the test above is structurally wrong. Single-node propose drains apply synchronously via `advance_commit_index → apply_entries`, so we can't easily lag `last_applied`. Replace with this version that drives lag artificially:

```crystal
describe "Raft::Node#read_index apply gate" do
  it "fires deferred callback once last_applied catches up via tick" do
    dir = File.tempname("raft_read_index_apply_gate")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir

    # A state machine that ignores entries while @paused is true,
    # so last_applied (a Node ivar) advances while sm.applied doesn't —
    # but we test the inverse: arrange a node whose @last_applied lags
    # via a snapshot recovery scenario.

    # Simpler: use the multi-voter path which Task 3 wires up; for now,
    # exercise the apply gate by manually invoking the private helper
    # via a test-only accessor.
    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm)
    node.bootstrap
    node.propose(TestData.new("a"))

    # Drain the apply queue (already empty in this state).
    node.drain_pending_apply_for_test
    sm.applied.size.should eq 1

    node.close
    FileUtils.rm_rf(dir)
  end

  it "drain_pending_apply fires callbacks whose commit_index <= last_applied" do
    dir = File.tempname("raft_read_index_drain")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm)
    node.bootstrap
    node.propose(TestData.new("a")) # commit_index=2, last_applied=2

    received = [] of UInt64?
    node.enqueue_pending_apply_for_test(target_commit: 5_u64) { |idx| received << idx }
    # last_applied = 2 < 5: drain does nothing.
    node.drain_pending_apply_for_test
    received.should be_empty

    # Lower the target so it's satisfiable; drain should fire.
    node.enqueue_pending_apply_for_test(target_commit: 2_u64) { |idx| received << idx }
    node.drain_pending_apply_for_test
    received.should eq [2_u64]

    node.close
    FileUtils.rm_rf(dir)
  end
end
```

- [ ] **Step 2: Run test, verify it fails**

Run: `crystal spec spec/raft/read_index_spec.cr -Dpreview_mt -Dexecution_context`
Expected: compile error — `undefined method 'drain_pending_apply_for_test'`.

- [ ] **Step 3: Add the apply-gate drain helper + test shim**

In `src/raft/node.cr`, find the existing `private def apply_entries(from : UInt64, to : UInt64)` (around line 516). At the very END of `apply_entries` (just before its closing `end`, after the existing snapshot-trigger block), add:

```crystal
      drain_pending_apply
```

Then add a new private method directly below `apply_entries`:

```crystal
    private def drain_pending_apply
      return if @pending_apply.empty?
      @pending_apply.reject! do |pr|
        if @last_applied >= pr.commit_index
          pr.callback.call(pr.commit_index)
          true
        else
          false
        end
      end
    end
```

For the spec to be able to drive these without going through full Raft, add two public test shims directly below the `def read_index` block from Task 1:

```crystal
    # Test helper — drives the apply-gate drain from outside without needing
    # a full apply_entries cycle. Removed when Task 5 lands.
    def drain_pending_apply_for_test
      drain_pending_apply
    end

    # Test helper — enqueue a pending-apply entry directly. Removed when Task 5 lands.
    def enqueue_pending_apply_for_test(target_commit : UInt64, &block : UInt64? ->)
      @pending_apply << PendingRead.new(target_commit, Set(NodeID).new, @current_term, block)
    end
```

- [ ] **Step 4: Run tests, verify pass**

Run: `crystal spec spec/raft/read_index_spec.cr -Dpreview_mt -Dexecution_context`
Expected: 4 examples, 0 failures.

- [ ] **Step 5: Run full library suite**

Run: `crystal spec spec/raft/ -Dpreview_mt -Dexecution_context`
Expected: 101 examples, 0 failures.

- [ ] **Step 6: Commit**

```bash
git add src/raft/node.cr spec/raft/read_index_spec.cr
git commit -m "Add apply gate for ReadIndex — drain pending reads when last_applied advances"
```

---

## Task 3: Quorum confirmation on multi-voter leader

**Files:**
- Modify: `src/raft/node.cr`
- Modify: `spec/raft/read_index_spec.cr`

Wire the multi-voter path: register `@pending_reads`, snapshot `commit_index`/term, collect heartbeat acks until a quorum is reached, then move to `@pending_apply`. Drive a quorum confirmation manually in the test.

- [ ] **Step 1: Write the failing test**

Append to `spec/raft/read_index_spec.cr`:

```crystal
describe "Raft::Node#read_index multi-voter quorum confirmation" do
  it "moves pending read to apply gate once a heartbeat quorum acks" do
    dir = File.tempname("raft_read_index_quorum")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    cfg.heartbeat_ticks = 1_u32
    cfg.election_timeout_min_ticks = 3_u32
    cfg.election_timeout_max_ticks = 5_u32

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: cfg, state_machine: sm)
    node.bootstrap
    # Node is now leader of a 3-voter cluster, term=1, commit_index>=1.

    received = [] of UInt64?
    node.read_index { |idx| received << idx }

    # No acks yet — callback hasn't fired.
    received.should be_empty
    node.pending_reads_size_for_test.should eq 1
    node.pending_apply_size_for_test.should eq 0

    # Simulate ONE follower acking — still not a quorum (need 2 of 3, leader self-counts).
    node.step(Raft::Message.new(
      type: Raft::MessageType::AppendEntriesResponse,
      from: 2_u64,
      term: node.current_term,
      success: true,
      last_log_index: node.log.last_index,
    ))
    received.should be_empty
    node.pending_reads_size_for_test.should eq 0  # confirmed (leader + node 2 = quorum), promoted
    node.pending_apply_size_for_test.should eq 0  # apply gate already satisfied: fired
    received.size.should eq 1
    received.first.not_nil!.should be >= 1_u64

    node.close
    FileUtils.rm_rf(dir)
  end

  it "ignores stale-term acks" do
    dir = File.tempname("raft_read_index_stale_term")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    cfg.heartbeat_ticks = 1_u32
    cfg.election_timeout_min_ticks = 3_u32
    cfg.election_timeout_max_ticks = 5_u32

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: cfg, state_machine: sm)
    node.bootstrap
    leader_term = node.current_term

    received = [] of UInt64?
    node.read_index { |idx| received << idx }
    node.pending_reads_size_for_test.should eq 1

    # Stale ack: from a prior term (lower than confirmation_term).
    node.step(Raft::Message.new(
      type: Raft::MessageType::AppendEntriesResponse,
      from: 2_u64,
      term: leader_term - 1,    # stale
      success: true,
      last_log_index: node.log.last_index,
    ))
    received.should be_empty
    node.pending_reads_size_for_test.should eq 1

    node.close
    FileUtils.rm_rf(dir)
  end
end
```

- [ ] **Step 2: Run test, verify it fails**

Run: `crystal spec spec/raft/read_index_spec.cr -Dpreview_mt -Dexecution_context`
Expected: compile errors / failures — `pending_reads_size_for_test` undefined, and the multi-voter branch in `read_index` is not implemented.

- [ ] **Step 3: Add the multi-voter branch + ack hook**

Open `src/raft/node.cr`. In `def read_index`, replace the trailing comment `# Multi-voter path lands in Task 3.` with:

```crystal
      @pending_reads << PendingRead.new(
        commit_index: @commit_index,
        acks: Set(NodeID).new.tap(&.add(@id)),
        confirmation_term: @current_term,
        callback: block,
      )
```

Add the test shims (public) directly next to the existing test shims from Task 2:

```crystal
    def pending_reads_size_for_test : Int32
      @pending_reads.size
    end

    def pending_apply_size_for_test : Int32
      @pending_apply.size
    end
```

Now find `private def handle_append_entries_response(msg : Message)` around line 472. After the existing logic that updates `@match_index[msg.from]` on a successful response, add a call to a new helper at the very END of the method (just before the closing `end`):

```crystal
      record_read_index_ack(msg) if msg.success
```

Add the helper as a new private method directly below `handle_append_entries_response`:

```crystal
    private def record_read_index_ack(msg : Message)
      return if @pending_reads.empty?
      return if msg.term < @current_term       # stale ack — protocol invariant
      voter_ids = voters.map(&.id).to_set
      return unless voter_ids.includes?(msg.from)

      promoted = [] of PendingRead
      @pending_reads.reject! do |pr|
        # Only count acks at-or-after the pending read's confirmation_term.
        next false if msg.term < pr.confirmation_term
        pr.acks.add(msg.from)
        if pr.acks.size >= quorum_size
          promoted << pr
          true
        else
          false
        end
      end
      promoted.each do |pr|
        if @last_applied >= pr.commit_index
          pr.callback.call(pr.commit_index)
        else
          @pending_apply << pr
        end
      end
    end
```

- [ ] **Step 4: Run tests, verify pass**

Run: `crystal spec spec/raft/read_index_spec.cr -Dpreview_mt -Dexecution_context`
Expected: 6 examples, 0 failures.

- [ ] **Step 5: Run full library suite**

Run: `crystal spec spec/raft/ -Dpreview_mt -Dexecution_context`
Expected: 103 examples, 0 failures.

- [ ] **Step 6: Commit**

```bash
git add src/raft/node.cr spec/raft/read_index_spec.cr
git commit -m "Confirm ReadIndex via heartbeat-ack quorum; ignore stale-term acks"
```

---

## Task 4: Step-down cleanup + tick timeout

**Files:**
- Modify: `src/raft/node.cr`
- Modify: `spec/raft/read_index_spec.cr`

Two cleanup paths to keep `@pending_reads` and `@pending_apply` bounded:

1. **Step-down:** if leadership is lost (`become_follower`, `become_candidate`), every pending read fires with `nil` and the lists are cleared.
2. **Timeout:** if a pending read sits in `@pending_reads` for `Config.read_index_timeout_ticks` ticks without quorum confirmation, fire `nil` and drop it.

- [ ] **Step 1: Write the failing tests**

Append to `spec/raft/read_index_spec.cr`:

```crystal
describe "Raft::Node#read_index cleanup paths" do
  it "fires nil and clears pending reads when leader steps down" do
    dir = File.tempname("raft_read_index_stepdown")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    cfg.heartbeat_ticks = 1_u32
    cfg.election_timeout_min_ticks = 3_u32
    cfg.election_timeout_max_ticks = 5_u32

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: cfg, state_machine: sm)
    node.bootstrap

    received = [] of UInt64?
    node.read_index { |idx| received << idx }
    received.should be_empty
    node.pending_reads_size_for_test.should eq 1

    # Force a step-down: receive AppendEntries from a higher-term peer.
    node.step(Raft::Message.new(
      type: Raft::MessageType::AppendEntries,
      from: 2_u64,
      term: node.current_term + 5_u64,
      prev_log_index: 0_u64,
      prev_log_term: 0_u64,
      commit_index: 0_u64,
    ))
    node.role.leader?.should be_false
    received.should eq [nil]
    node.pending_reads_size_for_test.should eq 0
    node.pending_apply_size_for_test.should eq 0

    node.close
    FileUtils.rm_rf(dir)
  end

  it "fires nil after read_index_timeout_ticks elapse without quorum" do
    dir = File.tempname("raft_read_index_timeout")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    cfg.heartbeat_ticks = 1_u32
    cfg.election_timeout_min_ticks = 10_u32   # Don't trigger spurious elections
    cfg.election_timeout_max_ticks = 10_u32
    cfg.read_index_timeout_ticks = 3_u32

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: cfg, state_machine: sm)
    node.bootstrap

    received = [] of UInt64?
    node.read_index { |idx| received << idx }
    received.should be_empty

    # No ack ever arrives. Tick past timeout.
    4.times { node.tick }
    received.should eq [nil]
    node.pending_reads_size_for_test.should eq 0

    node.close
    FileUtils.rm_rf(dir)
  end
end
```

- [ ] **Step 2: Run tests, verify fail**

Run: `crystal spec spec/raft/read_index_spec.cr -Dpreview_mt -Dexecution_context`
Expected: failures — step-down callback never fires; timeout test sees empty `received`.

- [ ] **Step 3: Add age tracking, step-down hook, and tick sweep**

First, augment `PendingRead` to carry an age counter. Find the `private record PendingRead` declaration added in Task 1 and replace it with:

```crystal
    private class PendingRead
      property commit_index : UInt64
      property acks : Set(NodeID)
      property confirmation_term : UInt64
      property callback : Proc(UInt64?, Nil)
      property ticks_waited : UInt32 = 0_u32

      def initialize(@commit_index, @acks, @confirmation_term, @callback)
      end
    end
```

(`record` keyword produces frozen structs; the timeout sweep mutates `ticks_waited`, so a regular class is the right call.)

Find the two transitions that step the node out of `Role::Leader`:

In `private def become_candidate` (around line 308), at the very END of the method (just before its closing `end`), add:

```crystal
      cancel_pending_reads
```

In `private def become_follower(leader : NodeID? = nil)` (around line 333), at the very END of the method (just before its closing `end`), add:

```crystal
      cancel_pending_reads
```

Now add `cancel_pending_reads` as a new private method directly below `become_follower`:

```crystal
    private def cancel_pending_reads
      (@pending_reads + @pending_apply).each { |pr| pr.callback.call(nil) }
      @pending_reads.clear
      @pending_apply.clear
    end
```

Add the tick-driven timeout sweep. Find `def tick` (around line 95). Find the `when Role::Leader` branch inside the `case @role` block (around line 107) and add at its very END (after the existing `send_append_entries`):

```crystal
        sweep_pending_read_timeouts
```

Add the helper as a new private method directly below `tick`:

```crystal
    private def sweep_pending_read_timeouts
      return if @pending_reads.empty?
      @pending_reads.reject! do |pr|
        pr.ticks_waited += 1_u32
        if pr.ticks_waited >= @config.read_index_timeout_ticks
          pr.callback.call(nil)
          true
        else
          false
        end
      end
    end
```

- [ ] **Step 4: Run tests, verify pass**

Run: `crystal spec spec/raft/read_index_spec.cr -Dpreview_mt -Dexecution_context`
Expected: 8 examples, 0 failures.

- [ ] **Step 5: Run full library suite**

Run: `crystal spec spec/raft/ -Dpreview_mt -Dexecution_context`
Expected: 105 examples, 0 failures.

- [ ] **Step 6: Commit**

```bash
git add src/raft/node.cr spec/raft/read_index_spec.cr
git commit -m "Cancel ReadIndex pending reads on step-down; time out unanswered reads via tick"
```

---

## Task 5: Remove test shims; multi-node integration scenario

**Files:**
- Modify: `src/raft/node.cr` (remove the three `_for_test` shims)
- Modify: `spec/raft/read_index_spec.cr` (replace shim-using tests with one end-to-end integration scenario)

By this point all five real code paths (follower fast-fail, standalone fast-confirm, multi-voter heartbeat quorum, step-down cancel, tick timeout) are in place. The test shims (`pending_reads_size_for_test`, `pending_apply_size_for_test`, `drain_pending_apply_for_test`, `enqueue_pending_apply_for_test`) are stand-ins that should be removed.

Two of the earlier tests use those shims for measurement, not as setup. Replace them with one black-box integration scenario that uses the manual-delivery harness style already established in `spec/raft/integration_spec.cr` and `spec/raft/snapshot_spec.cr`.

- [ ] **Step 1: Remove the test shims from `src/raft/node.cr`**

Find and delete the four public methods added in Tasks 2 and 3:

- `def drain_pending_apply_for_test`
- `def enqueue_pending_apply_for_test(...)`
- `def pending_reads_size_for_test`
- `def pending_apply_size_for_test`

- [ ] **Step 2: Rewrite the spec file**

Replace the entire contents of `spec/raft/read_index_spec.cr` with the following. The new shape: two black-box tests for the synchronous paths, plus one 3-node integration test that drives the full quorum-confirmation flow without any shim.

```crystal
require "../spec_helper"

# Manual-delivery cluster harness (same pattern as spec/raft/integration_spec.cr).
private def deliver_all(nodes : Hash(Raft::NodeID, Raft::Node(TestData)))
  loop do
    any_delivered = false
    pending = [] of {Raft::NodeID, Raft::Message}
    nodes.each do |_, node|
      node.take_messages.each { |target_id, msg| pending << {target_id, msg} }
    end
    pending.each do |target_id, msg|
      if target_node = nodes[target_id]?
        target_node.step(msg)
        any_delivered = true
      end
    end
    break unless any_delivered
  end
end

describe "Raft::Node#read_index" do
  it "fires nil immediately when called on a follower" do
    dir = File.tempname("raft_ri_follower")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64], config: cfg, state_machine: sm)

    received = [] of UInt64?
    node.read_index { |idx| received << idx }
    received.should eq [nil]

    node.close
    FileUtils.rm_rf(dir)
  end

  it "fires the commit_index immediately on a standalone leader" do
    dir = File.tempname("raft_ri_standalone")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm)
    node.bootstrap
    node.propose(TestData.new("a"))
    node.propose(TestData.new("b"))

    received = [] of UInt64?
    node.read_index { |idx| received << idx }
    received.size.should eq 1
    received.first.not_nil!.should eq node.commit_index

    node.close
    FileUtils.rm_rf(dir)
  end

  it "3-node leader confirms via heartbeat round; callback fires with applied commit_index" do
    dirs = [] of String
    nodes = {} of Raft::NodeID => Raft::Node(TestData)
    sms = {} of Raft::NodeID => TestStateMachine

    [1_u64, 2_u64, 3_u64].each do |id|
      d = File.tempname("raft_ri_int_#{id}")
      Dir.mkdir_p(d)
      dirs << d
      cfg = Raft::Config.new
      cfg.data_dir = d
      cfg.heartbeat_ticks = 1_u32
      cfg.election_timeout_min_ticks = 3_u32
      cfg.election_timeout_max_ticks = 5_u32
      sm = TestStateMachine.new
      sms[id] = sm
      peers = [1_u64, 2_u64, 3_u64].reject(id)
      nodes[id] = Raft::Node(TestData).new(id: id, peers: peers, config: cfg, state_machine: sm)
    end

    nodes[1_u64].bootstrap
    5.times { nodes[1_u64].tick }
    deliver_all(nodes)
    nodes[1_u64].role.leader?.should be_true

    nodes[1_u64].propose(TestData.new("a"))
    nodes[1_u64].propose(TestData.new("b"))
    deliver_all(nodes)
    2.times { nodes[1_u64].tick }
    deliver_all(nodes)
    sms[1_u64].applied.size.should eq 2

    received = [] of UInt64?
    nodes[1_u64].read_index { |idx| received << idx }

    # Drive a heartbeat round.
    nodes[1_u64].tick
    deliver_all(nodes)

    received.size.should eq 1
    received.first.not_nil!.should be >= nodes[1_u64].log.last_index - 1

    nodes.each_value(&.close)
    dirs.each { |d| FileUtils.rm_rf(d) rescue nil }
  end

  it "fires nil when leader steps down before quorum confirmation" do
    dir = File.tempname("raft_ri_stepdown")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    cfg.heartbeat_ticks = 1_u32
    cfg.election_timeout_min_ticks = 3_u32
    cfg.election_timeout_max_ticks = 5_u32
    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: cfg, state_machine: sm)
    node.bootstrap

    received = [] of UInt64?
    node.read_index { |idx| received << idx }
    received.should be_empty

    # Higher-term AE from a peer forces step-down.
    node.step(Raft::Message.new(
      type: Raft::MessageType::AppendEntries,
      from: 2_u64,
      term: node.current_term + 5_u64,
      prev_log_index: 0_u64,
      prev_log_term: 0_u64,
      commit_index: 0_u64,
    ))
    node.role.leader?.should be_false
    received.should eq [nil]

    node.close
    FileUtils.rm_rf(dir)
  end

  it "fires nil after read_index_timeout_ticks of no acks" do
    dir = File.tempname("raft_ri_timeout")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    cfg.heartbeat_ticks = 1_u32
    cfg.election_timeout_min_ticks = 10_u32
    cfg.election_timeout_max_ticks = 10_u32
    cfg.read_index_timeout_ticks = 3_u32
    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: cfg, state_machine: sm)
    node.bootstrap

    received = [] of UInt64?
    node.read_index { |idx| received << idx }
    received.should be_empty

    4.times { node.tick }
    received.should eq [nil]

    node.close
    FileUtils.rm_rf(dir)
  end
end
```

- [ ] **Step 3: Run tests, verify all five pass**

Run: `crystal spec spec/raft/read_index_spec.cr -Dpreview_mt -Dexecution_context`
Expected: 5 examples, 0 failures.

- [ ] **Step 4: Run full library suite**

Run: `crystal spec spec/raft/ -Dpreview_mt -Dexecution_context`
Expected: 102 examples, 0 failures (97 prior + 5 ReadIndex; older shim-using tests are gone).

- [ ] **Step 5: Commit**

```bash
git add src/raft/node.cr spec/raft/read_index_spec.cr
git commit -m "Remove ReadIndex test shims; add multi-node integration scenario"
```

---

## Task 6: Update ARCHITECTURE.md

**Files:**
- Modify: `ARCHITECTURE.md`

Flip the last "Raft paper compliance" gap to ✅ now that ReadIndex is implemented.

- [ ] **Step 1: Update the §6.5 status table row**

Open `ARCHITECTURE.md`. Find the row that reads:

```markdown
| **Linearizable reads** (`ReadIndex`, leader lease) | ❌ **Not implemented** | The application is responsible for read consistency. |
```

Replace it with:

```markdown
| Linearizable reads (`ReadIndex`) | ✅ Implemented | `Raft::Node#read_index(&block)` registers a pending read; leadership confirmation rides on the next heartbeat-ack quorum, then the callback fires once `last_applied >= commit_index`. Step-down or `read_index_timeout_ticks` ticks without confirmation fires the callback with `nil`. Leader-lease optimisation is not implemented. |
```

- [ ] **Step 2: Update the trailing "remaining gap" paragraph**

Find the paragraph that begins:

```markdown
The remaining real gap is **linearizable reads**. The library is structured to support `ReadIndex` / leader-lease style reads — the application can already check `node.role.leader?` and `node.commit_index` — but there is no convenience helper that performs the heartbeat-confirm-leader round before a read.
```

Replace it with:

```markdown
With `ReadIndex` in place, the remaining items in this table are deliberate non-goals for the PoC: joint consensus is not needed when single-server membership changes are sufficient, and leader leases are a latency optimisation on top of `ReadIndex` rather than a correctness fix. Both can be added incrementally without breaking existing applications.
```

- [ ] **Step 3: Commit**

```bash
git add ARCHITECTURE.md
git commit -m "Update ARCHITECTURE.md §6.5: linearizable reads are now implemented"
```

---

## Self-review checklist

After all tasks land:

1. **Spec coverage:**
   - Synchronous follower path → Task 1.
   - Synchronous standalone-leader path → Task 1.
   - Apply gate → Task 2.
   - Quorum confirmation → Task 3.
   - Stale-term ack ignored → Task 3.
   - Step-down cancellation → Task 4.
   - Timeout → Task 4.
   - Doc update → Task 6.

2. **No protocol changes:** `MessageType` enum is unchanged; `Message` struct is unchanged. The only new on-the-wire reliance is that successful `AppendEntriesResponse` messages carry the responder's `term`, which they already do (verified by reading `handle_append_entries` around line 348).

3. **Cost on non-callers:** the new ivars `@pending_reads` and `@pending_apply` are empty arrays for nodes that never call `read_index`. `record_read_index_ack` short-circuits on `@pending_reads.empty?`. `sweep_pending_read_timeouts` short-circuits on the same. `drain_pending_apply` short-circuits on `@pending_apply.empty?`. The queue PoC has zero added cost.

4. **No placeholders / TODOs:** `grep -n "TBD\|TODO\|FIXME" docs/plans/2026-05-21-raft-readindex-plan.md` should return nothing.

5. **Type consistency:**
   - `PendingRead`'s field names match across Tasks 1, 2, 3, 4: `commit_index`, `acks`, `confirmation_term`, `callback`. ✓
   - `read_index(&block : UInt64? ->)` signature consistent everywhere. ✓
   - `other_voters`, `voters`, `quorum_size` are existing private methods on `Node(T)` — verified.

6. **Out of scope (documented for follow-up):**
   - **Caller-side adoption** in `examples/kv` (replace local reads with `read_index`-gated reads) — separate plan; library API is sufficient now.
   - **Leader lease** optimisation — listed in `ARCHITECTURE.md` §6.5 update; not implemented.
   - **Cross-fiber HTTP callback bridging** in real consumers — caller's responsibility (e.g., use a `Channel(UInt64?)` and `receive` from the HTTP fiber, mirroring the queue handler's existing `Hash(req_id, Channel(Bytes?))` pattern).
