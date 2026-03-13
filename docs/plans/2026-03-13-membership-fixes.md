# Membership Changes Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix critical and important issues found during code review of the Raft membership changes implementation.

**Architecture:** Six targeted fixes to `node.cr`, `handler.cr`, and `main.cr` plus tests. Each fix is independent except Task 6 (tests) which depends on all prior tasks. The fixes harden the Raft protocol correctness: preventing concurrent config changes, ensuring bootstrap writes a Configuration log entry, preventing learners from starting elections, making `apply_entries` safe against removed-node edge cases, fixing mutation-during-iteration in the KV example, and moving membership admin endpoints out from behind the debug flag.

**Tech Stack:** Crystal, Raft consensus library

**Build/test command:** `crystal spec -Dpreview_mt -Dexecution_context -Draft_debug`

---

### Task 1: Prevent concurrent configuration changes

Only one uncommitted configuration entry should exist at a time (Raft paper Section 6). Without this, rapid `add_server` + auto-`promote_learner` can create overlapping config entries.

**Files:**
- Modify: `src/raft/node.cr`
- Test: `spec/raft/node_spec.cr`

**Step 1: Write the failing test**

Add to the `membership changes` describe block in `spec/raft/node_spec.cr`:

```crystal
it "rejects concurrent configuration changes" do
  config = Raft::Config.new
  config.election_timeout_min_ticks = 5_u32
  config.election_timeout_max_ticks = 5_u32
  config.heartbeat_ticks = 100_u32

  nodes = {
    1_u64 => create_test_node(1_u64, [2_u64, 3_u64], config),
    2_u64 => create_test_node(2_u64, [1_u64, 3_u64], config),
    3_u64 => create_test_node(3_u64, [1_u64, 2_u64], config),
  }
  elect_leader(nodes)
  leader = nodes[1_u64]

  # First config change should succeed
  leader.add_server(4_u64).should eq true

  # Second config change should be rejected (first is uncommitted)
  leader.add_server(5_u64).should eq false

  # Commit the first change by replicating and getting responses
  messages = leader.take_messages
  messages.each do |target_id, msg|
    nodes[target_id].step(msg) if nodes.has_key?(target_id)
  end
  [2_u64, 3_u64].each do |id|
    nodes[id].take_messages.each do |target_id, msg|
      leader.step(msg) if target_id == 1_u64
    end
  end

  # Now the second config change should succeed
  leader.add_server(5_u64).should eq true

  nodes.each_value(&.close)
end
```

**Step 2: Run test to verify it fails**

Run: `crystal spec spec/raft/node_spec.cr -Dpreview_mt -Dexecution_context -Draft_debug --tag "rejects concurrent"`
Expected: FAIL — `add_server(5)` returns `true` instead of `false`

**Step 3: Implement the fix**

In `src/raft/node.cr`:

1. Add a field to track whether a config change is pending:
```crystal
@pending_config_index : UInt64 = 0_u64
```

2. In `append_configuration`, record the index of the config entry:
```crystal
private def append_configuration(new_peers : Array(Peer))
  config_bytes = serialize_peers(new_peers)
  entry = @log.append(term: @current_term, entry_type: EntryType::Configuration, config_data: config_bytes)
  @pending_config_index = entry.index
  # ... rest unchanged
```

Note: `Log#append` already returns a `LogEntry(T)`. Use `entry.index`.

3. Add a guard at the top of `add_server`, `remove_server`, and `promote_learner`:
```crystal
return false if @pending_config_index > @commit_index
```

4. Clear the pending index when the config entry is committed. In `apply_configuration`:
```crystal
private def apply_configuration(entry : LogEntry(T))
  @pending_config_index = 0_u64
  @on_configuration_change.try(&.call(@peers))
end
```

**Step 4: Run test to verify it passes**

Run: `crystal spec -Dpreview_mt -Dexecution_context -Draft_debug`
Expected: All specs pass (69 existing + 1 new = 70)

**Step 5: Commit**

```bash
git add src/raft/node.cr spec/raft/node_spec.cr
git commit -m "fix: prevent concurrent configuration changes (Raft paper Section 6)"
```

---

### Task 2: Bootstrap appends a Configuration log entry

`bootstrap` currently only appends a Noop. The initial single-node configuration should be a Configuration entry so the log is the source of truth for all config changes.

**Files:**
- Modify: `src/raft/node.cr`
- Test: `spec/raft/node_spec.cr`

**Step 1: Write the failing test**

Add to the `membership changes` describe block:

```crystal
it "bootstrap writes a configuration entry to the log" do
  node = create_test_node(1_u64, [] of UInt64)
  node.bootstrap

  # Log should have one entry: the configuration
  node.log.last_index.should eq 1_u64
  entry = node.log.get(1_u64)
  entry.entry_type.should eq Raft::EntryType::Configuration
  entry.config_data.size.should be > 0

  node.close
end
```

**Step 2: Run test to verify it fails**

Run: `crystal spec spec/raft/node_spec.cr -Dpreview_mt -Dexecution_context -Draft_debug --tag "bootstrap writes"`
Expected: FAIL — `entry.entry_type` is `Noop`, not `Configuration`

**Step 3: Implement the fix**

In `src/raft/node.cr`, change the `bootstrap` method:

```crystal
def bootstrap : Bool
  return false unless @peers.empty?
  @peers = [Peer.new(@id)]
  @current_term = 1_u64
  @role = Role::Leader
  @leader_id = @id
  config_bytes = serialize_peers
  @log.append(term: @current_term, entry_type: EntryType::Configuration, config_data: config_bytes)
  @commit_index = @log.last_index  # single-node cluster, immediately committed
  persist_state
  true
end
```

Key changes:
- Append `Configuration` instead of `Noop`
- Serialize the single-node peer list into the entry
- Set `commit_index` to the entry's index (single-node = immediately committed)

**Step 4: Run test to verify it passes**

Run: `crystal spec -Dpreview_mt -Dexecution_context -Draft_debug`
Expected: All specs pass

**Step 5: Commit**

```bash
git add src/raft/node.cr spec/raft/node_spec.cr
git commit -m "fix: bootstrap appends Configuration entry instead of Noop"
```

---

### Task 3: Prevent learners from starting elections

A learner should never start pre-vote or become a candidate. Currently `start_pre_vote` only checks `@peers.empty?` but doesn't check if the local node is a voter.

**Files:**
- Modify: `src/raft/node.cr`
- Test: `spec/raft/node_spec.cr`

**Step 1: Write the failing test**

Add to the `membership changes` describe block:

```crystal
it "learner does not start elections" do
  config = Raft::Config.new
  config.election_timeout_min_ticks = 2_u32
  config.election_timeout_max_ticks = 2_u32
  config.heartbeat_ticks = 100_u32

  nodes = {
    1_u64 => create_test_node(1_u64, [2_u64, 3_u64], config),
    2_u64 => create_test_node(2_u64, [1_u64, 3_u64], config),
    3_u64 => create_test_node(3_u64, [1_u64, 2_u64], config),
  }
  elect_leader(nodes)
  leader = nodes[1_u64]

  # Add node 4 as learner — send config to node 4 so it knows it's a learner
  leader.add_server(4_u64)
  node4 = create_test_node(4_u64, [] of UInt64, config)
  messages = leader.take_messages
  messages.each do |target_id, msg|
    node4.step(msg) if target_id == 4_u64
  end

  # Node 4 should be a learner and should NOT start elections
  node4.peers.find { |p| p.id == 4_u64 }.not_nil!.learner?.should eq true
  10.times { node4.tick }
  node4.role.should eq Raft::Role::Follower
  node4.take_messages.should be_empty

  node4.close
  nodes.each_value(&.close)
end
```

**Step 2: Run test to verify it fails**

Run: `crystal spec spec/raft/node_spec.cr -Dpreview_mt -Dexecution_context -Draft_debug --tag "learner does not start"`
Expected: FAIL — node 4 sends PreVote messages despite being a learner

**Step 3: Implement the fix**

In `src/raft/node.cr`, add a voter check at the start of `start_pre_vote`:

```crystal
private def start_pre_vote
  return if @peers.empty? # standalone node, no elections
  return unless @peers.any? { |p| p.id == @id && p.voter? } # learners don't elect
  # ... rest unchanged
```

**Step 4: Run test to verify it passes**

Run: `crystal spec -Dpreview_mt -Dexecution_context -Draft_debug`
Expected: All specs pass

**Step 5: Commit**

```bash
git add src/raft/node.cr spec/raft/node_spec.cr
git commit -m "fix: prevent learners from starting elections"
```

---

### Task 4: Guard `apply_entries` against removed nodes

If code is reordered in the future, a removed node (empty peers) could reach `apply_entries` and fire `on_configuration_change` with stale state. Add a defensive guard.

**Files:**
- Modify: `src/raft/node.cr`

**Step 1: Implement the fix**

In `src/raft/node.cr`, update `apply_entries` to break early if a configuration entry causes removal:

```crystal
private def apply_entries(from : UInt64, to : UInt64)
  (from..to).each do |i|
    entry = @log.get(i)
    if entry.entry_type == EntryType::Configuration
      apply_configuration(entry)
      break if @peers.empty? # removed from cluster
    elsif data = entry.data
      @state_machine.apply(data)
      @metrics.try(&.increment("raft_entries_applied_total"))
    end
  end
end
```

**Step 2: Run tests to verify nothing breaks**

Run: `crystal spec -Dpreview_mt -Dexecution_context -Draft_debug`
Expected: All specs pass

**Step 3: Commit**

```bash
git add src/raft/node.cr
git commit -m "fix: guard apply_entries against removed-node edge case"
```

---

### Task 5: Fix mutation-during-iteration in `on_configuration_change` callback

In `examples/kv/src/main.cr`, the removal loop calls `remove_server` which mutates `@peers` while iterating over it. Fix by collecting IDs to remove first.

**Files:**
- Modify: `examples/kv/src/main.cr`

**Step 1: Implement the fix**

Replace the `on_configuration_change` callback (lines 97-117) with:

```crystal
meta_node.on_configuration_change do |new_peers|
  nodes.each do |gid, data_node|
    next if gid == 0_u64 # skip meta group itself
    next unless data_node.role == Raft::Role::Leader

    # Add new peers
    new_peers.each do |p|
      next if p.id == node_id
      unless data_node.peers.any? { |dp| dp.id == p.id }
        data_node.add_server(p.id)
      end
    end

    # Collect IDs to remove first, then remove (avoids mutation during iteration)
    to_remove = data_node.peers.select do |dp|
      dp.id != node_id && !new_peers.any? { |p| p.id == dp.id }
    end.map(&.id)
    to_remove.each { |id| data_node.remove_server(id) }
  end
end
```

**Step 2: Build to verify**

Run: `crystal build examples/kv/src/main.cr -Dpreview_mt -Dexecution_context -Draft_debug --no-codegen`
Expected: Compiles without errors

**Step 3: Commit**

```bash
git add examples/kv/src/main.cr
git commit -m "fix: avoid mutation during iteration in on_configuration_change callback"
```

---

### Task 6: Move membership admin endpoints out of `raft_debug` flag

Bootstrap, add_server, remove_server, register_peer, promote_learner, and transfer_leadership are needed for production. Only pause/resume/partition/heal/reset should be debug-only.

**Files:**
- Modify: `src/raft/http/handler.cr`

**Step 1: Implement the fix**

Restructure `handler.cr` to split admin routes into two groups. The `call` method should check for admin routes outside the debug flag:

```crystal
def call(context : ::HTTP::Server::Context)
  method = context.request.method
  path = context.request.path

  case {method, path}
  when {"GET", "/raft/status"}
    handle_status(context)
  when {"GET", "/raft/log"}
    handle_log(context)
  when {"GET", "/raft/metrics"}
    handle_metrics(context)
  else
    if method == "POST" && path.starts_with?("/raft/admin/")
      handle_admin(context, path)
      return
    end
    call_next(context)
  end
end
```

Then split `handle_admin` — membership endpoints are always available, debug endpoints are gated:

```crystal
private def handle_admin(context, path)
  case path
  when "/raft/admin/bootstrap"
    if @node.bootstrap
      json_response(context, 200, {"status" => "bootstrapped"})
    else
      json_response(context, 400, {"error" => "failed to bootstrap (node may already have peers)"})
    end
  when "/raft/admin/register_peer"
    if transport = @transport
      body = context.request.body.try(&.gets_to_end)
      if body
        data = JSON.parse(body)
        id = data["id"].as_i64.to_u64
        host = data["host"].as_s
        port = data["port"].as_i
        transport.register_peer(id, host, port)
        json_response(context, 200, {"status" => "registered", "id" => id.to_s})
      else
        json_response(context, 400, {"error" => "missing body"})
      end
    else
      json_response(context, 503, {"error" => "no transport configured"})
    end
  else
    if path.starts_with?("/raft/admin/add_server/")
      node_id = path.split("/").last.to_u64
      if @node.add_server(node_id)
        json_response(context, 200, {"status" => "added", "node_id" => node_id.to_s})
      else
        json_response(context, 400, {"error" => "failed to add server"})
      end
    elsif path.starts_with?("/raft/admin/remove_server/")
      node_id = path.split("/").last.to_u64
      if @node.remove_server(node_id)
        json_response(context, 200, {"status" => "removed", "node_id" => node_id.to_s})
      else
        json_response(context, 400, {"error" => "failed to remove server"})
      end
    elsif path.starts_with?("/raft/admin/promote_learner/")
      node_id = path.split("/").last.to_u64
      if @node.promote_learner(node_id)
        json_response(context, 200, {"status" => "promoted", "node_id" => node_id.to_s})
      else
        json_response(context, 400, {"error" => "failed to promote learner"})
      end
    elsif path.starts_with?("/raft/admin/transfer_leadership/")
      node_id = path.split("/").last.to_u64
      if @node.transfer_leadership(to: node_id)
        json_response(context, 200, {"status" => "transferring", "target" => node_id.to_s})
      else
        json_response(context, 400, {"error" => "failed to transfer leadership"})
      end
    else
      {% if flag?(:raft_debug) %}
        handle_debug_admin(context, path)
      {% else %}
        context.response.status_code = 404
        context.response.print "Unknown admin action"
      {% end %}
    end
  end
end

{% if flag?(:raft_debug) %}
  private def handle_debug_admin(context, path)
    case path
    when "/raft/admin/pause"
      @node.pause
      json_response(context, 200, {"status" => "paused"})
    when "/raft/admin/resume"
      @node.resume
      json_response(context, 200, {"status" => "resumed"})
    when "/raft/admin/partition"
      @node.partition
      json_response(context, 200, {"status" => "partitioned"})
    when "/raft/admin/heal"
      @node.heal
      json_response(context, 200, {"status" => "healed"})
    when "/raft/admin/reset"
      @node.reset
      json_response(context, 200, {"status" => "reset"})
    else
      context.response.status_code = 404
      context.response.print "Unknown admin action"
    end
  end
{% end %}
```

**Step 2: Build to verify**

Run both with and without debug flag:
```bash
crystal build examples/kv/src/main.cr -Dpreview_mt -Dexecution_context --no-codegen
crystal build examples/kv/src/main.cr -Dpreview_mt -Dexecution_context -Draft_debug --no-codegen
```
Expected: Both compile without errors

**Step 3: Commit**

```bash
git add src/raft/http/handler.cr
git commit -m "fix: move membership admin endpoints out of raft_debug flag"
```

---

### Task 7: Add missing test coverage

Add tests for vote rejection from non-members and configuration propagation to followers.

**Files:**
- Modify: `spec/raft/node_spec.cr`

**Step 1: Write the tests**

Add to the `membership changes` describe block:

```crystal
it "rejects RequestVote from non-member" do
  config = Raft::Config.new
  config.election_timeout_min_ticks = 5_u32
  config.election_timeout_max_ticks = 5_u32

  node = create_test_node(1_u64, [2_u64, 3_u64], config)

  # Node 99 is not a member — its vote request should be rejected
  vote_msg = Raft::Message.new(
    type: Raft::MessageType::RequestVote,
    from: 99_u64,
    term: 1_u64,
    last_log_index: 0_u64,
    last_log_term: 0_u64,
  )
  node.step(vote_msg)

  messages = node.take_messages
  messages.size.should eq 1
  messages[0][0].should eq 99_u64
  messages[0][1].success.should eq false

  node.close
end

it "rejects PreVote from non-member" do
  config = Raft::Config.new
  config.election_timeout_min_ticks = 5_u32
  config.election_timeout_max_ticks = 5_u32

  node = create_test_node(1_u64, [2_u64, 3_u64], config)

  prevote_msg = Raft::Message.new(
    type: Raft::MessageType::PreVote,
    from: 99_u64,
    term: 1_u64,
    last_log_index: 0_u64,
    last_log_term: 0_u64,
  )
  node.step(prevote_msg)

  messages = node.take_messages
  messages.size.should eq 1
  messages[0][0].should eq 99_u64
  messages[0][1].success.should eq false

  node.close
end

it "follower applies configuration entry from leader (add node)" do
  config = Raft::Config.new
  config.election_timeout_min_ticks = 5_u32
  config.election_timeout_max_ticks = 5_u32
  config.heartbeat_ticks = 100_u32

  nodes = {
    1_u64 => create_test_node(1_u64, [2_u64, 3_u64], config),
    2_u64 => create_test_node(2_u64, [1_u64, 3_u64], config),
    3_u64 => create_test_node(3_u64, [1_u64, 2_u64], config),
  }
  elect_leader(nodes)
  leader = nodes[1_u64]

  # Add node 4 on the leader
  leader.add_server(4_u64)

  # Deliver to follower node 2
  messages = leader.take_messages
  messages.each do |target_id, msg|
    nodes[target_id].step(msg) if target_id == 2_u64
  end

  # Follower should now know about node 4
  nodes[2_u64].peers.any? { |p| p.id == 4_u64 }.should eq true

  nodes.each_value(&.close)
end
```

**Step 2: Run tests**

Run: `crystal spec -Dpreview_mt -Dexecution_context -Draft_debug`
Expected: All specs pass

**Step 3: Commit**

```bash
git add spec/raft/node_spec.cr
git commit -m "test: add coverage for vote rejection and follower config propagation"
```
