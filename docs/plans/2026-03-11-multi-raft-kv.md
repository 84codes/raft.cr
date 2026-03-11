# Multi-Raft KV Store Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** One Raft group per key in the KV example, with a meta group (group 0) that coordinates group creation across the cluster. Each group runs in its own execution context for true parallelism.

**Architecture:**
- Main EC: TCP read-loop dispatches to per-group inbox channels. TCP write-loop drains a shared outbox channel.
- Per-group EC: each Raft group runs its own event loop fiber, selecting on inbox channel + timeout for ticks. Outgoing messages sent to shared outbox channel.
- Meta group (group 0): replicates "create group" commands. On apply, creates new Raft group with initial value.
- Data groups: one per key, replicates Put/Delete commands.

**Tech Stack:** Crystal, existing Raft library, channels for cross-EC communication

---

## Threading Model

```
Main EC:
  ┌─ TCP read-loop fiber (one per inbound connection):
  │    msg = Message.from_io(socket)
  │    nodes[msg.group_id].inbox.send(msg)     ← per-group channel
  │
  └─ TCP write-loop fiber:
       {to, msg} = outbox.receive               ← shared outbox channel
       socket.write(msg.to_io)

Group EC (one per Raft group):
  loop:
    select
    | msg = inbox.receive → node.step(msg)
    | timeout 50ms        → node.tick
    node.take_messages.each { |to, msg| outbox.send({to, msg}) }
```

**Channels:**
- N `inbox` channels (one per group) — main EC → group EC
- 1 `outbox` channel (shared) — all group ECs → main EC

---

## Library Changes

### Task 1: Add group_id to Node and set it on outgoing messages

Node needs to stamp outgoing messages with its group_id so the receiving transport can route them correctly.

**Files:**
- Modify: `src/raft/node.cr`
- Modify: `src/raft/server.cr`

**Step 1: Add group_id to Node constructor**

```crystal
getter group_id : UInt64

def initialize(@id : NodeID, @peers : Array(NodeID), @config : Config, @state_machine : StateMachine(T), @metrics : Metrics? = nil, @group_id : UInt64 = 0_u64)
```

**Step 2: Update take_messages to stamp group_id**

```crystal
def take_messages : Array({NodeID, Message})
  if @partitioned
    @outbox.clear
    return [] of {NodeID, Message}
  end
  messages = @outbox.map do |target_id, msg|
    msg.group_id = @group_id
    {target_id, msg}
  end
  @outbox.clear
  messages
end
```

**Step 3: Update Server.add_group to pass group_id**

```crystal
@nodes[group_id] = Node(T).new(id: node_id, peers: peers, config: group_config, state_machine: state_machine, group_id: group_id)
```

Remove `msg.group_id = group_id` from `take_all_messages` since Node now handles it.

**Step 4: Run tests**

Run: `crystal spec`
Expected: All tests pass

**Step 5: Commit**

```bash
git add src/raft/node.cr src/raft/server.cr
git commit -m "feat: add group_id to Node, stamp outgoing messages automatically"
```

---

### Task 2: Add outbox channel to TCPTransport

The transport needs a write-loop fiber that drains a shared outbox channel and sends messages over TCP. Group ECs send outgoing messages to this channel instead of calling transport.send directly (which would be a cross-EC socket write).

**Files:**
- Modify: `src/raft/transport/tcp_transport.cr`

**Step 1: Add outbox channel and write-loop**

```crystal
getter outbox : Channel({NodeID, Message}) = Channel({NodeID, Message}).new(256)

def start
  # ... existing accept loop ...

  # Write-loop: drains outbox and sends over TCP
  spawn(name: "raft-transport-write") do
    while @running
      target_id, msg = @outbox.receive
      send(to: target_id, message: msg)
    end
  end
end
```

Group ECs send via: `transport.outbox.send({target_id, msg})`

The existing `send` method stays as-is (used by the write-loop fiber in the main EC).

**Step 2: Run tests**

Run: `crystal spec`

**Step 3: Commit**

```bash
git add src/raft/transport/tcp_transport.cr
git commit -m "feat: add outbox channel and write-loop to TCPTransport"
```

---

## Example Changes

### Task 3: Add CreateGroup action to KVCommand

**Files:**
- Modify: `examples/kv/src/kv_command.cr`

**Step 1: Add CreateGroup to KVAction**

```crystal
enum KVAction : UInt8
  Put         = 0
  Delete      = 1
  CreateGroup = 2
end
```

No other changes needed — KVCommand already has key and value fields. CreateGroup will carry both key (the new key name) and value (the initial value).

**Step 2: Commit**

```bash
git add examples/kv/src/kv_command.cr
git commit -m "feat(kv): add CreateGroup action for multi-raft group management"
```

---

### Task 4: Create MetaStateMachine

**Files:**
- Create: `examples/kv/src/meta_state_machine.cr`

The meta state machine tracks which keys have Raft groups and assigns sequential group_ids. When a CreateGroup is committed, it calls a callback to create the actual Raft group with the initial value.

```crystal
require "../../../src/raft"

class MetaStateMachine < Raft::StateMachine(KVCommand)
  @groups : Hash(String, UInt64) = {} of String => UInt64
  @next_group_id : UInt64 = 1_u64
  @on_create_group : Proc(String, UInt64, String?, Nil)

  def initialize(&@on_create_group : String, UInt64, String? ->)
  end

  def apply(entry : KVCommand)
    return unless entry.action == KVAction::CreateGroup
    return if @groups.has_key?(entry.key)

    group_id = @next_group_id
    @next_group_id += 1
    @groups[entry.key] = group_id
    initial_value = entry.value.empty? ? nil : entry.value
    @on_create_group.call(entry.key, group_id, initial_value)
  end

  def group_for(key : String) : UInt64?
    @groups[key]?
  end

  def all_groups : Hash(String, UInt64)
    @groups
  end
end
```

**Step 1: Create the file**

**Step 2: Commit**

```bash
git add examples/kv/src/meta_state_machine.cr
git commit -m "feat(kv): add MetaStateMachine for multi-raft group coordination"
```

---

### Task 5: Create ValueStateMachine

**Files:**
- Create: `examples/kv/src/value_state_machine.cr`

Per-key state machine. Stores one key's value. Can be initialized with a value (from CreateGroup).

```crystal
require "../../../src/raft"

class ValueStateMachine < Raft::StateMachine(KVCommand)
  property value : String? = nil

  def initialize(@value : String? = nil)
  end

  def apply(entry : KVCommand)
    case entry.action
    when KVAction::Put    then @value = entry.value
    when KVAction::Delete then @value = nil
    end
  end
end
```

**Step 1: Create the file**

**Step 2: Commit**

```bash
git add examples/kv/src/value_state_machine.cr
git commit -m "feat(kv): add ValueStateMachine for per-key Raft groups"
```

---

### Task 6: Rewrite main.cr for multi-raft with per-group execution contexts

**Files:**
- Modify: `examples/kv/src/main.cr`

**Architecture:**
- `nodes`: `Hash(UInt64, Raft::Node(KVCommand))` — group_id → node
- `value_machines`: `Hash(UInt64, ValueStateMachine)` — group_id → SM
- Meta group (group_id 0) created at startup
- Data groups created dynamically by MetaStateMachine callback
- Each group gets its own event loop fiber
- Outgoing messages go through `transport.outbox` channel

```crystal
nodes = Hash(UInt64, Raft::Node(KVCommand)).new
value_machines = Hash(UInt64, ValueStateMachine).new
base_data_dir = ENV["DATA_DIR"]? || "/data/raft"

# Start a group's event loop — runs in its own fiber
start_group_loop = ->(node : Raft::Node(KVCommand), outbox : Channel({Raft::NodeID, Raft::Message})) {
  spawn(name: "raft-group-#{node.group_id}") do
    loop do
      select
      when msg = node.inbox.receive
        node.step(msg)
      when timeout(50.milliseconds)
        node.tick
      end
      node.take_messages.each do |target_id, msg|
        outbox.send({target_id, msg})
      end
    end
  end
}

# Helper to create a node for a group
create_node = ->(group_id : UInt64, sm : Raft::StateMachine(KVCommand)) {
  cfg = Raft::Config.new
  cfg.data_dir = File.join(base_data_dir, "group-#{group_id}")
  cfg.election_timeout_min_ticks = 10_u32
  cfg.election_timeout_max_ticks = 20_u32
  cfg.heartbeat_ticks = 2_u32
  Dir.mkdir_p(cfg.data_dir)
  node = Raft::Node(KVCommand).new(
    id: node_id, peers: peer_ids, config: cfg,
    state_machine: sm, group_id: group_id
  )
  nodes[group_id] = node
  transport.register_channel(group_id, node.inbox)
  start_group_loop.call(node, transport.outbox)
  node
}

# Meta group (group 0)
meta_sm = MetaStateMachine.new do |key, gid, initial_value|
  vsm = ValueStateMachine.new(initial_value)
  create_node.call(gid, vsm)
  value_machines[gid] = vsm
end

create_node.call(0_u64, meta_sm)
```

**Step 1: Rewrite main.cr**

Remove: tick_ch, shared event loop, old single-node setup
Add: per-group event loops, create_node helper, meta group setup

**Step 2: Verify it compiles**

Run: `crystal build examples/kv/src/main.cr -o /dev/null --no-codegen`

**Step 3: Commit**

```bash
git add examples/kv/src/main.cr
git commit -m "feat(kv): multi-raft with per-group event loops and channel-based transport"
```

---

### Task 7: Update HTTP handler for multi-raft routing

**Files:**
- Modify: `examples/kv/src/kv_http_handler.cr`

The handler needs meta_sm, nodes, and value_machines to route requests.

**PUT /kv/:key flow:**
1. Check `meta_sm.group_for(key)`
2. If group exists and this node is leader for it → `node.propose(Put(key, value))`
3. If group exists but not leader → return 503 with leader_id
4. If no group → propose `CreateGroup(key, value)` to meta node (initial value included!)
   - Return 202 accepted (group will be created + value set via meta consensus)

**GET /kv/:key flow:**
1. Look up group_id from meta_sm
2. Read value from value_machines[group_id]

**GET / (web UI):**
- Aggregate all values from all value_machines
- Show which node is leader for each key

```crystal
class KVHttpHandler
  include HTTP::Handler

  @meta_node : Raft::Node(KVCommand)
  @meta_sm : MetaStateMachine
  @nodes : Hash(UInt64, Raft::Node(KVCommand))
  @value_machines : Hash(UInt64, ValueStateMachine)

  def initialize(@meta_node, @meta_sm, @nodes, @value_machines)
  end

  private def handle_put(context, key)
    value = context.request.body.try(&.gets_to_end) || ""

    if group_id = @meta_sm.group_for(key)
      if node = @nodes[group_id]?
        unless node.role == Raft::Role::Leader
          context.response.status_code = 503
          context.response.content_type = "application/json"
          context.response.print({error: "not leader for key", leader_id: node.leader_id}.to_json)
          return
        end
        node.propose(KVCommand.new(KVAction::Put, key, value))
        context.response.status_code = 202
        context.response.content_type = "application/json"
        context.response.print({status: "accepted", key: key}.to_json)
      end
    else
      # Create group with initial value via meta consensus
      unless @meta_node.role == Raft::Role::Leader
        context.response.status_code = 503
        context.response.content_type = "application/json"
        context.response.print({error: "not meta leader", leader_id: @meta_node.leader_id}.to_json)
        return
      end
      @meta_node.propose(KVCommand.new(KVAction::CreateGroup, key, value))
      context.response.status_code = 202
      context.response.content_type = "application/json"
      context.response.print({status: "accepted", key: key}.to_json)
    end
  end

  private def handle_get(context, key)
    if group_id = @meta_sm.group_for(key)
      if vsm = @value_machines[group_id]?
        if v = vsm.value
          context.response.content_type = "application/json"
          context.response.print({key: key, value: v}.to_json)
          return
        end
      end
    end
    context.response.status_code = 404
    context.response.content_type = "application/json"
    context.response.print({error: "key not found"}.to_json)
  end
end
```

**Step 1: Rewrite kv_http_handler.cr**

Update web UI to show per-key leaders and aggregate values from all value_machines.

**Step 2: Verify it compiles**

**Step 3: Commit**

```bash
git add examples/kv/src/kv_http_handler.cr
git commit -m "feat(kv): multi-raft HTTP handler with group routing and initial values"
```

---

### Task 8: Docker verification

**Step 1: Build and run**

```bash
cd examples/kv && docker compose up --build
```

**Step 2: Test multi-raft flow**

```bash
# Create key "foo" with initial value — one request, no retry needed
curl -X PUT http://localhost:8081/kv/foo -d "hello"
# Returns 202 accepted

# Wait for meta consensus + data group creation
sleep 2

# Read the value (may need to hit the right node)
curl http://localhost:8081/kv/foo
curl http://localhost:8082/kv/foo
curl http://localhost:8083/kv/foo

# Create another key — may get different leader
curl -X PUT http://localhost:8081/kv/bar -d "world"
sleep 2
curl http://localhost:8081/kv/bar

# Update existing key
curl -X PUT http://localhost:8081/kv/foo -d "updated"
sleep 1
curl http://localhost:8081/kv/foo
```

**Step 3: Commit any fixes**

---

## Data Flow Summary

```
PUT /kv/foo "hello" → node 1 (first time, no group exists)

1. meta_sm.group_for("foo") → nil
2. meta_node.propose(CreateGroup("foo", "hello"))
3. Meta group replicates to majority via outbox channel → TCP → peer inboxes
4. On commit, each node's meta_sm.apply:
   - Assigns group_id = 1 to "foo"
   - Calls on_create_group("foo", 1, "hello")
   - Creates ValueStateMachine with initial_value = "hello"
   - Creates Raft Node for group 1 with own inbox channel
   - Starts group event loop fiber
   - Registers channel with transport
5. Group 1 elects a leader (could be any node)
6. Value is already set from creation — no second request needed

PUT /kv/foo "updated" → node 1 (group exists)

1. meta_sm.group_for("foo") → 1
2. nodes[1].role == Leader? If yes: nodes[1].propose(Put("foo", "updated"))
3. Data group replicates to majority
4. On commit, value_machines[1].value = "updated"
```

## Channel Architecture

```
Per inbound TCP connection (Main EC):
  Message.from_io(socket) → channels[msg.group_id].send(msg)

Per group (own EC):
  inbox.receive → node.step(msg) → node.take_messages → outbox.send({to, msg})
                  ↑ timeout 50ms → node.tick ↗

Write-loop (Main EC):
  outbox.receive → socket.write(msg.to_io)
```

## Notes

- Group IDs are assigned sequentially by MetaStateMachine (deterministic across all nodes)
- All nodes run the same callback, so all create the same groups at the same commit index
- Different data groups naturally elect different leaders → distributed writes
- The meta group is a single point of serialization for group creation only, not for data writes
- Initial value is set via meta consensus — no client retry needed for first PUT
- Each group has its own inbox channel (cross-EC safe) and shares one outbox channel
