# Channel-Based Message Routing Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the inbox array + notify pattern with per-Node buffered channels. The read-loop on each TCP connection deserializes messages and dispatches them to the correct Node's channel by `group_id`. No intermediate arrays, no mutex, thread-safe by design.

**Architecture:** Each Node gets a `Channel(Message)` (capacity 64). The Server owns the routing: its read-loop fibers deserialize from TCP sockets and `send` to the correct Node's channel. Each Node's event loop `select`s on its channel alongside the tick channel. The Transport becomes a simpler "send-only" abstraction — `receive` is removed entirely.

**Tech Stack:** Crystal, `Channel(Message)`, TCP sockets

---

### Current Flow

```
handle_connection fiber:
  Message.from_io(socket) → @inbox << msg → notify.send(nil)

event loop:
  select
  | tick_ch.receive → node.tick
  | transport.notify.receive → transport.receive { |msg| node.step(msg) }
  node.take_messages.each { transport.send(...) }
```

### New Flow

```
Server read-loop fiber (one per inbound TCP connection):
  Message.from_io(socket) → @nodes[msg.group_id].inbox.send(msg)

Node event loop:
  select
  | tick_ch.receive → node.tick
  | msg = node.inbox.receive → node.step(msg)
  node.take_messages.each { transport.send(...) }
```

Key changes:
- `receive` removed from Transport — it's now send-only
- No inbox array, no notify channel — replaced by per-Node `Channel(Message)`
- Read-loop moves from Transport into Server (Server knows about Nodes and group routing)
- Future-ready: the read-loop is where LavinMQ would hook in to write message body to msgstore and substitute a SegmentPosition before dispatching

---

### Task 1: Add inbox channel to Node

**Files:**
- Modify: `src/raft/node.cr`

**Step 1: Add channel property**

Add after line 14 (`getter metrics`):

```crystal
getter inbox : Channel(Message) = Channel(Message).new(64)
```

**Step 2: Run tests to verify no breakage**

Run: `crystal spec`
Expected: All tests pass (channel exists but nothing uses it yet)

**Step 3: Commit**

```bash
git add src/raft/node.cr
git commit -m "feat: add inbox channel to Node for direct message dispatch"
```

---

### Task 2: Remove receive from Transport, make it send-only

**Files:**
- Modify: `src/raft/transport.cr`
- Modify: `src/raft/transport/tcp_transport.cr`
- Modify: `src/raft/transport/memory_transport.cr`
- Modify: `src/raft/server.cr`
- Modify: `spec/raft/transport/memory_transport_spec.cr`
- Modify: `spec/raft/transport/tcp_transport_spec.cr`

**Step 1: Remove abstract receive from Transport**

`src/raft/transport.cr` becomes:

```crystal
module Raft
  abstract class Transport
    abstract def send(to : NodeID, message : Message)
  end
end
```

**Step 2: Rewrite TCPTransport**

Remove: `@inbox`, `notify` channel, `receive` method, message deserialization from `handle_connection`.

Add: `@nodes` hash for routing, `register_nodes` method, read-loop that dispatches to Node channels.

```crystal
class TCPTransport < Transport
  @listen_address : String
  @listen_port : Int32
  @peers : Hash(NodeID, {String, Int32}) = {} of NodeID => {String, Int32}
  @connections : Hash(NodeID, TCPSocket) = {} of NodeID => TCPSocket
  @nodes : Hash(UInt64, Node) = {} of UInt64 => Node   # group_id → Node (for routing)
  @server : TCPServer? = nil
  @running : Bool = false

  def initialize(@listen_address : String, @listen_port : Int32)
  end

  def register_peer(id : NodeID, host : String, port : Int32)
    @peers[id] = {host, port}
  end

  def register_node(group_id : UInt64, node : Node)
    @nodes[group_id] = node
  end

  def start
    @running = true
    @server = server = TCPServer.new(@listen_address, @listen_port)
    spawn(name: "raft-transport-accept") do
      while @running
        if conn = server.accept?
          client = conn
          spawn(name: "raft-transport-conn") do
            handle_connection(client)
          end
        end
      end
    end
  end

  def stop
    @running = false
    @server.try(&.close)
    @connections.each_value { |conn| conn.close rescue nil }
    @connections.clear
  end

  def send(to : NodeID, message : Message)
    conn = get_connection(to)
    return unless conn
    begin
      message.to_io(conn)
      conn.flush
    rescue ex : IO::Error
      @connections.delete(to)
      conn.close rescue nil
    end
  end

  private def handle_connection(client : TCPSocket)
    client.tcp_nodelay = true
    while @running
      msg = Message.from_io(client)
      if node = @nodes[msg.group_id]?
        node.inbox.send(msg)
      end
    end
  rescue IO::EOFError | IO::Error
    client.close rescue nil
  end

  # get_connection stays the same
end
```

Note: `@node_id` removed from constructor — Transport no longer needs to know its own node ID.

**Step 3: Update MemoryTransport**

Remove `receive`. Add node registration and direct channel dispatch on `send`:

```crystal
class MemoryTransport < Transport
  @nodes : Hash(UInt64, Hash(NodeID, Node)) = {} of UInt64 => Hash(NodeID, Node)
  @isolated = Set(NodeID).new

  def register_node(node_id : NodeID, group_id : UInt64, node : Node)
    (@nodes[group_id] ||= {} of NodeID => Node)[node_id] = node
  end

  def send(to : NodeID, message : Message)
    return if @isolated.includes?(to) || @isolated.includes?(message.from)
    if group = @nodes[message.group_id]?
      if node = group[to]?
        node.inbox.send(message)
      end
    end
  end

  def isolate(node : NodeID)
    @isolated.add(node)
  end

  def heal(node : NodeID)
    @isolated.delete(node)
  end
end
```

**Step 4: Remove process_messages from Server**

`src/raft/server.cr` — remove `process_messages` method (lines 29-35). Messages now route directly to Nodes via channels.

**Step 5: Update transport specs**

Rewrite specs to test send → channel receive pattern instead of send → receive method.

**Step 6: Run tests**

Run: `crystal spec`
Expected: All tests pass

**Step 7: Commit**

```bash
git add src/raft/transport.cr src/raft/transport/tcp_transport.cr src/raft/transport/memory_transport.cr src/raft/server.cr spec/
git commit -m "refactor: replace transport inbox with per-Node channel dispatch"
```

---

### Task 3: Update KV example event loop

**Files:**
- Modify: `examples/kv/src/main.cr`

**Step 1: Rewrite event loop**

Replace the `transport.notify.receive` + `transport.receive` pattern with direct channel receive:

```crystal
# Register node with transport for message routing
transport.register_node(0_u64, node)  # group_id 0 for single-group setup

spawn(name: "raft-event-loop") do
  loop do
    select
    when tick_ch.receive
      node.tick
    when msg = node.inbox.receive
      node.step(msg)
    end

    node.take_messages.each do |target_id, msg|
      transport.send(to: target_id, message: msg)
    end

    if node.role != last_role || node.current_term != last_term || node.leader_id != last_leader
      Log.info { "role=#{node.role} term=#{node.current_term} leader=#{node.leader_id}" }
      last_role = node.role
      last_term = node.current_term
      last_leader = node.leader_id
    end
  end
end
```

Also remove the `node_id` from `TCPTransport.new` constructor call.

**Step 2: Build KV example**

Run: `crystal build examples/kv/src/main.cr -o /dev/null --no-codegen`
Expected: Compiles without errors

**Step 3: Commit**

```bash
git add examples/kv/src/main.cr
git commit -m "refactor: update KV example to use Node inbox channel"
```

---

### Task 4: Update node specs that use MemoryTransport

**Files:**
- Modify: `spec/raft/node_spec.cr`
- Modify: `spec/raft/integration_spec.cr`

**Step 1: Update any specs that call transport.receive or process_messages**

Check if node_spec or integration_spec use transport.receive or Server.process_messages. If they use direct `step()` calls (likely), no changes needed.

**Step 2: Run full test suite**

Run: `crystal spec`
Expected: All tests pass

**Step 3: Commit if changes were needed**

---

## Design Notes for Future LavinMQ Integration

The read-loop in `handle_connection` is the natural hook point for LavinMQ:

```crystal
private def handle_connection(client : TCPSocket)
  while @running
    msg = Message.from_io(client)  # ← future: decode_entry writes body to msgstore
    if node = @nodes[msg.group_id]?
      node.inbox.send(msg)         # ← msg now contains SegmentPosition, not full body
    end
  end
end
```

The channel carries lightweight messages (with SegmentPosition) rather than full message bodies. The heavy data is written to disk before it ever enters the channel.

## Verification

1. `crystal spec` — all tests pass
2. `crystal build examples/kv/src/main.cr -o /dev/null --no-codegen` — KV example compiles
3. Optional: `docker compose up --build` in examples/kv to verify cluster operation
