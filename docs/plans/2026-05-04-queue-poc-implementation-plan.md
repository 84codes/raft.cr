# Queue PoC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a multi-raft queue PoC at `examples/queue/` that mirrors the KV example's structure, so we can demonstrate the unbounded-log problem in `ARCHITECTURE.md` §6.5.

**Architecture:** Meta group (group 0) maps `queue_name → group_id`; each queue is its own Raft group with a `Deque(Bytes)` state machine. HTTP API supports publish + one-shot consume. The consume return-value problem is bridged via a per-state-machine `Hash(String, Channel(Bytes?))` keyed by per-request UUIDs (see spec §4).

**Tech Stack:** Crystal, the existing `Raft::*` library (`-Dpreview_mt -Dexecution_context`), `HTTP::Server`/`HTTP::Handler` (stdlib), `UUID` (stdlib). No external dependencies.

**Spec:** See `docs/plans/2026-05-04-queue-poc-design.md` for the design this plan implements.

---

## File Structure

```
examples/queue/
├── docker-compose.yml          # 3-node cluster + Prometheus + Grafana
├── Dockerfile                  # build container
├── prometheus.yml              # scrape config
├── grafana/                    # dashboards (queue depth vs log size)
├── README.md                   # how to run / what to look for
├── spec/
│   ├── queue_state_machine_spec.cr   # unit tests for the queue SM
│   ├── meta_state_machine_spec.cr    # unit tests for the meta SM
│   └── queue_integration_spec.cr     # 3-node MemoryTransport cluster tests
└── src/
    ├── main.cr                       # wires up transport, groups, HTTP server
    ├── queue_command.cr              # the T type + QueueAction enum
    ├── meta_state_machine.cr         # queue_name → group_id mapping
    ├── queue_state_machine.cr        # one queue's Deque + req_id channel hash
    └── queue_http_handler.cr         # publish, consume, list, delete, SSE, web UI
```

Each file has one responsibility:

- `queue_command.cr` — wire/log format for all queue commands
- `meta_state_machine.cr` — admin (create/delete queue) state
- `queue_state_machine.cr` — one queue's contents + req_id bridge
- `queue_http_handler.cr` — HTTP API + leader proxy + web UI
- `main.cr` — process entrypoint, wires everything together

---

## Task 1: QueueCommand type with serialization

**Files:**
- Create: `examples/queue/src/queue_command.cr`
- Test: `examples/queue/spec/queue_command_spec.cr`

The `QueueCommand` struct is the `T` parameter for both meta and queue state machines, mirroring how `KVCommand` works for KV. Since `Publish` carries arbitrary message bytes, the `body` field is `Bytes` (not `String`). The `req_id` field is a UUID rendered as a 36-char string.

- [ ] **Step 1: Write the failing serialization test**

Create `examples/queue/spec/queue_command_spec.cr`:

```crystal
require "spec"
require "../src/queue_command"

describe QueueCommand do
  it "round-trips Publish through to_io / from_io" do
    cmd = QueueCommand.new(
      action: QueueAction::Publish,
      queue_name: "orders",
      body: Bytes[1, 2, 3, 4, 5],
      req_id: "",
    )
    io = IO::Memory.new
    cmd.to_io(io)
    io.rewind
    parsed = QueueCommand.from_io(io)
    parsed.action.should eq QueueAction::Publish
    parsed.queue_name.should eq "orders"
    parsed.body.should eq Bytes[1, 2, 3, 4, 5]
    parsed.req_id.should eq ""
  end

  it "round-trips Consume with a req_id" do
    cmd = QueueCommand.new(
      action: QueueAction::Consume,
      queue_name: "orders",
      body: Bytes.new(0),
      req_id: "550e8400-e29b-41d4-a716-446655440000",
    )
    io = IO::Memory.new
    cmd.to_io(io)
    io.rewind
    parsed = QueueCommand.from_io(io)
    parsed.action.should eq QueueAction::Consume
    parsed.queue_name.should eq "orders"
    parsed.req_id.should eq "550e8400-e29b-41d4-a716-446655440000"
  end

  it "round-trips CreateQueue and DeleteQueue" do
    [QueueAction::CreateQueue, QueueAction::DeleteQueue].each do |action|
      cmd = QueueCommand.new(action: action, queue_name: "x", body: Bytes.new(0), req_id: "")
      io = IO::Memory.new
      cmd.to_io(io)
      io.rewind
      parsed = QueueCommand.from_io(io)
      parsed.action.should eq action
      parsed.queue_name.should eq "x"
    end
  end
end
```

- [ ] **Step 2: Run test to verify it fails**

Run: `crystal spec examples/queue/spec/queue_command_spec.cr -Dpreview_mt -Dexecution_context`
Expected: compile error — `queue_command.cr` does not exist yet.

- [ ] **Step 3: Implement the QueueCommand struct**

Create `examples/queue/src/queue_command.cr`:

```crystal
enum QueueAction : UInt8
  Publish     = 0
  Consume     = 1
  CreateQueue = 2
  DeleteQueue = 3
end

struct QueueCommand
  getter action : QueueAction
  getter queue_name : String
  getter body : Bytes
  getter req_id : String

  def initialize(@action : QueueAction, @queue_name : String, @body : Bytes = Bytes.new(0), @req_id : String = "")
  end

  def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian)
    io.write_bytes(@action.value, format)
    io.write_bytes(@queue_name.bytesize.to_u32, format)
    io.write(@queue_name.to_slice)
    io.write_bytes(@body.size.to_u32, format)
    io.write(@body)
    io.write_bytes(@req_id.bytesize.to_u32, format)
    io.write(@req_id.to_slice)
  end

  def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
    action = QueueAction.new(io.read_bytes(UInt8, format))
    qn_size = io.read_bytes(UInt32, format)
    qn_buf = Bytes.new(qn_size)
    io.read_fully(qn_buf)
    body_size = io.read_bytes(UInt32, format)
    body = Bytes.new(body_size)
    io.read_fully(body) if body_size > 0
    req_size = io.read_bytes(UInt32, format)
    req_buf = Bytes.new(req_size)
    io.read_fully(req_buf)
    new(action: action, queue_name: String.new(qn_buf), body: body, req_id: String.new(req_buf))
  end
end
```

- [ ] **Step 4: Run test to verify it passes**

Run: `crystal spec examples/queue/spec/queue_command_spec.cr -Dpreview_mt -Dexecution_context`
Expected: 3 examples, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add examples/queue/src/queue_command.cr examples/queue/spec/queue_command_spec.cr
git commit -m "Add QueueCommand type for queue PoC"
```

---

## Task 2: MetaStateMachine

**Files:**
- Create: `examples/queue/src/meta_state_machine.cr`
- Test: `examples/queue/spec/meta_state_machine_spec.cr`

Mirrors `examples/kv/src/meta_state_machine.cr` exactly — the only differences are the type name and that the create callback takes no `initial_value` (queues start empty).

- [ ] **Step 1: Write failing tests**

Create `examples/queue/spec/meta_state_machine_spec.cr`:

```crystal
require "spec"
require "../../../src/raft"
require "../src/queue_command"
require "../src/meta_state_machine"

describe MetaStateMachine do
  it "assigns increasing group IDs on CreateQueue and fires create callback" do
    created = [] of {String, UInt64}
    deleted = [] of {String, UInt64}
    sm = MetaStateMachine.new(
      on_delete_group: ->(name : String, gid : UInt64) { deleted << {name, gid}; nil },
    ) { |name, gid| created << {name, gid}; nil }

    sm.apply(QueueCommand.new(QueueAction::CreateQueue, "orders"))
    sm.apply(QueueCommand.new(QueueAction::CreateQueue, "events"))

    created.should eq [{"orders", 1_u64}, {"events", 2_u64}]
    sm.group_for("orders").should eq 1_u64
    sm.group_for("events").should eq 2_u64
  end

  it "ignores duplicate CreateQueue" do
    created = [] of {String, UInt64}
    sm = MetaStateMachine.new(
      on_delete_group: ->(name : String, gid : UInt64) { nil },
    ) { |name, gid| created << {name, gid}; nil }

    sm.apply(QueueCommand.new(QueueAction::CreateQueue, "orders"))
    sm.apply(QueueCommand.new(QueueAction::CreateQueue, "orders"))

    created.size.should eq 1
  end

  it "fires delete callback on DeleteQueue" do
    deleted = [] of {String, UInt64}
    sm = MetaStateMachine.new(
      on_delete_group: ->(name : String, gid : UInt64) { deleted << {name, gid}; nil },
    ) { |name, gid| nil }

    sm.apply(QueueCommand.new(QueueAction::CreateQueue, "orders"))
    sm.apply(QueueCommand.new(QueueAction::DeleteQueue, "orders"))

    deleted.should eq [{"orders", 1_u64}]
    sm.group_for("orders").should be_nil
  end

  it "round-trips snapshot" do
    sm1 = MetaStateMachine.new(
      on_delete_group: ->(n : String, g : UInt64) { nil },
    ) { |n, g| nil }
    sm1.apply(QueueCommand.new(QueueAction::CreateQueue, "a"))
    sm1.apply(QueueCommand.new(QueueAction::CreateQueue, "b"))

    io = IO::Memory.new
    sm1.snapshot(io)
    io.rewind

    restored_creates = [] of {String, UInt64}
    sm2 = MetaStateMachine.new(
      on_delete_group: ->(n : String, g : UInt64) { nil },
    ) { |n, g| restored_creates << {n, g}; nil }
    sm2.restore(io)

    sm2.group_for("a").should eq 1_u64
    sm2.group_for("b").should eq 2_u64
    sm2.apply(QueueCommand.new(QueueAction::CreateQueue, "c"))
    sm2.group_for("c").should eq 3_u64
  end
end
```

- [ ] **Step 2: Run test to verify it fails**

Run: `crystal spec examples/queue/spec/meta_state_machine_spec.cr -Dpreview_mt -Dexecution_context`
Expected: compile error — `meta_state_machine.cr` does not exist.

- [ ] **Step 3: Implement MetaStateMachine**

Create `examples/queue/src/meta_state_machine.cr`:

```crystal
require "../../../src/raft"

class MetaStateMachine < Raft::StateMachine(QueueCommand)
  @groups : Hash(String, UInt64) = {} of String => UInt64
  @next_group_id : UInt64 = 1_u64
  @on_create_group : Proc(String, UInt64, Nil)
  @on_delete_group : Proc(String, UInt64, Nil)

  def initialize(*, on_delete_group : Proc(String, UInt64, Nil), &@on_create_group : String, UInt64 ->)
    @on_delete_group = on_delete_group
  end

  def apply(entry : QueueCommand)
    case entry.action
    when QueueAction::CreateQueue
      return if @groups.has_key?(entry.queue_name)
      group_id = @next_group_id
      @next_group_id += 1
      @groups[entry.queue_name] = group_id
      @on_create_group.call(entry.queue_name, group_id)
    when QueueAction::DeleteQueue
      if group_id = @groups.delete(entry.queue_name)
        @on_delete_group.call(entry.queue_name, group_id)
      end
    end
  end

  def group_for(name : String) : UInt64?
    @groups[name]?
  end

  def all_groups : Hash(String, UInt64)
    @groups
  end

  def snapshot(io : IO)
    io.write_bytes(@next_group_id, IO::ByteFormat::LittleEndian)
    io.write_bytes(@groups.size.to_u32, IO::ByteFormat::LittleEndian)
    @groups.each do |name, group_id|
      io.write_bytes(name.bytesize.to_u32, IO::ByteFormat::LittleEndian)
      io.write(name.to_slice)
      io.write_bytes(group_id, IO::ByteFormat::LittleEndian)
    end
  end

  def restore(io : IO)
    @next_group_id = io.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
    count = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
    @groups.clear
    count.times do
      name_size = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
      name_buf = Bytes.new(name_size)
      io.read_fully(name_buf)
      group_id = io.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
      @groups[String.new(name_buf)] = group_id
    end
  end
end
```

- [ ] **Step 4: Run test to verify it passes**

Run: `crystal spec examples/queue/spec/meta_state_machine_spec.cr -Dpreview_mt -Dexecution_context`
Expected: 4 examples, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add examples/queue/src/meta_state_machine.cr examples/queue/spec/meta_state_machine_spec.cr
git commit -m "Add MetaStateMachine for queue PoC"
```

---

## Task 3: QueueStateMachine

**Files:**
- Create: `examples/queue/src/queue_state_machine.cr`
- Test: `examples/queue/spec/queue_state_machine_spec.cr`

The queue's state machine. Holds a `Deque(Bytes)` of message bodies and a `Hash(String, Channel(Bytes?))` keyed by request id.

- `Publish` — push the body to the tail.
- `Consume` — pop from the head (or `nil` if empty); look up `req_id` in the request map and send the popped value to that channel. If the `req_id` is not in the map (we're not the originating node), silently drop.

The request map is populated by the HTTP handler **before** it proposes (Task 5). The state machine just looks it up.

- [ ] **Step 1: Write failing tests**

Create `examples/queue/spec/queue_state_machine_spec.cr`:

```crystal
require "spec"
require "../../../src/raft"
require "../src/queue_command"
require "../src/queue_state_machine"

describe QueueStateMachine do
  it "publishes and consumes in FIFO order" do
    sm = QueueStateMachine.new
    sm.apply(QueueCommand.new(QueueAction::Publish, "q", body: "first".to_slice))
    sm.apply(QueueCommand.new(QueueAction::Publish, "q", body: "second".to_slice))
    sm.depth.should eq 2

    ch1 = Channel(Bytes?).new(1)
    sm.register_request("r1", ch1)
    sm.apply(QueueCommand.new(QueueAction::Consume, "q", req_id: "r1"))
    String.new(ch1.receive.not_nil!).should eq "first"

    ch2 = Channel(Bytes?).new(1)
    sm.register_request("r2", ch2)
    sm.apply(QueueCommand.new(QueueAction::Consume, "q", req_id: "r2"))
    String.new(ch2.receive.not_nil!).should eq "second"

    sm.depth.should eq 0
  end

  it "delivers nil when consuming from an empty queue" do
    sm = QueueStateMachine.new
    ch = Channel(Bytes?).new(1)
    sm.register_request("r1", ch)
    sm.apply(QueueCommand.new(QueueAction::Consume, "q", req_id: "r1"))
    ch.receive.should be_nil
  end

  it "silently drops Consume when no channel is registered (follower behavior)" do
    sm = QueueStateMachine.new
    sm.apply(QueueCommand.new(QueueAction::Publish, "q", body: "x".to_slice))
    # no register_request — simulating a follower
    sm.apply(QueueCommand.new(QueueAction::Consume, "q", req_id: "unknown"))
    sm.depth.should eq 0 # pop still happened
  end

  it "round-trips snapshot of queue contents" do
    sm1 = QueueStateMachine.new
    sm1.apply(QueueCommand.new(QueueAction::Publish, "q", body: "a".to_slice))
    sm1.apply(QueueCommand.new(QueueAction::Publish, "q", body: "b".to_slice))

    io = IO::Memory.new
    sm1.snapshot(io)
    io.rewind

    sm2 = QueueStateMachine.new
    sm2.restore(io)
    sm2.depth.should eq 2

    ch = Channel(Bytes?).new(1)
    sm2.register_request("r", ch)
    sm2.apply(QueueCommand.new(QueueAction::Consume, "q", req_id: "r"))
    String.new(ch.receive.not_nil!).should eq "a"
  end

  it "removes a registered channel after delivering a result" do
    sm = QueueStateMachine.new
    ch = Channel(Bytes?).new(1)
    sm.register_request("r1", ch)
    sm.apply(QueueCommand.new(QueueAction::Consume, "q", req_id: "r1"))
    sm.has_pending_request?("r1").should be_false
  end
end
```

- [ ] **Step 2: Run test to verify it fails**

Run: `crystal spec examples/queue/spec/queue_state_machine_spec.cr -Dpreview_mt -Dexecution_context`
Expected: compile error — `queue_state_machine.cr` does not exist.

- [ ] **Step 3: Implement QueueStateMachine**

Create `examples/queue/src/queue_state_machine.cr`:

```crystal
require "../../../src/raft"

class QueueStateMachine < Raft::StateMachine(QueueCommand)
  @messages : Deque(Bytes) = Deque(Bytes).new
  @pending : Hash(String, Channel(Bytes?)) = {} of String => Channel(Bytes?)

  def apply(entry : QueueCommand)
    case entry.action
    when QueueAction::Publish
      @messages << entry.body
    when QueueAction::Consume
      popped = @messages.shift?
      if ch = @pending.delete(entry.req_id)
        ch.send(popped)
      end
    end
  end

  def depth : Int32
    @messages.size
  end

  def register_request(req_id : String, ch : Channel(Bytes?))
    @pending[req_id] = ch
  end

  def cancel_request(req_id : String)
    @pending.delete(req_id)
  end

  def has_pending_request?(req_id : String) : Bool
    @pending.has_key?(req_id)
  end

  def snapshot(io : IO)
    io.write_bytes(@messages.size.to_u32, IO::ByteFormat::LittleEndian)
    @messages.each do |body|
      io.write_bytes(body.size.to_u32, IO::ByteFormat::LittleEndian)
      io.write(body)
    end
  end

  def restore(io : IO)
    @messages.clear
    count = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
    count.times do
      size = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
      buf = Bytes.new(size)
      io.read_fully(buf) if size > 0
      @messages << buf
    end
  end
end
```

- [ ] **Step 4: Run test to verify it passes**

Run: `crystal spec examples/queue/spec/queue_state_machine_spec.cr -Dpreview_mt -Dexecution_context`
Expected: 5 examples, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add examples/queue/src/queue_state_machine.cr examples/queue/spec/queue_state_machine_spec.cr
git commit -m "Add QueueStateMachine with request bridge for queue PoC"
```

---

## Task 4: HTTP handler skeleton + publish endpoint

**Files:**
- Create: `examples/queue/src/queue_http_handler.cr`
- Test: integration tested in Task 9; smoke-tested in Task 8 via curl after `main.cr` is built.

The handler is structured to mirror `examples/kv/src/kv_http_handler.cr`. This task creates the skeleton and the `POST /queues/{name}` (publish) endpoint with leader proxy. Other endpoints come in subsequent tasks.

- [ ] **Step 1: Create the file with routing skeleton + publish**

Create `examples/queue/src/queue_http_handler.cr`:

```crystal
require "http/server/handler"
require "json"
require "uuid"
require "log"

class QueueHttpHandler
  include HTTP::Handler

  @meta_node : Raft::Node(QueueCommand)
  @meta_sm : MetaStateMachine
  @nodes : Hash(UInt64, Raft::Node(QueueCommand))
  @state_machines : Hash(UInt64, QueueStateMachine)

  def initialize(@meta_node, @meta_sm, @nodes, @state_machines)
  end

  def call(context : HTTP::Server::Context)
    path = context.request.path
    method = context.request.method

    case {method, path}
    when {"GET", "/"}
      handle_web_ui(context)
    when {"GET", "/queues"}
      handle_list_queues(context)
    else
      if path.starts_with?("/queues/")
        rest = path[8..]
        # /queues/{name}            POST → publish, DELETE → delete
        # /queues/{name}/messages   GET  → consume
        # /queues/{name}/events     GET  → SSE
        if idx = rest.index('/')
          name = rest[0...idx]
          tail = rest[(idx + 1)..]
          case {method, tail}
          when {"GET", "messages"}
            handle_consume(context, name)
          when {"GET", "events"}
            handle_events(context, name)
          else
            context.response.status_code = 404
            context.response.print "Not found"
          end
        else
          name = rest
          case method
          when "POST"
            handle_publish(context, name)
          when "DELETE"
            handle_delete(context, name)
          else
            context.response.status_code = 405
            context.response.print "Method not allowed"
          end
        end
      else
        call_next(context)
      end
    end
  end

  private def handle_publish(context, name : String)
    if group_id = @meta_sm.group_for(name)
      if node = @nodes[group_id]?
        unless node.role == Raft::Role::Leader
          return if forward_to_leader(context, node)
          context.response.status_code = 503
          context.response.content_type = "application/json"
          context.response.print({error: "not leader for queue", leader_id: node.leader_id}.to_json)
          return
        end
        body = context.request.body.try(&.gets_to_end) || ""
        node.propose(QueueCommand.new(QueueAction::Publish, name, body: body.to_slice))
        context.response.status_code = 202
        context.response.content_type = "application/json"
        context.response.print({status: "accepted", queue: name}.to_json)
      else
        context.response.status_code = 503
        context.response.content_type = "application/json"
        context.response.print({error: "queue group not loaded on this node"}.to_json)
      end
    else
      # Auto-create via meta consensus, then accept the publish on retry
      unless @meta_node.role == Raft::Role::Leader
        return if forward_to_leader(context, @meta_node)
        context.response.status_code = 503
        context.response.content_type = "application/json"
        context.response.print({error: "not meta leader", leader_id: @meta_node.leader_id}.to_json)
        return
      end
      @meta_node.propose(QueueCommand.new(QueueAction::CreateQueue, name))
      context.response.status_code = 202
      context.response.content_type = "application/json"
      context.response.print({status: "queue_creation_accepted", queue: name}.to_json)
    end
  end

  private def handle_consume(context, name : String)
    context.response.status_code = 501
    context.response.print "Not implemented yet"
  end

  private def handle_delete(context, name : String)
    context.response.status_code = 501
    context.response.print "Not implemented yet"
  end

  private def handle_list_queues(context)
    context.response.status_code = 501
    context.response.print "Not implemented yet"
  end

  private def handle_events(context, name : String)
    context.response.status_code = 501
    context.response.print "Not implemented yet"
  end

  private def handle_web_ui(context)
    context.response.status_code = 501
    context.response.print "Not implemented yet"
  end

  private def forward_to_leader(context, node : Raft::Node(QueueCommand)) : Bool
    leader_id = node.leader_id
    return false unless leader_id
    peer = node.peers.find { |p| p.id == leader_id }
    return false unless peer && !peer.address.empty?
    host = peer.address.split(":").first
    http_port = 8000 + leader_id
    begin
      client = ::HTTP::Client.new(host, http_port.to_i)
      client.connect_timeout = 2.seconds
      client.read_timeout = 5.seconds
      body = context.request.body.try(&.gets_to_end)
      query = context.request.query
      resource = query ? "#{context.request.path}?#{query}" : context.request.path
      response = client.exec(context.request.method, resource, context.request.headers, body)
      context.response.status_code = response.status_code
      context.response.content_type = response.content_type || "application/json"
      context.response.print response.body
      true
    rescue ex
      Log.warn { "Forward to leader #{leader_id} failed: #{ex.message}" }
      false
    end
  end
end
```

- [ ] **Step 2: Verify it compiles**

Run: `crystal build examples/queue/src/queue_http_handler.cr -Dpreview_mt -Dexecution_context --no-codegen 2>&1 || true`
Expected: compiles cleanly (file references `QueueCommand`, `MetaStateMachine`, `QueueStateMachine`, `Raft::*` — all defined).

Note: this task has no behavior test of its own — the handler is exercised end-to-end in Task 8's smoke test and Task 9's integration spec. Splitting handler tests into per-method specs requires a fake `Raft::Node`, which is more scaffolding than the PoC needs.

- [ ] **Step 3: Commit**

```bash
git add examples/queue/src/queue_http_handler.cr
git commit -m "Add QueueHttpHandler skeleton with publish + leader proxy"
```

---

## Task 5: HTTP consume endpoint with bridge

**Files:**
- Modify: `examples/queue/src/queue_http_handler.cr` (replace stub `handle_consume`)

The consume endpoint is the load-bearing piece of the PoC. It implements the request-id bridge from spec §4: register a channel, propose, wait with deadline, deliver or 503.

- [ ] **Step 1: Replace `handle_consume` with the bridge implementation**

In `examples/queue/src/queue_http_handler.cr`, find:

```crystal
  private def handle_consume(context, name : String)
    context.response.status_code = 501
    context.response.print "Not implemented yet"
  end
```

Replace with:

```crystal
  private def handle_consume(context, name : String)
    group_id = @meta_sm.group_for(name)
    unless group_id
      context.response.status_code = 404
      context.response.content_type = "application/json"
      context.response.print({error: "queue not found"}.to_json)
      return
    end

    node = @nodes[group_id]?
    sm = @state_machines[group_id]?
    unless node && sm
      context.response.status_code = 503
      context.response.content_type = "application/json"
      context.response.print({error: "queue group not loaded on this node"}.to_json)
      return
    end

    unless node.role == Raft::Role::Leader
      return if forward_to_leader(context, node)
      context.response.status_code = 503
      context.response.content_type = "application/json"
      context.response.print({error: "not leader for queue", leader_id: node.leader_id}.to_json)
      return
    end

    req_id = UUID.random.to_s
    ch = Channel(Bytes?).new(1)
    sm.register_request(req_id, ch)
    node.propose(QueueCommand.new(QueueAction::Consume, name, req_id: req_id))

    select
    when result = ch.receive
      sm.cancel_request(req_id) # safe even if already delivered
      if popped = result
        context.response.status_code = 200
        context.response.content_type = "application/octet-stream"
        context.response.write(popped)
      else
        context.response.status_code = 204
      end
    when timeout(5.seconds)
      sm.cancel_request(req_id)
      context.response.status_code = 503
      context.response.content_type = "application/json"
      context.response.print({error: "consume timeout — leader may have changed"}.to_json)
    end
  end
```

- [ ] **Step 2: Verify it compiles**

Run: `crystal build examples/queue/src/queue_http_handler.cr -Dpreview_mt -Dexecution_context --no-codegen 2>&1 || true`
Expected: compiles cleanly.

- [ ] **Step 3: Commit**

```bash
git add examples/queue/src/queue_http_handler.cr
git commit -m "Add consume endpoint with request-id bridge to queue PoC"
```

---

## Task 6: HTTP list, delete, and events endpoints

**Files:**
- Modify: `examples/queue/src/queue_http_handler.cr` (replace stubs)

- [ ] **Step 1: Replace `handle_list_queues`**

Find:

```crystal
  private def handle_list_queues(context)
    context.response.status_code = 501
    context.response.print "Not implemented yet"
  end
```

Replace with:

```crystal
  private def handle_list_queues(context)
    queues = [] of NamedTuple(name: String, group_id: UInt64, depth: Int32, is_leader: Bool, leader_id: Raft::NodeID?)
    @meta_sm.all_groups.each do |name, group_id|
      sm = @state_machines[group_id]?
      node = @nodes[group_id]?
      depth = sm.try(&.depth) || 0
      is_leader = node.try(&.role) == Raft::Role::Leader
      leader_id = node.try(&.leader_id)
      queues << {name: name, group_id: group_id, depth: depth, is_leader: is_leader, leader_id: leader_id}
    end
    context.response.content_type = "application/json"
    context.response.print queues.to_json
  end
```

- [ ] **Step 2: Replace `handle_delete`**

Find:

```crystal
  private def handle_delete(context, name : String)
    context.response.status_code = 501
    context.response.print "Not implemented yet"
  end
```

Replace with:

```crystal
  private def handle_delete(context, name : String)
    unless @meta_sm.group_for(name)
      context.response.status_code = 404
      context.response.content_type = "application/json"
      context.response.print({error: "queue not found"}.to_json)
      return
    end
    unless @meta_node.role == Raft::Role::Leader
      return if forward_to_leader(context, @meta_node)
      context.response.status_code = 503
      context.response.content_type = "application/json"
      context.response.print({error: "not meta leader", leader_id: @meta_node.leader_id}.to_json)
      return
    end
    @meta_node.propose(QueueCommand.new(QueueAction::DeleteQueue, name))
    context.response.status_code = 202
    context.response.content_type = "application/json"
    context.response.print({status: "delete_accepted", queue: name}.to_json)
  end
```

- [ ] **Step 3: Replace `handle_events` with a basic SSE stream**

Find:

```crystal
  private def handle_events(context, name : String)
    context.response.status_code = 501
    context.response.print "Not implemented yet"
  end
```

Replace with:

```crystal
  private def handle_events(context, name : String)
    unless @meta_sm.group_for(name)
      context.response.status_code = 404
      context.response.print "queue not found"
      return
    end
    context.response.headers["Content-Type"] = "text/event-stream"
    context.response.headers["Cache-Control"] = "no-cache"
    context.response.headers["Connection"] = "keep-alive"

    # Naive polling SSE — emits a depth snapshot every 500ms.
    # Sufficient for the PoC; a future iteration can hook into apply().
    last_depth = -1
    deadline_check_interval = 500.milliseconds
    begin
      loop do
        group_id = @meta_sm.group_for(name)
        break unless group_id
        sm = @state_machines[group_id]?
        depth = sm.try(&.depth) || 0
        if depth != last_depth
          context.response << "data: " << {queue: name, depth: depth}.to_json << "\n\n"
          context.response.flush
          last_depth = depth
        end
        sleep deadline_check_interval
      end
    rescue IO::Error
      # client disconnected
    end
  end
```

- [ ] **Step 4: Verify it compiles**

Run: `crystal build examples/queue/src/queue_http_handler.cr -Dpreview_mt -Dexecution_context --no-codegen 2>&1 || true`
Expected: compiles cleanly.

- [ ] **Step 5: Commit**

```bash
git add examples/queue/src/queue_http_handler.cr
git commit -m "Add list, delete, and SSE events endpoints to queue PoC"
```

---

## Task 7: Web UI

**Files:**
- Modify: `examples/queue/src/queue_http_handler.cr` (replace stub `handle_web_ui`)

A small admin page showing live queues with their depth. The visual story is the depth-vs-log-size divergence (log-size column comes from `Raft::Node#log.last_index`, which gives entry count).

- [ ] **Step 1: Replace `handle_web_ui`**

Find:

```crystal
  private def handle_web_ui(context)
    context.response.status_code = 501
    context.response.print "Not implemented yet"
  end
```

Replace with:

```crystal
  private def handle_web_ui(context)
    context.response.content_type = "text/html"
    context.response << <<-HTML
      <!doctype html>
      <html>
      <head>
        <title>Queue PoC</title>
        <style>
          body { font-family: -apple-system, sans-serif; margin: 2em; }
          h1 { font-size: 18px; }
          table { border-collapse: collapse; margin-top: 1em; }
          th, td { border: 1px solid #ccc; padding: 6px 12px; text-align: left; }
          th { background: #f0f0f0; }
          .leader { color: #060; font-weight: bold; }
          .info { color: #666; font-size: 12px; }
          form { margin-top: 1em; }
          input { padding: 4px; }
          button { padding: 4px 12px; }
        </style>
      </head>
      <body>
        <h1>Queue PoC — live state</h1>
        <p class="info">Each queue is its own Raft group. Watch the gap grow between in-memory depth and on-disk log entries.</p>
        <table id="queues">
          <thead>
            <tr><th>Queue</th><th>Group</th><th>Depth (memory)</th><th>Log entries (disk)</th><th>Leader</th><th>Role here</th></tr>
          </thead>
          <tbody></tbody>
        </table>
        <form id="publish-form" onsubmit="publish(); return false;">
          <input id="qname" placeholder="queue name" required>
          <input id="body" placeholder="message body" required>
          <button>Publish</button>
        </form>
        <form id="consume-form" onsubmit="consume(); return false;">
          <input id="qname-c" placeholder="queue name" required>
          <button>Consume one</button>
        </form>
        <pre id="last-consume"></pre>

        <script>
          async function refresh() {
            const r = await fetch('/queues');
            const list = await r.json();
            const tbody = document.querySelector('#queues tbody');
            tbody.innerHTML = list.map(q =>
              '<tr>' +
                '<td>' + q.name + '</td>' +
                '<td>' + q.group_id + '</td>' +
                '<td>' + q.depth + '</td>' +
                '<td><span data-log="' + q.group_id + '">…</span></td>' +
                '<td>' + (q.leader_id || 'unknown') + '</td>' +
                '<td>' + (q.is_leader ? '<span class="leader">leader</span>' : 'follower') + '</td>' +
              '</tr>'
            ).join('');
            // log-entry counts come from the library's HTTP admin endpoint
            const stats = await fetch('/raft/status').then(r => r.json()).catch(() => ({groups: []}));
            (stats.groups || []).forEach(g => {
              const el = document.querySelector('[data-log="' + g.group_id + '"]');
              if (el) el.textContent = g.log_last_index;
            });
          }

          async function publish() {
            const name = document.getElementById('qname').value;
            const body = document.getElementById('body').value;
            const r = await fetch('/queues/' + encodeURIComponent(name), {method: 'POST', body: body});
            console.log('publish:', r.status);
            refresh();
          }

          async function consume() {
            const name = document.getElementById('qname-c').value;
            const r = await fetch('/queues/' + encodeURIComponent(name) + '/messages');
            const out = document.getElementById('last-consume');
            if (r.status === 204) {
              out.textContent = '(empty)';
            } else if (r.status === 200) {
              out.textContent = await r.text();
            } else {
              out.textContent = 'error: ' + r.status;
            }
            refresh();
          }

          refresh();
          setInterval(refresh, 1000);
        </script>
      </body>
      </html>
    HTML
  end
```

Note: the `/raft/status` endpoint referenced in the JS is part of the library's existing HTTP handler (`src/raft/http/handler.cr`). The example's HTTP server will already include that handler (see Task 8).

- [ ] **Step 2: Verify it compiles**

Run: `crystal build examples/queue/src/queue_http_handler.cr -Dpreview_mt -Dexecution_context --no-codegen 2>&1 || true`
Expected: compiles cleanly.

- [ ] **Step 3: Commit**

```bash
git add examples/queue/src/queue_http_handler.cr
git commit -m "Add web UI to queue PoC"
```

---

## Task 8: main.cr wiring

**Files:**
- Create: `examples/queue/src/main.cr`

Mirrors `examples/kv/src/main.cr` very closely — same transport, same per-group event loop, same meta-group + per-thing-group pattern, same on_configuration callbacks. The only differences are: the state machine types and the HTTP handler.

- [ ] **Step 1: Create main.cr**

Create `examples/queue/src/main.cr`:

```crystal
require "../../../src/raft"
require "./queue_command"
require "./queue_state_machine"
require "./meta_state_machine"
require "./queue_http_handler"
require "http/server"
require "log"

Log.setup_from_env(default_level: :info)

node_id = (ENV["NODE_ID"]? || "1").to_u64
http_port = (ENV["HTTP_PORT"]? || "8001").to_i
raft_port = (ENV["RAFT_PORT"]? || "9000").to_i
raft_advertise_address = ENV["RAFT_ADVERTISE_ADDRESS"]? || ""
base_data_dir = ENV["DATA_DIR"]? || "/data/raft"

Dir.mkdir_p(base_data_dir)

transport = Raft::TCPTransport.new(listen_address: "0.0.0.0", listen_port: raft_port, data_dir: base_data_dir)

nodes = Hash(UInt64, Raft::Node(QueueCommand)).new
state_machines = Hash(UInt64, QueueStateMachine).new
meta_node_holder = [] of Raft::Node(QueueCommand)

start_group_loop = ->(node : Raft::Node(QueueCommand)) {
  spawn(name: "raft-group-#{node.group_id}") do
    loop do
      select
      when msg = node.inbox.receive
        node.step(msg)
      when timeout(50.milliseconds)
        node.tick
      end
      node.take_messages.each do |target_id, msg|
        transport.outbox.send({target_id, msg})
      end
    end
  rescue Channel::ClosedError
    Log.info { "Raft group #{node.group_id} stopped" }
  end
}

create_node = ->(group_id : UInt64, sm : Raft::StateMachine(QueueCommand)) {
  current_peer_ids = if group_id > 0_u64 && !meta_node_holder.empty?
                       meta_node_holder[0].peers.map(&.id).reject { |id| id == node_id }
                     else
                       [] of UInt64
                     end
  cfg = Raft::Config.new
  cfg.data_dir = File.join(base_data_dir, "group-#{group_id}")
  cfg.election_timeout_min_ticks = 10_u32
  cfg.election_timeout_max_ticks = 20_u32
  cfg.heartbeat_ticks = 2_u32
  Dir.mkdir_p(cfg.data_dir)
  metrics = Raft::Metrics.new(node_id: node_id, group_id: group_id)
  node = Raft::Node(QueueCommand).new(
    id: node_id, peers: current_peer_ids, config: cfg,
    state_machine: sm, metrics: metrics, group_id: group_id,
    address: raft_advertise_address
  )
  nodes[group_id] = node
  node.on_configuration_applied do |peers|
    peers.each do |p|
      next if p.id == node_id || p.address.empty?
      parts = p.address.split(":")
      next if parts.size < 2
      transport.register_peer(p.id, parts[0], parts[1].to_i)
    end
  end
  transport.register_channel(group_id, node.inbox)
  start_group_loop.call(node)
  Log.info { "Created Raft group #{group_id}" }
  node
}

on_delete_group = ->(name : String, gid : UInt64) {
  if node = nodes.delete(gid)
    node.close
    transport.unregister_channel(gid)
  end
  state_machines.delete(gid)
  Log.info { "Deleted queue group #{gid} for queue '#{name}'" }
}

meta_sm = MetaStateMachine.new(on_delete_group: on_delete_group) do |name, gid|
  qsm = QueueStateMachine.new
  create_node.call(gid, qsm)
  state_machines[gid] = qsm
  Log.info { "Created queue group #{gid} for queue '#{name}'" }
end

meta_node = create_node.call(0_u64, meta_sm)
meta_node_holder << meta_node

meta_node.on_configuration_change do |new_peers|
  nodes.each do |gid, data_node|
    next if gid == 0_u64
    next unless data_node.role == Raft::Role::Leader
    new_peers.each do |p|
      next if p.id == node_id
      unless data_node.peers.any? { |dp| dp.id == p.id }
        data_node.add_server(p.id)
      end
    end
    to_remove = data_node.peers.select do |dp|
      dp.id != node_id && !new_peers.any? { |p| p.id == dp.id }
    end.map(&.id)
    to_remove.each { |id| data_node.remove_server(id) }
  end
end

transport.start

raft_handler = Raft::HTTP::Handler(QueueCommand).new(meta_node, transport, raft_advertise_address)
queue_handler = QueueHttpHandler.new(meta_node, meta_sm, nodes, state_machines)

server = ::HTTP::Server.new([queue_handler, raft_handler]) do |context|
  context.response.status_code = 404
  context.response.print "Not found"
end

shutdown = ->(_signal : Signal) do
  puts "\nShutting down node #{node_id}..."
  server.close
  transport.stop
  nodes.each_value(&.close)
  exit 0
end

Signal::INT.trap(&shutdown)
Signal::TERM.trap(&shutdown)

puts "Node #{node_id} starting on HTTP :#{http_port}, Raft :#{raft_port}"
server.bind_tcp("0.0.0.0", http_port)
server.listen
```

- [ ] **Step 2: Verify it compiles**

Run: `crystal build examples/queue/src/main.cr -o /tmp/queue_node -Dpreview_mt -Dexecution_context 2>&1`
Expected: builds successfully (a `/tmp/queue_node` binary is produced).

- [ ] **Step 3: Smoke test the binary single-node**

Open two terminals.

Terminal 1:
```bash
rm -rf /tmp/queue-data && mkdir -p /tmp/queue-data
NODE_ID=1 HTTP_PORT=8001 RAFT_PORT=9000 RAFT_ADVERTISE_ADDRESS=127.0.0.1:9000 DATA_DIR=/tmp/queue-data /tmp/queue_node
```

Terminal 2:
```bash
curl -X POST http://localhost:8001/raft/bootstrap
sleep 2
curl http://localhost:8001/queues                                # → []
curl -X POST -d 'hello' http://localhost:8001/queues/test        # → 202
sleep 1
curl -X POST -d 'world' http://localhost:8001/queues/test        # → 202
curl http://localhost:8001/queues                                # → [{"name":"test","depth":2,...}]
curl http://localhost:8001/queues/test/messages                  # → "hello" (200)
curl http://localhost:8001/queues/test/messages                  # → "world" (200)
curl http://localhost:8001/queues/test/messages                  # → "" (204)
```

Expected: every step returns the indicated response.

- [ ] **Step 4: Stop the binary**

In Terminal 1: Ctrl-C. The graceful shutdown should print `"Shutting down node 1..."`.

- [ ] **Step 5: Commit**

```bash
git add examples/queue/src/main.cr
git commit -m "Wire up queue PoC main.cr"
```

---

## Task 9: Multi-node integration spec

**Files:**
- Create: `examples/queue/spec/queue_integration_spec.cr`

Spin up a 3-node `MemoryTransport` cluster, exercise publish on one node and consume on another (proving leader proxy works), and exercise the deadline-expiry path.

- [ ] **Step 1: Look at the KV integration spec for reference**

If `examples/kv/spec/` has a multi-node integration spec, read it as a template. (At time of writing, only `kv_state_machine_spec.cr` exists, so we're creating the integration pattern fresh — keep it small.)

Run: `ls examples/kv/spec/`

- [ ] **Step 2: Write the integration spec**

Create `examples/queue/spec/queue_integration_spec.cr`:

```crystal
require "spec"
require "../../../src/raft"
require "../src/queue_command"
require "../src/meta_state_machine"
require "../src/queue_state_machine"

# Three-node cluster sharing a MemoryTransport, manually driven.
# We don't go through the HTTP handler in this spec — we exercise the
# Node#propose path directly, then assert state machines converged and
# the request bridge delivered the popped value.

private class TestCluster
  getter nodes : Hash({UInt64, UInt64}, Raft::Node(QueueCommand)) # (group_id, node_id) → Node
  getter state_machines : Hash({UInt64, UInt64}, QueueStateMachine)
  @transports : Hash(UInt64, Raft::MemoryTransport)

  def initialize(node_ids : Array(UInt64))
    @nodes = {} of {UInt64, UInt64} => Raft::Node(QueueCommand)
    @state_machines = {} of {UInt64, UInt64} => QueueStateMachine
    @transports = {} of UInt64 => Raft::MemoryTransport
    @data_dirs = [] of String
    node_ids.each do |id|
      @transports[id] = Raft::MemoryTransport.new(id)
    end
    # MemoryTransport-specific wiring: each transport knows about all peers
    @transports.each do |id, t|
      @transports.each { |other_id, other_t| t.add_peer(other_id, other_t) if other_id != id }
    end
  end

  def add_queue_group(group_id : UInt64, node_ids : Array(UInt64))
    node_ids.each do |id|
      tmpdir = File.join(Dir.tempdir, "queue_spec_#{group_id}_#{id}_#{Random.rand(1_000_000)}")
      Dir.mkdir_p(tmpdir)
      @data_dirs << tmpdir
      cfg = Raft::Config.new
      cfg.data_dir = tmpdir
      cfg.heartbeat_ticks = 1_u32
      cfg.election_timeout_min_ticks = 3_u32
      cfg.election_timeout_max_ticks = 5_u32
      sm = QueueStateMachine.new
      peers = node_ids.reject { |x| x == id }
      node = Raft::Node(QueueCommand).new(id: id, peers: peers, config: cfg, state_machine: sm, group_id: group_id)
      @nodes[{group_id, id}] = node
      @state_machines[{group_id, id}] = sm
      @transports[id].register_channel(group_id, node.inbox)
    end
  end

  def tick_until_stable(rounds : Int32 = 50)
    rounds.times do
      @nodes.each_value(&.tick)
      @nodes.each_value do |n|
        n.take_messages.each { |target_id, msg| @transports[n.id].send(target_id, msg) }
      end
      Fiber.yield
    end
  end

  def cleanup
    @nodes.each_value(&.close)
    @data_dirs.each { |d| FileUtils.rm_rf(d) rescue nil }
  end

  def leader_for(group_id : UInt64) : Raft::Node(QueueCommand)?
    @nodes.each do |(gid, _), node|
      return node if gid == group_id && node.role == Raft::Role::Leader
    end
    nil
  end
end

describe "queue integration" do
  it "propagates publishes across replicas in FIFO order" do
    cluster = TestCluster.new([1_u64, 2_u64, 3_u64])
    # Group 0 = "meta-substitute"; here we just create a single queue group directly
    cluster.add_queue_group(1_u64, [1_u64, 2_u64, 3_u64])
    # Manually bootstrap node 1 as leader of group 1
    cluster.nodes[{1_u64, 1_u64}].bootstrap
    cluster.tick_until_stable

    leader = cluster.leader_for(1_u64).not_nil!
    leader.propose(QueueCommand.new(QueueAction::Publish, "q", body: "first".to_slice))
    leader.propose(QueueCommand.new(QueueAction::Publish, "q", body: "second".to_slice))
    cluster.tick_until_stable

    # Every replica's state machine should have depth 2
    [1_u64, 2_u64, 3_u64].each do |id|
      cluster.state_machines[{1_u64, id}].depth.should eq 2
    end

    # Consume on the leader using the bridge
    sm_leader = cluster.state_machines[{1_u64, leader.id}]
    ch = Channel(Bytes?).new(1)
    sm_leader.register_request("rA", ch)
    leader.propose(QueueCommand.new(QueueAction::Consume, "q", req_id: "rA"))
    cluster.tick_until_stable
    String.new(ch.receive.not_nil!).should eq "first"
  ensure
    cluster.try(&.cleanup)
  end

  it "bridge channel delivers nil on empty-queue consume" do
    cluster = TestCluster.new([1_u64])
    cluster.add_queue_group(1_u64, [1_u64])
    cluster.nodes[{1_u64, 1_u64}].bootstrap
    cluster.tick_until_stable

    leader = cluster.leader_for(1_u64).not_nil!
    sm = cluster.state_machines[{1_u64, leader.id}]
    ch = Channel(Bytes?).new(1)
    sm.register_request("rB", ch)
    leader.propose(QueueCommand.new(QueueAction::Consume, "q", req_id: "rB"))
    cluster.tick_until_stable
    ch.receive.should be_nil
  ensure
    cluster.try(&.cleanup)
  end
end
```

Note: this spec uses `Raft::MemoryTransport` and may need to verify the `add_peer` API exists. If it doesn't, look at existing specs under `spec/` that use `MemoryTransport` for the actual setup pattern and mirror it.

- [ ] **Step 3: Run the spec**

Run: `crystal spec examples/queue/spec/queue_integration_spec.cr -Dpreview_mt -Dexecution_context`
Expected: 2 examples, 0 failures. If `MemoryTransport`'s API differs from what's assumed, fix the harness to match.

- [ ] **Step 4: Commit**

```bash
git add examples/queue/spec/queue_integration_spec.cr
git commit -m "Add multi-node integration spec for queue PoC"
```

---

## Task 10: Docker, Compose, Prometheus, Grafana, README

**Files:**
- Create: `examples/queue/Dockerfile`
- Create: `examples/queue/docker-compose.yml`
- Create: `examples/queue/prometheus.yml`
- Copy: `examples/queue/grafana/` (mirroring `examples/kv/grafana/`)
- Create: `examples/queue/README.md`

The Dockerfile, compose, and prometheus configs are direct copies of the KV equivalents with `kv_node` → `queue_node` and `examples/kv/` → `examples/queue/`. Grafana provisioning files can be copied as-is.

- [ ] **Step 1: Create Dockerfile**

Create `examples/queue/Dockerfile`:

```dockerfile
# examples/queue/Dockerfile
FROM 84codes/crystal:1.19.0-alpine AS builder
WORKDIR /app
COPY . .
RUN crystal build examples/queue/src/main.cr -o queue_node -Dpreview_mt -Dexecution_context -Draft_debug --release --static

FROM alpine:3.19
COPY --from=builder /app/queue_node /usr/local/bin/queue_node
RUN mkdir -p /data/raft
CMD ["/usr/local/bin/queue_node"]
```

- [ ] **Step 2: Create docker-compose.yml**

Copy `examples/kv/docker-compose.yml` to `examples/queue/docker-compose.yml`, then in the new file replace every occurrence of:

- `dockerfile: examples/kv/Dockerfile` → `dockerfile: examples/queue/Dockerfile`
- volume names `node1-data`, `node2-data`, `node3-data` are fine to keep (they're scoped to the compose project)

Run:
```bash
cp examples/kv/docker-compose.yml examples/queue/docker-compose.yml
sed -i.bak 's|examples/kv/Dockerfile|examples/queue/Dockerfile|g' examples/queue/docker-compose.yml
rm examples/queue/docker-compose.yml.bak
```

Verify: `grep Dockerfile examples/queue/docker-compose.yml` should only show `examples/queue/Dockerfile`.

- [ ] **Step 3: Create prometheus.yml**

Create `examples/queue/prometheus.yml`:

```yaml
# examples/queue/prometheus.yml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: "raft"
    metrics_path: "/raft/metrics"
    static_configs:
      - targets:
          - "node-1:8001"
          - "node-2:8002"
          - "node-3:8003"
```

(The metrics path differs from KV because we don't define a custom `/queue/metrics` endpoint in the queue handler — we use the library's built-in `/raft/metrics` from `src/raft/http/handler.cr`.)

- [ ] **Step 4: Copy Grafana provisioning**

Run: `cp -R examples/kv/grafana examples/queue/grafana`

The dashboards are queue-agnostic Raft metrics (peers, term, commit index, etc.) and work for any multi-raft application without modification. A queue-specific dashboard ("queue depth vs log size") can be added later if needed for the demo.

- [ ] **Step 5: Create README.md**

Create `examples/queue/README.md`:

```markdown
# Queue PoC

A multi-raft queue example demonstrating the unbounded-log problem from `ARCHITECTURE.md` §6.5.

Each queue is its own Raft group. A meta group (group 0) maps `queue_name → group_id`.
The HTTP API supports publish + one-shot consume; consume uses a request-id bridge to
deliver the popped value back to the HTTP handler (see `docs/plans/2026-05-04-queue-poc-design.md` §4).

## Run locally with Docker Compose

```bash
docker compose -f examples/queue/docker-compose.yml up --build
```

This starts 3 nodes (HTTP on 8001, 8002, 8003), Prometheus, and Grafana.

Bootstrap the cluster:
```bash
curl -X POST http://localhost:8001/raft/bootstrap
curl -X POST -d 'node-2:9000' http://localhost:8001/raft/peers/2
curl -X POST -d 'node-3:9000' http://localhost:8001/raft/peers/3
```

(Or use the TUI in `bin/raft-tui` if available — see top-level README.)

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/queues/{name}` | Publish — body is the message bytes. Auto-creates the queue. |
| `GET` | `/queues/{name}/messages` | Consume one message. 200 + body, or 204 if empty. |
| `GET` | `/queues` | List queue names + depth. |
| `DELETE` | `/queues/{name}` | Delete the queue and its Raft group. |
| `GET` | `/queues/{name}/events` | SSE stream of depth changes. |
| `GET` | `/` | Web UI showing live queue state. |

## What to look for

After publishing then draining N messages, inspect each node's `data/raft/group-*/`
directory: the on-disk segments hold ~2N entries (publish + consume) even though every
queue is empty in memory. This is the unbounded-log problem the PoC was built to surface.
Compaction is tracked separately.
```

- [ ] **Step 6: Verify the docker setup builds**

Run: `docker compose -f examples/queue/docker-compose.yml build`
Expected: builds successfully (may take ~2 minutes for the Crystal build).

- [ ] **Step 7: Commit**

```bash
git add examples/queue/Dockerfile examples/queue/docker-compose.yml examples/queue/prometheus.yml examples/queue/grafana examples/queue/README.md
git commit -m "Add Docker setup and README for queue PoC"
```

---

## Self-Review Checklist (run after all tasks)

After completing all 10 tasks:

1. **Spec coverage:**
   - §1 Scope → Tasks 1-10 (the example itself).
   - §2 State machines (Meta + Queue) → Tasks 2, 3.
   - §3 Compaction problem → README §"What to look for" calls it out.
   - §4 Consume-return-value bridge → Task 5.
   - §5 HTTP API (5 endpoints + UI) → Tasks 4, 5, 6, 7.
   - §6 File layout → matches the file structure in this plan.
   - §7 TUI integration → existing library TUI works for queue example as-is (mentioned in README); Task 7 covers the queue-aware web UI.
   - §8 Testing → Tasks 1-3 (unit tests) + Task 9 (integration spec).

2. **Placeholder scan:** confirm no `TBD` / `TODO` / `implement later` / "fill in details" appear anywhere in this plan. (Self-check by `grep -n "TBD\|TODO\|FIXME" docs/plans/2026-05-04-queue-poc-implementation-plan.md`.)

3. **Type/method consistency:**
   - `QueueCommand` fields used in tests match the struct in Task 1: `action`, `queue_name`, `body`, `req_id`. ✓
   - `MetaStateMachine.new` keyword args: `on_delete_group:` plus block. Used consistently in Tasks 2 and 8. ✓
   - `QueueStateMachine` methods: `register_request`, `cancel_request`, `has_pending_request?`, `depth`. Used in Tasks 3, 5, 6, 9. ✓
   - `QueueAction` values: `Publish`, `Consume`, `CreateQueue`, `DeleteQueue`. Consistent. ✓

4. **All file paths absolute and correct:** every `Files:` block uses `examples/queue/...` paths.
