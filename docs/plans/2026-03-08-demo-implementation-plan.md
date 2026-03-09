# Raft Demo Setup Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add HTTP admin API, Prometheus metrics, and TUI to the Raft library, then build a KV store demo with Docker Compose.

**Architecture:** Library-level tooling (Raft::Metrics, Raft::HTTP::Handler, Raft::TUI) reusable by any app. Demo KV store in examples/kv/ wires it together with Docker Compose for a 3-node cluster.

**Tech Stack:** Crystal >= 1.19.1, Crystal's built-in HTTP::Server, ANSI terminal control, Docker multi-stage builds, Prometheus.

**Build flags:** Always use `-Dpreview_mt -Dexecution_context` for crystal spec and crystal build.

**Design doc:** `docs/plans/2026-03-08-demo-setup-design.md`

---

### Task 1: Raft::Metrics — Counters, Gauges, Histograms

A self-contained metrics collector that formats Prometheus text output. No external dependencies.

**Files:**
- Create: `src/raft/metrics.cr`
- Create: `spec/raft/metrics_spec.cr`
- Modify: `src/raft.cr` (add require)

**Step 1: Write failing test**

```crystal
# spec/raft/metrics_spec.cr
require "../spec_helper"

describe Raft::Metrics do
  it "tracks gauges" do
    m = Raft::Metrics.new(node_id: 1_u64)
    m.set_gauge("raft_node_term", 5_i64)
    m.get_gauge("raft_node_term").should eq 5_i64
  end

  it "tracks counters" do
    m = Raft::Metrics.new(node_id: 1_u64)
    m.increment("raft_elections_total")
    m.increment("raft_elections_total")
    m.get_counter("raft_elections_total").should eq 2_i64
  end

  it "tracks histograms" do
    m = Raft::Metrics.new(node_id: 1_u64)
    m.observe("raft_append_latency_seconds", 0.005)
    m.observe("raft_append_latency_seconds", 0.0005)
    m.observe("raft_append_latency_seconds", 0.05)
    output = m.to_prometheus
    output.should contain("raft_append_latency_seconds_count{node_id=\"1\"} 3")
  end

  it "outputs prometheus text format" do
    m = Raft::Metrics.new(node_id: 1_u64)
    m.set_gauge("raft_node_term", 5_i64)
    m.increment("raft_elections_total")
    output = m.to_prometheus
    output.should contain("raft_node_term{node_id=\"1\"} 5")
    output.should contain("raft_elections_total{node_id=\"1\"} 1")
  end

  it "supports labeled histograms" do
    m = Raft::Metrics.new(node_id: 1_u64)
    m.observe("raft_replication_latency_seconds", 0.001, labels: {"peer" => "2"})
    output = m.to_prometheus
    output.should contain("peer=\"2\"")
  end
end
```

**Step 2: Run test to verify it fails**

Run: `crystal spec spec/raft/metrics_spec.cr -Dpreview_mt -Dexecution_context`
Expected: FAIL — `Raft::Metrics` not defined

**Step 3: Implement Metrics**

```crystal
# src/raft/metrics.cr
module Raft
  class Metrics
    HISTOGRAM_BUCKETS = [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]

    @node_id : NodeID
    @gauges : Hash(String, Int64) = {} of String => Int64
    @counters : Hash(String, Int64) = {} of String => Int64
    @histograms : Hash(String, HistogramData) = {} of String => HistogramData

    struct HistogramData
      property buckets : Array(Int64)
      property sum : Float64
      property count : Int64
      property labels : Hash(String, String)

      def initialize(@labels : Hash(String, String) = {} of String => String)
        @buckets = Array(Int64).new(HISTOGRAM_BUCKETS.size + 1, 0_i64) # +1 for +Inf
        @sum = 0.0
        @count = 0_i64
      end
    end

    def initialize(@node_id : NodeID)
    end

    def set_gauge(name : String, value : Int64)
      @gauges[name] = value
    end

    def get_gauge(name : String) : Int64
      @gauges.fetch(name, 0_i64)
    end

    def increment(name : String, by : Int64 = 1_i64)
      @counters[name] = @counters.fetch(name, 0_i64) + by
    end

    def get_counter(name : String) : Int64
      @counters.fetch(name, 0_i64)
    end

    def observe(name : String, value : Float64, labels : Hash(String, String) = {} of String => String)
      key = labels.empty? ? name : "#{name}{#{labels.map { |k, v| "#{k}=#{v}" }.join(",")}}"
      hist = @histograms[key]? || HistogramData.new(labels)
      hist.sum += value
      hist.count += 1
      HISTOGRAM_BUCKETS.each_with_index do |bound, i|
        hist.buckets[i] += 1 if value <= bound
      end
      hist.buckets[HISTOGRAM_BUCKETS.size] += 1 # +Inf always
      @histograms[key] = hist
    end

    def to_prometheus : String
      String.build do |io|
        @gauges.each do |name, value|
          io << name << "{node_id=\"" << @node_id << "\"} " << value << "\n"
        end
        @counters.each do |name, value|
          io << name << "{node_id=\"" << @node_id << "\"} " << value << "\n"
        end
        @histograms.each do |key, hist|
          # Extract base name (before { if present)
          base_name = key.includes?('{') ? key.split('{').first : key
          label_str = String.build do |ls|
            ls << "node_id=\"" << @node_id << "\""
            hist.labels.each do |k, v|
              ls << "," << k << "=\"" << v << "\""
            end
          end
          HISTOGRAM_BUCKETS.each_with_index do |bound, i|
            io << base_name << "_bucket{" << label_str << ",le=\"" << bound << "\"} " << hist.buckets[i] << "\n"
          end
          io << base_name << "_bucket{" << label_str << ",le=\"+Inf\"} " << hist.buckets[HISTOGRAM_BUCKETS.size] << "\n"
          io << base_name << "_sum{" << label_str << "} " << hist.sum << "\n"
          io << base_name << "_count{" << label_str << "} " << hist.count << "\n"
        end
      end
    end
  end
end
```

Add `require "./raft/metrics"` to `src/raft.cr`.

**Step 4: Run test to verify it passes**

Run: `crystal spec spec/raft/metrics_spec.cr -Dpreview_mt -Dexecution_context`
Expected: PASS

**Step 5: Commit**

```bash
git add src/raft/metrics.cr src/raft.cr spec/raft/metrics_spec.cr
git commit -m "feat: add Raft::Metrics — counters, gauges, histograms with Prometheus export"
```

---

### Task 2: Wire Metrics into Node — Pause/Resume + Peers Getter

Add `paused` flag, expose `peers`, and optionally accept a `Metrics` instance. When paused, `tick` is a no-op and `step` drops messages.

**Files:**
- Modify: `src/raft/node.cr`
- Modify: `spec/raft/node_spec.cr` (add tests)

**Step 1: Write failing tests**

Add to `spec/raft/node_spec.cr`:

```crystal
  describe "pause and resume" do
    it "does not tick when paused" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 3_u32
      config.election_timeout_max_ticks = 3_u32

      node = create_test_node(1_u64, [2_u64, 3_u64], config)
      node.pause
      10.times { node.tick }
      node.role.should eq Raft::Role::Follower # should not have become candidate

      node.resume
      3.times { node.tick }
      node.role.should eq Raft::Role::Candidate

      node.close
    end
  end

  describe "peers" do
    it "exposes peer list" do
      node = create_test_node(1_u64, [2_u64, 3_u64])
      node.peers.should eq [2_u64, 3_u64]
      node.close
    end
  end
```

**Step 2: Run test to verify it fails**

Run: `crystal spec spec/raft/node_spec.cr -Dpreview_mt -Dexecution_context`
Expected: FAIL — `pause`, `resume`, `peers` not defined

**Step 3: Implement**

In `src/raft/node.cr`:

- Add `getter peers : Array(NodeID)` (change `@peers` to `getter`)
- Add `getter paused : Bool = false`
- Add `property metrics : Metrics? = nil`
- Add methods:
  ```crystal
  def pause
    @paused = true
  end

  def resume
    @paused = false
  end
  ```
- At top of `tick`: `return if @paused`
- At top of `step`: `return if @paused`
- In `become_candidate`: `@metrics.try(&.increment("raft_elections_total"))`
- In `send_append_entries`: `@metrics.try(&.increment("raft_heartbeats_sent_total"))` (when entries_count == 0, it's a heartbeat)
- In `handle_append_entries`: `@metrics.try(&.increment("raft_heartbeats_received_total"))` (when entries_count == 0)
- In `apply_entries`: `@metrics.try(&.increment("raft_entries_applied_total"))` inside the loop
- In `send_append_entries` and `handle_append_entries`: `@metrics.try(&.increment("raft_messages_sent_total"))` / `raft_messages_received_total`

**Step 4: Run test to verify it passes**

Run: `crystal spec spec/raft/node_spec.cr -Dpreview_mt -Dexecution_context`
Expected: PASS

**Step 5: Run all specs**

Run: `crystal spec -Dpreview_mt -Dexecution_context`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/raft/node.cr spec/raft/node_spec.cr
git commit -m "feat: add pause/resume, peers getter, and metrics wiring to Node"
```

---

### Task 3: Raft::HTTP::Handler

Crystal HTTP handler exposing Raft status, log info, metrics, and admin controls.

**Files:**
- Create: `src/raft/http/handler.cr`
- Create: `spec/raft/http/handler_spec.cr`
- Modify: `src/raft.cr` (add require)

**Step 1: Write failing test**

```crystal
# spec/raft/http/handler_spec.cr
require "../../spec_helper"
require "http/server"
require "http/client"

describe Raft::HTTP::Handler do
  it "returns node status as JSON" do
    dir = File.tempname("raft_http")
    Dir.mkdir_p(dir)
    config = Raft::Config.new
    config.data_dir = dir
    config.election_timeout_min_ticks = 100_u32
    config.election_timeout_max_ticks = 100_u32

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: config, state_machine: sm)

    handler = Raft::HTTP::Handler(TestData).new(node)
    server = ::HTTP::Server.new([handler])
    address = server.bind_tcp("127.0.0.1", 0)
    spawn server.listen

    response = ::HTTP::Client.get("http://127.0.0.1:#{address.port}/raft/status")
    response.status_code.should eq 200
    body = response.body
    body.should contain("\"id\":1")
    body.should contain("\"role\":\"follower\"")

    server.close
    node.close
    FileUtils.rm_rf(dir)
  end

  it "returns metrics in prometheus format" do
    dir = File.tempname("raft_http")
    Dir.mkdir_p(dir)
    config = Raft::Config.new
    config.data_dir = dir
    config.election_timeout_min_ticks = 100_u32
    config.election_timeout_max_ticks = 100_u32

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: config, state_machine: sm)
    metrics = Raft::Metrics.new(node_id: 1_u64)
    node.metrics = metrics

    handler = Raft::HTTP::Handler(TestData).new(node)
    server = ::HTTP::Server.new([handler])
    address = server.bind_tcp("127.0.0.1", 0)
    spawn server.listen

    response = ::HTTP::Client.get("http://127.0.0.1:#{address.port}/raft/metrics")
    response.status_code.should eq 200
    response.body.should contain("raft_node_term")

    server.close
    node.close
    FileUtils.rm_rf(dir)
  end

  it "pauses and resumes node via admin endpoints" do
    dir = File.tempname("raft_http")
    Dir.mkdir_p(dir)
    config = Raft::Config.new
    config.data_dir = dir
    config.election_timeout_min_ticks = 100_u32
    config.election_timeout_max_ticks = 100_u32

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: config, state_machine: sm)

    handler = Raft::HTTP::Handler(TestData).new(node)
    server = ::HTTP::Server.new([handler])
    address = server.bind_tcp("127.0.0.1", 0)
    spawn server.listen

    response = ::HTTP::Client.post("http://127.0.0.1:#{address.port}/raft/admin/pause")
    response.status_code.should eq 200
    node.paused.should be_true

    response = ::HTTP::Client.post("http://127.0.0.1:#{address.port}/raft/admin/resume")
    response.status_code.should eq 200
    node.paused.should be_false

    server.close
    node.close
    FileUtils.rm_rf(dir)
  end
end
```

**Step 2: Run test to verify it fails**

Run: `crystal spec spec/raft/http/handler_spec.cr -Dpreview_mt -Dexecution_context`
Expected: FAIL — `Raft::HTTP::Handler` not defined

**Step 3: Implement Handler**

```crystal
# src/raft/http/handler.cr
require "http/server/handler"
require "json"

module Raft
  module HTTP
    class Handler(T)
      include ::HTTP::Handler

      @node : Node(T)

      def initialize(@node : Node(T))
      end

      def call(context : ::HTTP::Server::Context)
        case {context.request.method, context.request.path}
        when {"GET", "/raft/status"}
          handle_status(context)
        when {"GET", "/raft/log"}
          handle_log(context)
        when {"GET", "/raft/metrics"}
          handle_metrics(context)
        when {"POST", "/raft/admin/pause"}
          @node.pause
          json_response(context, 200, {"status" => "paused"})
        when {"POST", "/raft/admin/resume"}
          @node.resume
          json_response(context, 200, {"status" => "resumed"})
        else
          call_next(context)
        end
      end

      private def handle_status(context)
        leader_id = @node.leader_id
        json = JSON.build do |j|
          j.object do
            j.field "id", @node.id
            j.field "role", @node.role.to_s.downcase
            j.field "term", @node.current_term
            j.field "leader_id", leader_id
            j.field "commit_index", @node.commit_index
            j.field "last_log_index", @node.log.last_index
            j.field "peers" do
              j.array do
                @node.peers.each { |p| j.number(p) }
              end
            end
            j.field "paused", @node.paused
          end
        end
        context.response.content_type = "application/json"
        context.response.status_code = 200
        context.response.print json
      end

      private def handle_log(context)
        json = JSON.build do |j|
          j.object do
            j.field "last_index", @node.log.last_index
            j.field "last_term", @node.log.last_term
            j.field "segment_count", @node.log.segment_count
            j.field "commit_index", @node.commit_index
          end
        end
        context.response.content_type = "application/json"
        context.response.status_code = 200
        context.response.print json
      end

      private def handle_metrics(context)
        if metrics = @node.metrics
          # Update gauges from current node state
          metrics.set_gauge("raft_node_role", @node.role.value.to_i64)
          metrics.set_gauge("raft_node_term", @node.current_term.to_i64)
          metrics.set_gauge("raft_node_commit_index", @node.commit_index.to_i64)
          metrics.set_gauge("raft_node_last_log_index", @node.log.last_index.to_i64)
          metrics.set_gauge("raft_node_peers", @node.peers.size.to_i64)

          context.response.content_type = "text/plain; version=0.0.4"
          context.response.status_code = 200
          context.response.print metrics.to_prometheus
        else
          context.response.status_code = 503
          context.response.print "Metrics not configured"
        end
      end

      private def json_response(context, status : Int32, data : Hash)
        context.response.content_type = "application/json"
        context.response.status_code = status
        context.response.print data.to_json
      end
    end
  end
end
```

Add `require "./raft/http/handler"` to `src/raft.cr`.

**Step 4: Run test to verify it passes**

Run: `crystal spec spec/raft/http/handler_spec.cr -Dpreview_mt -Dexecution_context`
Expected: PASS

**Step 5: Commit**

```bash
git add src/raft/http/handler.cr src/raft.cr spec/raft/http/handler_spec.cr
git commit -m "feat: add Raft::HTTP::Handler — status, log, metrics, admin endpoints"
```

---

### Task 4: KV Demo — Command + StateMachine

**Files:**
- Create: `examples/kv/src/kv_command.cr`
- Create: `examples/kv/src/kv_state_machine.cr`
- Create: `examples/kv/spec/kv_state_machine_spec.cr`

**Step 1: Write failing test**

```crystal
# examples/kv/spec/kv_state_machine_spec.cr
require "spec"
require "../../../src/raft"
require "../src/kv_command"
require "../src/kv_state_machine"

describe KVStateMachine do
  it "applies put and get" do
    sm = KVStateMachine.new
    sm.apply(KVCommand.new(KVAction::Put, "foo", "bar"))
    sm.get("foo").should eq "bar"
  end

  it "applies delete" do
    sm = KVStateMachine.new
    sm.apply(KVCommand.new(KVAction::Put, "foo", "bar"))
    sm.apply(KVCommand.new(KVAction::Delete, "foo", ""))
    sm.get("foo").should be_nil
  end

  it "round-trips snapshot" do
    sm = KVStateMachine.new
    sm.apply(KVCommand.new(KVAction::Put, "a", "1"))
    sm.apply(KVCommand.new(KVAction::Put, "b", "2"))

    io = IO::Memory.new
    sm.snapshot(io)
    io.rewind

    sm2 = KVStateMachine.new
    sm2.restore(io)
    sm2.get("a").should eq "1"
    sm2.get("b").should eq "2"
  end
end
```

**Step 2: Run test to verify it fails**

Run: `crystal spec examples/kv/spec/kv_state_machine_spec.cr -Dpreview_mt -Dexecution_context`
Expected: FAIL

**Step 3: Implement**

```crystal
# examples/kv/src/kv_command.cr
enum KVAction : UInt8
  Put    = 0
  Delete = 1
end

struct KVCommand
  getter action : KVAction
  getter key : String
  getter value : String

  def initialize(@action : KVAction, @key : String, @value : String = "")
  end

  def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian)
    io.write_bytes(@action.value, format)
    io.write_bytes(@key.bytesize.to_u32, format)
    io.write(@key.to_slice)
    io.write_bytes(@value.bytesize.to_u32, format)
    io.write(@value.to_slice)
  end

  def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
    action = KVAction.new(io.read_bytes(UInt8, format))
    key_size = io.read_bytes(UInt32, format)
    key_slice = Bytes.new(key_size)
    io.read_fully(key_slice)
    value_size = io.read_bytes(UInt32, format)
    value_slice = Bytes.new(value_size)
    io.read_fully(value_slice)
    new(action, String.new(key_slice), String.new(value_slice))
  end
end
```

```crystal
# examples/kv/src/kv_state_machine.cr
require "../../../src/raft"

class KVStateMachine < Raft::StateMachine(KVCommand)
  @store : Hash(String, String) = {} of String => String

  def apply(entry : KVCommand)
    case entry.action
    when KVAction::Put    then @store[entry.key] = entry.value
    when KVAction::Delete then @store.delete(entry.key)
    end
  end

  def get(key : String) : String?
    @store[key]?
  end

  def snapshot(io : IO)
    io.write_bytes(@store.size.to_u32, IO::ByteFormat::LittleEndian)
    @store.each do |key, value|
      io.write_bytes(key.bytesize.to_u32, IO::ByteFormat::LittleEndian)
      io.write(key.to_slice)
      io.write_bytes(value.bytesize.to_u32, IO::ByteFormat::LittleEndian)
      io.write(value.to_slice)
    end
  end

  def restore(io : IO)
    @store.clear
    count = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
    count.times do
      key_size = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
      key_slice = Bytes.new(key_size)
      io.read_fully(key_slice)
      value_size = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
      value_slice = Bytes.new(value_size)
      io.read_fully(value_slice)
      @store[String.new(key_slice)] = String.new(value_slice)
    end
  end
end
```

**Step 4: Run test to verify it passes**

Run: `crystal spec examples/kv/spec/kv_state_machine_spec.cr -Dpreview_mt -Dexecution_context`
Expected: PASS

**Step 5: Commit**

```bash
git add examples/kv/src/kv_command.cr examples/kv/src/kv_state_machine.cr examples/kv/spec/kv_state_machine_spec.cr
git commit -m "feat: add KV demo — KVCommand and KVStateMachine"
```

---

### Task 5: KV Demo — HTTP Handler

**Files:**
- Create: `examples/kv/src/kv_http_handler.cr`

**Step 1: Implement KV HTTP handler**

```crystal
# examples/kv/src/kv_http_handler.cr
require "http/server/handler"
require "json"

class KVHttpHandler
  include HTTP::Handler

  @node : Raft::Node(KVCommand)
  @state_machine : KVStateMachine

  def initialize(@node : Raft::Node(KVCommand), @state_machine : KVStateMachine)
  end

  def call(context : HTTP::Server::Context)
    path = context.request.path
    method = context.request.method

    if path.starts_with?("/kv/")
      key = path[4..]
      case method
      when "GET"
        handle_get(context, key)
      when "PUT"
        handle_put(context, key)
      when "DELETE"
        handle_delete(context, key)
      else
        context.response.status_code = 405
        context.response.print "Method not allowed"
      end
    else
      call_next(context)
    end
  end

  private def handle_get(context, key)
    if value = @state_machine.get(key)
      context.response.content_type = "application/json"
      context.response.print({key: key, value: value}.to_json)
    else
      context.response.status_code = 404
      context.response.content_type = "application/json"
      context.response.print({error: "key not found"}.to_json)
    end
  end

  private def handle_put(context, key)
    unless @node.role == Raft::Role::Leader
      context.response.status_code = 503
      context.response.content_type = "application/json"
      leader = @node.leader_id
      context.response.print({error: "not leader", leader_id: leader}.to_json)
      return
    end

    value = context.request.body.try(&.gets_to_end) || ""
    cmd = KVCommand.new(KVAction::Put, key, value)
    @node.propose(cmd)
    context.response.status_code = 202
    context.response.content_type = "application/json"
    context.response.print({status: "accepted", key: key}.to_json)
  end

  private def handle_delete(context, key)
    unless @node.role == Raft::Role::Leader
      context.response.status_code = 503
      context.response.content_type = "application/json"
      leader = @node.leader_id
      context.response.print({error: "not leader", leader_id: leader}.to_json)
      return
    end

    cmd = KVCommand.new(KVAction::Delete, key)
    @node.propose(cmd)
    context.response.status_code = 202
    context.response.content_type = "application/json"
    context.response.print({status: "accepted", key: key}.to_json)
  end
end
```

**Step 2: Commit**

```bash
git add examples/kv/src/kv_http_handler.cr
git commit -m "feat: add KV HTTP handler — GET/PUT/DELETE /kv/:key"
```

---

### Task 6: KV Demo — Main + Node Runner

The main.cr that wires everything together: reads env vars, creates node, starts transport, HTTP server, and tick loop.

**Files:**
- Create: `examples/kv/src/main.cr`

**Step 1: Implement main.cr**

```crystal
# examples/kv/src/main.cr
require "../../../src/raft"
require "./kv_command"
require "./kv_state_machine"
require "./kv_http_handler"
require "http/server"

# Read configuration from environment
node_id = (ENV["NODE_ID"]? || "1").to_u64
http_port = (ENV["HTTP_PORT"]? || "8001").to_i
raft_port = (ENV["RAFT_PORT"]? || "9000").to_i
peers_str = ENV["PEERS"]? || ""

# Parse peers: "node-2:9000,node-3:9000" -> [{id, host, port}]
peer_configs = [] of {UInt64, String, Int32}
peers_str.split(",").each_with_index do |peer, i|
  next if peer.empty?
  parts = peer.strip.split(":")
  host = parts[0]
  port = parts[1].to_i
  # Peer IDs: skip our own ID, assign sequentially
  # Convention: node-1=1, node-2=2, node-3=3
  # Extract ID from hostname if possible, otherwise use index
  peer_id = if host.matches?(/node-(\d+)/)
    host.match(/node-(\d+)/).not_nil![1].to_u64
  else
    (i + 1).to_u64
    end
  peer_id = (i + 1).to_u64  # Simplified: peers listed in order, IDs skip self
  # Actually, let's just derive IDs from the full set
  peer_configs << {peer_id, host, port}
end

# Assign peer IDs: all node IDs are 1,2,3. We know our ID, peers are the others.
all_ids = [1_u64, 2_u64, 3_u64]
peer_ids = all_ids.reject(node_id)

# Raft config
config = Raft::Config.new
config.data_dir = ENV["DATA_DIR"]? || "/data/raft"
config.election_timeout_min_ticks = 10_u32
config.election_timeout_max_ticks = 20_u32
config.heartbeat_ticks = 2_u32

# Create state machine and node
state_machine = KVStateMachine.new
metrics = Raft::Metrics.new(node_id: node_id)
node = Raft::Node(KVCommand).new(id: node_id, peers: peer_ids, config: config, state_machine: state_machine)
node.metrics = metrics

# Setup TCP transport
transport = Raft::TCPTransport.new(node_id: node_id, listen_address: "0.0.0.0", listen_port: raft_port)
peer_configs.each_with_index do |pc, i|
  transport.register_peer(peer_ids[i], pc[1], pc[2])
end
transport.start

# Tick loop
spawn do
  loop do
    sleep 50.milliseconds
    node.tick

    # Send outgoing messages
    node.take_messages.each do |msg|
      # Send to all peers (messages don't have a `to` field in POC)
      peer_ids.each do |peer_id|
        transport.send(to: peer_id, message: msg)
      end
    end

    # Process incoming messages
    transport.receive(for_node: node_id).each do |msg|
      node.step(msg)
    end
  end
end

# HTTP server
raft_handler = Raft::HTTP::Handler(KVCommand).new(node)
kv_handler = KVHttpHandler.new(node, state_machine)

server = ::HTTP::Server.new([raft_handler, kv_handler]) do |context|
  context.response.status_code = 404
  context.response.print "Not found"
end

puts "Node #{node_id} starting on HTTP :#{http_port}, Raft :#{raft_port}"
server.bind_tcp("0.0.0.0", http_port)
server.listen
```

**Step 2: Test locally (optional, manual)**

```bash
crystal build examples/kv/src/main.cr -o kv_node -Dpreview_mt -Dexecution_context
NODE_ID=1 HTTP_PORT=8001 RAFT_PORT=9001 PEERS="127.0.0.1:9002,127.0.0.1:9003" ./kv_node
```

**Step 3: Commit**

```bash
git add examples/kv/src/main.cr
git commit -m "feat: add KV demo main.cr — wires node, transport, HTTP server"
```

---

### Task 7: Docker Setup

**Files:**
- Create: `examples/kv/Dockerfile`
- Create: `examples/kv/docker-compose.yml`
- Create: `examples/kv/prometheus.yml`

**Step 1: Create Dockerfile**

```dockerfile
# examples/kv/Dockerfile
FROM crystallang/crystal:1.15.1-alpine AS builder
WORKDIR /app
COPY ../../src/ src/
COPY src/ examples/kv/src/
RUN crystal build examples/kv/src/main.cr -o kv_node -Dpreview_mt -Dexecution_context --release --static

FROM alpine:3.19
RUN apk add --no-cache libgcc
COPY --from=builder /app/kv_node /usr/local/bin/kv_node
RUN mkdir -p /data/raft
CMD ["/usr/local/bin/kv_node"]
```

Note: The Dockerfile needs to be built from the project root so it can access `src/raft`. We'll use `docker compose build` with the appropriate context.

Actually, for simplicity, let's use a Dockerfile at the project root that takes the example as a build arg, or just build from root:

```dockerfile
# examples/kv/Dockerfile
FROM crystallang/crystal:1.15.1-alpine AS builder
WORKDIR /app
COPY . .
RUN crystal build examples/kv/src/main.cr -o kv_node -Dpreview_mt -Dexecution_context --release --static

FROM alpine:3.19
COPY --from=builder /app/kv_node /usr/local/bin/kv_node
RUN mkdir -p /data/raft
CMD ["/usr/local/bin/kv_node"]
```

**Step 2: Create docker-compose.yml**

```yaml
# examples/kv/docker-compose.yml
services:
  node-1:
    build:
      context: ../..
      dockerfile: examples/kv/Dockerfile
    environment:
      NODE_ID: "1"
      PEERS: "node-2:9000,node-3:9000"
      HTTP_PORT: "8001"
      RAFT_PORT: "9000"
      DATA_DIR: "/data/raft"
    ports:
      - "8001:8001"
    volumes:
      - node1-data:/data/raft

  node-2:
    build:
      context: ../..
      dockerfile: examples/kv/Dockerfile
    environment:
      NODE_ID: "2"
      PEERS: "node-1:9000,node-3:9000"
      HTTP_PORT: "8002"
      RAFT_PORT: "9000"
      DATA_DIR: "/data/raft"
    ports:
      - "8002:8002"
    volumes:
      - node2-data:/data/raft

  node-3:
    build:
      context: ../..
      dockerfile: examples/kv/Dockerfile
    environment:
      NODE_ID: "3"
      PEERS: "node-1:9000,node-2:9000"
      HTTP_PORT: "8003"
      RAFT_PORT: "9000"
      DATA_DIR: "/data/raft"
    ports:
      - "8003:8003"
    volumes:
      - node3-data:/data/raft

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"

volumes:
  node1-data:
  node2-data:
  node3-data:
```

**Step 3: Create prometheus.yml**

```yaml
# examples/kv/prometheus.yml
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

**Step 4: Commit**

```bash
git add examples/kv/Dockerfile examples/kv/docker-compose.yml examples/kv/prometheus.yml
git commit -m "feat: add Docker Compose setup — 3-node cluster with Prometheus"
```

---

### Task 8: TUI — Cluster Dashboard

The TUI connects to all nodes via HTTP and displays a live dashboard with chaos controls. This is a separate binary in `src/raft/tui/`.

**Files:**
- Create: `src/raft/tui/dashboard.cr`
- Create: `src/raft/tui/main.cr`

**Step 1: Implement the Dashboard class**

```crystal
# src/raft/tui/dashboard.cr
require "http/client"
require "json"

module Raft
  module TUI
    struct NodeStatus
      property id : UInt64 = 0_u64
      property role : String = "unknown"
      property term : UInt64 = 0_u64
      property leader_id : UInt64? = nil
      property commit_index : UInt64 = 0_u64
      property last_log_index : UInt64 = 0_u64
      property peers : Array(UInt64) = [] of UInt64
      property paused : Bool = false
      property reachable : Bool = false
      property address : String = ""

      def initialize(@address)
      end
    end

    class Dashboard
      @nodes : Array(NodeStatus)
      @events : Array(String) = [] of String
      @running : Bool = true
      @previous_states : Hash(String, String) = {} of String => String

      def initialize(addresses : Array(String))
        @nodes = addresses.map { |addr| NodeStatus.new(addr) }
      end

      def run
        # Enter raw mode
        print "\e[?25l"     # hide cursor
        print "\e[2J"       # clear screen
        STDIN.raw!

        # Poll loop in background
        spawn do
          while @running
            poll_nodes
            render
            sleep 500.milliseconds
          end
        end

        # Input loop
        while @running
          handle_input
        end
      ensure
        print "\e[?25l"
        print "\e[?25h"     # show cursor
        STDIN.cooked!
        puts "\nGoodbye."
      end

      private def poll_nodes
        @nodes.each do |node|
          begin
            response = ::HTTP::Client.get("#{node.address}/raft/status", connect_timeout: 1, read_timeout: 1)
            if response.status_code == 200
              data = JSON.parse(response.body)
              old_role = node.role
              node.id = data["id"].as_i64.to_u64
              node.role = data["role"].as_s
              node.term = data["term"].as_i64.to_u64
              node.leader_id = data["leader_id"].as_i64?.try(&.to_u64)
              node.commit_index = data["commit_index"].as_i64.to_u64
              node.last_log_index = data["last_log_index"].as_i64.to_u64
              node.paused = data["paused"].as_bool
              node.reachable = true

              # Detect state changes
              if old_role != "unknown" && old_role != node.role
                add_event("Node #{node.id} became #{node.role} (term #{node.term})")
              end
            else
              node.reachable = false
            end
          rescue ex
            node.reachable = false
          end
        end
      end

      private def render
        print "\e[H" # move to top-left

        # Header
        puts "\e[1m\e[36m── Raft Cluster Dashboard ─────────────────────────────\e[0m"
        puts ""

        # Node status
        @nodes.each do |node|
          if !node.reachable
            puts "  Node #{node.id} (#{node.address})    \e[31m██ UNREACHABLE\e[0m                    "
            puts "                                                                  "
          elsif node.paused
            puts "  Node #{node.id} (#{node.address})    \e[33m██ PAUSED\e[0m       term: #{node.term}        "
            puts "  commit: #{node.commit_index}  log: #{node.last_log_index}                                    "
          else
            role_display = case node.role
              when "leader"    then "\e[32m██ LEADER\e[0m"
              when "candidate" then "\e[33m░░ CANDIDATE\e[0m"
              else                  "\e[34m░░ FOLLOWER\e[0m"
              end
            puts "  Node #{node.id} (#{node.address})    #{role_display}   term: #{node.term}        "
            if node.role == "leader"
              puts "  commit: #{node.commit_index}  log: #{node.last_log_index}                                    "
            else
              puts "  commit: #{node.commit_index}  log: #{node.last_log_index}  leader: #{node.leader_id}              "
            end
          end
          puts ""
        end

        # Event log
        puts "\e[1m\e[36m── Event Log ─────────────────────────────────────────\e[0m"
        last_events = @events.last(6)
        last_events.each do |event|
          puts "  #{event}                                              "
        end
        (6 - last_events.size).times { puts "                                                       " }

        # Controls
        puts ""
        puts "\e[1m\e[36m── Controls ──────────────────────────────────────────\e[0m"
        puts "  \e[1m[1-#{@nodes.size}]\e[0m Select node  \e[1m[p]\e[0m Pause  \e[1m[r]\e[0m Resume  \e[1m[x]\e[0m Partition  \e[1m[h]\e[0m Heal"
        puts "  \e[1m[k]\e[0m Kill leader  \e[1m[a]\e[0m Heal all   \e[1m[q]\e[0m Quit"
      end

      private def handle_input
        char = STDIN.read_char
        return unless char

        case char
        when 'q'
          @running = false
        when 'k'
          kill_leader
        when 'a'
          heal_all
        when 'p'
          print "\e[#{@nodes.size * 3 + 14};3H\e[KPause which node? [1-#{@nodes.size}] "
          if num = STDIN.read_char
            if idx = num.to_i?
              pause_node(idx)
            end
          end
        when 'r'
          print "\e[#{@nodes.size * 3 + 14};3H\e[KResume which node? [1-#{@nodes.size}] "
          if num = STDIN.read_char
            if idx = num.to_i?
              resume_node(idx)
            end
          end
        when 'x'
          print "\e[#{@nodes.size * 3 + 14};3H\e[KPartition which node? [1-#{@nodes.size}] "
          if num = STDIN.read_char
            if idx = num.to_i?
              partition_node(idx)
            end
          end
        when 'h'
          print "\e[#{@nodes.size * 3 + 14};3H\e[KHeal which node? [1-#{@nodes.size}] "
          if num = STDIN.read_char
            if idx = num.to_i?
              heal_node(idx)
            end
          end
        end
      end

      private def kill_leader
        if leader = @nodes.find { |n| n.role == "leader" && n.reachable }
          post_admin(leader.address, "pause")
          add_event("Paused leader (Node #{leader.id})")
        else
          add_event("No reachable leader found")
        end
      end

      private def heal_all
        @nodes.each do |node|
          post_admin(node.address, "heal")
          post_admin(node.address, "resume")
        end
        add_event("Healed all nodes")
      end

      private def pause_node(id : Int32)
        if node = @nodes.find { |n| n.id == id.to_u64 }
          post_admin(node.address, "pause")
          add_event("Paused Node #{id}")
        end
      end

      private def resume_node(id : Int32)
        if node = @nodes.find { |n| n.id == id.to_u64 }
          post_admin(node.address, "resume")
          add_event("Resumed Node #{id}")
        end
      end

      private def partition_node(id : Int32)
        if node = @nodes.find { |n| n.id == id.to_u64 }
          post_admin(node.address, "partition")
          add_event("Partitioned Node #{id}")
        end
      end

      private def heal_node(id : Int32)
        if node = @nodes.find { |n| n.id == id.to_u64 }
          post_admin(node.address, "heal")
          add_event("Healed Node #{id}")
        end
      end

      private def post_admin(address : String, action : String)
        ::HTTP::Client.post("#{address}/raft/admin/#{action}", connect_timeout: 1, read_timeout: 1)
      rescue ex
        add_event("Failed to #{action}: #{ex.message}")
      end

      private def add_event(message : String)
        timestamp = Time.local.to_s("%H:%M:%S")
        @events << "#{timestamp}  #{message}"
      end
    end
  end
end
```

**Step 2: Implement TUI main binary**

```crystal
# src/raft/tui/main.cr
require "./dashboard"

addresses = ARGV
if addresses.empty?
  addresses = [
    "http://127.0.0.1:8001",
    "http://127.0.0.1:8002",
    "http://127.0.0.1:8003",
  ]
  STDERR.puts "No addresses provided, using defaults: #{addresses.join(", ")}"
end

dashboard = Raft::TUI::Dashboard.new(addresses)
dashboard.run
```

**Step 3: Test manually**

Build: `crystal build src/raft/tui/main.cr -o raft-tui -Dpreview_mt -Dexecution_context`
Run: `./raft-tui http://127.0.0.1:8001 http://127.0.0.1:8002 http://127.0.0.1:8003`

**Step 4: Commit**

```bash
git add src/raft/tui/dashboard.cr src/raft/tui/main.cr
git commit -m "feat: add Raft::TUI — live cluster dashboard with chaos controls"
```

---

### Task 9: Node — Add Partition Support

The admin API needs `partition`/`heal` support. Since TCPTransport doesn't have this, we add it to Node itself (drop all messages when partitioned).

**Files:**
- Modify: `src/raft/node.cr`
- Modify: `src/raft/http/handler.cr` (add partition/heal routes)
- Modify: `spec/raft/node_spec.cr` (add test)

**Step 1: Write failing test**

Add to `spec/raft/node_spec.cr`:

```crystal
  describe "partition" do
    it "drops messages when partitioned" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 100_u32
      config.election_timeout_max_ticks = 100_u32

      node = create_test_node(1_u64, [2_u64, 3_u64], config)
      node.partition

      heartbeat = Raft::Message.new(
        type: Raft::MessageType::AppendEntries,
        from: 2_u64,
        term: 1_u64,
      )
      node.step(heartbeat)
      node.take_messages.size.should eq 0 # no response generated

      node.heal
      node.step(heartbeat)
      node.take_messages.size.should be > 0 # responds now

      node.close
    end
  end
```

**Step 2: Implement**

In `src/raft/node.cr`:
- Add `getter partitioned : Bool = false`
- Add `def partition; @partitioned = true; end`
- Add `def heal; @partitioned = false; end`
- At top of `step`: `return if @partitioned`
- In `take_messages`: return empty array if `@partitioned`

In `src/raft/http/handler.cr`:
- Add routes for `POST /raft/admin/partition` and `POST /raft/admin/heal`

**Step 3: Run tests**

Run: `crystal spec -Dpreview_mt -Dexecution_context`
Expected: All PASS

**Step 4: Commit**

```bash
git add src/raft/node.cr src/raft/http/handler.cr spec/raft/node_spec.cr
git commit -m "feat: add partition/heal support to Node and HTTP handler"
```

---

## Summary

| Task | Component | Description |
|------|-----------|-------------|
| 1 | Raft::Metrics | Counters, gauges, histograms with Prometheus text export |
| 2 | Node changes | Pause/resume, peers getter, metrics wiring |
| 3 | Raft::HTTP::Handler | Status, log, metrics, admin endpoints |
| 4 | KV demo | KVCommand + KVStateMachine with tests |
| 5 | KV demo | HTTP handler for /kv/:key routes |
| 6 | KV demo | main.cr wiring node + transport + HTTP |
| 7 | Docker | Dockerfile, docker-compose.yml, prometheus.yml |
| 8 | Raft::TUI | Live dashboard with chaos controls |
| 9 | Node partition | Partition/heal support for chaos testing |
