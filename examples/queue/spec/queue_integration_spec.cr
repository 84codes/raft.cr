require "spec"
require "file_utils"
require "../../../src/raft"
require "../src/queue_command"
require "../src/queue_state_machine"

# Helper: deliver all pending messages between nodes until no more to deliver.
# Mirrors spec/raft/integration_spec.cr's deterministic delivery loop.
private def deliver_all(nodes : Hash(Raft::NodeID, Raft::Node(QueueCommand)))
  loop do
    any_delivered = false
    pending = [] of {Raft::NodeID, Raft::Message}

    nodes.each_value do |node|
      node.take_messages.each do |target_id, msg|
        pending << {target_id, msg}
      end
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

private def make_queue_cluster(node_ids : Array(UInt64) = [1_u64, 2_u64, 3_u64], group_id : UInt64 = 1_u64)
  state_machines = {} of Raft::NodeID => QueueStateMachine
  nodes = {} of Raft::NodeID => Raft::Node(QueueCommand)
  dirs = [] of String

  node_ids.each do |id|
    sm = QueueStateMachine.new
    state_machines[id] = sm
    dir = File.tempname("queue_integration_#{id}_#{group_id}")
    Dir.mkdir_p(dir)
    dirs << dir
    cfg = Raft::Config.new
    cfg.data_dir = dir
    cfg.election_timeout_min_ticks = 5_u32
    cfg.election_timeout_max_ticks = 5_u32
    cfg.heartbeat_ticks = 2_u32
    peers = node_ids.reject(id)
    nodes[id] = Raft::Node(QueueCommand).new(
      id: id, peers: peers, config: cfg, state_machine: sm, group_id: group_id,
    )
  end

  {nodes, state_machines, dirs}
end

private def cleanup(nodes : Hash(Raft::NodeID, Raft::Node(QueueCommand)), dirs : Array(String))
  nodes.each_value(&.close)
  dirs.each { |d| FileUtils.rm_rf(d) rescue nil }
end

describe "Queue integration: 3-node cluster" do
  it "replicates publishes to all replicas in FIFO order" do
    nodes, sms, dirs = make_queue_cluster
    begin
      # Trigger election on node 1
      5.times { nodes[1_u64].tick }
      deliver_all(nodes)
      nodes[1_u64].role.should eq Raft::Role::Leader

      # Propose two Publish commands on the leader
      nodes[1_u64].propose(QueueCommand.new(QueueAction::Publish, "q", body: "first".to_slice)).should be_true
      nodes[1_u64].propose(QueueCommand.new(QueueAction::Publish, "q", body: "second".to_slice)).should be_true
      deliver_all(nodes)

      # Followers need a heartbeat carrying the new commit index to apply
      2.times { nodes[1_u64].tick }
      deliver_all(nodes)

      # All three replicas should have depth 2
      sms[1_u64].depth.should eq 2
      sms[2_u64].depth.should eq 2
      sms[3_u64].depth.should eq 2

      # Verify FIFO order on each replica by consuming from each independently
      [1_u64, 2_u64, 3_u64].each do |id|
        ch1 = Channel(Bytes?).new(1)
        sms[id].register_request("verify-#{id}-1", ch1)
        sms[id].apply(QueueCommand.new(QueueAction::Consume, "q", req_id: "verify-#{id}-1"))
        String.new(ch1.receive.not_nil!).should eq "first"

        ch2 = Channel(Bytes?).new(1)
        sms[id].register_request("verify-#{id}-2", ch2)
        sms[id].apply(QueueCommand.new(QueueAction::Consume, "q", req_id: "verify-#{id}-2"))
        String.new(ch2.receive.not_nil!).should eq "second"
      end
    ensure
      cleanup(nodes, dirs)
    end
  end

  it "delivers consumed value via the bridge on the leader" do
    nodes, sms, dirs = make_queue_cluster
    begin
      # Elect node 1
      5.times { nodes[1_u64].tick }
      deliver_all(nodes)
      nodes[1_u64].role.should eq Raft::Role::Leader

      # Publish "first" and "second"
      nodes[1_u64].propose(QueueCommand.new(QueueAction::Publish, "q", body: "first".to_slice)).should be_true
      nodes[1_u64].propose(QueueCommand.new(QueueAction::Publish, "q", body: "second".to_slice)).should be_true
      deliver_all(nodes)
      2.times { nodes[1_u64].tick }
      deliver_all(nodes)

      sms[1_u64].depth.should eq 2

      # Register channel on leader's state machine, propose Consume
      ch = Channel(Bytes?).new(1)
      sms[1_u64].register_request("req-1", ch)
      nodes[1_u64].propose(QueueCommand.new(QueueAction::Consume, "q", req_id: "req-1")).should be_true
      deliver_all(nodes)

      # The leader should have applied the Consume and bridged the value to the channel
      result = ch.receive
      result.should_not be_nil
      String.new(result.not_nil!).should eq "first"

      # Pending request slot should have been cleared by the bridge
      sms[1_u64].has_pending_request?("req-1").should be_false
    ensure
      cleanup(nodes, dirs)
    end
  end

  it "delivers nil from the bridge when the queue is empty" do
    nodes, sms, dirs = make_queue_cluster
    begin
      # Elect node 1
      5.times { nodes[1_u64].tick }
      deliver_all(nodes)
      nodes[1_u64].role.should eq Raft::Role::Leader

      # Empty queue — register channel and propose Consume
      ch = Channel(Bytes?).new(1)
      sms[1_u64].register_request("req-empty", ch)
      nodes[1_u64].propose(QueueCommand.new(QueueAction::Consume, "q", req_id: "req-empty")).should be_true
      deliver_all(nodes)

      # Bridge should deliver nil for an empty queue
      ch.receive.should be_nil
      sms[1_u64].has_pending_request?("req-empty").should be_false
    ensure
      cleanup(nodes, dirs)
    end
  end
end
