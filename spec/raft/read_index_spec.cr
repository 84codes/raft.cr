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

private def make_three_node_cluster : {Hash(Raft::NodeID, Raft::Node(TestData)), Hash(Raft::NodeID, TestStateMachine), Array(String)}
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
  {nodes, sms, dirs}
end

private def tick_until_leader(nodes : Hash(Raft::NodeID, Raft::Node(TestData)), candidate_id : Raft::NodeID, max_rounds : Int32 = 30) : Raft::Node(TestData)?
  max_rounds.times do
    nodes[candidate_id].tick
    deliver_all(nodes)
    if leader = nodes.values.find(&.role.leader?)
      return leader
    end
  end
  nil
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
    nodes, sms, dirs = make_three_node_cluster
    leader = tick_until_leader(nodes, 1_u64)
    leader.should_not be_nil
    leader = leader.not_nil!

    leader.propose(TestData.new("a"))
    leader.propose(TestData.new("b"))
    deliver_all(nodes)
    2.times { leader.tick }
    deliver_all(nodes)

    # Drain the cluster — all nodes apply.
    sms.each_value { |sm| sm.applied.size.should eq 2 }

    received = [] of UInt64?
    leader.read_index { |idx| received << idx }

    # Heartbeat round: tick the leader, deliver, see acks arrive.
    leader.tick
    deliver_all(nodes)

    received.size.should eq 1
    received.first.not_nil!.should be >= leader.log.last_index - 1

    nodes.each_value(&.close)
    dirs.each { |d| FileUtils.rm_rf(d) rescue nil }
  end

  it "fires nil when leader steps down before quorum confirmation" do
    nodes, sms, dirs = make_three_node_cluster
    leader = tick_until_leader(nodes, 1_u64).not_nil!

    received = [] of UInt64?
    leader.read_index { |idx| received << idx }
    received.should be_empty

    # Higher-term AE from a peer forces step-down. Don't deliver_all
    # afterwards — we want to observe the immediate step-down effect.
    leader.step(Raft::Message.new(
      type: Raft::MessageType::AppendEntries,
      from: 2_u64,
      term: leader.current_term + 5_u64,
      prev_log_index: 0_u64,
      prev_log_term: 0_u64,
      commit_index: 0_u64,
    ))
    leader.role.leader?.should be_false
    received.should eq [nil]

    nodes.each_value(&.close)
    dirs.each { |d| FileUtils.rm_rf(d) rescue nil }
  end

  it "fires nil after read_index_timeout_ticks when followers stop acking" do
    dirs = [] of String
    nodes = {} of Raft::NodeID => Raft::Node(TestData)
    sms = {} of Raft::NodeID => TestStateMachine
    [1_u64, 2_u64, 3_u64].each do |id|
      d = File.tempname("raft_ri_timeout_int_#{id}")
      Dir.mkdir_p(d)
      dirs << d
      cfg = Raft::Config.new
      cfg.data_dir = d
      cfg.heartbeat_ticks = 1_u32
      cfg.election_timeout_min_ticks = 50_u32
      cfg.election_timeout_max_ticks = 50_u32
      cfg.read_index_timeout_ticks = 3_u32
      sm = TestStateMachine.new
      sms[id] = sm
      peers = [1_u64, 2_u64, 3_u64].reject(id)
      nodes[id] = Raft::Node(TestData).new(id: id, peers: peers, config: cfg, state_machine: sm)
    end

    leader = tick_until_leader(nodes, 1_u64, max_rounds: 60).not_nil!
    leader.propose(TestData.new("a"))
    deliver_all(nodes)

    received = [] of UInt64?
    leader.read_index { |idx| received << idx }
    received.should be_empty

    # Tick the leader 4 times WITHOUT delivering — no acks arrive.
    4.times { leader.tick }
    received.should eq [nil]

    nodes.each_value(&.close)
    dirs.each { |d| FileUtils.rm_rf(d) rescue nil }
  end
end
