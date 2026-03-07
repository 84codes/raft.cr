require "../spec_helper"
require "file_utils"
require "./helpers/test_state_machine"

def create_test_node(id : Raft::NodeID, peers : Array(Raft::NodeID), config : Raft::Config? = nil) : Raft::Node(TestData)
  cfg = Raft::Config.new
  if c = config
    cfg.election_timeout_min_ticks = c.election_timeout_min_ticks
    cfg.election_timeout_max_ticks = c.election_timeout_max_ticks
    cfg.heartbeat_ticks = c.heartbeat_ticks
    cfg.max_segment_size = c.max_segment_size
  end
  dir = File.tempname("raft_node_#{id}")
  Dir.mkdir_p(dir)
  cfg.data_dir = dir
  sm = TestStateMachine.new
  Raft::Node(TestData).new(id: id, peers: peers, config: cfg, state_machine: sm)
end

# Helper to elect node 1 as leader in a 3-node cluster
def elect_leader(nodes : Hash(Raft::NodeID, Raft::Node(TestData)), leader_id : Raft::NodeID = 1_u64)
  config_ticks = 5
  config_ticks.times { nodes[leader_id].tick }

  # Deliver RequestVote to all peers
  nodes[leader_id].take_messages.each do |msg|
    nodes.each_value do |node|
      next if node.id == leader_id
      node.step(msg)
    end
  end

  # Deliver vote responses back
  nodes.each do |id, node|
    next if id == leader_id
    node.take_messages.each do |msg|
      nodes[leader_id].step(msg) if msg.type == Raft::MessageType::RequestVoteResponse
    end
  end
end

describe Raft::Node do
  describe "initial state" do
    it "starts as a follower" do
      node = create_test_node(1_u64, [2_u64, 3_u64])
      node.role.should eq Raft::Role::Follower
      node.current_term.should eq 0_u64
      node.close
    end
  end

  describe "election timeout" do
    it "becomes candidate after election timeout ticks" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 5_u32
      config.election_timeout_max_ticks = 5_u32

      node = create_test_node(1_u64, [2_u64, 3_u64], config)

      4.times { node.tick }
      node.role.should eq Raft::Role::Follower

      node.tick
      messages = node.take_messages
      node.role.should eq Raft::Role::Candidate
      node.current_term.should eq 1_u64

      messages.size.should eq 2
      messages.all? { |m| m.type == Raft::MessageType::RequestVote }.should be_true

      node.close
    end

    it "resets election timer on receiving AppendEntries" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 5_u32
      config.election_timeout_max_ticks = 5_u32

      node = create_test_node(1_u64, [2_u64, 3_u64], config)

      3.times { node.tick }

      heartbeat = Raft::Message.new(
        type: Raft::MessageType::AppendEntries,
        from: 2_u64,
        term: 1_u64,
        commit_index: 0_u64,
      )
      node.step(heartbeat)

      4.times { node.tick }
      node.role.should eq Raft::Role::Follower

      node.close
    end
  end

  describe "leader election" do
    it "elects a leader in a 3-node cluster" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 5_u32
      config.election_timeout_max_ticks = 5_u32

      nodes = {
        1_u64 => create_test_node(1_u64, [2_u64, 3_u64], config),
        2_u64 => create_test_node(2_u64, [1_u64, 3_u64], config),
        3_u64 => create_test_node(3_u64, [1_u64, 2_u64], config),
      }

      elect_leader(nodes)

      nodes[1_u64].role.should eq Raft::Role::Leader
      nodes[1_u64].current_term.should eq 1_u64

      nodes.each_value(&.close)
    end
  end

  describe "leader behavior" do
    it "sends heartbeats on tick" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 5_u32
      config.election_timeout_max_ticks = 5_u32
      config.heartbeat_ticks = 2_u32

      nodes = {
        1_u64 => create_test_node(1_u64, [2_u64, 3_u64], config),
        2_u64 => create_test_node(2_u64, [1_u64, 3_u64], config),
        3_u64 => create_test_node(3_u64, [1_u64, 2_u64], config),
      }

      elect_leader(nodes)
      nodes[1_u64].take_messages # clear initial messages from become_leader

      2.times { nodes[1_u64].tick }
      heartbeats = nodes[1_u64].take_messages

      heartbeats.size.should eq 2
      heartbeats.all? { |m| m.type == Raft::MessageType::AppendEntries }.should be_true

      nodes.each_value(&.close)
    end

    it "replicates a proposed entry to followers" do
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
      nodes[1_u64].take_messages # clear

      nodes[1_u64].propose(TestData.new("command1"))
      messages = nodes[1_u64].take_messages

      messages.size.should eq 2
      messages.all? { |m| m.type == Raft::MessageType::AppendEntries }.should be_true
      messages.all? { |m| m.entries_count == 1_u32 }.should be_true

      nodes.each_value(&.close)
    end
  end
end
