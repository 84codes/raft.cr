require "../spec_helper"

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

  # Deliver PreVote to peers
  nodes[leader_id].take_messages.each do |target_id, msg|
    nodes[target_id].step(msg)
  end

  # Deliver PreVoteResponses back — triggers become_candidate + RequestVote
  nodes.each do |id, node|
    next if id == leader_id
    node.take_messages.each do |target_id, msg|
      nodes[target_id].step(msg) if msg.type == Raft::MessageType::PreVoteResponse
    end
  end

  # Deliver RequestVote to peers
  nodes[leader_id].take_messages.each do |target_id, msg|
    nodes[target_id].step(msg)
  end

  # Deliver RequestVoteResponses back — triggers become_leader
  nodes.each do |id, node|
    next if id == leader_id
    node.take_messages.each do |target_id, msg|
      nodes[target_id].step(msg) if msg.type == Raft::MessageType::RequestVoteResponse
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
    it "sends pre-vote after election timeout ticks" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 5_u32
      config.election_timeout_max_ticks = 5_u32

      node = create_test_node(1_u64, [2_u64, 3_u64], config)

      4.times { node.tick }
      node.role.should eq Raft::Role::Follower

      node.tick
      messages = node.take_messages
      # Node stays Follower until pre-vote succeeds — term unchanged
      node.role.should eq Raft::Role::Follower
      node.current_term.should eq 0_u64

      messages.size.should eq 2
      messages.all? { |_, m| m.type == Raft::MessageType::PreVote }.should be_true
      messages.all? { |_, m| m.term == 1_u64 }.should be_true # proposed term

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
      heartbeats.all? { |_, m| m.type == Raft::MessageType::AppendEntries }.should be_true

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
      messages.all? { |_, m| m.type == Raft::MessageType::AppendEntries }.should be_true
      # entries_count includes the no-op from leader election + the proposed entry
      messages.all? { |_, m| m.entries_count >= 1_u32 }.should be_true

      nodes.each_value(&.close)
    end
  end

  describe "log replication and commit" do
    it "commits entry when majority replicates" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 5_u32
      config.election_timeout_max_ticks = 5_u32
      config.heartbeat_ticks = 100_u32

      sm1 = TestStateMachine.new
      sm2 = TestStateMachine.new
      sm3 = TestStateMachine.new

      dir1 = File.tempname("raft_node_1")
      dir2 = File.tempname("raft_node_2")
      dir3 = File.tempname("raft_node_3")
      Dir.mkdir_p(dir1); Dir.mkdir_p(dir2); Dir.mkdir_p(dir3)

      c1 = Raft::Config.new; c1.data_dir = dir1; c1.election_timeout_min_ticks = 5_u32; c1.election_timeout_max_ticks = 5_u32; c1.heartbeat_ticks = 100_u32
      c2 = Raft::Config.new; c2.data_dir = dir2; c2.election_timeout_min_ticks = 5_u32; c2.election_timeout_max_ticks = 5_u32; c2.heartbeat_ticks = 100_u32
      c3 = Raft::Config.new; c3.data_dir = dir3; c3.election_timeout_min_ticks = 5_u32; c3.election_timeout_max_ticks = 5_u32; c3.heartbeat_ticks = 100_u32

      nodes = {
        1_u64 => Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: c1, state_machine: sm1),
        2_u64 => Raft::Node(TestData).new(id: 2_u64, peers: [1_u64, 3_u64], config: c2, state_machine: sm2),
        3_u64 => Raft::Node(TestData).new(id: 3_u64, peers: [1_u64, 2_u64], config: c3, state_machine: sm3),
      }

      # Elect node 1 as leader
      elect_leader(nodes)
      nodes[1_u64].take_messages # clear initial heartbeats

      # Propose an entry
      nodes[1_u64].propose(TestData.new("cmd1"))
      append_messages = nodes[1_u64].take_messages

      # Deliver AppendEntries to correct followers
      append_messages.each do |target_id, msg|
        nodes[target_id].step(msg)
      end

      # Followers respond with success
      [2_u64, 3_u64].each do |id|
        nodes[id].take_messages.each do |target_id, msg|
          nodes[target_id].step(msg) if msg.type == Raft::MessageType::AppendEntriesResponse
        end
      end

      # Leader should have committed and applied (index 1 = no-op, index 2 = cmd1)
      nodes[1_u64].commit_index.should eq 2_u64
      sm1.applied.size.should eq 1
      sm1.applied[0].value.should eq "cmd1"

      nodes.each_value(&.close)
      [dir1, dir2, dir3].each { |d| FileUtils.rm_rf(d) }
    end

    it "follower appends entries from leader and applies on commit" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 100_u32
      config.election_timeout_max_ticks = 100_u32

      sm2 = TestStateMachine.new
      dir2 = File.tempname("raft_node_2")
      Dir.mkdir_p(dir2)
      c2 = Raft::Config.new
      c2.data_dir = dir2
      c2.election_timeout_min_ticks = 100_u32
      c2.election_timeout_max_ticks = 100_u32

      node2 = Raft::Node(TestData).new(id: 2_u64, peers: [1_u64, 3_u64], config: c2, state_machine: sm2)

      # Simulate receiving AppendEntries from leader with one entry
      entry = Raft::LogEntry(TestData).new(term: 1_u64, index: 1_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("x"))
      entries_io = IO::Memory.new
      entry.to_io(entries_io)

      msg = Raft::Message.new(
        type: Raft::MessageType::AppendEntries,
        from: 1_u64,
        term: 1_u64,
        prev_log_index: 0_u64,
        prev_log_term: 0_u64,
        commit_index: 1_u64,
        entries_data: entries_io.to_slice.dup,
        entries_count: 1_u32,
      )
      node2.step(msg)

      responses = node2.take_messages
      responses.size.should eq 1
      responses[0][1].success.should be_true

      sm2.applied.size.should eq 1
      sm2.applied[0].value.should eq "x"

      node2.close
      FileUtils.rm_rf(dir2)
    end
  end

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
      # With pre-vote, timeout sends PreVote but stays Follower
      messages = node.take_messages
      messages.size.should eq 2
      messages.all? { |_, m| m.type == Raft::MessageType::PreVote }.should be_true

      node.close
    end
  end

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

  describe "pre-vote" do
    it "prevents term inflation during partition" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 3_u32
      config.election_timeout_max_ticks = 3_u32

      node = create_test_node(1_u64, [2_u64, 3_u64], config)
      node.partition

      # Tick many election timeouts — node sends PreVotes but they're dropped
      30.times { node.tick }
      node.take_messages # clear (all dropped by partition anyway)

      # Term should NOT have inflated — still 0
      node.current_term.should eq 0_u64
      node.role.should eq Raft::Role::Follower

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

  describe "persistence" do
    it "persists and recovers term and voted_for" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 5_u32
      config.election_timeout_max_ticks = 5_u32

      dir = File.tempname("raft_persist")
      Dir.mkdir_p(dir)

      c1 = Raft::Config.new
      c1.data_dir = dir
      c1.election_timeout_min_ticks = 5_u32
      c1.election_timeout_max_ticks = 5_u32

      sm = TestStateMachine.new
      node = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: c1, state_machine: sm)

      # Trigger pre-vote
      5.times { node.tick }
      pre_votes = node.take_messages

      # Simulate pre-vote success from both peers — triggers become_candidate
      pre_votes.each do |_, msg|
        response = Raft::Message.new(
          type: Raft::MessageType::PreVoteResponse,
          from: msg.type == Raft::MessageType::PreVote ? 2_u64 : 3_u64,
          term: 1_u64,
          success: true,
        )
        node.step(response)
      end

      node.current_term.should eq 1_u64
      node.voted_for.should eq 1_u64
      node.close

      c2 = Raft::Config.new
      c2.data_dir = dir
      c2.election_timeout_min_ticks = 5_u32
      c2.election_timeout_max_ticks = 5_u32

      sm2 = TestStateMachine.new
      node2 = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: c2, state_machine: sm2)
      node2.current_term.should eq 1_u64
      node2.voted_for.should eq 1_u64
      node2.role.should eq Raft::Role::Follower

      node2.close
      FileUtils.rm_rf(dir)
    end
  end
end
