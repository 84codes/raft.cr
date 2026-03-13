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

  {% if flag?(:raft_debug) %}
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
  {% end %}

  describe "peers" do
    it "exposes peer list including self" do
      node = create_test_node(1_u64, [2_u64, 3_u64])
      node.peers.map(&.id).sort.should eq [1_u64, 2_u64, 3_u64]
      node.peers.all?(&.voter?).should eq true
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

  describe "membership changes" do
    it "add_server adds a learner" do
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
      nodes[1_u64].role.should eq Raft::Role::Leader

      result = nodes[1_u64].add_server(4_u64)
      result.should eq true

      # Node 4 should be a learner
      peer4 = nodes[1_u64].peers.find { |p| p.id == 4_u64 }
      peer4.should_not be_nil
      peer4.not_nil!.learner?.should eq true

      # Quorum should still be based on 3 voters (self + 2 original peers)
      nodes[1_u64].voters.size.should eq 3

      nodes.each_value(&.close)
    end

    it "add_server rejects if not leader" do
      node = create_test_node(1_u64, [2_u64, 3_u64])
      node.role.should eq Raft::Role::Follower
      node.add_server(4_u64).should eq false
      node.close
    end

    it "add_server rejects duplicate node" do
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
      nodes[1_u64].add_server(2_u64).should eq false
      nodes.each_value(&.close)
    end

    it "promote_learner promotes to voter" do
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

      leader.add_server(4_u64)

      # Commit the add_server config change before promoting
      messages = leader.take_messages
      messages.each do |target_id, msg|
        nodes[target_id].step(msg) if nodes.has_key?(target_id)
      end
      [2_u64, 3_u64].each do |id|
        nodes[id].take_messages.each do |target_id, msg|
          leader.step(msg) if target_id == 1_u64
        end
      end

      leader.promote_learner(4_u64).should eq true

      peer4 = leader.peers.find { |p| p.id == 4_u64 }
      peer4.not_nil!.voter?.should eq true
      leader.voters.size.should eq 4

      nodes.each_value(&.close)
    end

    it "promote_learner rejects if node is already a voter" do
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
      nodes[1_u64].promote_learner(2_u64).should eq false
      nodes.each_value(&.close)
    end

    it "remove_server removes a node" do
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

      nodes[1_u64].remove_server(3_u64).should eq true
      nodes[1_u64].peers.any? { |p| p.id == 3_u64 }.should eq false
      nodes[1_u64].voters.size.should eq 2

      nodes.each_value(&.close)
    end

    it "remove_server rejects removing self" do
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
      nodes[1_u64].remove_server(1_u64).should eq false
      nodes.each_value(&.close)
    end

    it "remove_server rejects if not leader" do
      node = create_test_node(1_u64, [2_u64, 3_u64])
      node.remove_server(2_u64).should eq false
      node.close
    end

    it "removed node goes back to standalone" do
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

      # Remove node 3
      leader.remove_server(3_u64).should eq true

      # Deliver AppendEntries (with config entry in log) to node 3
      messages = leader.take_messages
      messages.each do |target_id, msg|
        nodes[target_id].step(msg) if target_id == 3_u64
      end

      # Node 3 should be standalone now
      nodes[3_u64].peers.should be_empty
      nodes[3_u64].role.should eq Raft::Role::Follower
      nodes[3_u64].commit_index.should eq 0_u64
      nodes[3_u64].log.last_index.should eq 0_u64

      # Drain any pending responses from the removal notification
      nodes[3_u64].take_messages

      # Node 3 should not start elections
      10.times { nodes[3_u64].tick }
      nodes[3_u64].role.should eq Raft::Role::Follower
      nodes[3_u64].take_messages.should be_empty

      nodes.each_value(&.close)
    end

    it "remove_server rejects if it would leave zero voters" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 5_u32
      config.election_timeout_max_ticks = 5_u32
      config.heartbeat_ticks = 100_u32

      nodes = {
        1_u64 => create_test_node(1_u64, [2_u64], config),
        2_u64 => create_test_node(2_u64, [1_u64], config),
      }
      elect_leader(nodes)

      # Removing node 2 would leave only node 1 (self) — allowed
      nodes[1_u64].remove_server(2_u64).should eq true

      # But now we can't remove self (existing guard)
      nodes[1_u64].remove_server(1_u64).should eq false

      nodes.each_value(&.close)
    end

    it "learners receive replication but don't affect quorum" do
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

      # Add node 4 as learner
      leader.add_server(4_u64)

      # Leader should send AppendEntries to all peers including the learner
      messages = leader.take_messages
      target_ids = messages.map { |tid, _| tid }.uniq.sort
      target_ids.should eq [2_u64, 3_u64, 4_u64]

      # Propose a value — quorum is still 2 (out of 3 voters)
      leader.propose(TestData.new("test"))

      # Deliver to node 2 only (quorum = self + node2 = 2 out of 3 voters)
      messages = leader.take_messages
      messages.each do |target_id, msg|
        if target_id == 2_u64
          nodes[2_u64].step(msg)
        end
      end

      # Deliver response from node 2
      nodes[2_u64].take_messages.each do |target_id, msg|
        leader.step(msg) if target_id == 1_u64
      end

      # Commit should advance — learner node 4 not needed for quorum
      leader.commit_index.should be > 0

      nodes.each_value(&.close)
    end

    it "persists and recovers membership changes" do
      dir = File.tempname("raft_membership")
      Dir.mkdir_p(dir)

      c1 = Raft::Config.new
      c1.data_dir = dir
      c1.election_timeout_min_ticks = 5_u32
      c1.election_timeout_max_ticks = 5_u32
      c1.heartbeat_ticks = 100_u32

      c2 = Raft::Config.new
      c2.data_dir = File.tempname("raft_node_2")
      c2.election_timeout_min_ticks = 5_u32
      c2.election_timeout_max_ticks = 5_u32
      c2.heartbeat_ticks = 100_u32
      Dir.mkdir_p(c2.data_dir)

      c3 = Raft::Config.new
      c3.data_dir = File.tempname("raft_node_3")
      c3.election_timeout_min_ticks = 5_u32
      c3.election_timeout_max_ticks = 5_u32
      c3.heartbeat_ticks = 100_u32
      Dir.mkdir_p(c3.data_dir)

      nodes = {
        1_u64 => Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: c1, state_machine: TestStateMachine.new),
        2_u64 => Raft::Node(TestData).new(id: 2_u64, peers: [1_u64, 3_u64], config: c2, state_machine: TestStateMachine.new),
        3_u64 => Raft::Node(TestData).new(id: 3_u64, peers: [1_u64, 2_u64], config: c3, state_machine: TestStateMachine.new),
      }
      elect_leader(nodes)

      # Add node 4 and commit the config change before promoting
      leader = nodes[1_u64]
      leader.add_server(4_u64)

      messages = leader.take_messages
      messages.each do |target_id, msg|
        nodes[target_id].step(msg) if nodes.has_key?(target_id)
      end
      [2_u64, 3_u64].each do |id|
        nodes[id].take_messages.each do |target_id, msg|
          leader.step(msg) if target_id == 1_u64
        end
      end

      leader.promote_learner(4_u64)
      leader.peers.size.should eq 4
      leader.close

      # Recover node 1 — should have 4 peers from persisted state
      c1_recover = Raft::Config.new
      c1_recover.data_dir = dir
      c1_recover.election_timeout_min_ticks = 5_u32
      c1_recover.election_timeout_max_ticks = 5_u32

      node_recovered = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: c1_recover, state_machine: TestStateMachine.new)
      node_recovered.peers.size.should eq 4
      node_recovered.peers.map(&.id).sort.should eq [1_u64, 2_u64, 3_u64, 4_u64]
      node_recovered.peers.all?(&.voter?).should eq true

      node_recovered.close
      nodes[2_u64].close
      nodes[3_u64].close
      FileUtils.rm_rf(dir)
      FileUtils.rm_rf(c2.data_dir)
      FileUtils.rm_rf(c3.data_dir)
    end

    it "bootstrap writes a configuration entry to the log" do
      node = create_test_node(1_u64, [] of UInt64)
      node.bootstrap

      node.log.last_index.should eq 1_u64
      entry = node.log.get(1_u64)
      entry.entry_type.should eq Raft::EntryType::Configuration
      entry.config_data.size.should be > 0

      node.close
    end

    it "bootstrap creates single-node cluster" do
      node = create_test_node(1_u64, [] of UInt64)
      node.role.should eq Raft::Role::Follower
      node.peers.should be_empty

      result = node.bootstrap
      result.should eq true
      node.role.should eq Raft::Role::Leader
      node.current_term.should eq 1_u64
      node.peers.size.should eq 1
      node.peers[0].id.should eq 1_u64
      node.peers[0].voter?.should eq true
      node.leader_id.should eq 1_u64

      node.close
    end

    it "bootstrap fails if node already has peers" do
      node = create_test_node(1_u64, [2_u64, 3_u64])
      node.bootstrap.should eq false
      node.close
    end

    it "bootstrap node can then add servers" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 5_u32
      config.election_timeout_max_ticks = 5_u32
      config.heartbeat_ticks = 100_u32

      node = create_test_node(1_u64, [] of UInt64, config)
      node.bootstrap.should eq true
      node.role.should eq Raft::Role::Leader

      node.add_server(2_u64).should eq true
      node.peers.size.should eq 2
      peer2 = node.peers.find { |p| p.id == 2_u64 }
      peer2.not_nil!.learner?.should eq true

      node.close
    end

    it "standalone node does not start elections" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 2_u32
      config.election_timeout_max_ticks = 2_u32

      node = create_test_node(1_u64, [] of UInt64, config)
      10.times { node.tick }
      node.role.should eq Raft::Role::Follower
      node.current_term.should eq 0_u64
      node.take_messages.should be_empty

      node.close
    end

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

    it "automatically promotes learner when caught up" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 5_u32
      config.election_timeout_max_ticks = 5_u32
      config.heartbeat_ticks = 100_u32

      nodes = {
        1_u64 => create_test_node(1_u64, [2_u64, 3_u64], config),
        2_u64 => create_test_node(2_u64, [1_u64, 3_u64], config),
        3_u64 => create_test_node(3_u64, [1_u64, 2_u64], config),
        4_u64 => create_test_node(4_u64, [] of UInt64, config),
      }
      elect_leader(nodes)
      leader = nodes[1_u64]

      # Add node 4 as learner
      leader.add_server(4_u64)
      leader.peers.find { |p| p.id == 4_u64 }.not_nil!.learner?.should eq true

      # Send AppendEntries to all peers including node 4
      messages = leader.take_messages
      messages.each do |target_id, msg|
        nodes[target_id].step(msg) if nodes.has_key?(target_id)
      end

      # Deliver responses from voters first to commit the config change
      [2_u64, 3_u64].each do |id|
        nodes[id].take_messages.each do |target_id, msg|
          leader.step(msg) if target_id == 1_u64
        end
      end

      # Now deliver response from node 4 — should trigger auto-promotion
      # since the add_server config is committed and node 4 is caught up
      nodes[4_u64].take_messages.each do |target_id, msg|
        leader.step(msg) if target_id == 1_u64
      end

      # Node 4 should now be a voter
      peer4 = leader.peers.find { |p| p.id == 4_u64 }
      peer4.not_nil!.voter?.should eq true
      leader.voters.size.should eq 4

      nodes.each_value(&.close)
    end

    it "rejects RequestVote from non-member" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 5_u32
      config.election_timeout_max_ticks = 5_u32

      node = create_test_node(1_u64, [2_u64, 3_u64], config)

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

      # Node 4 should have peers now (applied config from leader)
      node4.peers.size.should be > 0
      # And should be a learner
      node4.peers.find { |p| p.id == 4_u64 }.not_nil!.learner?.should eq true

      # Drain any pending responses from replication
      node4.take_messages

      # Node 4 should NOT start elections even after many ticks
      10.times { node4.tick }
      node4.role.should eq Raft::Role::Follower
      node4.take_messages.should be_empty

      node4.close
      nodes.each_value(&.close)
    end
  end
end
