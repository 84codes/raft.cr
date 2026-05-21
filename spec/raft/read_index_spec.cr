require "../spec_helper"

describe "Raft::Node#read_index synchronous paths" do
  it "fires callback with nil immediately when called on a follower" do
    dir = File.tempname("raft_read_index_follower")
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

  it "fires callback with commit_index immediately on a standalone leader" do
    dir = File.tempname("raft_read_index_standalone")
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
    confirmed = received.first.not_nil!
    confirmed.should eq node.commit_index
    confirmed.should be >= 2_u64

    node.close
    FileUtils.rm_rf(dir)
  end
end

describe "Raft::Node#read_index apply gate" do
  it "drain_pending_apply fires no callbacks when last_applied < target" do
    dir = File.tempname("raft_read_index_drain_no_op")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm)
    node.bootstrap
    node.propose(TestData.new("a")) # commit_index=2, last_applied=2

    received = [] of UInt64?
    node.enqueue_pending_apply_for_test(target_commit: 5_u64) { |idx| received << idx }
    # last_applied = 2 < 5: drain does nothing.
    node.drain_pending_apply_for_test
    received.should be_empty

    node.close
    FileUtils.rm_rf(dir)
  end

  it "drain_pending_apply fires callback when last_applied >= target" do
    dir = File.tempname("raft_read_index_drain_fires")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm)
    node.bootstrap
    node.propose(TestData.new("a")) # commit_index=2, last_applied=2

    received = [] of UInt64?
    node.enqueue_pending_apply_for_test(target_commit: 2_u64) { |idx| received << idx }
    node.drain_pending_apply_for_test
    received.should eq [2_u64]

    node.close
    FileUtils.rm_rf(dir)
  end
end

describe "Raft::Node#read_index multi-voter quorum confirmation" do
  it "moves pending read to apply gate once a heartbeat quorum acks" do
    dir = File.tempname("raft_read_index_quorum")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    cfg.heartbeat_ticks = 1_u32
    cfg.election_timeout_min_ticks = 3_u32
    cfg.election_timeout_max_ticks = 5_u32

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm)
    # Force the node into leader state of a 3-voter cluster with a committed noop.
    node.force_leader_for_test([2_u64, 3_u64])
    # Node is now leader of a 3-voter cluster, term=1, commit_index>=1, last_applied>=1.

    received = [] of UInt64?
    node.read_index { |idx| received << idx }

    # No acks yet — callback hasn't fired.
    received.should be_empty
    node.pending_reads_size_for_test.should eq 1
    node.pending_apply_size_for_test.should eq 0

    # Simulate ONE follower acking — quorum of 3 needs 2 acks; leader self-acks already.
    node.step(Raft::Message.new(
      type: Raft::MessageType::AppendEntriesResponse,
      from: 2_u64,
      term: node.current_term,
      success: true,
      last_log_index: node.log.last_index,
    ))
    node.pending_reads_size_for_test.should eq 0  # confirmed (leader + node 2 = quorum), promoted
    node.pending_apply_size_for_test.should eq 0  # apply gate already satisfied: fired immediately
    received.size.should eq 1
    received.first.not_nil!.should be >= 1_u64

    node.close
    FileUtils.rm_rf(dir)
  end

  it "ignores stale-term acks" do
    dir = File.tempname("raft_read_index_stale_term")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    cfg.heartbeat_ticks = 1_u32
    cfg.election_timeout_min_ticks = 3_u32
    cfg.election_timeout_max_ticks = 5_u32

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm)
    node.force_leader_for_test([2_u64, 3_u64])
    leader_term = node.current_term

    received = [] of UInt64?
    node.read_index { |idx| received << idx }
    node.pending_reads_size_for_test.should eq 1

    # Stale ack: from a prior term (lower than confirmation_term).
    node.step(Raft::Message.new(
      type: Raft::MessageType::AppendEntriesResponse,
      from: 2_u64,
      term: leader_term - 1,    # stale
      success: true,
      last_log_index: node.log.last_index,
    ))
    received.should be_empty
    node.pending_reads_size_for_test.should eq 1

    node.close
    FileUtils.rm_rf(dir)
  end
end
