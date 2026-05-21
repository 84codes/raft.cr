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
