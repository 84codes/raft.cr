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
