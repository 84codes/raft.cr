require "../spec_helper"

describe "Raft::Node snapshot persistence" do
  it "persists and reloads snapshot across Node restart" do
    dir = File.tempname("raft_snapshot")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir

    sm1 = TestStateMachine.new
    sm1.apply(TestData.new("a"))
    sm1.apply(TestData.new("b"))

    node1 = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm1)
    # Manually persist a snapshot at index=2, term=1
    node1.persist_snapshot_for_test(2_u64, 1_u64)
    node1.close

    sm2 = TestStateMachine.new
    node2 = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm2)
    node2.snapshot_index.should eq 2_u64
    node2.snapshot_term.should eq 1_u64
    sm2.applied.size.should eq 2
    sm2.applied[0].value.should eq "a"
    sm2.applied[1].value.should eq "b"

    node2.close
    FileUtils.rm_rf(dir)
  end
end

describe "Raft::Node snapshot trigger" do
  it "snapshots after snapshot_interval_entries entries have been applied" do
    dir = File.tempname("raft_snapshot_trigger")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    cfg.snapshot_interval_entries = 5_u64

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm)
    node.bootstrap
    7.times { |i| node.propose(TestData.new("v#{i}")) }

    node.snapshot_index.should be >= 5_u64
    File.exists?(File.join(dir, "snapshot")).should be_true

    node.close
    FileUtils.rm_rf(dir)
  end
end

describe "Raft::Node snapshot + log tail recovery" do
  it "applies snapshot then replays only entries past snapshot_index" do
    dir = File.tempname("raft_snapshot_tail")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir

    sm1 = TestStateMachine.new
    node1 = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm1)
    node1.bootstrap
    node1.propose(TestData.new("a"))
    node1.propose(TestData.new("b"))
    # Single-node cluster commits immediately; sm1 now has ["a", "b"].
    sm1.applied.map(&.value).should eq ["a", "b"]

    # Snapshot at current last_applied (covers bootstrap config + "a" + "b").
    # sm1.applied == ["a", "b"] at this moment — the snapshot encodes exactly that.
    node1.persist_snapshot_for_test(node1.last_applied, node1.current_term)

    # Now append "c" — it lands in the log tail AFTER the snapshot index.
    node1.propose(TestData.new("c"))
    sm1.applied.map(&.value).should eq ["a", "b", "c"]
    node1.close

    # Restart: recover from snapshot (restores "a"+"b"), then replay "c" from log tail.
    sm2 = TestStateMachine.new
    node2 = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm2)
    sm2.applied.map(&.value).should eq ["a", "b", "c"]

    node2.close
    FileUtils.rm_rf(dir)
  end
end
