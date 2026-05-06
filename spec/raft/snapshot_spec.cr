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
