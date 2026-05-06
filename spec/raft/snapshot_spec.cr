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

describe "Raft::Node log compaction after snapshot" do
  it "drops log segments whose last index is at or below snapshot_index" do
    dir = File.tempname("raft_snapshot_compact")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir
    cfg.snapshot_interval_entries = 5_u64
    cfg.max_segment_size = 60_u32 # tiny — forces multi-segment

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [] of UInt64, config: cfg, state_machine: sm)
    node.bootstrap
    20.times { |i| node.propose(TestData.new("v#{i}")) }

    # After 20 entries with snapshot every 5, snapshot_index should be ~15+.
    node.snapshot_index.should be >= 15_u64
    # The first segment should now start past index 1.
    node.log.first_index.should be > 1_u64

    node.close
    FileUtils.rm_rf(dir)
  end
end

describe "Raft::Node InstallSnapshot receive" do
  it "follower restores from chunked snapshot and resets log" do
    dir = File.tempname("raft_install_snapshot")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir

    # Build a "leader" snapshot file manually using the same framing as persist_snapshot:
    # [u64 index][u64 term][u32 peer_len][peers][sm_bytes]
    fake_body = IO::Memory.new
    fake_body.write_bytes(42_u64, IO::ByteFormat::LittleEndian)  # snapshot_index
    fake_body.write_bytes(1_u64, IO::ByteFormat::LittleEndian)   # snapshot_term
    fake_body.write_bytes(4_u32, IO::ByteFormat::LittleEndian)   # peer_len (just the u32 count below)
    fake_body.write_bytes(0_u32, IO::ByteFormat::LittleEndian)   # 0 peers
    sm_seed = TestStateMachine.new
    sm_seed.apply(TestData.new("x"))
    sm_seed.apply(TestData.new("y"))
    sm_seed.apply(TestData.new("z"))
    sm_seed.snapshot(fake_body)
    body_bytes = fake_body.to_slice

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 2_u64, peers: [1_u64], config: cfg, state_machine: sm)

    # Single-chunk install
    msg = Raft::Message.new(
      type: Raft::MessageType::InstallSnapshot,
      from: 1_u64,
      term: 1_u64,
      prev_log_index: 42_u64,        # snapshot_index
      prev_log_term: 1_u64,          # snapshot_term
      last_log_index: 0_u64,         # chunk offset
      last_log_term: body_bytes.size.to_u64, # total size
      success: true,                 # is_last_chunk
      entries_data: body_bytes,
    )
    node.step(msg)

    node.snapshot_index.should eq 42_u64
    node.snapshot_term.should eq 1_u64
    node.last_applied.should eq 42_u64
    node.commit_index.should eq 42_u64
    sm.applied.map(&.value).should eq ["x", "y", "z"]

    # Should have emitted an InstallSnapshotResponse with success=true
    out = node.take_messages
    out.size.should eq 1
    out[0][0].should eq 1_u64
    out[0][1].type.should eq Raft::MessageType::InstallSnapshotResponse
    out[0][1].success.should be_true
    out[0][1].last_log_index.should eq body_bytes.size.to_u64

    node.close
    FileUtils.rm_rf(dir)
  end
end
