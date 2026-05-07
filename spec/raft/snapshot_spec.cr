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

# ---------------------------------------------------------------------------
# Helpers local to this file — avoid touching integration_spec.cr
# ---------------------------------------------------------------------------

private def make_compact_cluster : {Hash(Raft::NodeID, Raft::Node(TestData)), Hash(Raft::NodeID, TestStateMachine), Array(String)}
  state_machines = {} of Raft::NodeID => TestStateMachine
  nodes = {} of Raft::NodeID => Raft::Node(TestData)
  dirs = [] of String

  [1_u64, 2_u64, 3_u64].each do |id|
    sm = TestStateMachine.new
    state_machines[id] = sm
    dir = File.tempname("raft_compact_#{id}")
    Dir.mkdir_p(dir)
    dirs << dir

    cfg = Raft::Config.new
    cfg.data_dir = dir
    cfg.snapshot_interval_entries = 10_u64
    cfg.max_segment_size = 200_u32
    cfg.heartbeat_ticks = 1_u32
    cfg.election_timeout_min_ticks = 3_u32
    cfg.election_timeout_max_ticks = 5_u32

    peers = [1_u64, 2_u64, 3_u64].reject(id)
    nodes[id] = Raft::Node(TestData).new(id: id, peers: peers, config: cfg, state_machine: sm)
  end

  {nodes, state_machines, dirs}
end

# Deliver all pending messages between nodes, skipping the excluded target.
private def deliver_except(nodes : Hash(Raft::NodeID, Raft::Node(TestData)), exclude_target : Raft::NodeID)
  loop do
    any_delivered = false
    pending = [] of {Raft::NodeID, Raft::Message}

    nodes.each do |_id, node|
      node.take_messages.each do |target_id, msg|
        pending << {target_id, msg}
      end
    end

    pending.each do |target_id, msg|
      next if target_id == exclude_target
      if target_node = nodes[target_id]?
        target_node.step(msg)
        any_delivered = true
      end
    end

    break unless any_delivered
  end
end

describe "Raft snapshot integration" do
  it "lagging follower catches up via InstallSnapshot" do
    nodes, sms, dirs = make_compact_cluster

    # -----------------------------------------------------------------------
    # Step 1: elect node 1 as leader
    # -----------------------------------------------------------------------
    5.times { nodes[1_u64].tick }
    # deliver_all inline (integration_spec.cr defines it at top level but private
    # helpers in the same spec binary are accessible; use inline to be safe)
    loop do
      any_delivered = false
      pending = [] of {Raft::NodeID, Raft::Message}
      nodes.each { |_id, n| n.take_messages.each { |tid, m| pending << {tid, m} } }
      pending.each do |tid, msg|
        if tn = nodes[tid]?
          tn.step(msg)
          any_delivered = true
        end
      end
      break unless any_delivered
    end

    nodes[1_u64].role.leader?.should be_true

    # -----------------------------------------------------------------------
    # Step 2: isolate node 3, propose 30 entries, drive until leader has
    #         applied all and taken at least 2 snapshots.
    # -----------------------------------------------------------------------
    30.times { |i| nodes[1_u64].propose(TestData.new("v#{i}")) }

    # Drive the leader + node 2 while excluding node 3.
    # Many rounds because each heartbeat tick advances one step.
    100.times do
      nodes[1_u64].tick
      deliver_except(nodes, 3_u64)
      Fiber.yield
    end

    # Leader and node 2 must have applied all 30 entries and snapshotted at least twice.
    sms[1_u64].applied.size.should eq 30
    sms[2_u64].applied.size.should eq 30
    nodes[1_u64].snapshot_index.should be >= 20_u64
    nodes[1_u64].log.first_index.should be > 1_u64

    # Node 3 should be completely behind.
    sms[3_u64].applied.size.should eq 0

    # -----------------------------------------------------------------------
    # Step 3: resume normal delivery — leader will send InstallSnapshot to
    #         node 3 because next_index_for_3 <= snapshot_index.
    # -----------------------------------------------------------------------
    80.times do
      nodes[1_u64].tick
      # deliver_all inline
      loop do
        any_delivered = false
        pending = [] of {Raft::NodeID, Raft::Message}
        nodes.each { |_id, n| n.take_messages.each { |tid, m| pending << {tid, m} } }
        pending.each do |tid, msg|
          if tn = nodes[tid]?
            tn.step(msg)
            any_delivered = true
          end
        end
        break unless any_delivered
      end
      Fiber.yield
    end

    # Verify the InstallSnapshot path was exercised.
    nodes[3_u64].snapshot_index.should be > 0_u64

    # Node 3 must have fully caught up.
    sms[3_u64].applied.size.should eq 30
    sms[3_u64].applied.map(&.value).should eq sms[1_u64].applied.map(&.value)

    # -----------------------------------------------------------------------
    # Phase 2: with node 3 caught up, leader proposes 5 more entries.
    #          All three nodes must converge — this exercises post-snapshot
    #          AppendEntries and would regress if log index was misaligned.
    #
    # The structural assertion (log.first_index >= snapshot_index) is the key
    # canary: with Bug 1 unfixed, reset leaves first_index=1 while
    # snapshot_index=30, so the leader re-sends the full history (masking the
    # data corruption), but the log structure is still wrong.
    # -----------------------------------------------------------------------
    nodes[3_u64].log.first_index.should be >= nodes[3_u64].snapshot_index

    5.times { |i| nodes[1_u64].propose(TestData.new("post#{i}")) }
    50.times do
      nodes[1_u64].tick
      loop do
        any_delivered = false
        pending = [] of {Raft::NodeID, Raft::Message}
        nodes.each { |_id, n| n.take_messages.each { |tid, m| pending << {tid, m} } }
        pending.each do |tid, msg|
          if tn = nodes[tid]?
            tn.step(msg)
            any_delivered = true
          end
        end
        break unless any_delivered
      end
      Fiber.yield
    end

    [1_u64, 2_u64, 3_u64].each do |id|
      sms[id].applied.size.should eq 35
      sms[id].applied.last(5).map(&.value).should eq ["post0", "post1", "post2", "post3", "post4"]
    end

    nodes.each_value(&.close)
    dirs.each { |d| FileUtils.rm_rf(d) }
  end
end

describe "Raft::Node post-snapshot AppendEntries (regression)" do
  it "appends entries at correct semantic indexes after InstallSnapshot" do
    dir = File.tempname("raft_post_snapshot_ae")
    Dir.mkdir_p(dir)
    cfg = Raft::Config.new
    cfg.data_dir = dir

    # Build a snapshot file at index=30 covering 5 SM entries (s0..s4).
    # snapshot_index=30 makes the bug-scenario crystal-clear:
    # - buggy  path: reset sets last_index=0 → appended entries land at 1,2,3
    # - fixed  path: reset_to(30) sets last_index=30 → entries land at 31,32,33
    fake_body = IO::Memory.new
    fake_body.write_bytes(30_u64, IO::ByteFormat::LittleEndian)  # snapshot_index
    fake_body.write_bytes(1_u64, IO::ByteFormat::LittleEndian)   # snapshot_term
    fake_body.write_bytes(4_u32, IO::ByteFormat::LittleEndian)   # peer_len (byte count below)
    fake_body.write_bytes(0_u32, IO::ByteFormat::LittleEndian)   # 0 peers
    sm_seed = TestStateMachine.new
    5.times { |i| sm_seed.apply(TestData.new("s#{i}")) }
    sm_seed.snapshot(fake_body)
    body_bytes = fake_body.to_slice

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 2_u64, peers: [1_u64], config: cfg, state_machine: sm)

    # Install the snapshot (single-chunk, same framing as the existing test).
    install = Raft::Message.new(
      type: Raft::MessageType::InstallSnapshot,
      from: 1_u64,
      term: 1_u64,
      prev_log_index: 30_u64,        # snapshot_index
      prev_log_term: 1_u64,          # snapshot_term
      last_log_index: 0_u64,         # chunk offset
      last_log_term: body_bytes.size.to_u64, # total size
      success: true,                 # is_last_chunk
      entries_data: body_bytes,
    )
    node.step(install)
    node.take_messages # drain the InstallSnapshotResponse ack

    # Verify post-install state before sending AppendEntries.
    node.snapshot_index.should eq 30_u64
    node.last_applied.should eq 30_u64
    node.commit_index.should eq 30_u64

    # Build AppendEntries with entries at semantic indices 31, 32, 33.
    entries_io = IO::Memory.new
    [31_u64, 32_u64, 33_u64].each_with_index do |idx, i|
      Raft::LogEntry(TestData).new(
        term: 1_u64,
        index: idx,
        entry_type: Raft::EntryType::Normal,
        data: TestData.new("post#{i}"),
      ).to_io(entries_io)
    end
    entries_data = entries_io.to_slice

    ae = Raft::Message.new(
      type: Raft::MessageType::AppendEntries,
      from: 1_u64,
      term: 1_u64,
      prev_log_index: 30_u64,
      prev_log_term: 1_u64,
      commit_index: 33_u64,
      entries_data: entries_data,
      entries_count: 3_u32,
    )
    node.step(ae)

    # -----------------------------------------------------------------------
    # Load-bearing assertions — all four would fail under the buggy @log.reset
    # code path (which left last_index=0 after InstallSnapshot):
    #
    #   log.last_index  would be 3 (not 33)
    #   log.first_index would be 1 (not 31)
    #   commit_index    would be clamped to 3 by Math.min(33, 3)
    #   last_applied    would be 3
    #   SM contents     would be wrong (indices 1-3 treated as post0-2,
    #                   with snapshot entries never appearing)
    # -----------------------------------------------------------------------
    node.log.last_index.should eq 33_u64
    node.log.first_index.should eq 31_u64
    node.commit_index.should eq 33_u64
    node.last_applied.should eq 33_u64
    sm.applied.map(&.value).should eq ["s0", "s1", "s2", "s3", "s4", "post0", "post1", "post2"]

    node.close
    FileUtils.rm_rf(dir)
  end
end
