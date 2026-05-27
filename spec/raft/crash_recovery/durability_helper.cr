# AUTO-GENERATED from test plan: docs/testing-plans/raft-cr-project-stability.md
# Scenario S05 helper: durability_survives_crash_mid_persist_state
# Plan sha at generation: c58aefdaa18581a3382c6594af7e995e6bc2af4b
#
# Standalone Crystal program. Spawned as a subprocess by the spec; loops
# appending entries until SIGKILLed at a random offset by the parent. The
# spec then reopens the data dir and verifies durability invariants.
#
# argv[1] = data_dir

require "../../../src/raft"

struct H_TestData
  getter value : String

  def initialize(@value : String)
  end

  def bytesize : Int32
    sizeof(UInt32) + @value.bytesize
  end

  def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian)
    io.write_bytes(@value.bytesize.to_u32, format)
    io.write(@value.to_slice)
  end

  def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
    sz = io.read_bytes(UInt32, format)
    buf = Bytes.new(sz)
    io.read_fully(buf) if sz > 0
    new(String.new(buf))
  end
end

class H_SM < Raft::StateMachine(H_TestData)
  def apply(entry : H_TestData)
  end

  def snapshot(io : IO)
  end

  def restore(io : IO)
  end
end

dir = ARGV[0]? || abort("usage: durability_helper <data_dir>")

cfg = Raft::Config.new
cfg.data_dir = dir
cfg.election_timeout_min_ticks = 5_u32
cfg.election_timeout_max_ticks = 5_u32
cfg.heartbeat_ticks = 100_u32
# Disable snapshotting so the spec process (which uses a different SM type for
# this scenario) doesn't encounter a snapshot body written by H_SM. Snapshot
# durability is exercised by S07; S05 exercises raft_meta + log durability only.
cfg.snapshot_interval_entries = 1_000_000_000_u64

sm = H_SM.new
node = Raft::Node(H_TestData).new(
  id: 1_u64, peers: [] of Raft::NodeID, config: cfg, state_machine: sm,
)
node.bootstrap

# Tight propose loop — single-voter cluster commits each immediately.
i = 0_u64
loop do
  node.propose(H_TestData.new("v#{i}"))
  i += 1_u64
end
