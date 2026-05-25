require "../spec_helper"

describe "Raft::LogEntry#bytesize" do
  it "matches to_io's actual byte count for a Normal entry" do
    entry = Raft::LogEntry(TestData).new(
      term: 1_u64, index: 1_u64,
      entry_type: Raft::EntryType::Normal,
      data: TestData.new("hello"),
    )
    io = IO::Memory.new
    entry.to_io(io)
    entry.bytesize.should eq io.pos
  end

  it "matches to_io's actual byte count for a Configuration entry" do
    config_bytes = Bytes[0x01, 0x02, 0x03, 0x04, 0x05]
    entry = Raft::LogEntry(TestData).new(
      term: 1_u64, index: 1_u64,
      entry_type: Raft::EntryType::Configuration,
      config_data: config_bytes,
    )
    io = IO::Memory.new
    entry.to_io(io)
    entry.bytesize.should eq io.pos
  end

  it "matches to_io's actual byte count for a Noop entry" do
    entry = Raft::LogEntry(TestData).new(
      term: 1_u64, index: 1_u64,
      entry_type: Raft::EntryType::Noop,
    )
    io = IO::Memory.new
    entry.to_io(io)
    entry.bytesize.should eq io.pos
  end
end
