require "../spec_helper"

describe Raft::LogEntry do
  it "round-trips through IO" do
    entry = Raft::LogEntry(TestData).new(
      term: 1_u64,
      index: 1_u64,
      entry_type: Raft::EntryType::Normal,
      data: TestData.new("hello")
    )

    io = IO::Memory.new
    entry.to_io(io)
    io.rewind
    restored = Raft::LogEntry(TestData).from_io(io)

    restored.term.should eq 1_u64
    restored.index.should eq 1_u64
    restored.entry_type.should eq Raft::EntryType::Normal
    restored.data.not_nil!.value.should eq "hello"
  end

  it "reports correct byte size" do
    entry = Raft::LogEntry(TestData).new(
      term: 1_u64,
      index: 1_u64,
      entry_type: Raft::EntryType::Normal,
      data: TestData.new("hello")
    )

    io = IO::Memory.new
    entry.to_io(io)
    io.pos.should eq(8 + 8 + 1 + 4 + 4 + 5) # term + index + type + data_size + string_size + "hello"
  end
end
