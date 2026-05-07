require "../spec_helper"

describe Raft::Message do
  it "round-trips AppendEntries through IO" do
    msg = Raft::Message.new(
      protocol_version: 1_u8,
      group_id: 42_u64,
      type: Raft::MessageType::AppendEntries,
      from: 1_u64,
      term: 5_u64,
      prev_log_index: 10_u64,
      prev_log_term: 4_u64,
      commit_index: 9_u64,
      last_log_index: 0_u64,
      last_log_term: 0_u64,
      success: false,
      reject_hint: 0_u64,
      entries_data: Bytes.new(0),
      entries_count: 0_u32,
    )

    io = IO::Memory.new
    msg.to_io(io)
    io.rewind
    restored = Raft::Message.from_io(io, 64_u32 * 1024_u32 * 1024_u32)

    restored.protocol_version.should eq 1_u8
    restored.group_id.should eq 42_u64
    restored.type.should eq Raft::MessageType::AppendEntries
    restored.from.should eq 1_u64
    restored.term.should eq 5_u64
    restored.prev_log_index.should eq 10_u64
    restored.prev_log_term.should eq 4_u64
    restored.commit_index.should eq 9_u64
  end

  it "round-trips RequestVote through IO" do
    msg = Raft::Message.new(
      type: Raft::MessageType::RequestVote,
      from: 2_u64,
      term: 3_u64,
      last_log_index: 5_u64,
      last_log_term: 2_u64,
    )

    io = IO::Memory.new
    msg.to_io(io)
    io.rewind
    restored = Raft::Message.from_io(io, 64_u32 * 1024_u32 * 1024_u32)

    restored.type.should eq Raft::MessageType::RequestVote
    restored.last_log_index.should eq 5_u64
    restored.last_log_term.should eq 2_u64
  end

  it "round-trips with entries_data payload" do
    data = Bytes[1, 2, 3, 4, 5]
    msg = Raft::Message.new(
      type: Raft::MessageType::AppendEntries,
      from: 1_u64,
      term: 1_u64,
      entries_data: data,
      entries_count: 1_u32,
    )

    io = IO::Memory.new
    msg.to_io(io)
    io.rewind
    restored = Raft::Message.from_io(io, 64_u32 * 1024_u32 * 1024_u32)

    restored.entries_count.should eq 1_u32
    restored.entries_data.should eq data
  end

  it "rejects oversized entries_data in from_io" do
    max_payload = 64_u32 * 1024_u32 * 1024_u32
    io = IO::Memory.new
    # Write a valid message header manually
    io.write_bytes(1_u8, IO::ByteFormat::LittleEndian)                                   # protocol_version
    io.write_bytes(0_u64, IO::ByteFormat::LittleEndian)                                  # group_id
    io.write_bytes(Raft::MessageType::AppendEntries.value, IO::ByteFormat::LittleEndian) # type
    io.write_bytes(1_u64, IO::ByteFormat::LittleEndian)                                  # from
    io.write_bytes(1_u64, IO::ByteFormat::LittleEndian)                                  # term
    io.write_bytes(0_u64, IO::ByteFormat::LittleEndian)                                  # prev_log_index
    io.write_bytes(0_u64, IO::ByteFormat::LittleEndian)                                  # prev_log_term
    io.write_bytes(0_u64, IO::ByteFormat::LittleEndian)                                  # commit_index
    io.write_bytes(0_u64, IO::ByteFormat::LittleEndian)                                  # last_log_index
    io.write_bytes(0_u64, IO::ByteFormat::LittleEndian)                                  # last_log_term
    io.write_bytes(0_u8, IO::ByteFormat::LittleEndian)                                   # success
    io.write_bytes(0_u64, IO::ByteFormat::LittleEndian)                                  # reject_hint
    io.write_bytes(1_u32, IO::ByteFormat::LittleEndian)                                  # entries_count
    io.write_bytes(max_payload + 1_u32, IO::ByteFormat::LittleEndian)                    # data_size (oversized)
    io.rewind

    expect_raises(IO::Error, /exceeds max_message_payload_bytes/) do
      Raft::Message.from_io(io, max_payload)
    end
  end

  it "round-trips RequestVoteResponse through IO" do
    msg = Raft::Message.new(
      type: Raft::MessageType::RequestVoteResponse,
      from: 3_u64,
      term: 3_u64,
      success: true,
    )

    io = IO::Memory.new
    msg.to_io(io)
    io.rewind
    restored = Raft::Message.from_io(io, 64_u32 * 1024_u32 * 1024_u32)

    restored.type.should eq Raft::MessageType::RequestVoteResponse
    restored.success.should eq true
  end
end
