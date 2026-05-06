module Raft
  enum EntryType : UInt8
    Normal        = 0
    Configuration = 1
    Noop          = 2
  end

  enum MessageType : UInt8
    AppendEntries           = 0
    AppendEntriesResponse   = 1
    RequestVote             = 2
    RequestVoteResponse     = 3
    InstallSnapshot         = 4
    InstallSnapshotResponse = 5
    PreVote                 = 6
    PreVoteResponse         = 7
    TimeoutNow              = 8
  end

  enum Role
    Follower
    Candidate
    Leader
  end

  struct Message
    PROTOCOL_VERSION = 1_u8

    property protocol_version : UInt8
    property group_id : UInt64
    property type : MessageType
    property from : NodeID
    property term : UInt64
    property prev_log_index : UInt64
    property prev_log_term : UInt64
    property commit_index : UInt64
    property last_log_index : UInt64
    property last_log_term : UInt64
    property success : Bool
    property reject_hint : UInt64
    property entries_data : Bytes
    property entries_count : UInt32

    def initialize(
      @protocol_version : UInt8 = PROTOCOL_VERSION,
      @group_id : UInt64 = 0_u64,
      @type : MessageType = MessageType::AppendEntries,
      @from : NodeID = 0_u64,
      @term : UInt64 = 0_u64,
      @prev_log_index : UInt64 = 0_u64,
      @prev_log_term : UInt64 = 0_u64,
      @commit_index : UInt64 = 0_u64,
      @last_log_index : UInt64 = 0_u64,
      @last_log_term : UInt64 = 0_u64,
      @success : Bool = false,
      @reject_hint : UInt64 = 0_u64,
      @entries_data : Bytes = Bytes.new(0),
      @entries_count : UInt32 = 0_u32
    )
    end

    def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian)
      io.write_bytes(@protocol_version, format)
      io.write_bytes(@group_id, format)
      io.write_bytes(@type.value, format)
      io.write_bytes(@from, format)
      io.write_bytes(@term, format)
      io.write_bytes(@prev_log_index, format)
      io.write_bytes(@prev_log_term, format)
      io.write_bytes(@commit_index, format)
      io.write_bytes(@last_log_index, format)
      io.write_bytes(@last_log_term, format)
      io.write_bytes(@success ? 1_u8 : 0_u8, format)
      io.write_bytes(@reject_hint, format)
      io.write_bytes(@entries_count, format)
      io.write_bytes(@entries_data.size.to_u32, format)
      io.write(@entries_data) if @entries_data.size > 0
    end

    def self.from_io(io : IO, max_payload : UInt32, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
      msg = new(
        protocol_version: io.read_bytes(UInt8, format),
        group_id: io.read_bytes(UInt64, format),
        type: MessageType.new(io.read_bytes(UInt8, format)),
        from: io.read_bytes(UInt64, format),
        term: io.read_bytes(UInt64, format),
        prev_log_index: io.read_bytes(UInt64, format),
        prev_log_term: io.read_bytes(UInt64, format),
        commit_index: io.read_bytes(UInt64, format),
        last_log_index: io.read_bytes(UInt64, format),
        last_log_term: io.read_bytes(UInt64, format),
        success: io.read_bytes(UInt8, format) == 1_u8,
        reject_hint: io.read_bytes(UInt64, format),
      )
      msg.entries_count = io.read_bytes(UInt32, format)
      data_size = io.read_bytes(UInt32, format)
      raise IO::Error.new("entries_data size #{data_size} exceeds max_message_payload_bytes #{max_payload}") if data_size > max_payload
      if data_size > 0
        entries_data = Bytes.new(data_size)
        io.read_fully(entries_data)
        msg.entries_data = entries_data
      end
      msg
    end
  end
end
