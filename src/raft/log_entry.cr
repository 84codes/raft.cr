module Raft
  struct LogEntry(T)
    getter term : UInt64
    getter index : UInt64
    getter entry_type : EntryType
    getter data : T

    def initialize(@term : UInt64, @index : UInt64, @entry_type : EntryType, @data : T)
    end

    def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian)
      io.write_bytes(@term, format)
      io.write_bytes(@index, format)
      io.write_bytes(@entry_type.value, format)
      # Serialize data to a temporary buffer to get size
      data_io = IO::Memory.new
      @data.to_io(data_io, format)
      io.write_bytes(data_io.pos.to_u32, format)
      io.write(data_io.to_slice)
    end

    def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
      term = io.read_bytes(UInt64, format)
      index = io.read_bytes(UInt64, format)
      entry_type = EntryType.new(io.read_bytes(UInt8, format))
      data_size = io.read_bytes(UInt32, format)
      data = T.from_io(io, format)
      new(term: term, index: index, entry_type: entry_type, data: data)
    end
  end
end
