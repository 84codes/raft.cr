require "./message"

module Raft
  struct LogEntry(T)
    # term + index + entry_type + data_size field
    HEADER_BYTESIZE = sizeof(UInt64) + sizeof(UInt64) + sizeof(UInt8) + sizeof(UInt32)

    getter term : UInt64
    getter index : UInt64
    getter entry_type : EntryType
    getter data : T?
    getter config_data : Bytes

    def initialize(@term : UInt64, @index : UInt64, @entry_type : EntryType, @data : T? = nil, @config_data : Bytes = Bytes.new(0))
    end

    # Number of bytes this entry will occupy when serialized via `to_io`.
    # Requires `T#bytesize` to return the same byte count `T#to_io` writes.
    def bytesize : Int32
      data_payload =
        if @entry_type == EntryType::Configuration && @config_data.size > 0
          @config_data.size
        elsif d = @data
          d.bytesize
        else
          0
        end
      HEADER_BYTESIZE + data_payload
    end

    def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian)
      io.write_bytes(@term, format)
      io.write_bytes(@index, format)
      io.write_bytes(@entry_type.value, format)
      # Configuration entries store peer data in the data slot;
      # normal entries store the typed application command.
      if @entry_type == EntryType::Configuration && @config_data.size > 0
        io.write_bytes(@config_data.size.to_u32, format)
        io.write(@config_data)
      elsif d = @data
        io.write_bytes(d.bytesize.to_u32, format)
        d.to_io(io, format)
      else
        io.write_bytes(0_u32, format)
      end
    end

    def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
      term = io.read_bytes(UInt64, format)
      index = io.read_bytes(UInt64, format)
      entry_type = EntryType.new(io.read_bytes(UInt8, format))
      data_size = io.read_bytes(UInt32, format)

      data = nil
      config_data = Bytes.new(0)

      if data_size > 0
        if entry_type == EntryType::Configuration
          config_data = Bytes.new(data_size)
          io.read_fully(config_data)
        else
          data = T.from_io(io, format)
        end
      end

      new(term: term, index: index, entry_type: entry_type, data: data, config_data: config_data)
    end
  end
end
