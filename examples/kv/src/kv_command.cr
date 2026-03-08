enum KVAction : UInt8
  Put    = 0
  Delete = 1
end

struct KVCommand
  getter action : KVAction
  getter key : String
  getter value : String

  def initialize(@action : KVAction, @key : String, @value : String = "")
  end

  def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian)
    io.write_bytes(@action.value, format)
    io.write_bytes(@key.bytesize.to_u32, format)
    io.write(@key.to_slice)
    io.write_bytes(@value.bytesize.to_u32, format)
    io.write(@value.to_slice)
  end

  def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
    action = KVAction.new(io.read_bytes(UInt8, format))
    key_size = io.read_bytes(UInt32, format)
    key_slice = Bytes.new(key_size)
    io.read_fully(key_slice)
    value_size = io.read_bytes(UInt32, format)
    value_slice = Bytes.new(value_size)
    io.read_fully(value_slice)
    new(action, String.new(key_slice), String.new(value_slice))
  end
end
