require "../../../src/raft"

class KVStateMachine < Raft::StateMachine(KVCommand)
  @store : Hash(String, String) = {} of String => String

  def apply(entry : KVCommand)
    case entry.action
    when KVAction::Put    then @store[entry.key] = entry.value
    when KVAction::Delete then @store.delete(entry.key)
    end
  end

  def get(key : String) : String?
    @store[key]?
  end

  def snapshot(io : IO)
    io.write_bytes(@store.size.to_u32, IO::ByteFormat::LittleEndian)
    @store.each do |key, value|
      io.write_bytes(key.bytesize.to_u32, IO::ByteFormat::LittleEndian)
      io.write(key.to_slice)
      io.write_bytes(value.bytesize.to_u32, IO::ByteFormat::LittleEndian)
      io.write(value.to_slice)
    end
  end

  def restore(io : IO)
    @store.clear
    count = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
    count.times do
      key_size = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
      key_slice = Bytes.new(key_size)
      io.read_fully(key_slice)
      value_size = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
      value_slice = Bytes.new(value_size)
      io.read_fully(value_slice)
      @store[String.new(key_slice)] = String.new(value_slice)
    end
  end
end
