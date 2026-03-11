require "../../../src/raft"

class ValueStateMachine < Raft::StateMachine(KVCommand)
  property value : String? = nil

  def initialize(@value : String? = nil)
  end

  def apply(entry : KVCommand)
    case entry.action
    when KVAction::Put    then @value = entry.value
    when KVAction::Delete then @value = nil
    end
  end

  def snapshot(io : IO)
    if v = @value
      io.write_bytes(v.bytesize.to_u32, IO::ByteFormat::LittleEndian)
      io.write(v.to_slice)
    else
      io.write_bytes(0_u32, IO::ByteFormat::LittleEndian)
    end
  end

  def restore(io : IO)
    size = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
    if size > 0
      slice = Bytes.new(size)
      io.read_fully(slice)
      @value = String.new(slice)
    else
      @value = nil
    end
  end
end
