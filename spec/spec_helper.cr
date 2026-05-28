require "spec"
require "file_utils"
require "../src/raft"

# Resize the default execution context to surface real cross-fiber sharing
# under MT. Specs that rely on single-threaded scheduling will fail here
# rather than in production.
count = Fiber::ExecutionContext.default_workers_count
Fiber::ExecutionContext.default.resize(count)

struct TestData
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
    size = io.read_bytes(UInt32, format)
    slice = Bytes.new(size)
    io.read_fully(slice)
    new(String.new(slice))
  end
end

class TestStateMachine < Raft::StateMachine(TestData)
  getter applied : Array(TestData) = [] of TestData

  def apply(entry : TestData)
    @applied << entry
  end

  def snapshot(io : IO)
    io.write_bytes(@applied.size.to_u32, IO::ByteFormat::LittleEndian)
    @applied.each do |entry|
      entry.to_io(io, IO::ByteFormat::LittleEndian)
    end
  end

  def restore(io : IO)
    @applied.clear
    count = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
    count.times do
      @applied << TestData.from_io(io, IO::ByteFormat::LittleEndian)
    end
  end
end
