enum QueueAction : UInt8
  Publish     = 0
  Consume     = 1
  CreateQueue = 2
  DeleteQueue = 3
end

struct QueueCommand
  getter action : QueueAction
  getter queue_name : String
  getter body : Bytes
  getter req_id : String

  def initialize(@action : QueueAction, @queue_name : String, @body : Bytes = Bytes.new(0), @req_id : String = "")
  end

  def bytesize : Int32
    sizeof(UInt8) +
      sizeof(UInt32) + @queue_name.bytesize +
      sizeof(UInt32) + @body.size +
      sizeof(UInt32) + @req_id.bytesize
  end

  def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian)
    io.write_bytes(@action.value, format)
    io.write_bytes(@queue_name.bytesize.to_u32, format)
    io.write(@queue_name.to_slice)
    io.write_bytes(@body.size.to_u32, format)
    io.write(@body)
    io.write_bytes(@req_id.bytesize.to_u32, format)
    io.write(@req_id.to_slice)
  end

  def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
    action = QueueAction.new(io.read_bytes(UInt8, format))
    qn_size = io.read_bytes(UInt32, format)
    qn_buf = Bytes.new(qn_size)
    io.read_fully(qn_buf)
    body_size = io.read_bytes(UInt32, format)
    body = Bytes.new(body_size)
    io.read_fully(body) if body_size > 0
    req_size = io.read_bytes(UInt32, format)
    req_buf = Bytes.new(req_size)
    io.read_fully(req_buf)
    new(action: action, queue_name: String.new(qn_buf), body: body, req_id: String.new(req_buf))
  end
end
