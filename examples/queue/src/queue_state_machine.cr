require "../../../src/raft"

class QueueStateMachine < Raft::StateMachine(QueueCommand)
  @messages : Deque(Bytes) = Deque(Bytes).new
  @pending : Hash(String, Channel(Bytes?)) = {} of String => Channel(Bytes?)

  def apply(entry : QueueCommand)
    case entry.action
    when QueueAction::Publish
      @messages << entry.body
    when QueueAction::Consume
      popped = @messages.shift?
      if ch = @pending.delete(entry.req_id)
        ch.send(popped)
      end
    end
  end

  def depth : Int32
    @messages.size
  end

  def register_request(req_id : String, ch : Channel(Bytes?))
    @pending[req_id] = ch
  end

  def cancel_request(req_id : String)
    @pending.delete(req_id)
  end

  def has_pending_request?(req_id : String) : Bool
    @pending.has_key?(req_id)
  end

  def snapshot(io : IO)
    io.write_bytes(@messages.size.to_u32, IO::ByteFormat::LittleEndian)
    @messages.each do |body|
      io.write_bytes(body.size.to_u32, IO::ByteFormat::LittleEndian)
      io.write(body)
    end
  end

  def restore(io : IO)
    @messages.clear
    count = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
    count.times do
      size = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
      buf = Bytes.new(size)
      io.read_fully(buf) if size > 0
      @messages << buf
    end
  end
end
