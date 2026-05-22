module Raft
  # `T` is the application command type stored in log entries. It must
  # respond to:
  #   - `to_io(io : IO, format : IO::ByteFormat)`
  #   - `self.from_io(io : IO, format : IO::ByteFormat) : T`
  #   - `bytesize : Int` — must equal the bytes written by `to_io`
  abstract class StateMachine(T)
    abstract def apply(entry : T)
    abstract def snapshot(io : IO)
    abstract def restore(io : IO)
  end
end
