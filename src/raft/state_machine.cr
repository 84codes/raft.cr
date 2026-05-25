module Raft
  # Abstract base for application state machines. Subclasses implement
  # `apply`, `snapshot`, and `restore`.
  #
  # The generic parameter `T` is the application's command type. It must
  # implement three methods that together form the on-disk/wire serialization
  # contract for log entries:
  #
  # - `def to_io(io : IO, format : IO::ByteFormat) : Nil` — write self to io
  # - `def self.from_io(io : IO, format : IO::ByteFormat) : T` — read from io
  # - `def bytesize : Int32` — number of bytes `to_io` will write
  #
  # `bytesize` must return exactly the byte count `to_io` writes. The library
  # uses `bytesize` to:
  #   - predict serialized size for segment-rotation decisions in `Log#append`
  #   - emit the size prefix in `LogEntry#to_io` without an intermediate buffer
  #
  # A `bytesize` that disagrees with `to_io` will silently corrupt the log:
  # offsets drift from actual byte positions, and subsequent reads return
  # garbage or fail.
  abstract class StateMachine(T)
    abstract def apply(entry : T)
    abstract def snapshot(io : IO)
    abstract def restore(io : IO)
  end
end
