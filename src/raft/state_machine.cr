module Raft
  abstract class StateMachine(T)
    abstract def apply(entry : T)
    abstract def snapshot(io : IO)
    abstract def restore(io : IO)
  end
end
