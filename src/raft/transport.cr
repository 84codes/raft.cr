module Raft
  abstract class Transport
    abstract def send(to : NodeID, message : Message)
    abstract def receive(for_node : NodeID) : Array(Message)
  end
end
