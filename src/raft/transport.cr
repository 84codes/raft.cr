require "./config"
require "./message"

module Raft
  abstract class Transport
    abstract def send(to : NodeID, message : Message)
  end
end
