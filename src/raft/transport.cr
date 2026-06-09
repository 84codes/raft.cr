require "./config"
require "./message"

module Raft
  abstract class Transport
    abstract def outbox : Channel({NodeID, Message})
    abstract def register_channel(group_id : UInt64, channel : Channel(Message))
    abstract def register_peer(id : NodeID, host : String, port : Int32)
    abstract def send(to : NodeID, message : Message)
    abstract def start
    abstract def stop
  end
end
