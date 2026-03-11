module Raft
  class MemoryTransport < Transport
    @channels = Hash({UInt64, NodeID}, Channel(Message)).new
    @isolated = Set(NodeID).new

    def register_channel(group_id : UInt64, node_id : NodeID, channel : Channel(Message))
      @channels[{group_id, node_id}] = channel
    end

    def send(to : NodeID, message : Message)
      return if @isolated.includes?(to) || @isolated.includes?(message.from)
      if ch = @channels[{message.group_id, to}]?
        ch.send(message)
      end
    end

    def isolate(node : NodeID)
      @isolated.add(node)
    end

    def heal(node : NodeID)
      @isolated.delete(node)
    end
  end
end
