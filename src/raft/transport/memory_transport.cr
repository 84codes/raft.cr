require "sync/shared"
require "../transport"

module Raft
  class MemoryTransport < Transport
    @channels = Sync::Shared(Hash({UInt64, NodeID}, Channel(Message))).new(
      Hash({UInt64, NodeID}, Channel(Message)).new
    )
    @isolated = Sync::Shared(Set(NodeID)).new(Set(NodeID).new)

    def register_channel(group_id : UInt64, node_id : NodeID, channel : Channel(Message))
      @channels.lock { |h| h[{group_id, node_id}] = channel }
    end

    def send(to : NodeID, message : Message)
      isolated = @isolated.shared do |s|
        s.includes?(to) || s.includes?(message.from)
      end
      return if isolated
      ch = @channels.shared { |h| h[{message.group_id, to}]? }
      ch.send(message) if ch
    end

    def isolate(node : NodeID)
      @isolated.lock(&.add(node))
    end

    def heal(node : NodeID)
      @isolated.lock(&.delete(node))
    end
  end
end
