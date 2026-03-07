module Raft
  class MemoryTransport < Transport
    @mailboxes = Hash(NodeID, Array(Message)).new { |h, k| h[k] = [] of Message }
    @isolated = Set(NodeID).new

    def send(to : NodeID, message : Message)
      return if @isolated.includes?(to) || @isolated.includes?(message.from)
      @mailboxes[to] << message
    end

    def receive(for_node : NodeID) : Array(Message)
      messages = @mailboxes[for_node]
      result = messages.dup
      messages.clear
      result
    end

    def isolate(node : NodeID)
      @isolated.add(node)
    end

    def heal(node : NodeID)
      @isolated.delete(node)
    end
  end
end
