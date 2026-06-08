require "sync/exclusive"
require "sync/shared"
require "../message"

module Raft
  # In-memory transport backed by IO pipes. Wire format is identical to
  # TCPTransport — specs exercise the same encode/decode and outbox/drain
  # paths that run in production. Per-node: each instance owns one outbox,
  # one dispatcher fiber, and a reader+writer fiber pair for each peer.
  class MemoryTransport < Transport
    DEFAULT_OUTBOX_SIZE = 256
    DEFAULT_PEER_OUTBOX =  64
    DEFAULT_MAX_PAYLOAD = 64_u32 * 1024_u32 * 1024_u32

    getter node_id : NodeID
    getter outbox : Channel({NodeID, Message})

    @channels : Sync::Shared(Hash(UInt64, Channel(Message))) = Sync::Shared(Hash(UInt64, Channel(Message))).new(
      Hash(UInt64, Channel(Message)).new
    )
    @peer_readers : Sync::Shared(Hash(NodeID, IO)) = Sync::Shared(Hash(NodeID, IO)).new(
      Hash(NodeID, IO).new
    )
    @peer_writers : Sync::Shared(Hash(NodeID, IO)) = Sync::Shared(Hash(NodeID, IO)).new(
      Hash(NodeID, IO).new
    )
    @peer_outboxes : Sync::Exclusive(Hash(NodeID, Channel(Message))) = Sync::Exclusive(Hash(NodeID, Channel(Message))).new(
      Hash(NodeID, Channel(Message)).new
    )
    @isolated_flag : Atomic(Bool) = Atomic(Bool).new(false)
    @max_payload : UInt32
    @running : Bool = false

    def initialize(@node_id : NodeID,
                   outbox_size : Int32 = DEFAULT_OUTBOX_SIZE,
                   @max_payload : UInt32 = DEFAULT_MAX_PAYLOAD)
      @outbox = Channel({NodeID, Message}).new(outbox_size)
    end

    def isolated : Bool
      @isolated_flag.get
    end

    def isolated=(value : Bool)
      @isolated_flag.set(value)
    end

    def register_channel(group_id : UInt64, channel : Channel(Message))
      @channels.lock { |h| h[group_id] = channel }
    end

    def unregister_channel(group_id : UInt64)
      @channels.lock { |h| h.delete(group_id) }
    end

    # MemoryTransport routes by pipe, not network address — no-op for API
    # parity with TCPTransport.
    def register_peer(id : NodeID, host : String, port : Int32)
    end

    # Wire this transport to a peer via an IO pipe pair. Safe to call before
    # or after `start` — fibers spawn lazily for the peer.
    def connect_to(peer_id : NodeID, read : IO, write : IO)
      @peer_readers.lock { |h| h[peer_id] = read }
      @peer_writers.lock { |h| h[peer_id] = write }
      if @running
        spawn_reader(peer_id, read)
        ensure_peer_outbox(peer_id)
      end
    end

    # Synchronous write directly to the peer's pipe. Used by per-peer
    # writer fibers; tests can also call this to bypass the outbox path.
    def send(to : NodeID, message : Message)
      return if isolated
      writer = @peer_writers.shared(&.[to]?)
      return unless writer
      begin
        message.to_io(writer)
        writer.flush
      rescue IO::Error
      end
    end

    def start
      return if @running
      @running = true
      spawn(name: "memory-transport-dispatch-#{@node_id}") do
        run_dispatcher
      end
      @peer_readers.shared do |h|
        h.each do |peer_id, read|
          spawn_reader(peer_id, read)
        end
      end
      @peer_writers.shared do |h|
        h.each_key do |peer_id|
          ensure_peer_outbox(peer_id)
        end
      end
    end

    def stop
      return unless @running
      @running = false
      @outbox.close
      @peer_outboxes.lock do |h|
        h.each_value(&.close)
        h.clear
      end
      @peer_writers.lock do |h|
        h.each_value { |io| io.close rescue nil }
        h.clear
      end
      @peer_readers.lock do |h|
        h.each_value { |io| io.close rescue nil }
        h.clear
      end
    end

    private def run_dispatcher
      while @running
        target_id, msg = @outbox.receive
        route_to_peer(target_id, msg)
      end
    rescue Channel::ClosedError
    end

    private def route_to_peer(target_id : NodeID, msg : Message)
      return if isolated
      ch = @peer_outboxes.lock(&.[target_id]?)
      return unless ch
      select
      when ch.send(msg)
      else
        # Per-peer queue full — drop, Raft retries naturally
      end
    end

    private def ensure_peer_outbox(peer_id : NodeID)
      ch = @peer_outboxes.lock do |h|
        next nil if h.has_key?(peer_id)
        new_ch = Channel(Message).new(DEFAULT_PEER_OUTBOX)
        h[peer_id] = new_ch
        new_ch
      end
      return unless ch
      spawn(name: "memory-transport-write-#{@node_id}-to-#{peer_id}") do
        while @running
          msg = ch.receive
          send(to: peer_id, message: msg)
        end
      rescue Channel::ClosedError
      end
    end

    private def spawn_reader(peer_id : NodeID, read : IO)
      spawn(name: "memory-transport-read-#{@node_id}-from-#{peer_id}") do
        while @running
          begin
            msg = Message.from_io(read, @max_payload)
          rescue IO::Error
            break
          end
          next if isolated
          if ch = @channels.shared(&.[msg.group_id]?)
            ch.send(msg)
          end
        end
      end
    end

    # Test helper: create two transports wired by a pair of IO pipes (one
    # per direction). Returns them in the same order as `ids`.
    def self.pipe_pair(a_id : NodeID, b_id : NodeID) : {MemoryTransport, MemoryTransport}
      ta = new(a_id)
      tb = new(b_id)
      wire(ta, tb)
      {ta, tb}
    end

    # Test helper: full-mesh wiring for `node_ids`. Returns a hash so
    # specs can address transports by node id.
    def self.mesh(node_ids : Array(NodeID)) : Hash(NodeID, MemoryTransport)
      transports = node_ids.map { |id| {id, new(id)} }.to_h
      node_ids.each_combination(2, reuse: false) do |pair|
        wire(transports[pair[0]], transports[pair[1]])
      end
      transports
    end

    private def self.wire(a : MemoryTransport, b : MemoryTransport)
      a_read, b_write = IO.pipe
      b_read, a_write = IO.pipe
      a.connect_to(b.node_id, read: a_read, write: a_write)
      b.connect_to(a.node_id, read: b_read, write: b_write)
    end
  end
end
