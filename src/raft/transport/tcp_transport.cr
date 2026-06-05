require "socket"
require "sync/exclusive"
require "sync/shared"

module Raft
  class TCPTransport < Transport
    @listen_address : String
    @listen_port : Int32
    @peers : Sync::Shared(Hash(NodeID, {String, Int32})) = Sync::Shared(Hash(NodeID, {String, Int32})).new(
      Hash(NodeID, {String, Int32}).new
    )
    @connections : Sync::Exclusive(Hash(NodeID, TCPSocket)) = Sync::Exclusive(Hash(NodeID, TCPSocket)).new(
      Hash(NodeID, TCPSocket).new
    )
    @channels : Sync::Shared(Hash(UInt64, Channel(Message))) = Sync::Shared(Hash(UInt64, Channel(Message))).new(
      Hash(UInt64, Channel(Message)).new
    )
    @peer_outboxes : Sync::Exclusive(Hash(NodeID, Channel(Message))) = Sync::Exclusive(Hash(NodeID, Channel(Message))).new(
      Hash(NodeID, Channel(Message)).new
    )
    @server : TCPServer? = nil
    @running : Bool = false
    @data_dir : String?
    getter outbox : Channel({NodeID, Message}) = Channel({NodeID, Message}).new(256)
    @commands : Channel(TransportCommand) = Channel(TransportCommand).new(64)
    @outbox_drops : Sync::Exclusive(Hash(NodeID, Int64)) = Sync::Exclusive(Hash(NodeID, Int64)).new(
      Hash(NodeID, Int64).new(0_i64)
    )
    @inbox_drops : Sync::Exclusive(Hash(NodeID, Int64)) = Sync::Exclusive(Hash(NodeID, Int64)).new(
      Hash(NodeID, Int64).new(0_i64)
    )
    @max_payload : UInt32

    private abstract struct TransportCommand
    end

    private struct RegisterChannelCommand < TransportCommand
      getter group_id : UInt64
      getter channel : Channel(Message)

      def initialize(@group_id, @channel)
      end
    end

    private struct UnregisterChannelCommand < TransportCommand
      getter group_id : UInt64

      def initialize(@group_id)
      end
    end

    private struct RegisterPeerCommand < TransportCommand
      getter id : NodeID
      getter host : String
      getter port : Int32

      def initialize(@id, @host, @port)
      end
    end

    def initialize(@listen_address : String, @listen_port : Int32, @data_dir : String? = nil, @max_payload : UInt32 = 64_u32 * 1024_u32 * 1024_u32)
      recover_peers
    end

    def register_peer(id : NodeID, host : String, port : Int32)
      @commands.send(RegisterPeerCommand.new(id, host, port))
    end

    # Look up a registered peer's host:port under the shared lock.
    # Mutations happen on the dispatcher fiber via the commands channel.
    def peer_address?(id : NodeID) : {String, Int32}?
      @peers.shared(&.[id]?)
    end

    def register_channel(group_id : UInt64, channel : Channel(Message))
      @commands.send(RegisterChannelCommand.new(group_id, channel))
    end

    def unregister_channel(group_id : UInt64)
      @commands.send(UnregisterChannelCommand.new(group_id))
    end

    def start
      @running = true
      @server = server = TCPServer.new(@listen_address, @listen_port)
      spawn(name: "raft-transport-accept") do
        while @running
          if conn = server.accept?
            client = conn
            spawn(name: "raft-transport-conn") do
              handle_connection(client)
            end
          end
        end
      end
      spawn(name: "raft-transport-dispatch") do
        run_dispatcher
      end
    end

    def stop
      @running = false
      @server.try(&.close)
      @commands.close
      @outbox.close
      @peer_outboxes.lock do |h|
        h.each_value(&.close)
        h.clear
      end
      @connections.lock do |h|
        h.each_value { |conn| conn.close rescue nil }
        h.clear
      end
    end

    def send(to : NodeID, message : Message)
      try_send(to, message)
      nil
    end

    # Returns true if the message was written to a socket, false if no
    # connection was available or the write failed. Used by the per-peer
    # writer fiber to detect healthy↔unhealthy transitions and log once
    # per transition rather than once per heartbeat.
    private def try_send(to : NodeID, message : Message) : Bool
      conn = get_connection(to)
      return false unless conn
      begin
        message.to_io(conn)
        conn.flush
        true
      rescue ex : IO::Error
        ::Log.warn { "Transport: send to node #{to} failed: #{ex.message}" }
        @connections.lock(&.delete(to))
        conn.close rescue nil
        false
      end
    end

    private def run_dispatcher
      while @running
        select
        when cmd = @commands.receive
          process_command(cmd)
        when item = @outbox.receive
          route_to_peer(item[0], item[1])
        end
      end
    rescue Channel::ClosedError
    end

    private def process_command(cmd : TransportCommand)
      case cmd
      when RegisterChannelCommand
        @channels.lock { |h| h[cmd.group_id] = cmd.channel }
      when UnregisterChannelCommand
        @channels.lock { |h| h.delete(cmd.group_id) }
      when RegisterPeerCommand
        @peers.lock { |h| h[cmd.id] = {cmd.host, cmd.port} }
        persist_peers
      end
    end

    private def route_to_peer(target_id : NodeID, msg : Message)
      ensure_peer_fiber(target_id)
      ch = @peer_outboxes.lock(&.[target_id]?)
      return unless ch
      select
      when ch.send(msg)
      else
        # Per-peer queue full, drop — Raft retries naturally
        @outbox_drops.lock { |h| h.update(target_id) { |v| v + 1 } }
      end
    end

    private def ensure_peer_fiber(peer_id : NodeID)
      ch = @peer_outboxes.lock do |h|
        next nil if h.has_key?(peer_id)
        new_ch = Channel(Message).new(64)
        h[peer_id] = new_ch
        new_ch
      end
      return unless ch
      spawn(name: "raft-transport-peer-#{peer_id}") do
        # Local connectivity state — only this fiber sends to this peer, so
        # no synchronization needed. Bootstrap-rate retries (peer address
        # not yet registered) stay at DEBUG; once we know the address,
        # transitions healthy↔unhealthy each log exactly once.
        healthy = true
        while @running
          msg = ch.receive
          ok = try_send(to: peer_id, message: msg)
          if ok && !healthy
            ::Log.info { "Transport: connection to node #{peer_id} restored" }
            healthy = true
          elsif !ok && healthy
            if @peers.shared(&.has_key?(peer_id))
              ::Log.warn { "Transport: lost connection to node #{peer_id}" }
            else
              ::Log.debug { "Transport: no address registered for node #{peer_id} yet" }
            end
            healthy = false
          end
        end
      rescue Channel::ClosedError
      end
    end

    private def get_connection(to : NodeID) : TCPSocket?
      existing = @connections.lock(&.[to]?)
      if existing
        return existing unless existing.closed?
      end
      if peer = @peers.shared(&.[to]?)
        begin
          conn = TCPSocket.new(peer[0], peer[1])
          conn.tcp_nodelay = true
          @connections.lock { |h| h[to] = conn }
          conn
        rescue ex : Socket::Error
          # Catches both Socket::ConnectError (peer port closed) AND
          # Socket::Addrinfo::Error (DNS lookup failure, e.g. when a
          # peer container is stopped). Previously only ConnectError was
          # caught, so a DNS failure during a peer outage killed the
          # per-peer writer fiber and the leader couldn't reconnect when
          # the peer came back.
          nil
        end
      end
    end

    private def handle_connection(client : TCPSocket)
      client.tcp_nodelay = true
      while @running
        msg = Message.from_io(client, @max_payload)
        ch = @channels.shared(&.[msg.group_id]?)
        if ch
          select
          when ch.send(msg)
          else
            @inbox_drops.lock { |h| h.update(msg.from) { |v| v + 1 } }
            ::Log.warn { "Transport: inbox full for group #{msg.group_id}, dropping #{msg.type} from #{msg.from}" }
          end
        else
          ::Log.warn { "Transport: no channel for group #{msg.group_id}, dropping #{msg.type} from #{msg.from}" }
        end
      end
    rescue IO::EOFError | IO::Error
      client.close rescue nil
    end

    def to_prometheus(io : IO)
      io << "# HELP raft_transport_outbox_drops_total Messages dropped due to full per-peer outbox\n"
      io << "# TYPE raft_transport_outbox_drops_total counter\n"
      @outbox_drops.lock do |h|
        h.each do |peer_id, count|
          io << "raft_transport_outbox_drops_total{peer=\"" << peer_id << "\"} " << count << '\n'
        end
      end
      io << "# HELP raft_transport_inbox_drops_total Messages dropped due to full group inbox\n"
      io << "# TYPE raft_transport_inbox_drops_total counter\n"
      @inbox_drops.lock do |h|
        h.each do |peer_id, count|
          io << "raft_transport_inbox_drops_total{peer=\"" << peer_id << "\"} " << count << '\n'
        end
      end
    end

    private def persist_peers
      if dir = @data_dir
        snapshot = @peers.shared(&.dup)
        File.open(File.join(dir, "transport_peers"), "w") do |f|
          snapshot.each do |id, (host, port)|
            f.puts "#{id} #{host} #{port}"
          end
        end
      end
    end

    private def recover_peers
      if dir = @data_dir
        path = File.join(dir, "transport_peers")
        return unless File.exists?(path)
        @peers.lock do |h|
          File.each_line(path) do |line|
            parts = line.strip.split(" ")
            next if parts.size < 3
            h[parts[0].to_u64] = {parts[1], parts[2].to_i}
          end
        end
      end
    end
  end
end
