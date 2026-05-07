require "socket"

module Raft
  class TCPTransport < Transport
    @listen_address : String
    @listen_port : Int32
    @peers : Hash(NodeID, {String, Int32}) = {} of NodeID => {String, Int32}
    @connections : Hash(NodeID, TCPSocket) = {} of NodeID => TCPSocket
    @channels : Hash(UInt64, Channel(Message)) = {} of UInt64 => Channel(Message)
    @peer_outboxes : Hash(NodeID, Channel(Message)) = {} of NodeID => Channel(Message)
    @server : TCPServer? = nil
    @running : Bool = false
    @data_dir : String?
    getter outbox : Channel({NodeID, Message}) = Channel({NodeID, Message}).new(256)
    @commands : Channel(TransportCommand) = Channel(TransportCommand).new(64)
    @outbox_drops : Hash(NodeID, Int64) = Hash(NodeID, Int64).new(0_i64)
    @inbox_drops : Hash(NodeID, Int64) = Hash(NodeID, Int64).new(0_i64)
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

    # Look up a registered peer's host:port. Reads the registry directly;
    # mutations happen on the dispatcher fiber and the registry is mostly
    # stable after bootstrap, so unsynchronized reads are acceptable here.
    def peer_address?(id : NodeID) : {String, Int32}?
      @peers[id]?
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
      @peer_outboxes.each_value do |ch|
        ch.close
      end
      @peer_outboxes.clear
      @connections.each_value do |conn|
        conn.close rescue nil
      end
      @connections.clear
    end

    def send(to : NodeID, message : Message)
      conn = get_connection(to)
      unless conn
        ::Log.warn { "Transport: no connection to node #{to} (peer known: #{@peers.has_key?(to)})" }
        return
      end
      begin
        message.to_io(conn)
        conn.flush
      rescue ex : IO::Error
        ::Log.warn { "Transport: send to node #{to} failed: #{ex.message}" }
        @connections.delete(to)
        conn.close rescue nil
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
        @channels[cmd.group_id] = cmd.channel
      when UnregisterChannelCommand
        @channels.delete(cmd.group_id)
      when RegisterPeerCommand
        @peers[cmd.id] = {cmd.host, cmd.port}
        persist_peers
      end
    end

    private def route_to_peer(target_id : NodeID, msg : Message)
      ensure_peer_fiber(target_id)
      if ch = @peer_outboxes[target_id]?
        select
        when ch.send(msg)
        else
          # Per-peer queue full, drop — Raft retries naturally
          @outbox_drops[target_id] += 1
        end
      end
    end

    private def ensure_peer_fiber(peer_id : NodeID)
      return if @peer_outboxes.has_key?(peer_id)
      ch = Channel(Message).new(64)
      @peer_outboxes[peer_id] = ch
      spawn(name: "raft-transport-peer-#{peer_id}") do
        while @running
          msg = ch.receive
          send(to: peer_id, message: msg)
        end
      rescue Channel::ClosedError
      end
    end

    private def get_connection(to : NodeID) : TCPSocket?
      if conn = @connections[to]?
        return conn unless conn.closed?
      end
      if peer = @peers[to]?
        begin
          conn = TCPSocket.new(peer[0], peer[1])
          conn.tcp_nodelay = true
          @connections[to] = conn
          conn
        rescue ex : Socket::ConnectError
          nil
        end
      end
    end

    private def handle_connection(client : TCPSocket)
      client.tcp_nodelay = true
      while @running
        msg = Message.from_io(client, @max_payload)
        if ch = @channels[msg.group_id]?
          select
          when ch.send(msg)
          else
            @inbox_drops[msg.from] += 1
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
      @outbox_drops.each do |peer_id, count|
        io << "raft_transport_outbox_drops_total{peer=\"" << peer_id << "\"} " << count << '\n'
      end
      io << "# HELP raft_transport_inbox_drops_total Messages dropped due to full group inbox\n"
      io << "# TYPE raft_transport_inbox_drops_total counter\n"
      @inbox_drops.each do |peer_id, count|
        io << "raft_transport_inbox_drops_total{peer=\"" << peer_id << "\"} " << count << '\n'
      end
    end

    private def persist_peers
      if dir = @data_dir
        File.open(File.join(dir, "transport_peers"), "w") do |f|
          @peers.each do |id, (host, port)|
            f.puts "#{id} #{host} #{port}"
          end
        end
      end
    end

    private def recover_peers
      if dir = @data_dir
        path = File.join(dir, "transport_peers")
        return unless File.exists?(path)
        File.each_line(path) do |line|
          parts = line.strip.split(" ")
          next if parts.size < 3
          @peers[parts[0].to_u64] = {parts[1], parts[2].to_i}
        end
      end
    end
  end
end
