require "socket"

module Raft
  class TCPTransport < Transport
    @listen_address : String
    @listen_port : Int32
    @peers : Hash(NodeID, {String, Int32}) = {} of NodeID => {String, Int32}
    @connections : Hash(NodeID, TCPSocket) = {} of NodeID => TCPSocket
    @channels : Hash(UInt64, Channel(Message)) = {} of UInt64 => Channel(Message)
    @server : TCPServer? = nil
    @running : Bool = false
    getter outbox : Channel({NodeID, Message}) = Channel({NodeID, Message}).new(256)

    def initialize(@listen_address : String, @listen_port : Int32)
    end

    def register_peer(id : NodeID, host : String, port : Int32)
      @peers[id] = {host, port}
    end

    def register_channel(group_id : UInt64, channel : Channel(Message))
      @channels[group_id] = channel
    end

    def unregister_channel(group_id : UInt64)
      @channels.delete(group_id)
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
      spawn(name: "raft-transport-write") do
        while @running
          target_id, msg = @outbox.receive
          send(to: target_id, message: msg)
        end
      end
    end

    def stop
      @running = false
      @server.try(&.close)
      @connections.each_value do |conn|
        conn.close rescue nil
      end
      @connections.clear
    end

    def send(to : NodeID, message : Message)
      conn = get_connection(to)
      return unless conn
      begin
        message.to_io(conn)
        conn.flush
      rescue ex : IO::Error
        @connections.delete(to)
        conn.close rescue nil
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
        msg = Message.from_io(client)
        if ch = @channels[msg.group_id]?
          ch.send(msg)
        end
      end
    rescue IO::EOFError | IO::Error
      client.close rescue nil
    end
  end
end
