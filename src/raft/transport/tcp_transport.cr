require "socket"

module Raft
  class TCPTransport < Transport
    @node_id : NodeID
    @listen_address : String
    @listen_port : Int32
    @peers : Hash(NodeID, {String, Int32}) = {} of NodeID => {String, Int32}
    @connections : Hash(NodeID, TCPSocket) = {} of NodeID => TCPSocket
    @inbox : Array(Message) = [] of Message
    @inbox_mutex : Mutex = Mutex.new
    @server : TCPServer? = nil
    @running : Bool = false
    getter notify : Channel(Nil) = Channel(Nil).new(1)

    def initialize(@node_id : NodeID, @listen_address : String, @listen_port : Int32)
    end

    def register_peer(id : NodeID, host : String, port : Int32)
      @peers[id] = {host, port}
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

    def receive(for_node : NodeID) : Array(Message)
      @inbox_mutex.synchronize do
        result = @inbox.dup
        @inbox.clear
        result
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
        @inbox_mutex.synchronize do
          @inbox << msg
        end
        select
        when @notify.send(nil)
        else
          # Already notified, don't block
        end
      end
    rescue IO::EOFError | IO::Error
      client.close rescue nil
    end
  end
end
