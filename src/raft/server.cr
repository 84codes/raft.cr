module Raft
  class Server(T)
    @nodes : Hash(UInt64, Node(T)) = {} of UInt64 => Node(T)
    @transport : Transport
    @config : Config

    def initialize(@transport : Transport, @config : Config)
    end

    def add_group(group_id : UInt64, node_id : NodeID, peers : Array(NodeID), state_machine : StateMachine(T))
      group_config = Config.new
      group_config.data_dir = File.join(@config.data_dir, "group-#{group_id}")
      group_config.election_timeout_min_ticks = @config.election_timeout_min_ticks
      group_config.election_timeout_max_ticks = @config.election_timeout_max_ticks
      group_config.heartbeat_ticks = @config.heartbeat_ticks
      group_config.max_segment_size = @config.max_segment_size
      Dir.mkdir_p(group_config.data_dir)
      @nodes[group_id] = Node(T).new(id: node_id, peers: peers, config: group_config, state_machine: state_machine)
    end

    def node(group_id : UInt64) : Node(T)
      @nodes[group_id]
    end

    def tick
      @nodes.each_value(&.tick)
    end

    def process_messages(for_node : NodeID)
      messages = @transport.receive(for_node: for_node)
      messages.each do |msg|
        if node = @nodes[msg.group_id]?
          node.step(msg)
        end
      end
    end

    def take_all_messages : Array(Message)
      all = [] of Message
      @nodes.each do |group_id, node|
        node.take_messages.each do |msg|
          msg.group_id = group_id
          all << msg
        end
      end
      all
    end

    def start_ticker
      spawn do
        loop do
          sleep @config.tick_interval
          tick
        end
      end
    end

    def close
      @nodes.each_value(&.close)
    end
  end
end
