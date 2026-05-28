require "sync/shared"

module Raft
  class Server(T)
    @nodes : Sync::Shared(Hash(UInt64, Node(T))) = Sync::Shared(Hash(UInt64, Node(T))).new(
      Hash(UInt64, Node(T)).new
    )
    @config : Config

    def initialize(@config : Config)
    end

    def add_group(group_id : UInt64, node_id : NodeID, peers : Array(NodeID), state_machine : StateMachine(T))
      group_config = Config.new
      group_config.data_dir = File.join(@config.data_dir, "group-#{group_id}")
      group_config.election_timeout_min_ticks = @config.election_timeout_min_ticks
      group_config.election_timeout_max_ticks = @config.election_timeout_max_ticks
      group_config.heartbeat_ticks = @config.heartbeat_ticks
      group_config.max_segment_size = @config.max_segment_size
      Dir.mkdir_p(group_config.data_dir)
      node = Node(T).new(id: node_id, peers: peers, config: group_config, state_machine: state_machine, group_id: group_id)
      @nodes.lock { |h| h[group_id] = node }
    end

    def remove_group(group_id : UInt64)
      node = @nodes.lock(&.delete(group_id))
      node.try(&.close)
    end

    def node(group_id : UInt64) : Node(T)
      @nodes.shared(&.[group_id])
    end

    def tick
      @nodes.shared { |h| h.each_value(&.tick) }
    end

    def take_all_messages : Array({NodeID, Message})
      all = Array({NodeID, Message}).new
      @nodes.shared do |h|
        h.each_value do |node|
          node.take_messages.each do |target_id, msg|
            all << {target_id, msg}
          end
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
      @nodes.shared { |h| h.each_value(&.close) }
    end
  end
end
