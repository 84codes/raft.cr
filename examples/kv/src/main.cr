# examples/kv/src/main.cr
require "../../../src/raft"
require "./kv_command"
require "./kv_state_machine"
require "./meta_state_machine"
require "./value_state_machine"
require "./kv_http_handler"
require "http/server"
require "log"

Log.setup_from_env(default_level: :info)

# Read configuration from environment
node_id = (ENV["NODE_ID"]? || "1").to_u64
http_port = (ENV["HTTP_PORT"]? || "8001").to_i
raft_port = (ENV["RAFT_PORT"]? || "9000").to_i
peers_str = ENV["PEERS"]? || ""
base_data_dir = ENV["DATA_DIR"]? || "/data/raft"

# Parse peers: "node-2:9000,node-3:9000" -> [{id, host, port}]
peer_configs = [] of {UInt64, String, Int32}
peers_str.split(",").each do |peer|
  next if peer.strip.empty?
  parts = peer.strip.split(":")
  host = parts[0]
  port = parts[1].to_i
  if match = host.match(/node-(\d+)/)
    peer_id = match[1].to_u64
  else
    raise "Cannot extract peer ID from hostname '#{host}'. Expected format: node-N"
  end
  peer_configs << {peer_id, host, port}
end

peer_ids = peer_configs.map(&.[0])

# Setup TCP transport
transport = Raft::TCPTransport.new(listen_address: "0.0.0.0", listen_port: raft_port)
peer_configs.each do |pc|
  transport.register_peer(pc[0], pc[1], pc[2])
end

# Shared state
nodes = Hash(UInt64, Raft::Node(KVCommand)).new
value_machines = Hash(UInt64, ValueStateMachine).new

# Start a group's event loop
start_group_loop = ->(node : Raft::Node(KVCommand)) {
  spawn(name: "raft-group-#{node.group_id}") do
    loop do
      select
      when msg = node.inbox.receive
        node.step(msg)
      when timeout(50.milliseconds)
        node.tick
      end
      node.take_messages.each do |target_id, msg|
        transport.outbox.send({target_id, msg})
      end
    end
  end
}

# Helper to create a node for a group
create_node = ->(group_id : UInt64, sm : Raft::StateMachine(KVCommand)) {
  cfg = Raft::Config.new
  cfg.data_dir = File.join(base_data_dir, "group-#{group_id}")
  cfg.election_timeout_min_ticks = 10_u32
  cfg.election_timeout_max_ticks = 20_u32
  cfg.heartbeat_ticks = 2_u32
  Dir.mkdir_p(cfg.data_dir)
  node = Raft::Node(KVCommand).new(
    id: node_id, peers: peer_ids, config: cfg,
    state_machine: sm, group_id: group_id
  )
  nodes[group_id] = node
  transport.register_channel(group_id, node.inbox)
  start_group_loop.call(node)
  Log.info { "Created Raft group #{group_id}" }
  node
}

on_delete_group = ->(key : String, gid : UInt64) {
  if node = nodes.delete(gid)
    node.close
    transport.unregister_channel(gid)
  end
  value_machines.delete(gid)
  Log.info { "Deleted data group #{gid} for key '#{key}'" }
}

# Meta group (group 0) — manages key → group_id mapping
meta_sm = MetaStateMachine.new(on_delete_group) do |key, gid, initial_value|
  vsm = ValueStateMachine.new(initial_value)
  create_node.call(gid, vsm)
  value_machines[gid] = vsm
  Log.info { "Created data group #{gid} for key '#{key}' (initial: #{initial_value || "nil"})" }
end

meta_node = create_node.call(0_u64, meta_sm)

transport.start

# HTTP server
raft_handler = Raft::HTTP::Handler(KVCommand).new(meta_node)
kv_handler = KVHttpHandler.new(meta_node, meta_sm, nodes, value_machines)

server = ::HTTP::Server.new([kv_handler, raft_handler]) do |context|
  context.response.status_code = 404
  context.response.print "Not found"
end

# Graceful shutdown on SIGINT/SIGTERM
shutdown = ->(_signal : Signal) do
  puts "\nShutting down node #{node_id}..."
  server.close
  transport.stop
  nodes.each_value(&.close)
  exit 0
end

Signal::INT.trap(&shutdown)
Signal::TERM.trap(&shutdown)

puts "Node #{node_id} starting on HTTP :#{http_port}, Raft :#{raft_port}"
server.bind_tcp("0.0.0.0", http_port)
server.listen
