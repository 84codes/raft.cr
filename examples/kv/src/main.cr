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
raft_advertise_address = ENV["RAFT_ADVERTISE_ADDRESS"]? || ""
base_data_dir = ENV["DATA_DIR"]? || "/data/raft"

Dir.mkdir_p(base_data_dir)

# Setup TCP transport (peers registered dynamically via TUI or recovered from disk)
transport = Raft::TCPTransport.new(listen_address: "0.0.0.0", listen_port: raft_port, data_dir: base_data_dir, max_payload: 64_u32 * 1024_u32 * 1024_u32)

# Shared state
nodes = Hash(UInt64, Raft::Node(KVCommand)).new
value_machines = Hash(UInt64, ValueStateMachine).new
meta_node_holder = [] of Raft::Node(KVCommand)

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
  rescue Channel::ClosedError
    Log.info { "Raft group #{node.group_id} stopped" }
  end
}

# Helper to create a node for a group
create_node = ->(group_id : UInt64, sm : Raft::StateMachine(KVCommand)) {
  # Meta group starts with no peers (standalone until bootstrapped via TUI).
  # Data groups inherit current cluster membership from meta group.
  current_peer_ids = if group_id > 0_u64 && !meta_node_holder.empty?
                       meta_node_holder[0].peers.map(&.id).reject { |id| id == node_id }
                     else
                       [] of UInt64
                     end
  cfg = Raft::Config.new
  cfg.data_dir = File.join(base_data_dir, "group-#{group_id}")
  cfg.election_timeout_min_ticks = 10_u32
  cfg.election_timeout_max_ticks = 20_u32
  cfg.heartbeat_ticks = 2_u32
  Dir.mkdir_p(cfg.data_dir)
  metrics = Raft::Metrics.new(node_id: node_id, group_id: group_id)
  node = Raft::Node(KVCommand).new(
    id: node_id, peers: current_peer_ids, config: cfg,
    state_machine: sm, metrics: metrics, group_id: group_id,
    address: raft_advertise_address
  )
  nodes[group_id] = node
  # Register transport peers from config entries (addresses propagate via Raft log)
  node.on_configuration_applied do |peers|
    peers.each do |p|
      next if p.id == node_id || p.address.empty?
      parts = p.address.split(":")
      next if parts.size < 2
      transport.register_peer(p.id, parts[0], parts[1].to_i)
    end
  end
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
meta_node_holder << meta_node

# When meta group membership changes, reconcile all data groups
meta_node.on_configuration_change do |new_peers|
  peer_ids = new_peers.map(&.id).reject { |id| id == node_id }
  nodes.each do |gid, data_node|
    next if gid == 0_u64 # skip meta group itself
    next unless data_node.role == Raft::Role::Leader
    # Add new peers
    new_peers.each do |p|
      next if p.id == node_id
      unless data_node.peers.any? { |dp| dp.id == p.id }
        data_node.add_server(p.id)
      end
    end
    # Collect IDs to remove first, then remove (avoids mutation during iteration)
    to_remove = data_node.peers.select do |dp|
      dp.id != node_id && !new_peers.any? { |p| p.id == dp.id }
    end.map(&.id)
    to_remove.each { |id| data_node.remove_server(id) }
  end
end

transport.start

# HTTP server
raft_status_handler = Raft::HTTP::StatusHandler(KVCommand).new(meta_node, transport, raft_advertise_address)
raft_admin_handler = Raft::HTTP::AdminHandler(KVCommand).new(meta_node, transport)
kv_handler = KVHttpHandler.new(meta_node, meta_sm, nodes, value_machines)

server = ::HTTP::Server.new([kv_handler, raft_status_handler, raft_admin_handler]) do |context|
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
