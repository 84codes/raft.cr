# examples/kv/src/main.cr
require "../../../src/raft"
require "./kv_command"
require "./kv_state_machine"
require "./kv_http_handler"
require "http/server"
require "log"

Log.setup_from_env(default_level: :info)

# Read configuration from environment
node_id = (ENV["NODE_ID"]? || "1").to_u64
http_port = (ENV["HTTP_PORT"]? || "8001").to_i
raft_port = (ENV["RAFT_PORT"]? || "9000").to_i
peers_str = ENV["PEERS"]? || ""

# Parse peers: "node-2:9000,node-3:9000" -> [{id, host, port}]
# Extract peer ID from hostname pattern node-(\d+)
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

# Raft config
config = Raft::Config.new
config.data_dir = ENV["DATA_DIR"]? || "/data/raft"
config.election_timeout_min_ticks = 10_u32
config.election_timeout_max_ticks = 20_u32
config.heartbeat_ticks = 2_u32

# Create state machine and node
state_machine = KVStateMachine.new
metrics = Raft::Metrics.new(node_id: node_id)
node = Raft::Node(KVCommand).new(id: node_id, peers: peer_ids, config: config, state_machine: state_machine, metrics: metrics)

# Setup TCP transport
transport = Raft::TCPTransport.new(node_id: node_id, listen_address: "0.0.0.0", listen_port: raft_port)
peer_configs.each do |pc|
  transport.register_peer(pc[0], pc[1], pc[2])
end
transport.start

# HTTP server
raft_handler = Raft::HTTP::Handler(KVCommand).new(node)
kv_handler = KVHttpHandler.new(node, state_machine)

server = ::HTTP::Server.new([raft_handler, kv_handler]) do |context|
  context.response.status_code = 404
  context.response.print "Not found"
end

# Graceful shutdown on SIGINT/SIGTERM
shutdown = ->(_signal : Signal) do
  puts "\nShutting down node #{node_id}..."
  server.close
  transport.stop
  node.close
  exit 0
end

Signal::INT.trap(&shutdown)
Signal::TERM.trap(&shutdown)

# Tick loop
last_role = node.role
last_term = node.current_term
last_leader = node.leader_id

tick_ch = Channel(Nil).new(1)

spawn(name: "raft-tick") do
  last_tick = Time.instant
  loop do
    sleep 50.milliseconds
    now = Time.instant
    elapsed = now - last_tick
    last_tick = now
    # Skip tick if system was suspended (e.g. laptop sleep)
    if elapsed > 1.second
      Log.warn { "clock jump detected (#{elapsed.total_seconds.round(1)}s), skipping tick" }
      next
    end
    select
    when tick_ch.send(nil)
    else
    end
  end
end

spawn(name: "raft-event-loop") do
  loop do
    # Wait for either a tick or incoming message notification
    select
    when tick_ch.receive
      node.tick
    when transport.notify.receive
      transport.receive(for_node: node_id).each do |msg|
        node.step(msg)
      end
    end

    # Send outgoing messages immediately
    node.take_messages.each do |target_id, msg|
      transport.send(to: target_id, message: msg)
    end

    # Log state changes
    if node.role != last_role || node.current_term != last_term || node.leader_id != last_leader
      Log.info { "role=#{node.role} term=#{node.current_term} leader=#{node.leader_id}" }
      last_role = node.role
      last_term = node.current_term
      last_leader = node.leader_id
    end
  end
end

puts "Node #{node_id} starting on HTTP :#{http_port}, Raft :#{raft_port}"
server.bind_tcp("0.0.0.0", http_port)
server.listen
