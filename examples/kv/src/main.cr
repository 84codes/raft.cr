# examples/kv/src/main.cr
require "../../../src/raft"
require "./kv_command"
require "./kv_state_machine"
require "./kv_http_handler"
require "http/server"

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
node = Raft::Node(KVCommand).new(id: node_id, peers: peer_ids, config: config, state_machine: state_machine)
node.metrics = metrics

# Setup TCP transport
transport = Raft::TCPTransport.new(node_id: node_id, listen_address: "0.0.0.0", listen_port: raft_port)
peer_configs.each do |pc|
  transport.register_peer(pc[0], pc[1], pc[2])
end
transport.start

# Tick loop
spawn do
  loop do
    sleep 50.milliseconds
    node.tick

    # Send outgoing messages
    node.take_messages.each do |msg|
      peer_ids.each do |pid|
        transport.send(to: pid, message: msg)
      end
    end

    # Process incoming messages
    transport.receive(for_node: node_id).each do |msg|
      node.step(msg)
    end
  end
end

# HTTP server
raft_handler = Raft::HTTP::Handler(KVCommand).new(node)
kv_handler = KVHttpHandler.new(node, state_machine)

server = ::HTTP::Server.new([raft_handler, kv_handler]) do |context|
  context.response.status_code = 404
  context.response.print "Not found"
end

puts "Node #{node_id} starting on HTTP :#{http_port}, Raft :#{raft_port}"
server.bind_tcp("0.0.0.0", http_port)
server.listen
