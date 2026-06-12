require "../../spec_helper"
require "http/server"
require "http/client"

private def make_node(dir : String) : Raft::Node(TestData)
  Dir.mkdir_p(dir)
  config = Raft::Config.new
  config.data_dir = dir
  config.election_timeout_min_ticks = 100_u32
  config.election_timeout_max_ticks = 100_u32
  Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: config, state_machine: TestStateMachine.new)
end

private def make_node_with_metrics(dir : String) : Raft::Node(TestData)
  Dir.mkdir_p(dir)
  config = Raft::Config.new
  config.data_dir = dir
  config.election_timeout_min_ticks = 100_u32
  config.election_timeout_max_ticks = 100_u32
  metrics = Raft::Metrics.new(node_id: 1_u64)
  Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: config, state_machine: TestStateMachine.new, metrics: metrics)
end

describe Raft::HTTP::StatusHandler do
  it "returns node status as JSON" do
    dir = File.tempname("raft_status")
    node = make_node(dir)

    handler = Raft::HTTP::StatusHandler.new(node)
    server = ::HTTP::Server.new([handler])
    address = server.bind_tcp("127.0.0.1", 0)
    spawn server.listen

    response = ::HTTP::Client.get("http://127.0.0.1:#{address.port}/raft/status")
    response.status_code.should eq 200
    body = response.body
    body.should contain("\"id\":1")
    body.should contain("\"role\":\"follower\"")

    server.close
    node.close
    FileUtils.rm_rf(dir)
  end

  it "returns metrics in prometheus format" do
    dir = File.tempname("raft_status")
    node = make_node_with_metrics(dir)

    handler = Raft::HTTP::StatusHandler.new(node)
    server = ::HTTP::Server.new([handler])
    address = server.bind_tcp("127.0.0.1", 0)
    spawn server.listen

    response = ::HTTP::Client.get("http://127.0.0.1:#{address.port}/raft/metrics")
    response.status_code.should eq 200
    response.body.should contain("raft_node_term")
    # Fresh follower with no leader yet: is_leader=0, leader_id=0 (nil → 0).
    response.body.should match /raft_node_is_leader\{[^}]*\} 0\n/
    response.body.should match /raft_node_leader_id\{[^}]*\} 0\n/

    server.close
    node.close
    FileUtils.rm_rf(dir)
  end

  it "does not respond to POST /raft/admin/* (falls through to next handler)" do
    # StatusHandler alone — admin posts should fall through to the bare chain's
    # 404. This is the property that lets the metrics port stay safe from
    # cluster-mutating requests.
    dir = File.tempname("raft_status")
    node = make_node(dir)

    handler = Raft::HTTP::StatusHandler.new(node)
    server = ::HTTP::Server.new([handler])
    address = server.bind_tcp("127.0.0.1", 0)
    spawn server.listen

    response = ::HTTP::Client.post("http://127.0.0.1:#{address.port}/raft/admin/bootstrap")
    response.status_code.should eq 404

    # Node was not mutated — still a follower with the initial peer set.
    node.role.should eq Raft::Role::Follower
    node.current_term.should eq 0_u64

    server.close
    node.close
    FileUtils.rm_rf(dir)
  end
end
