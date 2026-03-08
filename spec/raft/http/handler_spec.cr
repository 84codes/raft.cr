require "../../spec_helper"
require "http/server"
require "http/client"

describe Raft::HTTP::Handler do
  it "returns node status as JSON" do
    dir = File.tempname("raft_http")
    Dir.mkdir_p(dir)
    config = Raft::Config.new
    config.data_dir = dir
    config.election_timeout_min_ticks = 100_u32
    config.election_timeout_max_ticks = 100_u32

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: config, state_machine: sm)

    handler = Raft::HTTP::Handler(TestData).new(node)
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
    dir = File.tempname("raft_http")
    Dir.mkdir_p(dir)
    config = Raft::Config.new
    config.data_dir = dir
    config.election_timeout_min_ticks = 100_u32
    config.election_timeout_max_ticks = 100_u32

    sm = TestStateMachine.new
    metrics = Raft::Metrics.new(node_id: 1_u64)
    node = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: config, state_machine: sm, metrics: metrics)

    handler = Raft::HTTP::Handler(TestData).new(node)
    server = ::HTTP::Server.new([handler])
    address = server.bind_tcp("127.0.0.1", 0)
    spawn server.listen

    response = ::HTTP::Client.get("http://127.0.0.1:#{address.port}/raft/metrics")
    response.status_code.should eq 200
    response.body.should contain("raft_node_term")

    server.close
    node.close
    FileUtils.rm_rf(dir)
  end

  it "pauses and resumes node via admin endpoints" do
    dir = File.tempname("raft_http")
    Dir.mkdir_p(dir)
    config = Raft::Config.new
    config.data_dir = dir
    config.election_timeout_min_ticks = 100_u32
    config.election_timeout_max_ticks = 100_u32

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: config, state_machine: sm)

    handler = Raft::HTTP::Handler(TestData).new(node)
    server = ::HTTP::Server.new([handler])
    address = server.bind_tcp("127.0.0.1", 0)
    spawn server.listen

    response = ::HTTP::Client.post("http://127.0.0.1:#{address.port}/raft/admin/pause")
    response.status_code.should eq 200
    node.paused.should be_true

    response = ::HTTP::Client.post("http://127.0.0.1:#{address.port}/raft/admin/resume")
    response.status_code.should eq 200
    node.paused.should be_false

    server.close
    node.close
    FileUtils.rm_rf(dir)
  end
end
