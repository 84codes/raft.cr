require "../../spec_helper"
require "http/server"
require "http/client"

private def make_node(dir : String, peers : Array(Raft::NodeID) = [2_u64, 3_u64]) : Raft::Node(TestData)
  Dir.mkdir_p(dir)
  config = Raft::Config.new
  config.data_dir = dir
  config.election_timeout_min_ticks = 100_u32
  config.election_timeout_max_ticks = 100_u32
  Raft::Node(TestData).new(id: 1_u64, peers: peers, config: config, state_machine: TestStateMachine.new)
end

describe Raft::HTTP::AdminHandler do
  it "bootstraps a single-node cluster via POST" do
    dir = File.tempname("raft_admin")
    node = make_node(dir, peers: [] of Raft::NodeID)

    handler = Raft::HTTP::AdminHandler(TestData).new(node)
    server = ::HTTP::Server.new([handler])
    address = server.bind_tcp("127.0.0.1", 0)
    spawn server.listen

    response = ::HTTP::Client.post("http://127.0.0.1:#{address.port}/raft/admin/bootstrap")
    response.status_code.should eq 200
    response.body.should contain("\"status\":\"bootstrapped\"")
    node.role.should eq Raft::Role::Leader

    server.close
    node.close
    FileUtils.rm_rf(dir)
  end

  it "returns 404 for unknown admin actions" do
    dir = File.tempname("raft_admin")
    node = make_node(dir)

    handler = Raft::HTTP::AdminHandler(TestData).new(node)
    server = ::HTTP::Server.new([handler])
    address = server.bind_tcp("127.0.0.1", 0)
    spawn server.listen

    response = ::HTTP::Client.post("http://127.0.0.1:#{address.port}/raft/admin/nonexistent")
    response.status_code.should eq 404

    server.close
    node.close
    FileUtils.rm_rf(dir)
  end

  it "does not respond to GET /raft/status (falls through)" do
    dir = File.tempname("raft_admin")
    node = make_node(dir)

    handler = Raft::HTTP::AdminHandler(TestData).new(node)
    server = ::HTTP::Server.new([handler])
    address = server.bind_tcp("127.0.0.1", 0)
    spawn server.listen

    response = ::HTTP::Client.get("http://127.0.0.1:#{address.port}/raft/status")
    response.status_code.should eq 404

    server.close
    node.close
    FileUtils.rm_rf(dir)
  end

  {% if flag?(:raft_debug) %}
    it "pauses and resumes node via debug admin endpoints" do
      dir = File.tempname("raft_admin")
      node = make_node(dir)

      handler = Raft::HTTP::AdminHandler(TestData).new(node)
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
  {% end %}
end

describe "Raft::HTTP::StatusHandler + AdminHandler chained" do
  it "serves both route sets when both handlers are mounted together" do
    dir = File.tempname("raft_chain")
    Dir.mkdir_p(dir)
    config = Raft::Config.new
    config.data_dir = dir
    config.election_timeout_min_ticks = 100_u32
    config.election_timeout_max_ticks = 100_u32
    node = Raft::Node(TestData).new(id: 1_u64, peers: [] of Raft::NodeID, config: config, state_machine: TestStateMachine.new)

    status_handler = Raft::HTTP::StatusHandler(TestData).new(node)
    admin_handler = Raft::HTTP::AdminHandler(TestData).new(node)
    server = ::HTTP::Server.new([status_handler, admin_handler])
    address = server.bind_tcp("127.0.0.1", 0)
    spawn server.listen

    # Status path hits StatusHandler.
    status_response = ::HTTP::Client.get("http://127.0.0.1:#{address.port}/raft/status")
    status_response.status_code.should eq 200
    status_response.body.should contain("\"id\":1")

    # Admin path falls through StatusHandler, lands on AdminHandler.
    bootstrap_response = ::HTTP::Client.post("http://127.0.0.1:#{address.port}/raft/admin/bootstrap")
    bootstrap_response.status_code.should eq 200
    bootstrap_response.body.should contain("\"status\":\"bootstrapped\"")
    node.role.should eq Raft::Role::Leader

    server.close
    node.close
    FileUtils.rm_rf(dir)
  end
end
