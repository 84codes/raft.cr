require "../../spec_helper"
require "http/server"
require "uri"

# Records the last request and replies with a canned status, so AdminClient
# specs can assert the exact wire format without a live Raft node.
private class RecordingHandler
  include ::HTTP::Handler

  property last_method : String? = nil
  property last_path : String? = nil
  property last_body : String? = nil
  property last_content_type : String? = nil
  property last_authorization : String? = nil
  property reply_status : Int32 = 200

  def call(context : ::HTTP::Server::Context)
    @last_method = context.request.method
    @last_path = context.request.path
    @last_body = context.request.body.try(&.gets_to_end)
    @last_content_type = context.request.headers["Content-Type"]?
    @last_authorization = context.request.headers["Authorization"]?
    context.response.status_code = @reply_status
  end
end

private def with_recording_server(&)
  handler = RecordingHandler.new
  server = ::HTTP::Server.new([handler])
  address = server.bind_tcp("127.0.0.1", 0)
  spawn server.listen
  begin
    yield handler, address.port
  ensure
    server.close
  end
end

describe Raft::HTTP::AdminClient do
  it "posts the add_server wire format and returns the status" do
    with_recording_server do |handler, port|
      uri = URI.parse("http://127.0.0.1:#{port}")
      status = Raft::HTTP::AdminClient.add_server(uri, 2_u64, "node-2:9000")

      status.should eq ::HTTP::Status::OK
      handler.last_method.should eq "POST"
      handler.last_path.should eq "/raft/admin/add_server/2"
      handler.last_content_type.should eq "application/json"
      handler.last_body.should eq %({"address":"node-2:9000"})
    end
  end

  it "returns non-200 statuses so callers can distinguish rejection from unavailability" do
    with_recording_server do |handler, port|
      uri = URI.parse("http://127.0.0.1:#{port}")

      handler.reply_status = 400
      Raft::HTTP::AdminClient.add_server(uri, 2_u64).should eq ::HTTP::Status::BAD_REQUEST

      handler.reply_status = 503
      Raft::HTTP::AdminClient.add_server(uri, 2_u64).should eq ::HTTP::Status::SERVICE_UNAVAILABLE
    end
  end

  it "sends basic auth from URI userinfo" do
    with_recording_server do |handler, port|
      uri = URI.parse("http://admin:secret@127.0.0.1:#{port}")
      Raft::HTTP::AdminClient.add_server(uri, 3_u64)

      auth = handler.last_authorization
      auth.should_not be_nil
      auth.to_s.should start_with("Basic ")
    end
  end

  it "respects a path prefix in the URI" do
    with_recording_server do |handler, port|
      uri = URI.parse("http://127.0.0.1:#{port}/api/v1")
      Raft::HTTP::AdminClient.add_server(uri, 4_u64)

      handler.last_path.should eq "/api/v1/raft/admin/add_server/4"
    end
  end

  it "raises on connection errors" do
    # Port 1 is essentially guaranteed closed.
    uri = URI.parse("http://127.0.0.1:1")
    expect_raises(Socket::ConnectError) do
      Raft::HTTP::AdminClient.add_server(uri, 2_u64)
    end
  end

  it "round-trips against the real AdminHandler" do
    dir = File.tempname("raft_admin_client")
    Dir.mkdir_p(dir)
    config = Raft::Config.new
    config.data_dir = dir
    config.election_timeout_min_ticks = 100_u32
    config.election_timeout_max_ticks = 100_u32
    node = Raft::Node(TestData).new(id: 1_u64, peers: [] of Raft::NodeID, config: config, state_machine: TestStateMachine.new)
    node.bootstrap.should be_true

    admin_handler = Raft::HTTP::AdminHandler.new(node)
    server = ::HTTP::Server.new([admin_handler] of ::HTTP::Handler)
    address = server.bind_tcp("127.0.0.1", 0)
    spawn server.listen

    uri = URI.parse("http://127.0.0.1:#{address.port}")
    status = Raft::HTTP::AdminClient.add_server(uri, 2_u64, "node-2:9000")
    status.should eq ::HTTP::Status::OK
    node.peers.any? { |p| p.id == 2_u64 }.should be_true

    server.close
    node.close
    FileUtils.rm_rf(dir)
  end
end
