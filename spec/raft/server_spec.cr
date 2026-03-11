require "../spec_helper"

describe Raft::Server do
  it "ticks all registered nodes" do
    dir = File.tempname("raft_server")
    Dir.mkdir_p(dir)

    config = Raft::Config.new
    config.data_dir = dir
    config.election_timeout_min_ticks = 3_u32
    config.election_timeout_max_ticks = 3_u32

    server = Raft::Server(TestData).new(config: config)

    sm1 = TestStateMachine.new
    sm2 = TestStateMachine.new
    server.add_group(group_id: 1_u64, node_id: 1_u64, peers: [2_u64, 3_u64], state_machine: sm1)
    server.add_group(group_id: 2_u64, node_id: 1_u64, peers: [2_u64, 3_u64], state_machine: sm2)

    3.times { server.tick }

    # With pre-vote, nodes stay Follower but produce PreVote messages
    messages = server.take_all_messages
    messages.all? { |_, m| m.type == Raft::MessageType::PreVote }.should be_true
    messages.size.should be > 0

    server.close
    FileUtils.rm_rf(dir)
  end

  it "routes incoming messages via node inbox channel" do
    dir = File.tempname("raft_server")
    Dir.mkdir_p(dir)

    config = Raft::Config.new
    config.data_dir = dir
    config.election_timeout_min_ticks = 100_u32
    config.election_timeout_max_ticks = 100_u32

    server = Raft::Server(TestData).new(config: config)

    sm = TestStateMachine.new
    server.add_group(group_id: 1_u64, node_id: 1_u64, peers: [2_u64, 3_u64], state_machine: sm)

    node = server.node(1_u64)

    # Send message directly to node's inbox channel
    msg = Raft::Message.new(
      type: Raft::MessageType::AppendEntries,
      from: 2_u64,
      term: 1_u64,
      group_id: 1_u64,
    )
    node.inbox.send(msg)

    # Node processes the message from its channel
    received = node.inbox.receive
    node.step(received)

    outgoing = server.take_all_messages
    outgoing.size.should be > 0

    server.close
    FileUtils.rm_rf(dir)
  end
end
