require "../../spec_helper"

describe Raft::TCPTransport do
  it "sends and receives messages over TCP" do
    t1 = Raft::TCPTransport.new(listen_address: "127.0.0.1", listen_port: 19741)
    t2 = Raft::TCPTransport.new(listen_address: "127.0.0.1", listen_port: 19742)

    ch = Channel(Raft::Message).new(64)
    t2.register_channel(1_u64, ch)

    t1.register_peer(2_u64, "127.0.0.1", 19742)
    t2.register_peer(1_u64, "127.0.0.1", 19741)

    t1.start
    t2.start
    sleep 50.milliseconds

    msg = Raft::Message.new(
      type: Raft::MessageType::RequestVote,
      from: 1_u64,
      term: 1_u64,
      group_id: 1_u64,
    )

    t1.send(to: 2_u64, message: msg)

    received = ch.receive
    received.type.should eq Raft::MessageType::RequestVote
    received.from.should eq 1_u64

    t1.stop
    t2.stop
  end

  it "routes messages via outbox through per-peer queues" do
    t1 = Raft::TCPTransport.new(listen_address: "127.0.0.1", listen_port: 19743)
    t2 = Raft::TCPTransport.new(listen_address: "127.0.0.1", listen_port: 19744)

    ch = Channel(Raft::Message).new(64)
    t2.register_channel(1_u64, ch)

    t1.register_peer(2_u64, "127.0.0.1", 19744)
    t2.register_peer(1_u64, "127.0.0.1", 19743)

    t1.start
    t2.start
    sleep 50.milliseconds

    msg = Raft::Message.new(
      type: Raft::MessageType::AppendEntries,
      from: 1_u64,
      term: 2_u64,
      group_id: 1_u64,
    )

    # Send via outbox — goes through dispatcher and per-peer fiber
    t1.outbox.send({2_u64, msg})

    received = ch.receive
    received.type.should eq Raft::MessageType::AppendEntries
    received.from.should eq 1_u64
    received.term.should eq 2_u64

    t1.stop
    t2.stop
  end

  it "registers channels and peers after start via command channel" do
    t1 = Raft::TCPTransport.new(listen_address: "127.0.0.1", listen_port: 19745)
    t2 = Raft::TCPTransport.new(listen_address: "127.0.0.1", listen_port: 19746)

    t1.start
    t2.start

    # Register after start — goes through command channel
    ch = Channel(Raft::Message).new(64)
    t2.register_channel(1_u64, ch)
    t1.register_peer(2_u64, "127.0.0.1", 19746)
    t2.register_peer(1_u64, "127.0.0.1", 19745)

    sleep 50.milliseconds

    msg = Raft::Message.new(
      type: Raft::MessageType::RequestVote,
      from: 1_u64,
      term: 3_u64,
      group_id: 1_u64,
    )

    t1.outbox.send({2_u64, msg})

    received = ch.receive
    received.type.should eq Raft::MessageType::RequestVote
    received.term.should eq 3_u64

    t1.stop
    t2.stop
  end

  it "starts and stops cleanly" do
    t = Raft::TCPTransport.new(listen_address: "127.0.0.1", listen_port: 19747)
    t.start
    sleep 10.milliseconds
    t.stop
  end

  it "skips persistence when data_dir is nil" do
    # nil data_dir means "don't persist" — register_peer must succeed
    # without raising and without writing anything to disk.
    t = Raft::TCPTransport.new(listen_address: "127.0.0.1", listen_port: 19749, data_dir: nil)
    t.start

    t.register_peer(2_u64, "127.0.0.1", 19999)
    sleep 50.milliseconds # let the dispatcher process the command

    t.stop
  end

  it "creates data_dir on construction so persist_peers doesn't crash" do
    parent = File.tempname("raft_tcp_persist")
    Dir.mkdir_p(parent)
    leaf = File.join(parent, "missing-leaf")
    File.exists?(leaf).should be_false

    t = Raft::TCPTransport.new(listen_address: "127.0.0.1", listen_port: 19748, data_dir: leaf)
    t.start

    # register_peer routes through the dispatcher; persist_peers writes
    # to disk. Without the mkdir_p this raised File::NotFoundError on
    # the dispatch fiber.
    t.register_peer(2_u64, "127.0.0.1", 19999)

    deadline = Time.instant + 1.second
    until File.exists?(File.join(leaf, "transport_peers")) || Time.instant > deadline
      sleep 10.milliseconds
    end
    File.exists?(File.join(leaf, "transport_peers")).should be_true

    t.stop
    FileUtils.rm_rf(parent)
  end
end
