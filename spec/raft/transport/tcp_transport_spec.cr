require "../../spec_helper"

describe Raft::TCPTransport do
  it "sends and receives messages over TCP" do
    t1 = Raft::TCPTransport.new(node_id: 1_u64, listen_address: "127.0.0.1", listen_port: 19741)
    t2 = Raft::TCPTransport.new(node_id: 2_u64, listen_address: "127.0.0.1", listen_port: 19742)

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
    sleep 100.milliseconds

    received = t2.receive(for_node: 2_u64)
    received.size.should eq 1
    received[0].type.should eq Raft::MessageType::RequestVote
    received[0].from.should eq 1_u64

    t1.stop
    t2.stop
  end
end
