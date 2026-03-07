require "../../spec_helper"

describe Raft::MemoryTransport do
  it "delivers messages between nodes" do
    transport = Raft::MemoryTransport.new

    msg = Raft::Message.new(
      from: 1_u64,
      term: 1_u64,
      type: Raft::MessageType::RequestVote,
      group_id: 1_u64,
    )

    transport.send(to: 2_u64, message: msg)
    received = transport.receive(for_node: 2_u64)
    received.size.should eq 1
    received[0].from.should eq 1_u64
    received[0].type.should eq Raft::MessageType::RequestVote
  end

  it "returns empty array when no messages" do
    transport = Raft::MemoryTransport.new
    transport.receive(for_node: 1_u64).size.should eq 0
  end

  it "can simulate partition by dropping messages" do
    transport = Raft::MemoryTransport.new
    transport.isolate(2_u64)

    msg = Raft::Message.new(from: 1_u64, term: 1_u64, type: Raft::MessageType::AppendEntries)
    transport.send(to: 2_u64, message: msg)
    transport.receive(for_node: 2_u64).size.should eq 0
  end

  it "restores connectivity after heal" do
    transport = Raft::MemoryTransport.new
    transport.isolate(2_u64)
    transport.heal(2_u64)

    msg = Raft::Message.new(from: 1_u64, term: 1_u64, type: Raft::MessageType::AppendEntries)
    transport.send(to: 2_u64, message: msg)
    transport.receive(for_node: 2_u64).size.should eq 1
  end
end
