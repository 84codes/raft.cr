require "../../spec_helper"

describe Raft::MemoryTransport do
  it "delivers messages to registered channel" do
    transport = Raft::MemoryTransport.new
    ch = Channel(Raft::Message).new(64)
    transport.register_channel(1_u64, 2_u64, ch)

    msg = Raft::Message.new(
      from: 1_u64,
      term: 1_u64,
      type: Raft::MessageType::RequestVote,
      group_id: 1_u64,
    )

    transport.send(to: 2_u64, message: msg)

    received = ch.receive
    received.from.should eq 1_u64
    received.type.should eq Raft::MessageType::RequestVote
  end

  it "does not block when no channel registered" do
    transport = Raft::MemoryTransport.new
    msg = Raft::Message.new(from: 1_u64, term: 1_u64, type: Raft::MessageType::RequestVote, group_id: 1_u64)
    # Should not raise or block
    transport.send(to: 2_u64, message: msg)
  end

  it "can simulate partition by dropping messages" do
    transport = Raft::MemoryTransport.new
    ch = Channel(Raft::Message).new(64)
    transport.register_channel(1_u64, 2_u64, ch)
    transport.isolate(2_u64)

    msg = Raft::Message.new(from: 1_u64, term: 1_u64, type: Raft::MessageType::AppendEntries, group_id: 1_u64)
    transport.send(to: 2_u64, message: msg)

    select
    when ch.receive
      raise "should not receive message"
    else
      # Expected: channel empty
    end
  end

  it "restores connectivity after heal" do
    transport = Raft::MemoryTransport.new
    ch = Channel(Raft::Message).new(64)
    transport.register_channel(1_u64, 2_u64, ch)
    transport.isolate(2_u64)
    transport.heal(2_u64)

    msg = Raft::Message.new(from: 1_u64, term: 1_u64, type: Raft::MessageType::AppendEntries, group_id: 1_u64)
    transport.send(to: 2_u64, message: msg)

    received = ch.receive
    received.type.should eq Raft::MessageType::AppendEntries
  end
end
