require "../../spec_helper"

private def receive_with_timeout(ch : Channel(Raft::Message), within : Time::Span = 1.second) : Raft::Message
  select
  when msg = ch.receive
    msg
  when timeout(within)
    raise "timeout waiting for message"
  end
end

describe Raft::MemoryTransport do
  it "delivers messages from one peer to another via the outbox path" do
    t1, t2 = Raft::MemoryTransport.pipe_pair(1_u64, 2_u64)
    ch = Channel(Raft::Message).new(64)
    t2.register_channel(1_u64, ch)
    t1.start
    t2.start

    msg = Raft::Message.new(
      from: 1_u64,
      term: 5_u64,
      type: Raft::MessageType::RequestVote,
      group_id: 1_u64,
    )
    t1.outbox.send({2_u64, msg})

    received = receive_with_timeout(ch)
    received.from.should eq 1_u64
    received.term.should eq 5_u64
    received.type.should eq Raft::MessageType::RequestVote

    t1.stop
    t2.stop
  end

  it "discards messages destined for an unregistered group on the receiver" do
    t1, t2 = Raft::MemoryTransport.pipe_pair(1_u64, 2_u64)
    # No channel registered on t2 — reader will discard.
    t1.start
    t2.start

    msg = Raft::Message.new(from: 1_u64, term: 1_u64, type: Raft::MessageType::RequestVote, group_id: 1_u64)
    t1.outbox.send({2_u64, msg})

    # Producer side should not block; give the dispatcher a moment.
    sleep(20.milliseconds)

    t1.stop
    t2.stop
  end

  it "drops inbound messages while the receiver is isolated" do
    t1, t2 = Raft::MemoryTransport.pipe_pair(1_u64, 2_u64)
    ch = Channel(Raft::Message).new(64)
    t2.register_channel(1_u64, ch)
    t1.start
    t2.start

    t2.isolated = true

    msg = Raft::Message.new(from: 1_u64, term: 1_u64, type: Raft::MessageType::AppendEntries, group_id: 1_u64)
    t1.outbox.send({2_u64, msg})

    select
    when ch.receive
      raise "should not have received message while isolated"
    when timeout(50.milliseconds)
      # Expected: dropped by t2's reader
    end

    t1.stop
    t2.stop
  end

  it "restores delivery after isolated = false" do
    t1, t2 = Raft::MemoryTransport.pipe_pair(1_u64, 2_u64)
    ch = Channel(Raft::Message).new(64)
    t2.register_channel(1_u64, ch)
    t1.start
    t2.start

    t2.isolated = true
    t2.isolated = false

    msg = Raft::Message.new(from: 1_u64, term: 1_u64, type: Raft::MessageType::AppendEntries, group_id: 1_u64)
    t1.outbox.send({2_u64, msg})

    received = receive_with_timeout(ch)
    received.type.should eq Raft::MessageType::AppendEntries

    t1.stop
    t2.stop
  end

  it "mesh wires every pair of nodes" do
    transports = Raft::MemoryTransport.mesh([1_u64, 2_u64, 3_u64])
    inboxes = {} of Raft::NodeID => Channel(Raft::Message)
    transports.each do |id, t|
      ch = Channel(Raft::Message).new(64)
      inboxes[id] = ch
      t.register_channel(1_u64, ch)
      t.start
    end

    msg = Raft::Message.new(from: 1_u64, term: 7_u64, type: Raft::MessageType::AppendEntries, group_id: 1_u64)
    transports[1_u64].outbox.send({2_u64, msg})
    transports[1_u64].outbox.send({3_u64, msg})

    receive_with_timeout(inboxes[2_u64]).from.should eq 1_u64
    receive_with_timeout(inboxes[3_u64]).from.should eq 1_u64

    transports.each_value(&.stop)
  end
end
