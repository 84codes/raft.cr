require "spec"
require "../../../src/raft"
require "../src/queue_command"
require "../src/queue_state_machine"

describe QueueStateMachine do
  it "publishes and consumes in FIFO order" do
    sm = QueueStateMachine.new
    sm.apply(QueueCommand.new(QueueAction::Publish, "q", body: "first".to_slice))
    sm.apply(QueueCommand.new(QueueAction::Publish, "q", body: "second".to_slice))
    sm.depth.should eq 2

    ch1 = Channel(Bytes?).new(1)
    sm.register_request("r1", ch1)
    sm.apply(QueueCommand.new(QueueAction::Consume, "q", req_id: "r1"))
    String.new(ch1.receive.not_nil!).should eq "first"

    ch2 = Channel(Bytes?).new(1)
    sm.register_request("r2", ch2)
    sm.apply(QueueCommand.new(QueueAction::Consume, "q", req_id: "r2"))
    String.new(ch2.receive.not_nil!).should eq "second"

    sm.depth.should eq 0
  end

  it "delivers nil when consuming from an empty queue" do
    sm = QueueStateMachine.new
    ch = Channel(Bytes?).new(1)
    sm.register_request("r1", ch)
    sm.apply(QueueCommand.new(QueueAction::Consume, "q", req_id: "r1"))
    ch.receive.should be_nil
  end

  it "silently drops Consume when no channel is registered (follower behavior)" do
    sm = QueueStateMachine.new
    sm.apply(QueueCommand.new(QueueAction::Publish, "q", body: "x".to_slice))
    # no register_request — simulating a follower
    sm.apply(QueueCommand.new(QueueAction::Consume, "q", req_id: "unknown"))
    sm.depth.should eq 0 # pop still happened
  end

  it "round-trips snapshot of queue contents" do
    sm1 = QueueStateMachine.new
    sm1.apply(QueueCommand.new(QueueAction::Publish, "q", body: "a".to_slice))
    sm1.apply(QueueCommand.new(QueueAction::Publish, "q", body: "b".to_slice))

    io = IO::Memory.new
    sm1.snapshot(io)
    io.rewind

    sm2 = QueueStateMachine.new
    sm2.restore(io)
    sm2.depth.should eq 2

    ch = Channel(Bytes?).new(1)
    sm2.register_request("r", ch)
    sm2.apply(QueueCommand.new(QueueAction::Consume, "q", req_id: "r"))
    String.new(ch.receive.not_nil!).should eq "a"
  end

  it "removes a registered channel after delivering a result" do
    sm = QueueStateMachine.new
    ch = Channel(Bytes?).new(1)
    sm.register_request("r1", ch)
    sm.apply(QueueCommand.new(QueueAction::Consume, "q", req_id: "r1"))
    sm.has_pending_request?("r1").should be_false
  end
end
