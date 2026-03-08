require "spec"
require "../../../src/raft"
require "../src/kv_command"
require "../src/kv_state_machine"

describe KVStateMachine do
  it "applies put and get" do
    sm = KVStateMachine.new
    sm.apply(KVCommand.new(KVAction::Put, "foo", "bar"))
    sm.get("foo").should eq "bar"
  end

  it "applies delete" do
    sm = KVStateMachine.new
    sm.apply(KVCommand.new(KVAction::Put, "foo", "bar"))
    sm.apply(KVCommand.new(KVAction::Delete, "foo", ""))
    sm.get("foo").should be_nil
  end

  it "round-trips snapshot" do
    sm = KVStateMachine.new
    sm.apply(KVCommand.new(KVAction::Put, "a", "1"))
    sm.apply(KVCommand.new(KVAction::Put, "b", "2"))

    io = IO::Memory.new
    sm.snapshot(io)
    io.rewind

    sm2 = KVStateMachine.new
    sm2.restore(io)
    sm2.get("a").should eq "1"
    sm2.get("b").should eq "2"
  end
end
