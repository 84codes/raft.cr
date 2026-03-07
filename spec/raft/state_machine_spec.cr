require "../spec_helper"
require "./helpers/test_state_machine"

describe Raft::StateMachine do
  it "applies entries and round-trips via snapshot" do
    sm = TestStateMachine.new
    sm.apply(TestData.new("a"))
    sm.apply(TestData.new("b"))
    sm.applied.size.should eq 2

    io = IO::Memory.new
    sm.snapshot(io)
    io.rewind

    sm2 = TestStateMachine.new
    sm2.restore(io)
    sm2.applied.size.should eq 2
    sm2.applied[0].value.should eq "a"
    sm2.applied[1].value.should eq "b"
  end
end
