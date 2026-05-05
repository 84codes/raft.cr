require "spec"
require "../../../src/raft"
require "../src/queue_command"
require "../src/meta_state_machine"

describe MetaStateMachine do
  it "assigns increasing group IDs on CreateQueue and fires create callback" do
    created = [] of {String, UInt64}
    deleted = [] of {String, UInt64}
    sm = MetaStateMachine.new(
      on_delete_group: ->(name : String, gid : UInt64) { deleted << {name, gid}; nil },
    ) { |name, gid| created << {name, gid}; nil }

    sm.apply(QueueCommand.new(QueueAction::CreateQueue, "orders"))
    sm.apply(QueueCommand.new(QueueAction::CreateQueue, "events"))

    created.should eq [{"orders", 1_u64}, {"events", 2_u64}]
    sm.group_for("orders").should eq 1_u64
    sm.group_for("events").should eq 2_u64
  end

  it "ignores duplicate CreateQueue" do
    created = [] of {String, UInt64}
    sm = MetaStateMachine.new(
      on_delete_group: ->(name : String, gid : UInt64) { nil },
    ) { |name, gid| created << {name, gid}; nil }

    sm.apply(QueueCommand.new(QueueAction::CreateQueue, "orders"))
    sm.apply(QueueCommand.new(QueueAction::CreateQueue, "orders"))

    created.size.should eq 1
  end

  it "fires delete callback on DeleteQueue" do
    deleted = [] of {String, UInt64}
    sm = MetaStateMachine.new(
      on_delete_group: ->(name : String, gid : UInt64) { deleted << {name, gid}; nil },
    ) { |name, gid| nil }

    sm.apply(QueueCommand.new(QueueAction::CreateQueue, "orders"))
    sm.apply(QueueCommand.new(QueueAction::DeleteQueue, "orders"))

    deleted.should eq [{"orders", 1_u64}]
    sm.group_for("orders").should be_nil
  end

  it "round-trips snapshot" do
    sm1 = MetaStateMachine.new(
      on_delete_group: ->(n : String, g : UInt64) { nil },
    ) { |n, g| nil }
    sm1.apply(QueueCommand.new(QueueAction::CreateQueue, "a"))
    sm1.apply(QueueCommand.new(QueueAction::CreateQueue, "b"))

    io = IO::Memory.new
    sm1.snapshot(io)
    io.rewind

    restored_creates = [] of {String, UInt64}
    sm2 = MetaStateMachine.new(
      on_delete_group: ->(n : String, g : UInt64) { nil },
    ) { |n, g| restored_creates << {n, g}; nil }
    sm2.restore(io)

    sm2.group_for("a").should eq 1_u64
    sm2.group_for("b").should eq 2_u64
    sm2.apply(QueueCommand.new(QueueAction::CreateQueue, "c"))
    sm2.group_for("c").should eq 3_u64
  end
end
