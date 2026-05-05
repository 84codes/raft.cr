require "spec"
require "../src/queue_command"

describe QueueCommand do
  it "round-trips Publish through to_io / from_io" do
    cmd = QueueCommand.new(
      action: QueueAction::Publish,
      queue_name: "orders",
      body: Bytes[1, 2, 3, 4, 5],
      req_id: "",
    )
    io = IO::Memory.new
    cmd.to_io(io)
    io.rewind
    parsed = QueueCommand.from_io(io)
    parsed.action.should eq QueueAction::Publish
    parsed.queue_name.should eq "orders"
    parsed.body.should eq Bytes[1, 2, 3, 4, 5]
    parsed.req_id.should eq ""
  end

  it "round-trips Consume with a req_id" do
    cmd = QueueCommand.new(
      action: QueueAction::Consume,
      queue_name: "orders",
      body: Bytes.new(0),
      req_id: "550e8400-e29b-41d4-a716-446655440000",
    )
    io = IO::Memory.new
    cmd.to_io(io)
    io.rewind
    parsed = QueueCommand.from_io(io)
    parsed.action.should eq QueueAction::Consume
    parsed.queue_name.should eq "orders"
    parsed.req_id.should eq "550e8400-e29b-41d4-a716-446655440000"
  end

  it "round-trips CreateQueue and DeleteQueue" do
    [QueueAction::CreateQueue, QueueAction::DeleteQueue].each do |action|
      cmd = QueueCommand.new(action: action, queue_name: "x", body: Bytes.new(0), req_id: "")
      io = IO::Memory.new
      cmd.to_io(io)
      io.rewind
      parsed = QueueCommand.from_io(io)
      parsed.action.should eq action
      parsed.queue_name.should eq "x"
    end
  end
end
