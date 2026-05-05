require "../../../src/raft"

class MetaStateMachine < Raft::StateMachine(QueueCommand)
  @groups : Hash(String, UInt64) = {} of String => UInt64
  @next_group_id : UInt64 = 1_u64
  @on_create_group : Proc(String, UInt64, Nil)
  @on_delete_group : Proc(String, UInt64, Nil)

  def initialize(*, on_delete_group : Proc(String, UInt64, Nil), &@on_create_group : String, UInt64 ->)
    @on_delete_group = on_delete_group
  end

  def apply(entry : QueueCommand)
    case entry.action
    when QueueAction::CreateQueue
      return if @groups.has_key?(entry.queue_name)
      group_id = @next_group_id
      @next_group_id += 1
      @groups[entry.queue_name] = group_id
      @on_create_group.call(entry.queue_name, group_id)
    when QueueAction::DeleteQueue
      if group_id = @groups.delete(entry.queue_name)
        @on_delete_group.call(entry.queue_name, group_id)
      end
    end
  end

  def group_for(name : String) : UInt64?
    @groups[name]?
  end

  def all_groups : Hash(String, UInt64)
    @groups
  end

  def snapshot(io : IO)
    io.write_bytes(@next_group_id, IO::ByteFormat::LittleEndian)
    io.write_bytes(@groups.size.to_u32, IO::ByteFormat::LittleEndian)
    @groups.each do |name, group_id|
      io.write_bytes(name.bytesize.to_u32, IO::ByteFormat::LittleEndian)
      io.write(name.to_slice)
      io.write_bytes(group_id, IO::ByteFormat::LittleEndian)
    end
  end

  def restore(io : IO)
    @next_group_id = io.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
    count = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
    @groups.clear
    count.times do
      name_size = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
      name_buf = Bytes.new(name_size)
      io.read_fully(name_buf)
      group_id = io.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
      @groups[String.new(name_buf)] = group_id
    end
  end
end
