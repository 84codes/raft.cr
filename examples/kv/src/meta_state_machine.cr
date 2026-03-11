require "../../../src/raft"

class MetaStateMachine < Raft::StateMachine(KVCommand)
  @groups : Hash(String, UInt64) = {} of String => UInt64
  @next_group_id : UInt64 = 1_u64
  @on_create_group : Proc(String, UInt64, String?, Nil)
  @on_delete_group : Proc(String, UInt64, Nil)

  def initialize(@on_delete_group : Proc(String, UInt64, Nil), &@on_create_group : String, UInt64, String? ->)
  end

  def apply(entry : KVCommand)
    case entry.action
    when KVAction::CreateGroup
      return if @groups.has_key?(entry.key)
      group_id = @next_group_id
      @next_group_id += 1
      @groups[entry.key] = group_id
      initial_value = entry.value.empty? ? nil : entry.value
      @on_create_group.call(entry.key, group_id, initial_value)
    when KVAction::DeleteGroup
      if group_id = @groups.delete(entry.key)
        @on_delete_group.call(entry.key, group_id)
      end
    end
  end

  def group_for(key : String) : UInt64?
    @groups[key]?
  end

  def all_groups : Hash(String, UInt64)
    @groups
  end

  def snapshot(io : IO)
    io.write_bytes(@next_group_id, IO::ByteFormat::LittleEndian)
    io.write_bytes(@groups.size.to_u32, IO::ByteFormat::LittleEndian)
    @groups.each do |key, group_id|
      io.write_bytes(key.bytesize.to_u32, IO::ByteFormat::LittleEndian)
      io.write(key.to_slice)
      io.write_bytes(group_id, IO::ByteFormat::LittleEndian)
    end
  end

  def restore(io : IO)
    @next_group_id = io.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
    count = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
    @groups.clear
    count.times do
      key_size = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
      key_slice = Bytes.new(key_size)
      io.read_fully(key_slice)
      group_id = io.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
      @groups[String.new(key_slice)] = group_id
    end
  end
end
