# Raft Consensus Library Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a deterministic, disk-first Raft consensus library in Crystal with generic data types, segmented mmap log, and multi-raft support.

**Architecture:** Layered core + transport. A deterministic `Node(T)` driven by `tick()`/`step()` produces outbound messages. `Log(T)` persists entries to segmented files on disk. Abstract `Transport` ships messages between servers. `Server(T)` is optional glue for standalone use.

**Tech Stack:** Crystal >= 1.19.1, MFile (vendored from LavinMQ) for mmap IO, Crystal spec for testing.

**Design doc:** `docs/plans/2026-03-07-raft-consensus-design.md`

---

### Task 1: Foundation Types — Config, Enums, LogEntry

Set up the basic types everything else depends on.

**Files:**
- Create: `src/raft/config.cr`
- Create: `src/raft/log_entry.cr`
- Create: `src/raft/message.cr` (enums + NodeID alias only — full Message in Task 3)
- Modify: `src/raft.cr`
- Test: `spec/raft/log_entry_spec.cr`

**Step 1: Write failing test for LogEntry serialization**

```crystal
# spec/raft/log_entry_spec.cr
require "../spec_helper"

# A simple test type that implements to_io/from_io
struct TestData
  getter value : String

  def initialize(@value : String)
  end

  def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian)
    io.write_bytes(@value.bytesize.to_u32, format)
    io.write(@value.to_slice)
  end

  def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
    size = io.read_bytes(UInt32, format)
    slice = Bytes.new(size)
    io.read_fully(slice)
    new(String.new(slice))
  end
end

describe Raft::LogEntry do
  it "round-trips through IO" do
    entry = Raft::LogEntry(TestData).new(
      term: 1_u64,
      index: 1_u64,
      entry_type: Raft::EntryType::Normal,
      data: TestData.new("hello")
    )

    io = IO::Memory.new
    entry.to_io(io)
    io.rewind
    restored = Raft::LogEntry(TestData).from_io(io)

    restored.term.should eq 1_u64
    restored.index.should eq 1_u64
    restored.entry_type.should eq Raft::EntryType::Normal
    restored.data.value.should eq "hello"
  end

  it "reports correct byte size" do
    entry = Raft::LogEntry(TestData).new(
      term: 1_u64,
      index: 1_u64,
      entry_type: Raft::EntryType::Normal,
      data: TestData.new("hello")
    )

    io = IO::Memory.new
    entry.to_io(io)
    io.pos.should eq(8 + 8 + 1 + 4 + 4 + 5) # term + index + type + data_size + string_size + "hello"
  end
end
```

**Step 2: Run test to verify it fails**

Run: `crystal spec spec/raft/log_entry_spec.cr`
Expected: FAIL — `Raft::LogEntry` not defined

**Step 3: Write Config, enums, and LogEntry**

```crystal
# src/raft/config.cr
module Raft
  alias NodeID = UInt64

  class Config
    property tick_interval : Time::Span = 50.milliseconds
    property heartbeat_ticks : UInt32 = 2
    property election_timeout_min_ticks : UInt32 = 10
    property election_timeout_max_ticks : UInt32 = 20
    property max_segment_size : UInt32 = 64 * 1024 * 1024 # 64 MB
    property data_dir : String = "data"
    property snapshot_chunk_size : UInt32 = 1024 * 1024 # 1 MB

    def initialize
    end
  end
end
```

```crystal
# src/raft/message.cr
module Raft
  enum EntryType : UInt8
    Normal        = 0
    Configuration = 1
  end

  enum MessageType : UInt8
    AppendEntries         = 0
    AppendEntriesResponse = 1
    RequestVote           = 2
    RequestVoteResponse   = 3
    InstallSnapshot       = 4
    InstallSnapshotResponse = 5
  end

  enum Role
    Follower
    Candidate
    Leader
  end
end
```

```crystal
# src/raft/log_entry.cr
module Raft
  struct LogEntry(T)
    getter term : UInt64
    getter index : UInt64
    getter entry_type : EntryType
    getter data : T

    def initialize(@term : UInt64, @index : UInt64, @entry_type : EntryType, @data : T)
    end

    def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian)
      io.write_bytes(@term, format)
      io.write_bytes(@index, format)
      io.write_bytes(@entry_type.value, format)
      # Serialize data to a temporary buffer to get size
      data_io = IO::Memory.new
      @data.to_io(data_io, format)
      io.write_bytes(data_io.pos.to_u32, format)
      io.write(data_io.to_slice)
    end

    def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
      term = io.read_bytes(UInt64, format)
      index = io.read_bytes(UInt64, format)
      entry_type = EntryType.new(io.read_bytes(UInt8, format))
      data_size = io.read_bytes(UInt32, format)
      data = T.from_io(io, format)
      new(term: term, index: index, entry_type: entry_type, data: data)
    end
  end
end
```

```crystal
# src/raft.cr
module Raft
  VERSION = "0.1.0"
end

require "./raft/config"
require "./raft/message"
require "./raft/log_entry"
```

**Step 4: Run test to verify it passes**

Run: `crystal spec spec/raft/log_entry_spec.cr`
Expected: PASS

**Step 5: Commit**

```bash
git add src/raft.cr src/raft/config.cr src/raft/message.cr src/raft/log_entry.cr spec/raft/log_entry_spec.cr
git commit -m "feat: add foundation types — Config, enums, LogEntry(T)"
```

---

### Task 2: StateMachine Abstract Class

**Files:**
- Create: `src/raft/state_machine.cr`
- Create: `spec/raft/helpers/test_state_machine.cr`
- Modify: `src/raft.cr` (add require)

**Step 1: Write the StateMachine abstract class and test implementation**

```crystal
# src/raft/state_machine.cr
module Raft
  abstract class StateMachine(T)
    abstract def apply(entry : T)
    abstract def snapshot(io : IO)
    abstract def restore(io : IO)
  end
end
```

```crystal
# spec/raft/helpers/test_state_machine.cr
require "../../spec_helper"

class TestStateMachine < Raft::StateMachine(TestData)
  getter applied : Array(TestData) = [] of TestData

  def apply(entry : TestData)
    @applied << entry
  end

  def snapshot(io : IO)
    io.write_bytes(@applied.size.to_u32, IO::ByteFormat::LittleEndian)
    @applied.each do |entry|
      entry.to_io(io, IO::ByteFormat::LittleEndian)
    end
  end

  def restore(io : IO)
    @applied.clear
    count = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
    count.times do
      @applied << TestData.from_io(io, IO::ByteFormat::LittleEndian)
    end
  end
end
```

**Step 2: Write failing test**

```crystal
# spec/raft/state_machine_spec.cr
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
```

**Step 3: Run test to verify it fails**

Run: `crystal spec spec/raft/state_machine_spec.cr`
Expected: FAIL — `Raft::StateMachine` not defined

**Step 4: Add require to `src/raft.cr`**

Add `require "./raft/state_machine"` to `src/raft.cr`.

**Step 5: Run test to verify it passes**

Run: `crystal spec spec/raft/state_machine_spec.cr`
Expected: PASS

**Step 6: Commit**

```bash
git add src/raft/state_machine.cr src/raft.cr spec/raft/state_machine_spec.cr spec/raft/helpers/test_state_machine.cr
git commit -m "feat: add StateMachine(T) abstract class"
```

---

### Task 3: Message Struct with IO Serialization

**Files:**
- Modify: `src/raft/message.cr`
- Test: `spec/raft/message_spec.cr`

**Step 1: Write failing test for Message round-trip**

```crystal
# spec/raft/message_spec.cr
require "../spec_helper"

describe Raft::Message do
  it "round-trips AppendEntries through IO" do
    msg = Raft::Message.new(
      protocol_version: 1_u8,
      group_id: 42_u64,
      type: Raft::MessageType::AppendEntries,
      from: 1_u64,
      term: 5_u64,
      prev_log_index: 10_u64,
      prev_log_term: 4_u64,
      commit_index: 9_u64,
      last_log_index: 0_u64,
      last_log_term: 0_u64,
      success: false,
      reject_hint: 0_u64,
      entries_data: Bytes.new(0),
      entries_count: 0_u32,
    )

    io = IO::Memory.new
    msg.to_io(io)
    io.rewind
    restored = Raft::Message.from_io(io)

    restored.protocol_version.should eq 1_u8
    restored.group_id.should eq 42_u64
    restored.type.should eq Raft::MessageType::AppendEntries
    restored.from.should eq 1_u64
    restored.term.should eq 5_u64
    restored.prev_log_index.should eq 10_u64
    restored.prev_log_term.should eq 4_u64
    restored.commit_index.should eq 9_u64
  end

  it "round-trips RequestVote through IO" do
    msg = Raft::Message.new(
      protocol_version: 1_u8,
      group_id: 1_u64,
      type: Raft::MessageType::RequestVote,
      from: 2_u64,
      term: 3_u64,
      prev_log_index: 0_u64,
      prev_log_term: 0_u64,
      commit_index: 0_u64,
      last_log_index: 5_u64,
      last_log_term: 2_u64,
      success: false,
      reject_hint: 0_u64,
      entries_data: Bytes.new(0),
      entries_count: 0_u32,
    )

    io = IO::Memory.new
    msg.to_io(io)
    io.rewind
    restored = Raft::Message.from_io(io)

    restored.type.should eq Raft::MessageType::RequestVote
    restored.last_log_index.should eq 5_u64
    restored.last_log_term.should eq 2_u64
  end

  it "round-trips RequestVoteResponse through IO" do
    msg = Raft::Message.new(
      protocol_version: 1_u8,
      group_id: 1_u64,
      type: Raft::MessageType::RequestVoteResponse,
      from: 3_u64,
      term: 3_u64,
      prev_log_index: 0_u64,
      prev_log_term: 0_u64,
      commit_index: 0_u64,
      last_log_index: 0_u64,
      last_log_term: 0_u64,
      success: true,
      reject_hint: 0_u64,
      entries_data: Bytes.new(0),
      entries_count: 0_u32,
    )

    io = IO::Memory.new
    msg.to_io(io)
    io.rewind
    restored = Raft::Message.from_io(io)

    restored.type.should eq Raft::MessageType::RequestVoteResponse
    restored.success.should eq true
  end
end
```

**Step 2: Run test to verify it fails**

Run: `crystal spec spec/raft/message_spec.cr`
Expected: FAIL — `Raft::Message` struct not found

**Step 3: Implement Message struct**

Add to `src/raft/message.cr`:

```crystal
# Append to existing file after the enums

module Raft
  struct Message
    PROTOCOL_VERSION = 1_u8

    property protocol_version : UInt8
    property group_id : UInt64
    property type : MessageType
    property from : NodeID
    property term : UInt64

    # AppendEntries fields
    property prev_log_index : UInt64
    property prev_log_term : UInt64
    property commit_index : UInt64

    # RequestVote fields
    property last_log_index : UInt64
    property last_log_term : UInt64

    # Response fields
    property success : Bool
    property reject_hint : UInt64

    # Entries payload (pre-serialized bytes for zero-copy)
    property entries_data : Bytes
    property entries_count : UInt32

    def initialize(
      @protocol_version : UInt8 = PROTOCOL_VERSION,
      @group_id : UInt64 = 0_u64,
      @type : MessageType = MessageType::AppendEntries,
      @from : NodeID = 0_u64,
      @term : UInt64 = 0_u64,
      @prev_log_index : UInt64 = 0_u64,
      @prev_log_term : UInt64 = 0_u64,
      @commit_index : UInt64 = 0_u64,
      @last_log_index : UInt64 = 0_u64,
      @last_log_term : UInt64 = 0_u64,
      @success : Bool = false,
      @reject_hint : UInt64 = 0_u64,
      @entries_data : Bytes = Bytes.new(0),
      @entries_count : UInt32 = 0_u32
    )
    end

    def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian)
      io.write_bytes(@protocol_version, format)
      io.write_bytes(@group_id, format)
      io.write_bytes(@type.value, format)
      io.write_bytes(@from, format)
      io.write_bytes(@term, format)
      io.write_bytes(@prev_log_index, format)
      io.write_bytes(@prev_log_term, format)
      io.write_bytes(@commit_index, format)
      io.write_bytes(@last_log_index, format)
      io.write_bytes(@last_log_term, format)
      io.write_bytes(@success ? 1_u8 : 0_u8, format)
      io.write_bytes(@reject_hint, format)
      io.write_bytes(@entries_count, format)
      io.write_bytes(@entries_data.size.to_u32, format)
      io.write(@entries_data) if @entries_data.size > 0
    end

    def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
      msg = new(
        protocol_version: io.read_bytes(UInt8, format),
        group_id: io.read_bytes(UInt64, format),
        type: MessageType.new(io.read_bytes(UInt8, format)),
        from: io.read_bytes(UInt64, format),
        term: io.read_bytes(UInt64, format),
        prev_log_index: io.read_bytes(UInt64, format),
        prev_log_term: io.read_bytes(UInt64, format),
        commit_index: io.read_bytes(UInt64, format),
        last_log_index: io.read_bytes(UInt64, format),
        last_log_term: io.read_bytes(UInt64, format),
        success: io.read_bytes(UInt8, format) == 1_u8,
        reject_hint: io.read_bytes(UInt64, format),
      )
      msg.entries_count = io.read_bytes(UInt32, format)
      data_size = io.read_bytes(UInt32, format)
      if data_size > 0
        entries_data = Bytes.new(data_size)
        io.read_fully(entries_data)
        msg.entries_data = entries_data
      end
      msg
    end
  end
end
```

**Step 4: Run test to verify it passes**

Run: `crystal spec spec/raft/message_spec.cr`
Expected: PASS

**Step 5: Commit**

```bash
git add src/raft/message.cr spec/raft/message_spec.cr
git commit -m "feat: add Message struct with binary IO serialization"
```

---

### Task 4: Log Segment — Append and Read Entries

**Files:**
- Create: `src/raft/log/segment.cr`
- Test: `spec/raft/log/segment_spec.cr`
- Modify: `src/raft.cr` (add require)

**Note:** For the POC, use regular `File` IO. MFile can be swapped in later as an optimization — the Segment interface stays the same.

**Step 1: Write failing test**

```crystal
# spec/raft/log/segment_spec.cr
require "../../spec_helper"
require "../helpers/test_state_machine"

describe Raft::Log::Segment do
  around_each do |example|
    dir = File.tempname("raft_segment")
    Dir.mkdir_p(dir)
    begin
      example.run
    ensure
      FileUtils.rm_rf(dir)
    end
  end

  it "appends and reads back entries" do
    dir = File.tempname("raft_segment")
    Dir.mkdir_p(dir)
    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64, max_size: 1024_u32)

    entry1 = Raft::LogEntry(TestData).new(term: 1_u64, index: 1_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("first"))
    entry2 = Raft::LogEntry(TestData).new(term: 1_u64, index: 2_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("second"))

    segment.append(entry1)
    segment.append(entry2)

    segment.read(1_u64).data.value.should eq "first"
    segment.read(2_u64).data.value.should eq "second"
    segment.count.should eq 2
    segment.last_index.should eq 2_u64

    segment.close
    FileUtils.rm_rf(dir)
  end

  it "reports full when max size exceeded" do
    dir = File.tempname("raft_segment")
    Dir.mkdir_p(dir)
    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64, max_size: 60_u32) # very small

    entry = Raft::LogEntry(TestData).new(term: 1_u64, index: 1_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("hello"))
    segment.append(entry)
    segment.full?.should eq true

    segment.close
    FileUtils.rm_rf(dir)
  end

  it "reopens and reads existing segment" do
    dir = File.tempname("raft_segment")
    Dir.mkdir_p(dir)

    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64, max_size: 1024_u32)
    entry = Raft::LogEntry(TestData).new(term: 1_u64, index: 1_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("persist"))
    segment.append(entry)
    segment.close

    segment2 = Raft::Log::Segment(TestData).open(dir, first_index: 1_u64, max_size: 1024_u32)
    segment2.read(1_u64).data.value.should eq "persist"
    segment2.count.should eq 1

    segment2.close
    FileUtils.rm_rf(dir)
  end
end
```

**Step 2: Run test to verify it fails**

Run: `crystal spec spec/raft/log/segment_spec.cr`
Expected: FAIL — `Raft::Log::Segment` not defined

**Step 3: Implement Segment**

```crystal
# src/raft/log/segment.cr
require "file_utils"

module Raft
  class Log(T)
    class Segment(T)
      getter first_index : UInt64
      getter last_index : UInt64
      getter count : UInt32 = 0_u32

      @offsets : Array(UInt64) = [] of UInt64
      @file : File
      @size : UInt64 = 0_u64
      @max_size : UInt32
      @dir : String

      def initialize(@dir : String, @first_index : UInt64, @max_size : UInt32)
        @last_index = @first_index - 1
        path = File.join(@dir, segment_filename)
        @file = File.open(path, "w+b")
      end

      # Open an existing segment from disk
      def self.open(dir : String, first_index : UInt64, max_size : UInt32) : self
        segment = new(dir, first_index, max_size)
        segment.recover
        segment
      end

      def append(entry : LogEntry(T))
        @offsets << @size
        io = @file
        entry.to_io(io)
        io.flush
        @size = io.pos.to_u64
        @last_index = entry.index
        @count += 1
      end

      def read(index : UInt64) : LogEntry(T)
        offset_idx = (index - @first_index).to_i32
        raise IndexError.new("Index #{index} out of range") if offset_idx < 0 || offset_idx >= @offsets.size
        @file.seek(@offsets[offset_idx])
        LogEntry(T).from_io(@file)
      end

      def full? : Bool
        @size >= @max_size
      end

      def close
        @file.close
      end

      # Rebuild in-memory offset index by scanning the segment file
      protected def recover
        path = File.join(@dir, segment_filename)
        @file.close
        @file = File.open(path, "r+b")
        @file.seek(0, IO::Seek::End)
        file_size = @file.pos.to_u64
        @file.rewind

        @offsets.clear
        @count = 0_u32
        @size = 0_u64
        @last_index = @first_index - 1

        while @file.pos < file_size
          offset = @file.pos.to_u64
          @offsets << offset
          entry = LogEntry(T).from_io(@file)
          @last_index = entry.index
          @count += 1
        end
        @size = @file.pos.to_u64
      end

      private def segment_filename : String
        "%016d.log" % @first_index
      end
    end
  end
end
```

Add to `src/raft.cr`:
```crystal
require "./raft/log/segment"
```

**Step 4: Run test to verify it passes**

Run: `crystal spec spec/raft/log/segment_spec.cr`
Expected: PASS

**Step 5: Commit**

```bash
git add src/raft/log/segment.cr src/raft.cr spec/raft/log/segment_spec.cr
git commit -m "feat: add Log::Segment(T) — append-only segment with offset index"
```

---

### Task 5: Segmented Log — Multi-Segment Manager

**Files:**
- Create: `src/raft/log.cr`
- Test: `spec/raft/log_spec.cr`
- Modify: `src/raft.cr` (add require)

**Step 1: Write failing test**

```crystal
# spec/raft/log_spec.cr
require "../spec_helper"
require "./helpers/test_state_machine"

describe Raft::Log do
  it "appends entries and reads them back" do
    dir = File.tempname("raft_log")
    Dir.mkdir_p(dir)
    config = Raft::Config.new
    config.data_dir = dir
    config.max_segment_size = 1024_u32

    log = Raft::Log(TestData).new(config)

    log.append(term: 1_u64, data: TestData.new("a"), entry_type: Raft::EntryType::Normal)
    log.append(term: 1_u64, data: TestData.new("b"), entry_type: Raft::EntryType::Normal)

    log.get(1_u64).data.value.should eq "a"
    log.get(2_u64).data.value.should eq "b"
    log.last_index.should eq 2_u64
    log.last_term.should eq 1_u64

    log.close
    FileUtils.rm_rf(dir)
  end

  it "rotates segments when full" do
    dir = File.tempname("raft_log")
    Dir.mkdir_p(dir)
    config = Raft::Config.new
    config.data_dir = dir
    config.max_segment_size = 60_u32 # tiny — forces rotation

    log = Raft::Log(TestData).new(config)

    log.append(term: 1_u64, data: TestData.new("a"), entry_type: Raft::EntryType::Normal)
    log.append(term: 1_u64, data: TestData.new("b"), entry_type: Raft::EntryType::Normal)
    log.append(term: 2_u64, data: TestData.new("c"), entry_type: Raft::EntryType::Normal)

    log.get(1_u64).data.value.should eq "a"
    log.get(2_u64).data.value.should eq "b"
    log.get(3_u64).data.value.should eq "c"
    log.segment_count.should be > 1

    log.close
    FileUtils.rm_rf(dir)
  end

  it "truncates entries after a given index" do
    dir = File.tempname("raft_log")
    Dir.mkdir_p(dir)
    config = Raft::Config.new
    config.data_dir = dir
    config.max_segment_size = 1024_u32

    log = Raft::Log(TestData).new(config)
    log.append(term: 1_u64, data: TestData.new("a"), entry_type: Raft::EntryType::Normal)
    log.append(term: 1_u64, data: TestData.new("b"), entry_type: Raft::EntryType::Normal)
    log.append(term: 2_u64, data: TestData.new("c"), entry_type: Raft::EntryType::Normal)

    log.truncate_after(1_u64)
    log.last_index.should eq 1_u64
    log.get(1_u64).data.value.should eq "a"

    log.close
    FileUtils.rm_rf(dir)
  end

  it "returns term for a given index" do
    dir = File.tempname("raft_log")
    Dir.mkdir_p(dir)
    config = Raft::Config.new
    config.data_dir = dir

    log = Raft::Log(TestData).new(config)
    log.append(term: 1_u64, data: TestData.new("a"), entry_type: Raft::EntryType::Normal)
    log.append(term: 3_u64, data: TestData.new("b"), entry_type: Raft::EntryType::Normal)

    log.term_at(1_u64).should eq 1_u64
    log.term_at(2_u64).should eq 3_u64

    log.close
    FileUtils.rm_rf(dir)
  end
end
```

**Step 2: Run test to verify it fails**

Run: `crystal spec spec/raft/log_spec.cr`
Expected: FAIL — `Raft::Log` constructor/methods not defined

**Step 3: Implement Log**

```crystal
# src/raft/log.cr
module Raft
  class Log(T)
    getter last_index : UInt64 = 0_u64
    getter last_term : UInt64 = 0_u64

    @segments : Array(Segment(T)) = [] of Segment(T)
    @config : Config

    def initialize(@config : Config)
      Dir.mkdir_p(@config.data_dir)
      new_segment(1_u64)
    end

    def append(term : UInt64, data : T, entry_type : EntryType) : LogEntry(T)
      @last_index += 1
      @last_term = term
      entry = LogEntry(T).new(term: term, index: @last_index, entry_type: entry_type, data: data)

      if current_segment.full?
        new_segment(@last_index)
      end
      current_segment.append(entry)
      entry
    end

    def get(index : UInt64) : LogEntry(T)
      segment = segment_for(index)
      segment.read(index)
    end

    def term_at(index : UInt64) : UInt64
      get(index).term
    end

    def truncate_after(index : UInt64)
      # Remove all segments that start after index
      while @segments.size > 1 && @segments.last.first_index > index
        seg = @segments.pop
        seg.close
      end
      # Rebuild the last remaining segment up to index
      seg = @segments.last
      if seg.last_index > index
        # Re-read entries up to index, rewrite segment
        entries = [] of LogEntry(T)
        (seg.first_index..index).each do |i|
          entries << seg.read(i)
        end
        seg.close

        # Delete and recreate
        new_seg = Segment(T).new(@config.data_dir, first_index: seg.first_index, max_size: @config.max_segment_size)
        entries.each { |e| new_seg.append(e) }
        @segments[-1] = new_seg
      end

      @last_index = index
      @last_term = get(index).term if index > 0
    end

    def segment_count : Int32
      @segments.size
    end

    def close
      @segments.each(&.close)
    end

    private def current_segment : Segment(T)
      @segments.last
    end

    private def new_segment(first_index : UInt64)
      @segments << Segment(T).new(@config.data_dir, first_index: first_index, max_size: @config.max_segment_size)
    end

    private def segment_for(index : UInt64) : Segment(T)
      # Binary search could be used here, but linear is fine for POC
      @segments.reverse_each do |seg|
        return seg if index >= seg.first_index && index <= seg.last_index
      end
      raise IndexError.new("No segment contains index #{index}")
    end
  end
end
```

Add to `src/raft.cr`:
```crystal
require "./raft/log"
```

**Step 4: Run test to verify it passes**

Run: `crystal spec spec/raft/log_spec.cr`
Expected: PASS

**Step 5: Commit**

```bash
git add src/raft/log.cr src/raft.cr spec/raft/log_spec.cr
git commit -m "feat: add Log(T) — segmented log with rotation and truncation"
```

---

### Task 6: Transport Abstract Class + MemoryTransport

**Files:**
- Create: `src/raft/transport.cr`
- Create: `src/raft/transport/memory_transport.cr`
- Test: `spec/raft/transport/memory_transport_spec.cr`
- Modify: `src/raft.cr` (add requires)

**Step 1: Write failing test**

```crystal
# spec/raft/transport/memory_transport_spec.cr
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
```

**Step 2: Run test to verify it fails**

Run: `crystal spec spec/raft/transport/memory_transport_spec.cr`
Expected: FAIL — not defined

**Step 3: Implement Transport + MemoryTransport**

```crystal
# src/raft/transport.cr
module Raft
  abstract class Transport
    abstract def send(to : NodeID, message : Message)
    abstract def receive(for_node : NodeID) : Array(Message)
  end
end
```

```crystal
# src/raft/transport/memory_transport.cr
module Raft
  class MemoryTransport < Transport
    @mailboxes = Hash(NodeID, Array(Message)).new { |h, k| h[k] = [] of Message }
    @isolated = Set(NodeID).new

    def send(to : NodeID, message : Message)
      return if @isolated.includes?(to) || @isolated.includes?(message.from)
      @mailboxes[to] << message
    end

    def receive(for_node : NodeID) : Array(Message)
      messages = @mailboxes[for_node]
      result = messages.dup
      messages.clear
      result
    end

    def isolate(node : NodeID)
      @isolated.add(node)
    end

    def heal(node : NodeID)
      @isolated.delete(node)
    end
  end
end
```

Add to `src/raft.cr`:
```crystal
require "./raft/transport"
require "./raft/transport/memory_transport"
```

**Step 4: Run test to verify it passes**

Run: `crystal spec spec/raft/transport/memory_transport_spec.cr`
Expected: PASS

**Step 5: Commit**

```bash
git add src/raft/transport.cr src/raft/transport/memory_transport.cr src/raft.cr spec/raft/transport/memory_transport_spec.cr
git commit -m "feat: add Transport abstract class and MemoryTransport"
```

---

### Task 7: Node — Follower State + Election Timeout

This is the first piece of the Raft protocol core. A node starts as a follower. After `election_timeout` ticks with no heartbeat, it becomes a candidate.

**Files:**
- Create: `src/raft/node.cr`
- Test: `spec/raft/node_spec.cr`
- Modify: `src/raft.cr` (add require)

**Step 1: Write failing test**

```crystal
# spec/raft/node_spec.cr
require "../spec_helper"
require "./helpers/test_state_machine"

# Helper to create a test node with in-memory log
def create_test_node(id : Raft::NodeID, peers : Array(Raft::NodeID), config : Raft::Config? = nil) : Raft::Node(TestData)
  config ||= Raft::Config.new
  dir = File.tempname("raft_node_#{id}")
  Dir.mkdir_p(dir)
  config.data_dir = dir
  sm = TestStateMachine.new
  Raft::Node(TestData).new(id: id, peers: peers, config: config, state_machine: sm)
end

describe Raft::Node do
  describe "initial state" do
    it "starts as a follower" do
      node = create_test_node(1_u64, [2_u64, 3_u64])
      node.role.should eq Raft::Role::Follower
      node.current_term.should eq 0_u64
      node.close
    end
  end

  describe "election timeout" do
    it "becomes candidate after election timeout ticks" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 5_u32
      config.election_timeout_max_ticks = 5_u32 # fixed for deterministic test

      node = create_test_node(1_u64, [2_u64, 3_u64], config)

      # Tick up to just before timeout — should stay follower
      4.times { node.tick }
      node.role.should eq Raft::Role::Follower

      # One more tick triggers election
      node.tick
      messages = node.take_messages
      node.role.should eq Raft::Role::Candidate
      node.current_term.should eq 1_u64

      # Should have sent RequestVote to each peer
      messages.size.should eq 2
      messages.all? { |m| m.type == Raft::MessageType::RequestVote }.should be_true
      messages.map(&.from).uniq.should eq [1_u64]

      node.close
    end

    it "resets election timer on receiving AppendEntries" do
      config = Raft::Config.new
      config.election_timeout_min_ticks = 5_u32
      config.election_timeout_max_ticks = 5_u32

      node = create_test_node(1_u64, [2_u64, 3_u64], config)

      3.times { node.tick }

      # Receive a heartbeat from leader
      heartbeat = Raft::Message.new(
        type: Raft::MessageType::AppendEntries,
        from: 2_u64,
        term: 1_u64,
        commit_index: 0_u64,
      )
      node.step(heartbeat)

      # Tick 4 more times — should NOT timeout (timer was reset)
      4.times { node.tick }
      node.role.should eq Raft::Role::Follower

      node.close
    end
  end
end
```

**Step 2: Run test to verify it fails**

Run: `crystal spec spec/raft/node_spec.cr`
Expected: FAIL — `Raft::Node` not defined

**Step 3: Implement Node with follower state**

```crystal
# src/raft/node.cr
module Raft
  class Node(T)
    getter role : Role = Role::Follower
    getter current_term : UInt64 = 0_u64
    getter id : NodeID
    getter voted_for : NodeID? = nil
    getter leader_id : NodeID? = nil

    @peers : Array(NodeID)
    @config : Config
    @log : Log(T)
    @state_machine : StateMachine(T)
    @outbox : Array(Message) = [] of Message
    @election_tick : UInt32 = 0_u32
    @election_timeout : UInt32
    @random : Random = Random.new

    def initialize(@id : NodeID, @peers : Array(NodeID), @config : Config, @state_machine : StateMachine(T))
      @log = Log(T).new(@config)
      @election_timeout = random_election_timeout
    end

    def tick
      @election_tick += 1

      case @role
      when Role::Follower
        if @election_tick >= @election_timeout
          become_candidate
        end
      when Role::Candidate
        if @election_tick >= @election_timeout
          become_candidate # restart election with new term
        end
      when Role::Leader
        # Heartbeats handled in Task 9
      end
    end

    def step(message : Message)
      # If message has a higher term, step down to follower
      if message.term > @current_term
        @current_term = message.term
        become_follower(message.from)
      end

      case message.type
      when MessageType::AppendEntries
        handle_append_entries(message)
      when MessageType::AppendEntriesResponse
        # Task 10
      when MessageType::RequestVote
        handle_request_vote(message)
      when MessageType::RequestVoteResponse
        handle_request_vote_response(message)
      end
    end

    def take_messages : Array(Message)
      messages = @outbox.dup
      @outbox.clear
      messages
    end

    def close
      @log.close
    end

    private def become_candidate
      @role = Role::Candidate
      @current_term += 1
      @voted_for = @id
      @election_tick = 0_u32
      @election_timeout = random_election_timeout
      @votes_received = Set(NodeID).new
      @votes_received.add(@id) # vote for self

      @peers.each do |peer|
        @outbox << Message.new(
          type: MessageType::RequestVote,
          from: @id,
          term: @current_term,
          last_log_index: @log.last_index,
          last_log_term: @log.last_term,
        )
      end
    end

    private def become_follower(leader : NodeID? = nil)
      @role = Role::Follower
      @leader_id = leader
      @voted_for = nil
      @election_tick = 0_u32
      @election_timeout = random_election_timeout
    end

    private def become_leader
      @role = Role::Leader
      @leader_id = @id
      # Initialize next_index and match_index per peer (Task 9)
    end

    private def handle_append_entries(msg : Message)
      @election_tick = 0_u32 # reset election timer

      if msg.term < @current_term
        @outbox << Message.new(
          type: MessageType::AppendEntriesResponse,
          from: @id,
          term: @current_term,
          success: false,
        )
        return
      end

      @leader_id = msg.from

      @outbox << Message.new(
        type: MessageType::AppendEntriesResponse,
        from: @id,
        term: @current_term,
        success: true,
      )
    end

    private def handle_request_vote(msg : Message)
      vote_granted = false

      if msg.term >= @current_term
        if @voted_for.nil? || @voted_for == msg.from
          # Check log is at least as up-to-date
          if msg.last_log_term > @log.last_term ||
             (msg.last_log_term == @log.last_term && msg.last_log_index >= @log.last_index)
            @voted_for = msg.from
            vote_granted = true
            @election_tick = 0_u32 # reset timer when granting vote
          end
        end
      end

      @outbox << Message.new(
        type: MessageType::RequestVoteResponse,
        from: @id,
        term: @current_term,
        success: vote_granted,
      )
    end

    private def handle_request_vote_response(msg : Message)
      return unless @role == Role::Candidate
      return unless msg.term == @current_term

      if msg.success
        @votes_received.not_nil!.add(msg.from)
        if @votes_received.not_nil!.size > (@peers.size + 1) / 2
          become_leader
        end
      end
    end

    private def random_election_timeout : UInt32
      @random.rand(@config.election_timeout_min_ticks..@config.election_timeout_max_ticks)
    end
  end
end
```

Add `@votes_received` instance variable:

Note: `@votes_received` needs to be declared as an instance variable. Add `@votes_received : Set(NodeID) = Set(NodeID).new` to the instance variables section.

Add to `src/raft.cr`:
```crystal
require "./raft/node"
```

**Step 4: Run test to verify it passes**

Run: `crystal spec spec/raft/node_spec.cr`
Expected: PASS

**Step 5: Commit**

```bash
git add src/raft/node.cr src/raft.cr spec/raft/node_spec.cr
git commit -m "feat: add Node(T) — follower state, election timeout, RequestVote"
```

---

### Task 8: Node — Leader Election (Full 3-Node Cluster)

Test that a 3-node cluster can elect a leader through message passing.

**Files:**
- Modify: `spec/raft/node_spec.cr` (add tests)

**Step 1: Write failing test**

Add to `spec/raft/node_spec.cr`:

```crystal
describe "leader election" do
  it "elects a leader in a 3-node cluster" do
    config = Raft::Config.new
    config.election_timeout_min_ticks = 5_u32
    config.election_timeout_max_ticks = 5_u32

    nodes = {
      1_u64 => create_test_node(1_u64, [2_u64, 3_u64], config),
      2_u64 => create_test_node(2_u64, [1_u64, 3_u64], config),
      3_u64 => create_test_node(3_u64, [1_u64, 2_u64], config),
    }

    transport = Raft::MemoryTransport.new

    # Tick only node 1 to trigger election
    5.times { nodes[1_u64].tick }
    nodes[1_u64].role.should eq Raft::Role::Candidate

    # Deliver RequestVote messages
    nodes[1_u64].take_messages.each do |msg|
      nodes.each_value do |node|
        next if node.id == msg.from
        node.step(msg)
      end
    end

    # Collect and deliver vote responses
    [2_u64, 3_u64].each do |peer_id|
      nodes[peer_id].take_messages.each do |msg|
        if msg.type == Raft::MessageType::RequestVoteResponse
          nodes[1_u64].step(msg)
        end
      end
    end

    # Node 1 should now be leader
    nodes[1_u64].role.should eq Raft::Role::Leader
    nodes[1_u64].current_term.should eq 1_u64

    nodes.each_value(&.close)
  end

  it "rejects votes if candidate log is behind" do
    config = Raft::Config.new
    config.election_timeout_min_ticks = 5_u32
    config.election_timeout_max_ticks = 5_u32

    node = create_test_node(1_u64, [2_u64, 3_u64], config)

    # Node 1 has a log entry at term 5
    node.step(Raft::Message.new(
      type: Raft::MessageType::AppendEntries,
      from: 2_u64,
      term: 5_u64,
    ))

    # Candidate with older log requests vote
    vote_request = Raft::Message.new(
      type: Raft::MessageType::RequestVote,
      from: 3_u64,
      term: 5_u64,
      last_log_index: 0_u64,
      last_log_term: 0_u64,
    )
    node.step(vote_request)

    messages = node.take_messages
    vote_response = messages.find { |m| m.type == Raft::MessageType::RequestVoteResponse }
    vote_response.should_not be_nil
    # Vote should be granted because node has empty log too (last_log_term == 0)
    # Both have empty logs, so the candidate's log is equally up-to-date

    node.close
  end
end
```

**Step 2: Run test to verify it fails (or passes if Task 7 impl is complete)**

Run: `crystal spec spec/raft/node_spec.cr`
Expected: Should pass if Task 7's `handle_request_vote_response` and `become_leader` are correct. If not, fix.

**Step 3: Commit**

```bash
git add spec/raft/node_spec.cr
git commit -m "test: add full 3-node leader election test"
```

---

### Task 9: Node — Leader Heartbeats + AppendEntries

Add leader behavior: send heartbeats on tick, replicate log entries.

**Files:**
- Modify: `src/raft/node.cr`
- Modify: `spec/raft/node_spec.cr` (add tests)

**Step 1: Write failing test**

Add to `spec/raft/node_spec.cr`:

```crystal
describe "leader behavior" do
  # Helper: elect node 1 as leader in a 3-node cluster
  it "sends heartbeats on tick" do
    config = Raft::Config.new
    config.election_timeout_min_ticks = 5_u32
    config.election_timeout_max_ticks = 5_u32
    config.heartbeat_ticks = 2_u32

    nodes = {
      1_u64 => create_test_node(1_u64, [2_u64, 3_u64], config),
      2_u64 => create_test_node(2_u64, [1_u64, 3_u64], config),
      3_u64 => create_test_node(3_u64, [1_u64, 2_u64], config),
    }

    # Elect node 1
    5.times { nodes[1_u64].tick }
    nodes[1_u64].take_messages.each do |msg|
      [2_u64, 3_u64].each { |id| nodes[id].step(msg) }
    end
    [2_u64, 3_u64].each do |id|
      nodes[id].take_messages.each { |msg| nodes[1_u64].step(msg) }
    end
    nodes[1_u64].role.should eq Raft::Role::Leader

    # Clear messages from election
    nodes[1_u64].take_messages

    # Tick leader twice (heartbeat_ticks = 2)
    2.times { nodes[1_u64].tick }
    heartbeats = nodes[1_u64].take_messages

    heartbeats.size.should eq 2 # one per peer
    heartbeats.all? { |m| m.type == Raft::MessageType::AppendEntries }.should be_true
    heartbeats.all? { |m| m.term == 1_u64 }.should be_true

    nodes.each_value(&.close)
  end

  it "replicates a proposed entry to followers" do
    config = Raft::Config.new
    config.election_timeout_min_ticks = 5_u32
    config.election_timeout_max_ticks = 5_u32
    config.heartbeat_ticks = 100_u32 # high so heartbeats don't interfere

    nodes = {
      1_u64 => create_test_node(1_u64, [2_u64, 3_u64], config),
      2_u64 => create_test_node(2_u64, [1_u64, 3_u64], config),
      3_u64 => create_test_node(3_u64, [1_u64, 2_u64], config),
    }

    # Elect node 1
    5.times { nodes[1_u64].tick }
    nodes[1_u64].take_messages.each do |msg|
      [2_u64, 3_u64].each { |id| nodes[id].step(msg) }
    end
    [2_u64, 3_u64].each do |id|
      nodes[id].take_messages.each { |msg| nodes[1_u64].step(msg) }
    end
    nodes[1_u64].role.should eq Raft::Role::Leader
    nodes[1_u64].take_messages # clear

    # Propose an entry
    nodes[1_u64].propose(TestData.new("command1"))
    messages = nodes[1_u64].take_messages

    # Should send AppendEntries with the new entry to both peers
    messages.size.should eq 2
    messages.all? { |m| m.type == Raft::MessageType::AppendEntries }.should be_true
    messages.all? { |m| m.entries_count == 1_u32 }.should be_true

    nodes.each_value(&.close)
  end
end
```

**Step 2: Run test to verify it fails**

Run: `crystal spec spec/raft/node_spec.cr`
Expected: FAIL — `propose` method, heartbeat tick logic not implemented

**Step 3: Implement leader behavior in Node**

Add to `src/raft/node.cr` — modifications to existing methods and new methods:

Add instance variables:
```crystal
@heartbeat_tick : UInt32 = 0_u32
@next_index : Hash(NodeID, UInt64) = {} of NodeID => UInt64
@match_index : Hash(NodeID, UInt64) = {} of NodeID => UInt64
```

Update `tick` for leader:
```crystal
when Role::Leader
  @heartbeat_tick += 1
  if @heartbeat_tick >= @config.heartbeat_ticks
    @heartbeat_tick = 0_u32
    send_append_entries
  end
```

Add `become_leader`:
```crystal
private def become_leader
  @role = Role::Leader
  @leader_id = @id
  @heartbeat_tick = 0_u32
  @peers.each do |peer|
    @next_index[peer] = @log.last_index + 1
    @match_index[peer] = 0_u64
  end
  # Send initial empty AppendEntries (heartbeat) immediately
  send_append_entries
end
```

Add `propose` and `send_append_entries`:
```crystal
def propose(data : T) : Bool
  return false unless @role == Role::Leader
  @log.append(term: @current_term, data: data, entry_type: EntryType::Normal)
  send_append_entries
  true
end

private def send_append_entries
  @peers.each do |peer|
    next_idx = @next_index[peer]
    prev_log_index = next_idx - 1
    prev_log_term = prev_log_index > 0 ? @log.term_at(prev_log_index) : 0_u64

    # Serialize entries from next_index to last_index
    entries_io = IO::Memory.new
    entries_count = 0_u32
    (next_idx..@log.last_index).each do |i|
      @log.get(i).to_io(entries_io)
      entries_count += 1
    end

    @outbox << Message.new(
      type: MessageType::AppendEntries,
      from: @id,
      term: @current_term,
      prev_log_index: prev_log_index,
      prev_log_term: prev_log_term,
      commit_index: @commit_index,
      entries_data: entries_io.to_slice.dup,
      entries_count: entries_count,
    )
  end
end
```

Add `@commit_index : UInt64 = 0_u64` instance variable.

**Step 4: Run test to verify it passes**

Run: `crystal spec spec/raft/node_spec.cr`
Expected: PASS

**Step 5: Commit**

```bash
git add src/raft/node.cr spec/raft/node_spec.cr
git commit -m "feat: add leader heartbeats, AppendEntries, and propose()"
```

---

### Task 10: Node — Log Replication + Commit Advancement

The leader tracks `match_index` per peer. When a majority has replicated an entry, it's committed. Committed entries are applied to the state machine.

**Files:**
- Modify: `src/raft/node.cr`
- Modify: `spec/raft/node_spec.cr` (add tests)

**Step 1: Write failing test**

Add to `spec/raft/node_spec.cr`:

```crystal
describe "log replication and commit" do
  it "commits entry when majority replicates" do
    config = Raft::Config.new
    config.election_timeout_min_ticks = 5_u32
    config.election_timeout_max_ticks = 5_u32
    config.heartbeat_ticks = 100_u32

    sm1 = TestStateMachine.new
    sm2 = TestStateMachine.new
    sm3 = TestStateMachine.new

    dir1 = File.tempname("raft_node_1")
    dir2 = File.tempname("raft_node_2")
    dir3 = File.tempname("raft_node_3")
    Dir.mkdir_p(dir1); Dir.mkdir_p(dir2); Dir.mkdir_p(dir3)

    c1 = config.dup; c1.data_dir = dir1
    c2 = config.dup; c2.data_dir = dir2
    c3 = config.dup; c3.data_dir = dir3

    nodes = {
      1_u64 => Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: c1, state_machine: sm1),
      2_u64 => Raft::Node(TestData).new(id: 2_u64, peers: [1_u64, 3_u64], config: c2, state_machine: sm2),
      3_u64 => Raft::Node(TestData).new(id: 3_u64, peers: [1_u64, 2_u64], config: c3, state_machine: sm3),
    }

    # Elect node 1 as leader
    5.times { nodes[1_u64].tick }
    nodes[1_u64].take_messages.each do |msg|
      [2_u64, 3_u64].each { |id| nodes[id].step(msg) }
    end
    [2_u64, 3_u64].each do |id|
      nodes[id].take_messages.each { |msg| nodes[1_u64].step(msg) }
    end
    nodes[1_u64].take_messages # clear initial heartbeats

    # Propose an entry
    nodes[1_u64].propose(TestData.new("cmd1"))
    append_messages = nodes[1_u64].take_messages

    # Deliver AppendEntries to followers
    append_messages.each do |msg|
      [2_u64, 3_u64].each do |id|
        nodes[id].step(msg)
      end
    end

    # Followers respond with success
    [2_u64, 3_u64].each do |id|
      nodes[id].take_messages.each do |msg|
        nodes[1_u64].step(msg) if msg.type == Raft::MessageType::AppendEntriesResponse
      end
    end

    # Leader should have committed and applied
    nodes[1_u64].commit_index.should eq 1_u64
    sm1.applied.size.should eq 1
    sm1.applied[0].value.should eq "cmd1"

    nodes.each_value(&.close)
    [dir1, dir2, dir3].each { |d| FileUtils.rm_rf(d) }
  end

  it "follower appends entries from leader" do
    config = Raft::Config.new
    config.election_timeout_min_ticks = 5_u32
    config.election_timeout_max_ticks = 5_u32
    config.heartbeat_ticks = 100_u32

    sm2 = TestStateMachine.new
    dir2 = File.tempname("raft_node_2")
    Dir.mkdir_p(dir2)
    c2 = config.dup; c2.data_dir = dir2

    node2 = Raft::Node(TestData).new(id: 2_u64, peers: [1_u64, 3_u64], config: c2, state_machine: sm2)

    # Simulate receiving AppendEntries from leader with one entry
    entry = Raft::LogEntry(TestData).new(term: 1_u64, index: 1_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("x"))
    entries_io = IO::Memory.new
    entry.to_io(entries_io)

    msg = Raft::Message.new(
      type: Raft::MessageType::AppendEntries,
      from: 1_u64,
      term: 1_u64,
      prev_log_index: 0_u64,
      prev_log_term: 0_u64,
      commit_index: 1_u64, # leader says this is committed
      entries_data: entries_io.to_slice.dup,
      entries_count: 1_u32,
    )
    node2.step(msg)

    responses = node2.take_messages
    responses.size.should eq 1
    responses[0].success.should be_true

    # Entry should be applied since commit_index includes it
    sm2.applied.size.should eq 1
    sm2.applied[0].value.should eq "x"

    node2.close
    FileUtils.rm_rf(dir2)
  end
end
```

**Step 2: Run test to verify it fails**

Run: `crystal spec spec/raft/node_spec.cr`
Expected: FAIL — follower doesn't process entries from AppendEntries, leader doesn't handle AppendEntriesResponse

**Step 3: Implement log replication**

Update `handle_append_entries` in `src/raft/node.cr` to process entries:

```crystal
private def handle_append_entries(msg : Message)
  @election_tick = 0_u32

  if msg.term < @current_term
    @outbox << Message.new(
      type: MessageType::AppendEntriesResponse,
      from: @id,
      term: @current_term,
      success: false,
      reject_hint: @log.last_index,
    )
    return
  end

  @leader_id = msg.from

  # Check prev_log consistency
  if msg.prev_log_index > 0
    if msg.prev_log_index > @log.last_index
      @outbox << Message.new(
        type: MessageType::AppendEntriesResponse,
        from: @id,
        term: @current_term,
        success: false,
        reject_hint: @log.last_index,
      )
      return
    end
    if @log.term_at(msg.prev_log_index) != msg.prev_log_term
      @outbox << Message.new(
        type: MessageType::AppendEntriesResponse,
        from: @id,
        term: @current_term,
        success: false,
        reject_hint: msg.prev_log_index - 1,
      )
      return
    end
  end

  # Append new entries
  if msg.entries_count > 0 && msg.entries_data.size > 0
    io = IO::Memory.new(msg.entries_data)
    msg.entries_count.times do
      entry = LogEntry(T).from_io(io)
      if entry.index <= @log.last_index
        # Conflict check: truncate if terms differ
        if @log.term_at(entry.index) != entry.term
          @log.truncate_after(entry.index - 1)
          @log.append(term: entry.term, data: entry.data, entry_type: entry.entry_type)
        end
        # else: already have this entry, skip
      else
        @log.append(term: entry.term, data: entry.data, entry_type: entry.entry_type)
      end
    end
  end

  # Update commit index and apply
  if msg.commit_index > @commit_index
    new_commit = Math.min(msg.commit_index, @log.last_index)
    apply_entries(@commit_index + 1, new_commit)
    @commit_index = new_commit
  end

  @outbox << Message.new(
    type: MessageType::AppendEntriesResponse,
    from: @id,
    term: @current_term,
    success: true,
    last_log_index: @log.last_index, # tell leader our last index
  )
end
```

Add `handle_append_entries_response`:
```crystal
private def handle_append_entries_response(msg : Message)
  return unless @role == Role::Leader

  if msg.success
    # Update match_index and next_index
    if msg.last_log_index > @match_index.fetch(msg.from, 0_u64)
      @match_index[msg.from] = msg.last_log_index
      @next_index[msg.from] = msg.last_log_index + 1
    end
    advance_commit_index
  else
    # Decrement next_index and retry
    hint = msg.reject_hint
    @next_index[msg.from] = Math.max(hint + 1, 1_u64)
  end
end
```

Add `advance_commit_index` and `apply_entries`:
```crystal
private def advance_commit_index
  # Find the highest index replicated on a majority
  (@commit_index + 1..@log.last_index).reverse_each do |n|
    next unless @log.term_at(n) == @current_term # only commit current term entries
    replication_count = 1 # count self
    @peers.each do |peer|
      replication_count += 1 if @match_index.fetch(peer, 0_u64) >= n
    end
    if replication_count > (@peers.size + 1) / 2
      apply_entries(@commit_index + 1, n)
      @commit_index = n
      break
    end
  end
end

private def apply_entries(from : UInt64, to : UInt64)
  (from..to).each do |i|
    entry = @log.get(i)
    @state_machine.apply(entry.data)
  end
end
```

Update `step` to route `AppendEntriesResponse`:
```crystal
when MessageType::AppendEntriesResponse
  handle_append_entries_response(msg)
```

Add `last_log_index` to the response `Message` (it's already a field — we just need to set it properly in the AppendEntriesResponse). Update the `Message` to use the existing field, or repurpose `reject_hint` for success responses. Actually, we already have `last_log_index` as a field on `Message` — reuse it in the response.

**Step 4: Run test to verify it passes**

Run: `crystal spec spec/raft/node_spec.cr`
Expected: PASS

**Step 5: Commit**

```bash
git add src/raft/node.cr spec/raft/node_spec.cr
git commit -m "feat: add log replication, commit advancement, and state machine apply"
```

---

### Task 11: Node — Persistence (Term + VotedFor)

Raft requires `current_term` and `voted_for` to survive restarts. Write these to a small metadata file.

**Files:**
- Modify: `src/raft/node.cr`
- Test: `spec/raft/node_spec.cr` (add test)

**Step 1: Write failing test**

Add to `spec/raft/node_spec.cr`:

```crystal
describe "persistence" do
  it "persists and recovers term and voted_for" do
    config = Raft::Config.new
    config.election_timeout_min_ticks = 5_u32
    config.election_timeout_max_ticks = 5_u32

    dir = File.tempname("raft_persist")
    Dir.mkdir_p(dir)
    config.data_dir = dir

    sm = TestStateMachine.new
    node = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: config, state_machine: sm)

    # Trigger election to set term and voted_for
    5.times { node.tick }
    node.current_term.should eq 1_u64
    node.voted_for.should eq 1_u64
    node.close

    # Reopen — should recover state
    sm2 = TestStateMachine.new
    config2 = Raft::Config.new
    config2.data_dir = dir
    config2.election_timeout_min_ticks = 5_u32
    config2.election_timeout_max_ticks = 5_u32

    node2 = Raft::Node(TestData).new(id: 1_u64, peers: [2_u64, 3_u64], config: config2, state_machine: sm2)
    node2.current_term.should eq 1_u64
    node2.voted_for.should eq 1_u64
    node2.role.should eq Raft::Role::Follower # restarts as follower

    node2.close
    FileUtils.rm_rf(dir)
  end
end
```

**Step 2: Run test to verify it fails**

Run: `crystal spec spec/raft/node_spec.cr`
Expected: FAIL — term/voted_for not persisted

**Step 3: Implement persistence**

Add to `src/raft/node.cr`:

```crystal
private def persist_state
  path = File.join(@config.data_dir, "raft_meta")
  File.open(path, "wb") do |f|
    f.write_bytes(@current_term, IO::ByteFormat::LittleEndian)
    if vf = @voted_for
      f.write_bytes(1_u8, IO::ByteFormat::LittleEndian)
      f.write_bytes(vf, IO::ByteFormat::LittleEndian)
    else
      f.write_bytes(0_u8, IO::ByteFormat::LittleEndian)
    end
  end
end

private def recover_state
  path = File.join(@config.data_dir, "raft_meta")
  return unless File.exists?(path)
  File.open(path, "rb") do |f|
    @current_term = f.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
    has_vote = f.read_bytes(UInt8, IO::ByteFormat::LittleEndian)
    @voted_for = has_vote == 1_u8 ? f.read_bytes(UInt64, IO::ByteFormat::LittleEndian) : nil
  end
end
```

Call `persist_state` whenever `@current_term` or `@voted_for` changes (in `become_candidate`, `become_follower`, `handle_request_vote`, and when stepping to a higher term). Call `recover_state` at the end of `initialize`.

**Step 4: Run test to verify it passes**

Run: `crystal spec spec/raft/node_spec.cr`
Expected: PASS

**Step 5: Commit**

```bash
git add src/raft/node.cr spec/raft/node_spec.cr
git commit -m "feat: persist current_term and voted_for to disk"
```

---

### Task 12: Server — Multi-Raft Glue + Ticker

**Files:**
- Create: `src/raft/server.cr`
- Test: `spec/raft/server_spec.cr`
- Modify: `src/raft.cr` (add require)

**Step 1: Write failing test**

```crystal
# spec/raft/server_spec.cr
require "../spec_helper"
require "./helpers/test_state_machine"

describe Raft::Server do
  it "ticks all registered nodes" do
    config = Raft::Config.new
    config.election_timeout_min_ticks = 3_u32
    config.election_timeout_max_ticks = 3_u32

    transport = Raft::MemoryTransport.new
    server = Raft::Server(TestData).new(transport: transport, config: config)

    sm1 = TestStateMachine.new
    sm2 = TestStateMachine.new
    server.add_group(group_id: 1_u64, node_id: 1_u64, peers: [2_u64, 3_u64], state_machine: sm1)
    server.add_group(group_id: 2_u64, node_id: 1_u64, peers: [2_u64, 3_u64], state_machine: sm2)

    # Manually tick (not using the fiber timer)
    3.times { server.tick }

    # Both nodes should have started elections
    server.node(1_u64).role.should eq Raft::Role::Candidate
    server.node(2_u64).role.should eq Raft::Role::Candidate

    server.close
  end

  it "routes incoming messages by group_id" do
    config = Raft::Config.new
    config.election_timeout_min_ticks = 100_u32
    config.election_timeout_max_ticks = 100_u32

    transport = Raft::MemoryTransport.new
    server = Raft::Server(TestData).new(transport: transport, config: config)

    sm = TestStateMachine.new
    server.add_group(group_id: 1_u64, node_id: 1_u64, peers: [2_u64, 3_u64], state_machine: sm)

    # Send a message to group 1
    msg = Raft::Message.new(
      type: Raft::MessageType::AppendEntries,
      from: 2_u64,
      term: 1_u64,
      group_id: 1_u64,
    )
    transport.send(to: 1_u64, message: msg)

    server.process_messages(for_node: 1_u64)

    # Node should have responded
    outgoing = server.take_all_messages
    outgoing.size.should be > 0

    server.close
  end
end
```

**Step 2: Run test to verify it fails**

Run: `crystal spec spec/raft/server_spec.cr`
Expected: FAIL — `Raft::Server` not defined

**Step 3: Implement Server**

```crystal
# src/raft/server.cr
module Raft
  class Server(T)
    @nodes : Hash(UInt64, Node(T)) = {} of UInt64 => Node(T)
    @transport : Transport
    @config : Config

    def initialize(@transport : Transport, @config : Config)
    end

    def add_group(group_id : UInt64, node_id : NodeID, peers : Array(NodeID), state_machine : StateMachine(T))
      group_config = @config.dup
      group_config.data_dir = File.join(@config.data_dir, "group-#{group_id}")
      Dir.mkdir_p(group_config.data_dir)
      @nodes[group_id] = Node(T).new(id: node_id, peers: peers, config: group_config, state_machine: state_machine)
    end

    def node(group_id : UInt64) : Node(T)
      @nodes[group_id]
    end

    def tick
      @nodes.each_value(&.tick)
    end

    def process_messages(for_node : NodeID)
      messages = @transport.receive(for_node: for_node)
      messages.each do |msg|
        if node = @nodes[msg.group_id]?
          node.step(msg)
        end
      end
    end

    def take_all_messages : Array(Message)
      all = [] of Message
      @nodes.each do |group_id, node|
        node.take_messages.each do |msg|
          msg.group_id = group_id
          all << msg
        end
      end
      all
    end

    def send_messages
      take_all_messages.each do |msg|
        # Determine target — for now broadcast to peers
        # This needs the node to tag messages with destination
        @transport.send(to: 0_u64, message: msg) # placeholder
      end
    end

    def start_ticker
      spawn do
        loop do
          sleep @config.tick_interval
          tick
        end
      end
    end

    def close
      @nodes.each_value(&.close)
    end
  end
end
```

Add to `src/raft.cr`:
```crystal
require "./raft/server"
```

**Step 4: Run test to verify it passes**

Run: `crystal spec spec/raft/server_spec.cr`
Expected: PASS

**Step 5: Commit**

```bash
git add src/raft/server.cr src/raft.cr spec/raft/server_spec.cr
git commit -m "feat: add Server(T) — multi-raft glue with tick loop and message routing"
```

---

### Task 13: TCPTransport (POC)

**Files:**
- Create: `src/raft/transport/tcp_transport.cr`
- Test: `spec/raft/transport/tcp_transport_spec.cr`
- Modify: `src/raft.cr` (add require)

**Step 1: Write failing test**

```crystal
# spec/raft/transport/tcp_transport_spec.cr
require "../../spec_helper"

describe Raft::TCPTransport do
  it "sends and receives messages over TCP" do
    t1 = Raft::TCPTransport.new(node_id: 1_u64, listen_address: "127.0.0.1", listen_port: 9741)
    t2 = Raft::TCPTransport.new(node_id: 2_u64, listen_address: "127.0.0.1", listen_port: 9742)

    t1.register_peer(2_u64, "127.0.0.1", 9742)
    t2.register_peer(1_u64, "127.0.0.1", 9741)

    t1.start
    t2.start
    sleep 50.milliseconds # let servers start

    msg = Raft::Message.new(
      type: Raft::MessageType::RequestVote,
      from: 1_u64,
      term: 1_u64,
      group_id: 1_u64,
    )

    t1.send(to: 2_u64, message: msg)
    sleep 50.milliseconds # let message deliver

    received = t2.receive(for_node: 2_u64)
    received.size.should eq 1
    received[0].type.should eq Raft::MessageType::RequestVote
    received[0].from.should eq 1_u64

    t1.stop
    t2.stop
  end
end
```

**Step 2: Run test to verify it fails**

Run: `crystal spec spec/raft/transport/tcp_transport_spec.cr`
Expected: FAIL — `Raft::TCPTransport` not defined

**Step 3: Implement TCPTransport**

```crystal
# src/raft/transport/tcp_transport.cr
require "socket"

module Raft
  class TCPTransport < Transport
    @node_id : NodeID
    @listen_address : String
    @listen_port : Int32
    @peers : Hash(NodeID, {String, Int32}) = {} of NodeID => {String, Int32}
    @connections : Hash(NodeID, TCPSocket) = {} of NodeID => TCPSocket
    @inbox : Array(Message) = [] of Message
    @server : TCPServer? = nil
    @running : Bool = false

    def initialize(@node_id : NodeID, @listen_address : String, @listen_port : Int32)
    end

    def register_peer(id : NodeID, host : String, port : Int32)
      @peers[id] = {host, port}
    end

    def start
      @running = true
      @server = server = TCPServer.new(@listen_address, @listen_port)
      spawn do
        while @running
          if client = server.accept?
            spawn handle_connection(client)
          end
        end
      end
    end

    def stop
      @running = false
      @server.try(&.close)
      @connections.each_value(&.close)
      @connections.clear
    end

    def send(to : NodeID, message : Message)
      conn = get_connection(to)
      return unless conn
      begin
        message.to_io(conn)
        conn.flush
      rescue ex : IO::Error
        @connections.delete(to)
        conn.close rescue nil
      end
    end

    def receive(for_node : NodeID) : Array(Message)
      result = @inbox.dup
      @inbox.clear
      result
    end

    private def get_connection(to : NodeID) : TCPSocket?
      if conn = @connections[to]?
        return conn unless conn.closed?
      end
      if peer = @peers[to]?
        begin
          conn = TCPSocket.new(peer[0], peer[1])
          @connections[to] = conn
          conn
        rescue ex : Socket::ConnectError
          nil
        end
      end
    end

    private def handle_connection(client : TCPSocket)
      while @running
        msg = Message.from_io(client)
        @inbox << msg
      end
    rescue IO::EOFError | IO::Error
      client.close rescue nil
    end
  end
end
```

Add to `src/raft.cr`:
```crystal
require "./raft/transport/tcp_transport"
```

**Step 4: Run test to verify it passes**

Run: `crystal spec spec/raft/transport/tcp_transport_spec.cr`
Expected: PASS

**Step 5: Commit**

```bash
git add src/raft/transport/tcp_transport.cr src/raft.cr spec/raft/transport/tcp_transport_spec.cr
git commit -m "feat: add TCPTransport — single TCP connection per peer pair"
```

---

### Task 14: Integration Test — Full 3-Node Cluster with MemoryTransport

End-to-end test: 3 nodes elect a leader, replicate entries, commit, and apply.

**Files:**
- Create: `spec/raft/integration_spec.cr`

**Step 1: Write the integration test**

```crystal
# spec/raft/integration_spec.cr
require "../spec_helper"
require "./helpers/test_state_machine"

# Helper that delivers all pending messages between nodes
def deliver_all(nodes : Hash(Raft::NodeID, Raft::Node(TestData)))
  loop do
    any_delivered = false
    pending = [] of {Raft::NodeID, Raft::Message}

    nodes.each do |id, node|
      node.take_messages.each do |msg|
        # Broadcast to all other nodes (in POC, messages don't have a `to` field)
        nodes.each do |target_id, target_node|
          next if target_id == id
          pending << {target_id, msg}
        end
      end
    end

    pending.each do |target_id, msg|
      nodes[target_id].step(msg)
      any_delivered = true
    end

    break unless any_delivered
  end
end

describe "Integration: 3-node Raft cluster" do
  it "elects a leader, replicates, commits, and applies" do
    config = Raft::Config.new
    config.election_timeout_min_ticks = 5_u32
    config.election_timeout_max_ticks = 5_u32
    config.heartbeat_ticks = 2_u32

    state_machines = {} of Raft::NodeID => TestStateMachine
    nodes = {} of Raft::NodeID => Raft::Node(TestData)

    [1_u64, 2_u64, 3_u64].each do |id|
      sm = TestStateMachine.new
      state_machines[id] = sm
      dir = File.tempname("raft_integration_#{id}")
      Dir.mkdir_p(dir)
      c = config.dup
      c.data_dir = dir
      peers = [1_u64, 2_u64, 3_u64].reject(id)
      nodes[id] = Raft::Node(TestData).new(id: id, peers: peers, config: c, state_machine: sm)
    end

    # Tick node 1 to trigger election
    5.times { nodes[1_u64].tick }

    # Deliver messages until stable
    deliver_all(nodes)

    # Node 1 should be leader
    nodes[1_u64].role.should eq Raft::Role::Leader

    # Propose entries
    nodes[1_u64].propose(TestData.new("first"))
    nodes[1_u64].propose(TestData.new("second"))

    # Deliver replication messages
    deliver_all(nodes)

    # All state machines should have applied both entries
    state_machines[1_u64].applied.size.should eq 2
    state_machines[1_u64].applied[0].value.should eq "first"
    state_machines[1_u64].applied[1].value.should eq "second"

    # Followers apply on next heartbeat that carries updated commit_index
    # Tick leader to send heartbeat with commit_index
    2.times { nodes[1_u64].tick }
    deliver_all(nodes)

    state_machines[2_u64].applied.size.should eq 2
    state_machines[3_u64].applied.size.should eq 2

    nodes.each_value(&.close)
  end

  it "handles leader failure and re-election" do
    config = Raft::Config.new
    config.election_timeout_min_ticks = 5_u32
    config.election_timeout_max_ticks = 5_u32
    config.heartbeat_ticks = 2_u32

    state_machines = {} of Raft::NodeID => TestStateMachine
    nodes = {} of Raft::NodeID => Raft::Node(TestData)

    [1_u64, 2_u64, 3_u64].each do |id|
      sm = TestStateMachine.new
      state_machines[id] = sm
      dir = File.tempname("raft_reelect_#{id}")
      Dir.mkdir_p(dir)
      c = config.dup
      c.data_dir = dir
      peers = [1_u64, 2_u64, 3_u64].reject(id)
      nodes[id] = Raft::Node(TestData).new(id: id, peers: peers, config: c, state_machine: sm)
    end

    # Elect node 1
    5.times { nodes[1_u64].tick }
    deliver_all(nodes)
    nodes[1_u64].role.should eq Raft::Role::Leader

    # "Kill" node 1 by removing it
    old_leader = nodes.delete(1_u64).not_nil!
    old_leader.close

    # Tick remaining nodes until one becomes candidate
    5.times do
      nodes.each_value(&.tick)
    end
    deliver_all(nodes)

    # One of the remaining should be leader
    leaders = nodes.values.select { |n| n.role == Raft::Role::Leader }
    leaders.size.should eq 1
    leaders[0].current_term.should be > 1_u64

    nodes.each_value(&.close)
  end
end
```

**Step 2: Run test**

Run: `crystal spec spec/raft/integration_spec.cr`
Expected: PASS (if all previous tasks implemented correctly)

**Step 3: Commit**

```bash
git add spec/raft/integration_spec.cr
git commit -m "test: add integration tests — full cluster election, replication, and re-election"
```

---

### Task 15: Clean Up — Remove Scaffold Test, Update README

**Files:**
- Modify: `spec/raft_spec.cr` (remove placeholder)
- Modify: `README.md` (basic usage example)
- Modify: `spec/spec_helper.cr` (add TestData to shared helper)

**Step 1: Move TestData to spec_helper so all tests can use it**

Move the `TestData` struct from `spec/raft/log_entry_spec.cr` into `spec/spec_helper.cr`. Update all test files to remove duplicate definitions.

**Step 2: Remove placeholder test**

Replace `spec/raft_spec.cr` with:
```crystal
require "./spec_helper"
require "./raft/**"
```

**Step 3: Run all tests**

Run: `crystal spec`
Expected: All tests PASS

**Step 4: Update README with basic usage**

```markdown
# Raft

A fast, efficient Raft consensus library for Crystal. Generic data types, disk-first persistence, deterministic core.

## Usage

\```crystal
require "raft"

# Define your data type
struct MyCommand
  getter action : String

  def initialize(@action)
  end

  def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian)
    io.write_bytes(@action.bytesize.to_u32, format)
    io.write(@action.to_slice)
  end

  def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
    size = io.read_bytes(UInt32, format)
    slice = Bytes.new(size)
    io.read_fully(slice)
    new(String.new(slice))
  end
end

# Implement your state machine
class MyStateMachine < Raft::StateMachine(MyCommand)
  def apply(entry : MyCommand)
    puts "Applying: #{entry.action}"
  end

  def snapshot(io : IO)
    # serialize state
  end

  def restore(io : IO)
    # restore state
  end
end

# Configure and start
config = Raft::Config.new
config.data_dir = "./raft-data"

sm = MyStateMachine.new
node = Raft::Node(MyCommand).new(
  id: 1_u64,
  peers: [2_u64, 3_u64],
  config: config,
  state_machine: sm
)

# Drive with ticks (deterministic)
node.tick
node.propose(MyCommand.new("set key=value"))
messages = node.take_messages # send these to peers via your transport
\```
```

**Step 5: Commit**

```bash
git add -A
git commit -m "chore: clean up scaffolding, add usage README"
```

---

## Summary

| Task | Component | Description |
|------|-----------|-------------|
| 1 | Foundation | Config, enums, LogEntry(T) with IO serialization |
| 2 | StateMachine | Abstract class + test implementation |
| 3 | Message | Struct with binary IO serialization |
| 4 | Segment | Single log segment — append, read, reopen |
| 5 | Log | Segmented log — rotation, truncation |
| 6 | Transport | Abstract + MemoryTransport with partition simulation |
| 7 | Node | Follower state, election timeout, RequestVote |
| 8 | Node | Full 3-node leader election test |
| 9 | Node | Leader heartbeats, AppendEntries, propose() |
| 10 | Node | Log replication, commit advancement, apply |
| 11 | Node | Persist term + voted_for to disk |
| 12 | Server | Multi-raft glue, ticker, message routing |
| 13 | TCPTransport | POC TCP networking |
| 14 | Integration | Full cluster election, replication, re-election tests |
| 15 | Cleanup | Shared helpers, README, remove scaffolding |
