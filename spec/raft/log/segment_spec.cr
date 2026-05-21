require "../../spec_helper"

describe Raft::Log::Segment do
  it "appends and reads back entries" do
    dir = File.tempname("raft_segment")
    Dir.mkdir_p(dir)
    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64, capacity: 1024_i64)

    entry1 = Raft::LogEntry(TestData).new(term: 1_u64, index: 1_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("first"))
    entry2 = Raft::LogEntry(TestData).new(term: 1_u64, index: 2_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("second"))

    segment.append(entry1)
    segment.append(entry2)

    segment.read(1_u64).data.not_nil!.value.should eq "first"
    segment.read(2_u64).data.not_nil!.value.should eq "second"
    segment.count.should eq 2
    segment.last_index.should eq 2_u64

    segment.close
    FileUtils.rm_rf(dir)
  end

  it "reports capacity correctly" do
    dir = File.tempname("raft_segment")
    Dir.mkdir_p(dir)
    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64, capacity: 50_i64)

    entry = Raft::LogEntry(TestData).new(term: 1_u64, index: 1_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("hello"))
    segment.has_capacity_for?(30).should eq true
    segment.append(entry)
    # After writing ~30 bytes, no room for another ~30 byte entry in 50 byte segment
    segment.has_capacity_for?(30).should eq false

    segment.close
    FileUtils.rm_rf(dir)
  end

  it "truncates in place and reads remaining entries" do
    dir = File.tempname("raft_segment")
    Dir.mkdir_p(dir)
    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64, capacity: 1024_i64)

    entry1 = Raft::LogEntry(TestData).new(term: 1_u64, index: 1_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("first"))
    entry2 = Raft::LogEntry(TestData).new(term: 1_u64, index: 2_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("second"))
    entry3 = Raft::LogEntry(TestData).new(term: 2_u64, index: 3_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("third"))

    segment.append(entry1)
    segment.append(entry2)
    segment.append(entry3)

    segment.truncate_to(2_u64)

    segment.count.should eq 2
    segment.last_index.should eq 2_u64
    segment.read(1_u64).data.not_nil!.value.should eq "first"
    segment.read(2_u64).data.not_nil!.value.should eq "second"
    expect_raises(IndexError) { segment.read(3_u64) }

    segment.close
    FileUtils.rm_rf(dir)
  end

  it "truncated segment file is smaller on disk" do
    dir = File.tempname("raft_segment")
    Dir.mkdir_p(dir)
    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64, capacity: 1024_i64)

    entry1 = Raft::LogEntry(TestData).new(term: 1_u64, index: 1_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("first"))
    entry2 = Raft::LogEntry(TestData).new(term: 1_u64, index: 2_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("second"))
    entry3 = Raft::LogEntry(TestData).new(term: 2_u64, index: 3_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("third"))

    segment.append(entry1)
    segment.append(entry2)
    segment.append(entry3)
    segment.close

    # Record full size on disk
    path = File.join(dir, "%016d.log" % 1)
    full_size = File.size(path)

    # Reopen and truncate
    segment2 = Raft::Log::Segment(TestData).open(dir, first_index: 1_u64)
    segment2.truncate_to(1_u64)
    segment2.close

    truncated_size = File.size(path)
    truncated_size.should be < full_size

    # Reopen after truncation — should recover only entry 1
    segment3 = Raft::Log::Segment(TestData).open(dir, first_index: 1_u64)
    segment3.count.should eq 1
    segment3.last_index.should eq 1_u64
    segment3.read(1_u64).data.not_nil!.value.should eq "first"
    segment3.close

    FileUtils.rm_rf(dir)
  end

  it "truncate_to recovers correctly after reopen" do
    dir = File.tempname("raft_segment")
    Dir.mkdir_p(dir)
    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64, capacity: 1024_i64)

    entry1 = Raft::LogEntry(TestData).new(term: 1_u64, index: 1_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("alpha"))
    entry2 = Raft::LogEntry(TestData).new(term: 1_u64, index: 2_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("beta"))
    entry3 = Raft::LogEntry(TestData).new(term: 2_u64, index: 3_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("gamma"))

    segment.append(entry1)
    segment.append(entry2)
    segment.append(entry3)

    segment.truncate_to(2_u64)
    segment.close

    # Reopen and verify recovery sees only the first 2 entries
    segment2 = Raft::Log::Segment(TestData).open(dir, first_index: 1_u64)
    segment2.count.should eq 2
    segment2.last_index.should eq 2_u64
    segment2.read(1_u64).data.not_nil!.value.should eq "alpha"
    segment2.read(2_u64).data.not_nil!.value.should eq "beta"
    segment2.close
    FileUtils.rm_rf(dir)
  end

  it "recovers partial entry by truncating garbage at end of segment" do
    dir = File.tempname("raft_segment")
    Dir.mkdir_p(dir)

    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64, capacity: 1024_i64)
    entry1 = Raft::LogEntry(TestData).new(term: 1_u64, index: 1_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("first"))
    entry2 = Raft::LogEntry(TestData).new(term: 1_u64, index: 2_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("second"))
    segment.append(entry1)
    segment.append(entry2)
    segment.close

    # Append garbage bytes to simulate a crash mid-write
    path = File.join(dir, "%016d.log" % 1)
    File.open(path, "a") { |f| f.write(Bytes.new(17, 0xDE_u8)) }

    # Reopen — recover should ignore the garbage and see only valid entries
    segment2 = Raft::Log::Segment(TestData).open(dir, first_index: 1_u64)
    segment2.count.should eq 2
    segment2.last_index.should eq 2_u64
    segment2.read(1_u64).data.not_nil!.value.should eq "first"
    segment2.read(2_u64).data.not_nil!.value.should eq "second"
    segment2.close

    FileUtils.rm_rf(dir)
  end

  it "recovers empty segment with only garbage bytes" do
    dir = File.tempname("raft_segment")
    Dir.mkdir_p(dir)

    # Create a segment so the file exists, then close it
    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64, capacity: 1024_i64)
    segment.close

    # Write garbage to the empty segment file
    path = File.join(dir, "%016d.log" % 1)
    File.open(path, "a") { |f| f.write(Bytes.new(11, 0xAB_u8)) }

    # Reopen — should recover with zero valid entries
    segment2 = Raft::Log::Segment(TestData).open(dir, first_index: 1_u64)
    segment2.count.should eq 0
    segment2.last_index.should eq 0_u64
    segment2.close

    FileUtils.rm_rf(dir)
  end

  it "reopens and reads existing segment" do
    dir = File.tempname("raft_segment")
    Dir.mkdir_p(dir)

    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64, capacity: 1024_i64)
    entry = Raft::LogEntry(TestData).new(term: 1_u64, index: 1_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("persist"))
    segment.append(entry)
    segment.close

    segment2 = Raft::Log::Segment(TestData).open(dir, first_index: 1_u64)
    segment2.read(1_u64).data.not_nil!.value.should eq "persist"
    segment2.count.should eq 1

    segment2.close
    FileUtils.rm_rf(dir)
  end

  it "recovers correctly from a pre-allocated file that was never appended to (clean close)" do
    dir = File.tempname("seg_empty_recover")
    Dir.mkdir_p(dir)
    seg = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64, capacity: 1024_i64)
    seg.close

    seg2 = Raft::Log::Segment(TestData).open(dir, first_index: 1_u64)
    seg2.count.should eq 0_u32
    seg2.last_index.should eq 0_u64
    seg2.close
    FileUtils.rm_rf(dir)
  end

  it "recovers correctly from a crash-without-close (full capacity, zero-filled tail)" do
    dir = File.tempname("seg_crash_recover")
    Dir.mkdir_p(dir)
    # Manually create a capacity-sized zero-filled file as if a process crashed before close.
    path = File.join(dir, "%016d.log" % 1_u64)
    File.open(path, "w+") do |f|
      f.truncate(2048_i64)  # 2 KB of zeros
    end

    seg = Raft::Log::Segment(TestData).open(dir, first_index: 1_u64)
    seg.count.should eq 0_u32
    seg.last_index.should eq 0_u64
    seg.close
    FileUtils.rm_rf(dir)
  end

  it "recovers correctly after a crash mid-append (valid entries + zero tail)" do
    dir = File.tempname("seg_partial_recover")
    Dir.mkdir_p(dir)
    seg = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64, capacity: 4096_i64)
    seg.append(Raft::LogEntry(TestData).new(term: 1_u64, index: 1_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("first")))
    seg.append(Raft::LogEntry(TestData).new(term: 1_u64, index: 2_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("second")))
    # Simulate crash: DO NOT call seg.close (which would truncate). Just open a new segment
    # pointing at the same file. The file is still at full 4096-byte capacity with a zero tail
    # beyond the 2 valid entries.

    seg2 = Raft::Log::Segment(TestData).open(dir, first_index: 1_u64)
    seg2.count.should eq 2_u32
    seg2.last_index.should eq 2_u64
    seg2.read(1_u64).data.not_nil!.value.should eq "first"
    seg2.read(2_u64).data.not_nil!.value.should eq "second"
    seg2.close
    FileUtils.rm_rf(dir)
  end

  it "expand_to re-extends capacity for continued appends" do
    dir = File.tempname("seg_expand")
    Dir.mkdir_p(dir)
    seg = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64, capacity: 1024_i64)
    seg.append(Raft::LogEntry(TestData).new(term: 1_u64, index: 1_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("a")))
    seg.close
    # File on disk is now shrunk to the size of one entry.

    seg2 = Raft::Log::Segment(TestData).open(dir, first_index: 1_u64)
    seg2.has_capacity_for?(100).should be_false  # capacity = logical_size after close
    seg2.expand_to(2048_i64)
    seg2.has_capacity_for?(100).should be_true   # now we can append again
    seg2.append(Raft::LogEntry(TestData).new(term: 1_u64, index: 2_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("b")))
    seg2.count.should eq 2_u32

    seg2.close
    FileUtils.rm_rf(dir)
  end

  it "byte_range_for returns disjoint ranges covering each entry" do
    dir = File.tempname("seg_byte_range")
    Dir.mkdir_p(dir)
    seg = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64, capacity: 4096_i64)
    seg.append(Raft::LogEntry(TestData).new(term: 1_u64, index: 1_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("aaa")))
    seg.append(Raft::LogEntry(TestData).new(term: 1_u64, index: 2_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("bb")))

    offset1, length1 = seg.byte_range_for(1_u64)
    offset2, length2 = seg.byte_range_for(2_u64)

    offset1.should eq 0_u64
    offset2.should eq (offset1 + length1)
    length1.should be > 0_u32
    length2.should be > 0_u32

    seg.close
    FileUtils.rm_rf(dir)
  end
end
