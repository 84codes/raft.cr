require "../../spec_helper"

describe Raft::Log::Segment do
  it "appends and reads back entries" do
    dir = File.tempname("raft_segment")
    Dir.mkdir_p(dir)
    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64)

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

  it "truncates in place and reads remaining entries" do
    dir = File.tempname("raft_segment")
    Dir.mkdir_p(dir)
    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64)

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
    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64)

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
    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64)

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

    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64)
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
    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64)
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

    segment = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64)
    entry = Raft::LogEntry(TestData).new(term: 1_u64, index: 1_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("persist"))
    segment.append(entry)
    segment.close

    segment2 = Raft::Log::Segment(TestData).open(dir, first_index: 1_u64)
    segment2.read(1_u64).data.not_nil!.value.should eq "persist"
    segment2.count.should eq 1

    segment2.close
    FileUtils.rm_rf(dir)
  end

  it "recovers correctly from a partially-written file (truncates parse-failing tail)" do
    dir = File.tempname("seg_partial_recover")
    Dir.mkdir_p(dir)
    seg = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64)
    seg.append(Raft::LogEntry(TestData).new(term: 1_u64, index: 1_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("first")))
    seg.append(Raft::LogEntry(TestData).new(term: 1_u64, index: 2_u64, entry_type: Raft::EntryType::Normal, data: TestData.new("second")))
    # Simulate crash: flush to kernel (as sync does at the Raft boundary) so
    # the two valid entries reach the file, then append garbage without close
    # to represent a partial trailing write at crash time.
    seg.sync
    path = Dir.glob(File.join(dir, "*.log")).first
    File.open(path, "ab") do |f|
      f.write(Bytes[0xDE, 0xAD, 0xBE, 0xEF])
    end

    seg2 = Raft::Log::Segment(TestData).open(dir, first_index: 1_u64)
    seg2.count.should eq 2_u32
    seg2.last_index.should eq 2_u64
    seg2.read(1_u64).data.not_nil!.value.should eq "first"
    seg2.read(2_u64).data.not_nil!.value.should eq "second"
    seg2.close
    FileUtils.rm_rf(dir)
  end

  it "byte_range_for returns disjoint ranges covering each entry" do
    dir = File.tempname("seg_byte_range")
    Dir.mkdir_p(dir)
    seg = Raft::Log::Segment(TestData).new(dir, first_index: 1_u64)
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
