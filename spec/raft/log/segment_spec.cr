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
end
