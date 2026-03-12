require "../spec_helper"

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

    log.get(1_u64).data.not_nil!.value.should eq "a"
    log.get(2_u64).data.not_nil!.value.should eq "b"
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
    config.max_segment_size = 50_u32 # fits ~1 entry, forces rotation

    log = Raft::Log(TestData).new(config)

    log.append(term: 1_u64, data: TestData.new("a"), entry_type: Raft::EntryType::Normal)
    log.append(term: 1_u64, data: TestData.new("b"), entry_type: Raft::EntryType::Normal)
    log.append(term: 2_u64, data: TestData.new("c"), entry_type: Raft::EntryType::Normal)

    log.get(1_u64).data.not_nil!.value.should eq "a"
    log.get(2_u64).data.not_nil!.value.should eq "b"
    log.get(3_u64).data.not_nil!.value.should eq "c"
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
    log.get(1_u64).data.not_nil!.value.should eq "a"

    log.close
    FileUtils.rm_rf(dir)
  end

  it "recovers entries from disk on restart" do
    dir = File.tempname("raft_log")
    Dir.mkdir_p(dir)
    config = Raft::Config.new
    config.data_dir = dir
    config.max_segment_size = 1024_u32

    log = Raft::Log(TestData).new(config)
    log.append(term: 1_u64, data: TestData.new("a"), entry_type: Raft::EntryType::Normal)
    log.append(term: 1_u64, data: TestData.new("b"), entry_type: Raft::EntryType::Normal)
    log.append(term: 2_u64, data: TestData.new("c"), entry_type: Raft::EntryType::Normal)
    log.close

    # Reopen — should recover all entries
    log2 = Raft::Log(TestData).new(config)
    log2.last_index.should eq 3_u64
    log2.last_term.should eq 2_u64
    log2.get(1_u64).data.not_nil!.value.should eq "a"
    log2.get(2_u64).data.not_nil!.value.should eq "b"
    log2.get(3_u64).data.not_nil!.value.should eq "c"
    log2.close
    FileUtils.rm_rf(dir)
  end

  it "recovers across multiple segments" do
    dir = File.tempname("raft_log")
    Dir.mkdir_p(dir)
    config = Raft::Config.new
    config.data_dir = dir
    config.max_segment_size = 50_u32 # fits ~1 entry, forces rotation

    log = Raft::Log(TestData).new(config)
    log.append(term: 1_u64, data: TestData.new("a"), entry_type: Raft::EntryType::Normal)
    log.append(term: 1_u64, data: TestData.new("b"), entry_type: Raft::EntryType::Normal)
    log.append(term: 2_u64, data: TestData.new("c"), entry_type: Raft::EntryType::Normal)
    segment_count = log.segment_count
    segment_count.should be > 1
    log.close

    # Reopen — should recover all segments
    log2 = Raft::Log(TestData).new(config)
    log2.segment_count.should eq segment_count
    log2.last_index.should eq 3_u64
    log2.last_term.should eq 2_u64
    log2.get(1_u64).data.not_nil!.value.should eq "a"
    log2.get(3_u64).data.not_nil!.value.should eq "c"
    log2.close
    FileUtils.rm_rf(dir)
  end

  it "can append after recovery" do
    dir = File.tempname("raft_log")
    Dir.mkdir_p(dir)
    config = Raft::Config.new
    config.data_dir = dir
    config.max_segment_size = 1024_u32

    log = Raft::Log(TestData).new(config)
    log.append(term: 1_u64, data: TestData.new("a"), entry_type: Raft::EntryType::Normal)
    log.close

    log2 = Raft::Log(TestData).new(config)
    log2.append(term: 2_u64, data: TestData.new("b"), entry_type: Raft::EntryType::Normal)
    log2.last_index.should eq 2_u64
    log2.get(2_u64).data.not_nil!.value.should eq "b"
    log2.close
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
