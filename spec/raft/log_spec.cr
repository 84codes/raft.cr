require "../spec_helper"
require "file_utils"
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
    config.max_segment_size = 30_u32 # tiny — forces rotation after each entry

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
