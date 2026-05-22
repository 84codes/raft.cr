require "../spec_helper"
require "../../src/raft/file_write_at"

describe "File#write_at" do
  it "writes bytes at the given offset" do
    path = File.tempname("write_at_basic")
    File.open(path, "w+") do |f|
      f.truncate(64)
      bytes_written = f.write_at(10_i64, "hello".to_slice)
      bytes_written.should eq 5

      f.seek(10)
      buf = Bytes.new(5)
      f.read_fully(buf)
      String.new(buf).should eq "hello"
    end
    File.delete(path)
  end

  it "does not move the file position" do
    path = File.tempname("write_at_pos")
    File.open(path, "w+") do |f|
      f.truncate(64)
      f.seek(20)
      f.write_at(50_i64, "X".to_slice)
      f.tell.should eq 20
    end
    File.delete(path)
  end

  it "writes via the block-yielding IO form" do
    path = File.tempname("write_at_block")
    File.open(path, "w+") do |f|
      f.truncate(64)
      bytes_written = f.write_at(0_i64) do |io|
        io.write("abc".to_slice)
        io.write_byte(0_u8)
        io.write("xyz".to_slice)
      end
      bytes_written.should eq 7

      f.seek(0)
      buf = Bytes.new(7)
      f.read_fully(buf)
      buf.should eq Bytes[0x61, 0x62, 0x63, 0x00, 0x78, 0x79, 0x7a]
    end
    File.delete(path)
  end

  it "writes at non-zero offset without affecting earlier bytes" do
    path = File.tempname("write_at_island")
    File.open(path, "w+") do |f|
      f.truncate(64)
      f.write_at(0_i64, "AAAA".to_slice)
      f.write_at(20_i64, "BBBB".to_slice)

      f.seek(0)
      buf = Bytes.new(24)
      f.read_fully(buf)
      String.new(buf[0, 4]).should eq "AAAA"
      String.new(buf[20, 4]).should eq "BBBB"
      buf[4, 16].all? { |b| b == 0_u8 }.should be_true
    end
    File.delete(path)
  end
end
