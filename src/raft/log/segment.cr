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

      def self.open(dir : String, first_index : UInt64, max_size : UInt32) : self
        segment = new(dir, first_index, max_size, _recover: true)
        segment
      end

      protected def initialize(@dir : String, @first_index : UInt64, @max_size : UInt32, *, _recover : Bool)
        @last_index = @first_index - 1
        path = File.join(@dir, segment_filename)
        @file = File.open(path, "r+b")
        recover
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

      protected def recover
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
