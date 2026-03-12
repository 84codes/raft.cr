require "file_utils"
require "../mfile"

module Raft
  class Log(T)
    class Segment(T)
      getter first_index : UInt64
      getter last_index : UInt64
      getter count : UInt32 = 0_u32

      @offsets : Array(UInt64) = [] of UInt64
      @file : MFile
      @dir : String

      def initialize(@dir : String, @first_index : UInt64, capacity : Int64)
        @last_index = @first_index - 1
        path = File.join(@dir, segment_filename)
        @file = MFile.new(path, capacity: capacity)
      end

      def self.open(dir : String, first_index : UInt64) : self
        segment = new(dir, first_index, _recover: true)
        segment
      end

      protected def initialize(@dir : String, @first_index : UInt64, *, _recover : Bool)
        @last_index = @first_index - 1
        path = File.join(@dir, segment_filename)
        @file = MFile.new(path)
        recover
      end

      def has_capacity_for?(bytesize : Int) : Bool
        @file.size + bytesize <= @file.capacity
      end

      def append(entry : LogEntry(T))
        @offsets << @file.size.to_u64
        entry.to_io(@file)
        @last_index = entry.index
        @count += 1
      end

      def read(index : UInt64) : LogEntry(T)
        offset_idx = (index - @first_index).to_i32
        raise IndexError.new("Index #{index} out of range") if offset_idx < 0 || offset_idx >= @offsets.size
        @file.seek(@offsets[offset_idx])
        LogEntry(T).from_io(@file)
      end

      def close
        @file.close
      end

      protected def recover
        file_size = @file.size
        @file.seek(0)

        @offsets.clear
        @count = 0_u32
        @last_index = @first_index - 1

        while @file.pos < file_size
          offset = @file.pos.to_u64
          @offsets << offset
          entry = LogEntry(T).from_io(@file)
          @last_index = entry.index
          @count += 1
        end

        # Set size to actual valid data (matters if process crashed —
        # file may be capacity-sized with garbage at the tail)
        @file.resize(@file.pos)
      end

      private def segment_filename : String
        "%016d.log" % @first_index
      end
    end
  end
end
