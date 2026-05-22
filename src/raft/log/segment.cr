require "file_utils"
require "../log_entry"

module Raft
  class Log(T)
    class Segment(T)
      getter first_index : UInt64
      getter last_index : UInt64
      getter count : UInt32 = 0_u32
      getter size : Int64 = 0_i64

      @offsets : Array(UInt64) = [] of UInt64
      @file : ::File
      @dir : String

      def initialize(@dir : String, @first_index : UInt64)
        @last_index = @first_index - 1
        path = ::File.join(@dir, segment_filename)
        @file = ::File.new(path, "a+")
        recover
      end

      def self.open(dir : String, first_index : UInt64) : self
        new(dir, first_index)
      end

      def append(entry : LogEntry(T))
        offset = @size
        entry.to_io(@file)
        @file.flush
        @file.fsync
        @offsets << offset.to_u64
        @size += entry.bytesize
        @last_index = entry.index
        @count += 1
      end

      def read(index : UInt64) : LogEntry(T)
        offset_idx = (index - @first_index).to_i32
        raise IndexError.new("Index #{index} out of range") if offset_idx < 0 || offset_idx >= @offsets.size
        @file.seek(@offsets[offset_idx])
        LogEntry(T).from_io(@file)
      end

      def truncate_to(index : UInt64)
        return if index >= @last_index
        return if index < @first_index
        offset_idx = (index - @first_index + 1).to_i32
        new_size = if offset_idx < @offsets.size
                     @offsets[offset_idx].to_i64
                   else
                     @size
                   end
        @file.truncate(new_size)
        @file.fsync
        @offsets = @offsets[0...offset_idx]
        @count = offset_idx.to_u32
        @last_index = index
        @size = new_size
      end

      def close
        @file.close
      end

      protected def recover
        @offsets.clear
        @count = 0_u32
        @last_index = @first_index - 1
        file_size = @file.size
        @file.seek(0)
        valid_end = 0_i64

        while @file.pos < file_size
          offset = @file.pos.to_u64
          begin
            entry = LogEntry(T).from_io(@file)
            @offsets << offset
            @last_index = entry.index
            @count += 1
            valid_end = @file.pos.to_i64
          rescue ex
            # Torn write or corruption — drop everything past the last valid entry.
            ::Log.warn { "Truncating partial entry at offset #{offset} in segment starting at index #{@first_index}: #{ex.message}" }
            break
          end
        end

        if valid_end < file_size
          @file.truncate(valid_end)
          @file.fsync
        end
        @size = valid_end
      end

      private def segment_filename : String
        "%016d.log" % @first_index
      end
    end
  end
end
