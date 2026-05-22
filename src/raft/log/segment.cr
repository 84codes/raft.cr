require "file_utils"
require "../file_write_at"

module Raft
  class Log(T)
    class Segment(T)
      getter first_index : UInt64
      getter last_index : UInt64
      getter count : UInt32 = 0_u32

      @offsets : Array(UInt64) = [] of UInt64
      @file : File
      @logical_size : Int64 = 0_i64
      @capacity : Int64
      @dir : String

      def initialize(@dir : String, @first_index : UInt64, capacity : Int64)
        @last_index = @first_index - 1
        @capacity = capacity
        path = File.join(@dir, segment_filename)
        @file = File.new(path, mode: "w+")
        @file.truncate(@capacity)
      end

      def self.open(dir : String, first_index : UInt64) : self
        segment = new(dir, first_index, _recover: true)
        segment
      end

      protected def initialize(@dir : String, @first_index : UInt64, *, _recover : Bool)
        @last_index = @first_index - 1
        path = File.join(@dir, segment_filename)
        @file = File.new(path, mode: "r+")
        @capacity = @file.size.to_i64
        recover
      end

      def has_capacity_for?(bytesize : Int) : Bool
        @logical_size + bytesize <= @capacity
      end

      def append(entry : LogEntry(T))
        @offsets << @logical_size.to_u64
        bytes_written = @file.write_at(@logical_size) do |io|
          entry.to_io(io)
        end
        @logical_size += bytes_written
        @last_index = entry.index
        @count += 1
      end

      def read(index : UInt64) : LogEntry(T)
        offset_idx = (index - @first_index).to_i32
        raise IndexError.new("Index #{index} out of range") if offset_idx < 0 || offset_idx >= @offsets.size
        start = @offsets[offset_idx]
        finish = offset_idx + 1 < @offsets.size ? @offsets[offset_idx + 1] : @logical_size.to_u64
        length = (finish - start).to_i64
        @file.read_at(start.to_i64, length) do |io|
          LogEntry(T).from_io(io)
        end
      end

      def truncate_to(index : UInt64)
        return if index >= @last_index
        return if index < @first_index
        offset_idx = (index - @first_index + 1).to_i32
        new_size = if offset_idx < @offsets.size
                     @offsets[offset_idx]
                   else
                     @logical_size.to_u64
                   end
        @logical_size = new_size.to_i64
        @offsets = @offsets[0...offset_idx]
        @count = offset_idx.to_u32
        @last_index = index
        # File stays at @capacity bytes physically; @logical_size tracks valid
        # data. Trailing bytes are garbage and never read since @offsets only
        # references valid ranges.
      end

      # Re-extends a recovered segment's file to a new (larger) capacity so it
      # can continue accepting appends. No-op if new_capacity <= @capacity. Used
      # by Log#recover_segments on the active segment to restore appendability
      # after a close+reopen cycle.
      def expand_to(new_capacity : Int64)
        return if new_capacity <= @capacity
        @file.truncate(new_capacity)
        @capacity = new_capacity
      end

      # Flush dirty page-cache pages for this segment to the device. Called
      # from the Raft commit path at durability boundaries.
      def sync
        @file.fsync
      end

      # Underlying file descriptor. Exposed for future zero-copy senders
      # (sendfile/splice/io_uring).
      def fd : Int32
        @file.fd
      end

      # Byte range within this segment file occupied by the entry at `index`.
      # Returns {offset, length}. Future zero-copy senders use this to compute
      # sendfile arguments.
      def byte_range_for(index : UInt64) : {UInt64, UInt32}
        offset_idx = (index - @first_index).to_i32
        raise IndexError.new("Index #{index} out of range") if offset_idx < 0 || offset_idx >= @offsets.size
        start = @offsets[offset_idx]
        finish = if offset_idx + 1 < @offsets.size
                   @offsets[offset_idx + 1]
                 else
                   @logical_size.to_u64
                 end
        {start, (finish - start).to_u32}
      end

      def close
        # Truncate the file to the valid-data size so that on the next open
        # File#size returns exactly @logical_size, giving recovery a clean
        # sentinel for recovery.
        @file.truncate(@logical_size)
        @file.close
      end

      protected def recover
        @offsets.clear
        @count = 0_u32
        @last_index = @first_index - 1
        @logical_size = 0_i64
        cursor = 0_i64

        while cursor < @capacity
          offset = cursor.to_u64
          begin
            # Read forward via the file's position-based IO (one-shot during
            # recovery is fine; we're not concurrent here).
            @file.seek(cursor)
            entry = LogEntry(T).from_io(@file)
            # Heuristic stop: real entries always have term >= 1 AND index >= 1
            # (bootstrap noop is the first real entry at term=1 index=1). An all-zero
            # parse means we hit the pre-allocated zero tail of a crashed segment.
            break if entry.term == 0_u64 && entry.index == 0_u64
            @offsets << offset
            @last_index = entry.index
            @count += 1
            cursor = @file.tell.to_i64
            @logical_size = cursor
          rescue ex
            # Partial trailing entry, or non-zero garbage tail. Stop —
            # future appends overwrite from @logical_size onward.
            ::Log.warn { "Truncating partial entry at offset #{offset} in segment starting at index #{@first_index}: #{ex.message}" }
            break
          end
        end
      end

      private def segment_filename : String
        "%016d.log" % @first_index
      end
    end
  end
end
