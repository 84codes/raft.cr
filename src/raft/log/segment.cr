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
        @offsets << @size.to_u64
        entry.to_io(@file)
        # Flush is deferred to sync (at Raft durability boundary) so that batched
        # AppendEntries replication issues O(1) write(2) syscalls per batch
        # instead of O(N). IO::Buffered keeps the in-flight bytes in user space
        # until sync flushes them all at once.
        @size += entry.bytesize
        @last_index = entry.index
        @count += 1
      end

      def read(index : UInt64) : LogEntry(T)
        offset_idx = (index - @first_index).to_i32
        raise IndexError.new("Index #{index} out of range") if offset_idx < 0 || offset_idx >= @offsets.size
        start = @offsets[offset_idx]
        finish = offset_idx + 1 < @offsets.size ? @offsets[offset_idx + 1] : @size.to_u64
        length = (finish - start).to_i64
        # read_at uses pread(2) which bypasses IO::Buffered's user-space buffer.
        # Flush any pending writes to the kernel first so pread sees them.
        # On the normal Raft path (sync already called) this is a no-op.
        @file.flush
        @file.read_at(start.to_i64, length) do |io|
          LogEntry(T).from_io(io)
        end
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
        # Push any buffered writes to the kernel before changing EOF; otherwise
        # the next flush would append past the truncated EOF.
        @file.flush
        @file.truncate(new_size)
        @offsets = @offsets[0...offset_idx]
        @count = offset_idx.to_u32
        @last_index = index
        @size = new_size
      end

      # Push buffered writes to the kernel page cache, then flush page cache to
      # the device. Called at Raft durability boundaries (see Node#propose,
      # Node#handle_append_entries, etc.). Two syscalls (write + fsync) per call.
      def sync
        @file.flush
        @file.fsync
      end

      # Underlying file descriptor. Exposed for future zero-copy senders
      # (sendfile/splice/io_uring).
      def fd : Int32
        @file.fd
      end

      # Byte range within this segment file occupied by the entry at `index`.
      # Returns {offset, length}.
      def byte_range_for(index : UInt64) : {UInt64, UInt32}
        offset_idx = (index - @first_index).to_i32
        raise IndexError.new("Index #{index} out of range") if offset_idx < 0 || offset_idx >= @offsets.size
        start = @offsets[offset_idx]
        finish = if offset_idx + 1 < @offsets.size
                   @offsets[offset_idx + 1]
                 else
                   @size.to_u64
                 end
        {start, (finish - start).to_u32}
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
            # Partial trailing entry from a crash — truncate the file back to the
            # last fully-valid offset. Fsync once since this is a one-time
            # recovery operation.
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
