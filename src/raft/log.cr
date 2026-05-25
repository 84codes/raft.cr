module Raft
  class Log(T)
    getter last_index : UInt64 = 0_u64
    getter last_term : UInt64 = 0_u64

    @segments = Array(Segment(T)).new
    @config : Config

    def initialize(@config : Config)
      Dir.mkdir_p(@config.data_dir)
      recover_segments
    end

    def sync
      @segments.last.sync unless @segments.empty?
    end

    def append(term : UInt64, data : T? = nil, entry_type : EntryType = EntryType::Normal, config_data : Bytes = Bytes.new(0)) : LogEntry(T)
      @last_index += 1
      @last_term = term
      entry = LogEntry(T).new(term: term, index: @last_index, entry_type: entry_type, data: data, config_data: config_data)

      if current_segment.size > 0 && current_segment.size + entry.bytesize > @config.max_segment_size
        new_segment(@last_index)
      end

      current_segment.append(entry)
      entry
    end

    def get(index : UInt64) : LogEntry(T)
      segment = segment_for(index)
      segment.read(index)
    end

    def term_at(index : UInt64) : UInt64
      get(index).term
    end

    # Resolve `index` to {segment_fd, byte_offset, byte_length} for zero-copy
    # senders (e.g., sendfile/splice/io_uring). Returns nil if the index has
    # been compacted away or is past EOF.
    def byte_range_for(index : UInt64) : {Int32, UInt64, UInt32}?
      return nil if index < first_index
      return nil if index > @last_index
      seg = segment_for(index)
      offset, length = seg.byte_range_for(index)
      {seg.fd, offset, length}
    end

    def truncate_after(index : UInt64)
      # Remove segments entirely past the target index
      while @segments.size > 1 && @segments.last.first_index > index
        seg = @segments.pop
        path = File.join(@config.data_dir, "%016d.log" % seg.first_index)
        seg.close
        File.delete(path) if File.exists?(path)
      end

      # Truncate the last segment in place
      if @segments.last.last_index > index && index >= @segments.last.first_index
        @segments.last.truncate_to(index)
      end

      @last_index = index
      @last_term = index > 0 ? get(index).term : 0_u64
    end

    def segment_count : Int32
      @segments.size
    end

    def first_index : UInt64
      return 0_u64 if @segments.empty?
      @segments.first.first_index
    end

    def truncate_before(index : UInt64)
      # Drop segments whose entire index range is <= the given index.
      # The segment containing `index` itself is kept (we don't split segments).
      while @segments.size > 1 && @segments.first.last_index <= index
        seg = @segments.shift
        path = File.join(@config.data_dir, "%016d.log" % seg.first_index)
        seg.close
        File.delete(path) if File.exists?(path)
      end
    end

    def reset
      @segments.each(&.close)
      @segments.clear
      # Delete all segment files and metadata
      Dir.glob(File.join(@config.data_dir, "*.log")) { |f| File.delete(f) }
      @last_index = 0_u64
      @last_term = 0_u64
      new_segment(1_u64)
    end

    # Reset the log to a specific index, as after installing a snapshot.
    # After this call, @last_index == index and the next append produces index + 1.
    def reset_to(index : UInt64)
      @segments.each(&.close)
      @segments.clear
      Dir.glob(File.join(@config.data_dir, "*.log")) { |f| File.delete(f) }
      @last_index = index
      @last_term = 0_u64
      new_segment(index + 1_u64)
    end

    def close
      @segments.each(&.close)
    end

    private def current_segment : Segment(T)
      @segments.last
    end

    private def new_segment(first_index : UInt64)
      @segments << Segment(T).new(@config.data_dir, first_index: first_index)
    end

    private def segment_for(index : UInt64) : Segment(T)
      @segments.reverse_each do |seg|
        return seg if index >= seg.first_index && index <= seg.last_index
      end
      raise IndexError.new("No segment contains index #{index}")
    end

    private def recover_segments
      files = Dir.glob(File.join(@config.data_dir, "*.log")).sort
      if files.empty?
        new_segment(1_u64)
      else
        files.each do |path|
          # Skip empty segment files (e.g. created but never written to)
          next if File.size(path) == 0
          filename = File.basename(path, ".log")
          first_index = filename.to_u64
          seg = Segment(T).open(@config.data_dir, first_index: first_index)
          @segments << seg
        end
        if @segments.empty?
          new_segment(1_u64)
        else
          last_seg = @segments.last
          @last_index = last_seg.last_index
          @last_term = last_seg.count > 0 ? get(@last_index).term : 0_u64
        end
      end
    end
  end
end
