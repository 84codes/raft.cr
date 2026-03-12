module Raft
  class Log(T)
    getter last_index : UInt64 = 0_u64
    getter last_term : UInt64 = 0_u64

    @segments : Array(Segment(T)) = [] of Segment(T)
    @config : Config

    def initialize(@config : Config)
      Dir.mkdir_p(@config.data_dir)
      recover_segments
    end

    def append(term : UInt64, data : T? = nil, entry_type : EntryType = EntryType::Normal) : LogEntry(T)
      @last_index += 1
      @last_term = term
      entry = LogEntry(T).new(term: term, index: @last_index, entry_type: entry_type, data: data)

      # Serialize to get the byte size, then check if it fits
      io = IO::Memory.new
      entry.to_io(io)
      bytesize = io.pos

      unless current_segment.has_capacity_for?(bytesize)
        new_segment(@last_index, bytesize)
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

    def truncate_after(index : UInt64)
      # Remove segments that start entirely after index
      while @segments.size > 1 && @segments.last.first_index > index
        seg = @segments.pop
        seg.close
      end

      # Rebuild the last segment up to index if needed
      seg = @segments.last
      if seg.last_index > index
        entries = [] of LogEntry(T)
        (seg.first_index..index).each do |i|
          entries << seg.read(i)
        end
        seg.close

        # Delete old segment file first
        old_path = File.join(@config.data_dir, "%016d.log" % seg.first_index)
        File.delete(old_path) if File.exists?(old_path)

        new_seg = Segment(T).new(@config.data_dir, first_index: seg.first_index, capacity: @config.max_segment_size.to_i64)
        entries.each { |e| new_seg.append(e) }
        @segments[-1] = new_seg
      end

      @last_index = index
      @last_term = index > 0 ? get(index).term : 0_u64
    end

    def segment_count : Int32
      @segments.size
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

    def close
      @segments.each(&.close)
    end

    private def current_segment : Segment(T)
      @segments.last
    end

    private def new_segment(first_index : UInt64, min_bytesize : Int = 0)
      capacity = Math.max(@config.max_segment_size.to_i64, min_bytesize.to_i64)
      @segments << Segment(T).new(@config.data_dir, first_index: first_index, capacity: capacity)
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
