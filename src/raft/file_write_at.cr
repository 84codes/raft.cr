# POSIX pwrite(2) extension for Crystal's File class. Mirrors stdlib's
# File#read_at, providing positional writes that do not affect the file's
# read/write position. Linux + macOS only; not portable to Windows.

lib LibC
  fun pwrite(fd : Int, buf : Void*, count : SizeT, offset : OffT) : SSizeT
end

class File
  # Writes the contents of `slice` to the file starting at `offset`. Returns
  # the number of bytes written. Does not move the file's read/write position.
  # Retries on EINTR; loops on short writes until the slice is fully written.
  #
  # POSIX-only. Works on Linux and macOS; not implemented for Windows.
  def write_at(offset : Int64, slice : Bytes) : Int32
    total_written = 0
    remaining = slice.size
    while remaining > 0
      bytes = LibC.pwrite(fd, slice + total_written, remaining.to_u64, offset + total_written)
      if bytes == -1
        next if Errno.value == Errno::EINTR
        raise IO::Error.from_errno("write_at failed")
      end
      total_written += bytes
      remaining -= bytes
    end
    total_written
  end

  # Block-yielding form: yields an IO::Memory; on block return, writes the
  # accumulated bytes in a single pwrite call. Useful for callers that need
  # to serialize via `to_io` (e.g., LogEntry).
  def write_at(offset : Int64, &block : IO ->) : Int32
    buf = IO::Memory.new
    yield buf
    write_at(offset, buf.to_slice)
  end
end
