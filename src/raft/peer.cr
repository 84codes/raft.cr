module Raft
  struct Peer
    enum Role : UInt8
      Voter   = 0
      Learner = 1
    end

    getter id : NodeID
    property role : Role
    getter address : String

    def initialize(@id : NodeID, @role : Role = Role::Voter, @address : String = "")
    end

    def voter? : Bool
      @role.voter?
    end

    def learner? : Bool
      @role.learner?
    end

    def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian)
      io.write_bytes(@id, format)
      io.write_bytes(@role.value, format)
      io.write_bytes(@address.bytesize.to_u16, format)
      io.write(@address.to_slice)
    end

    def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
      id = io.read_bytes(UInt64, format)
      role = Role.new(io.read_bytes(UInt8, format))
      addr_len = io.read_bytes(UInt16, format)
      if addr_len > 0
        buf = Bytes.new(addr_len)
        io.read_fully(buf)
        address = String.new(buf)
      else
        address = ""
      end
      new(id, role, address)
    end
  end
end
