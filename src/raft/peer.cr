module Raft
  struct Peer
    enum Role : UInt8
      Voter   = 0
      Learner = 1
    end

    getter id : NodeID
    property role : Role

    def initialize(@id : NodeID, @role : Role = Role::Voter)
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
    end

    def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
      id = io.read_bytes(UInt64, format)
      role = Role.new(io.read_bytes(UInt8, format))
      new(id, role)
    end
  end
end
