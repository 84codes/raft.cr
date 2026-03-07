module Raft
  enum EntryType : UInt8
    Normal        = 0
    Configuration = 1
  end

  enum MessageType : UInt8
    AppendEntries           = 0
    AppendEntriesResponse   = 1
    RequestVote             = 2
    RequestVoteResponse     = 3
    InstallSnapshot         = 4
    InstallSnapshotResponse = 5
  end

  enum Role
    Follower
    Candidate
    Leader
  end
end
