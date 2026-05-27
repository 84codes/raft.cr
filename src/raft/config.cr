module Raft
  alias NodeID = UInt64

  class Config
    property tick_interval : Time::Span = 50.milliseconds
    property heartbeat_ticks : UInt32 = 2
    property election_timeout_min_ticks : UInt32 = 10
    property election_timeout_max_ticks : UInt32 = 20
    property max_segment_size : UInt32 = 64_u32 * 1024_u32 * 1024_u32 # 64 MB
    property data_dir : String = "data"
    property snapshot_chunk_size : UInt32 = 1024_u32 * 1024_u32                # 1 MB
    property max_append_entries_size : UInt32 = 1_u32 * 1024_u32 * 1024_u32    # 1 MB
    property max_message_payload_bytes : UInt32 = 64_u32 * 1024_u32 * 1024_u32 # 64 MB
    property snapshot_interval_entries : UInt64 = 1000_u64
    # When set, Node seeds its election-timeout RNG from this value. When nil
    # (the default), Node uses Random.new — OS entropy, non-reproducible.
    # Tests that need bit-for-bit reproducibility (deterministic-simulation
    # arms) should set a per-node seed.
    property random_seed : UInt64? = nil

    def initialize
    end
  end
end
