require "./config"
require "./message"
require "./peer"
require "./metrics"

module Raft
  # The read-only surface a status/metrics endpoint needs from a node.
  # T-free by construction — every method here is independent of the
  # state-machine command type, which lets `HTTP::StatusHandler` be a
  # concrete (non-generic) class.
  #
  # Included by `Raft::Node(T)`. The threading contract still applies:
  # these read mutable node state without locks, so callers outside the
  # node's owning fiber must arrange their own safety (e.g. run the HTTP
  # server in the same execution context).
  module StatusSource
    abstract def id : NodeID
    abstract def role : Role
    abstract def current_term : UInt64
    abstract def leader_id : NodeID?
    abstract def commit_index : UInt64
    abstract def peers : Array(Peer)
    abstract def metrics : Metrics?
    abstract def snapshot_index : UInt64
    abstract def snapshot_size_bytes : Int64
    abstract def last_log_index : UInt64
    abstract def last_log_term : UInt64
    abstract def first_log_index : UInt64
    abstract def segment_count : Int32

    {% if flag?(:raft_debug) %}
      abstract def paused : Bool
    {% end %}
  end
end
