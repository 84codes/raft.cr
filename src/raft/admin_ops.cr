require "./config"

module Raft
  # The mutating cluster-administration surface an admin endpoint needs
  # from a node. T-free by construction — none of these depend on the
  # state-machine command type, which lets `HTTP::AdminHandler` be a
  # concrete (non-generic) class.
  #
  # Included by `Raft::Node(T)`. The threading contract still applies:
  # these mutate node state without locks, so callers outside the node's
  # owning fiber must arrange their own safety (e.g. run the HTTP server
  # in the same execution context, or marshal calls onto the node's fiber).
  module AdminOps
    abstract def bootstrap : Bool
    abstract def add_server(node_id : NodeID, address : String = "") : Bool
    abstract def remove_server(node_id : NodeID) : Bool
    abstract def promote_learner(node_id : NodeID) : Bool
    abstract def transfer_leadership(to target : NodeID) : Bool

    {% if flag?(:raft_debug) %}
      abstract def pause
      abstract def resume
      abstract def partition
      abstract def heal
      abstract def reset
    {% end %}
  end
end
