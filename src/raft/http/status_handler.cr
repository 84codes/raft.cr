require "http/server/handler"
require "json"
require "../node"
require "../transport/tcp_transport"

module Raft
  module HTTP
    # Read-only HTTP handler exposing node status, log metadata, and Prometheus
    # metrics. Safe to mount on an unauthenticated metrics port — does not
    # mutate cluster state.
    #
    # Routes:
    # - `GET /raft/status` — JSON snapshot of node role, term, leader, peers
    # - `GET /raft/log` — JSON snapshot of log/commit indexes
    # - `GET /raft/metrics` — Prometheus text format
    #
    # Anything else (including `POST /raft/admin/*`) falls through to the next
    # handler in the chain via `call_next`. Pair with `AdminHandler` to expose
    # the mutating surface on a separate (typically authenticated) endpoint.
    class StatusHandler(T)
      include ::HTTP::Handler

      @node : Node(T)
      @transport : TCPTransport?
      @raft_address : String?

      def initialize(@node : Node(T), @transport : TCPTransport? = nil, @raft_address : String? = nil)
      end

      def call(context : ::HTTP::Server::Context)
        method = context.request.method
        path = context.request.path

        case {method, path}
        when {"GET", "/raft/status"}
          handle_status(context)
        when {"GET", "/raft/log"}
          handle_log(context)
        when {"GET", "/raft/metrics"}
          handle_metrics(context)
        else
          call_next(context)
        end
      end

      private def handle_status(context)
        leader_id = @node.leader_id
        json = JSON.build do |j|
          j.object do
            j.field "id", @node.id
            j.field "role", @node.role.to_s.downcase
            j.field "term", @node.current_term
            j.field "raft_address", @raft_address if @raft_address
            j.field "leader_id", leader_id
            j.field "commit_index", @node.commit_index
            j.field "last_log_index", @node.log.last_index
            j.field "peers" do
              j.array do
                @node.peers.each do |p|
                  j.object do
                    j.field "id", p.id
                    j.field "role", p.role.to_s.downcase
                    j.field "address", p.address unless p.address.empty?
                  end
                end
              end
            end
            {% if flag?(:raft_debug) %}
              j.field "paused", @node.paused
            {% end %}
          end
        end
        context.response.content_type = "application/json"
        context.response.status_code = 200
        context.response.print json
      end

      private def handle_log(context)
        json = JSON.build do |j|
          j.object do
            j.field "last_index", @node.log.last_index
            j.field "last_term", @node.log.last_term
            j.field "segment_count", @node.log.segment_count
            j.field "commit_index", @node.commit_index
          end
        end
        context.response.content_type = "application/json"
        context.response.status_code = 200
        context.response.print json
      end

      private def handle_metrics(context)
        if metrics = @node.metrics
          # Update gauges from current node state
          metrics.set_gauge("raft_node_role", @node.role.value.to_i64)
          metrics.set_gauge("raft_node_term", @node.current_term.to_i64)
          metrics.set_gauge("raft_node_commit_index", @node.commit_index.to_i64)
          metrics.set_gauge("raft_node_last_log_index", @node.log.last_index.to_i64)
          metrics.set_gauge("raft_node_first_log_index", @node.log.first_index.to_i64)
          metrics.set_gauge("raft_node_segment_count", @node.log.segment_count.to_i64)
          metrics.set_gauge("raft_node_snapshot_index", @node.snapshot_index.to_i64)
          metrics.set_gauge("raft_node_snapshot_size_bytes", @node.snapshot_size_bytes)
          metrics.set_gauge("raft_node_peers", @node.peers.size.to_i64)
          metrics.set_gauge("raft_node_is_leader", @node.role.leader? ? 1_i64 : 0_i64)
          metrics.set_gauge("raft_node_leader_id", (@node.leader_id || 0_u64).to_i64)

          context.response.content_type = "text/plain; version=0.0.4"
          context.response.status_code = 200
          context.response.print metrics.to_prometheus
          @transport.try(&.to_prometheus(context.response))
        else
          context.response.status_code = 503
          context.response.print "Metrics not configured"
        end
      end
    end
  end
end
