require "http/server/handler"
require "json"

module Raft
  module HTTP
    class Handler(T)
      include ::HTTP::Handler

      @node : Node(T)

      def initialize(@node : Node(T))
      end

      def call(context : ::HTTP::Server::Context)
        case {context.request.method, context.request.path}
        when {"GET", "/raft/status"}
          handle_status(context)
        when {"GET", "/raft/log"}
          handle_log(context)
        when {"GET", "/raft/metrics"}
          handle_metrics(context)
        when {"POST", "/raft/admin/pause"}
          @node.pause
          json_response(context, 200, {"status" => "paused"})
        when {"POST", "/raft/admin/resume"}
          @node.resume
          json_response(context, 200, {"status" => "resumed"})
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
            j.field "leader_id", leader_id
            j.field "commit_index", @node.commit_index
            j.field "last_log_index", @node.log.last_index
            j.field "peers" do
              j.array do
                @node.peers.each { |p| j.number(p) }
              end
            end
            j.field "paused", @node.paused
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
          metrics.set_gauge("raft_node_peers", @node.peers.size.to_i64)

          context.response.content_type = "text/plain; version=0.0.4"
          context.response.status_code = 200
          context.response.print metrics.to_prometheus
        else
          context.response.status_code = 503
          context.response.print "Metrics not configured"
        end
      end

      private def json_response(context, status : Int32, data : Hash)
        context.response.content_type = "application/json"
        context.response.status_code = status
        context.response.print data.to_json
      end
    end
  end
end
