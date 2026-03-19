require "http/server/handler"
require "json"

module Raft
  module HTTP
    class Handler(T)
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
          if method == "POST" && path.starts_with?("/raft/admin/")
            handle_admin(context, path)
            return
          end
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
          metrics.set_gauge("raft_node_peers", @node.peers.size.to_i64)

          context.response.content_type = "text/plain; version=0.0.4"
          context.response.status_code = 200
          context.response.print metrics.to_prometheus
          @transport.try(&.to_prometheus(context.response))
        else
          context.response.status_code = 503
          context.response.print "Metrics not configured"
        end
      end

      private def handle_admin(context, path)
        handle_admin_inner(context, path)
      rescue ex : JSON::ParseException | KeyError | ArgumentError
        json_response(context, 400, {"error" => "invalid request: #{ex.message}"})
      end

      private def handle_admin_inner(context, path)
        case path
        when "/raft/admin/bootstrap"
          if @node.bootstrap
            json_response(context, 200, {"status" => "bootstrapped"})
          else
            json_response(context, 400, {"error" => "failed to bootstrap (node may already have peers)"})
          end
        when "/raft/admin/register_peer"
          if transport = @transport
            body = context.request.body.try(&.gets_to_end)
            if body
              data = JSON.parse(body)
              id = data["id"].as_i64.to_u64
              host = data["host"].as_s
              port = data["port"].as_i
              transport.register_peer(id, host, port)
              json_response(context, 200, {"status" => "registered", "id" => id.to_s})
            else
              json_response(context, 400, {"error" => "missing body"})
            end
          else
            json_response(context, 503, {"error" => "no transport configured"})
          end
        else
          if path.starts_with?("/raft/admin/add_server/")
            node_id = path.split("/").last.to_u64
            address = ""
            if body = context.request.body.try(&.gets_to_end)
              if !body.empty?
                data = JSON.parse(body)
                address = data["address"]?.try(&.as_s) || ""
              end
            end
            if @node.add_server(node_id, address)
              json_response(context, 200, {"status" => "added", "node_id" => node_id.to_s})
            else
              json_response(context, 400, {"error" => "failed to add server"})
            end
          elsif path.starts_with?("/raft/admin/remove_server/")
            node_id = path.split("/").last.to_u64
            if @node.remove_server(node_id)
              json_response(context, 200, {"status" => "removed", "node_id" => node_id.to_s})
            else
              json_response(context, 400, {"error" => "failed to remove server"})
            end
          elsif path.starts_with?("/raft/admin/promote_learner/")
            node_id = path.split("/").last.to_u64
            if @node.promote_learner(node_id)
              json_response(context, 200, {"status" => "promoted", "node_id" => node_id.to_s})
            else
              json_response(context, 400, {"error" => "failed to promote learner"})
            end
          elsif path.starts_with?("/raft/admin/transfer_leadership/")
            node_id = path.split("/").last.to_u64
            if @node.transfer_leadership(to: node_id)
              json_response(context, 200, {"status" => "transferring", "target" => node_id.to_s})
            else
              json_response(context, 400, {"error" => "failed to transfer leadership"})
            end
          else
            {% if flag?(:raft_debug) %}
              handle_debug_admin(context, path)
            {% else %}
              context.response.status_code = 404
              context.response.print "Unknown admin action"
            {% end %}
          end
        end
      end

      {% if flag?(:raft_debug) %}
        private def handle_debug_admin(context, path)
          case path
          when "/raft/admin/pause"
            @node.pause
            json_response(context, 200, {"status" => "paused"})
          when "/raft/admin/resume"
            @node.resume
            json_response(context, 200, {"status" => "resumed"})
          when "/raft/admin/partition"
            @node.partition
            json_response(context, 200, {"status" => "partitioned"})
          when "/raft/admin/heal"
            @node.heal
            json_response(context, 200, {"status" => "healed"})
          when "/raft/admin/reset"
            @node.reset
            json_response(context, 200, {"status" => "reset"})
          else
            context.response.status_code = 404
            context.response.print "Unknown admin action"
          end
        end
      {% end %}

      private def json_response(context, status : Int32, data : Hash)
        context.response.content_type = "application/json"
        context.response.status_code = status
        context.response.print data.to_json
      end
    end
  end
end
