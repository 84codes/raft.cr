require "http/server/handler"
require "json"
require "../admin_ops"
require "../transport/tcp_transport"

module Raft
  module HTTP
    # Mutating HTTP handler exposing cluster administration over `POST
    # /raft/admin/*`. Mount behind authentication — every route mutates
    # cluster state (membership, leadership, or under `-Draft_debug`, the
    # chaos primitives).
    #
    # Routes (all `POST`):
    # - `/raft/admin/bootstrap` — turn an empty node into a single-node cluster
    # - `/raft/admin/register_peer` — register a peer's address with the transport
    # - `/raft/admin/add_server/{id}` — add a learner with optional `{"address": "..."}` body
    # - `/raft/admin/remove_server/{id}` — remove a server from the configuration
    # - `/raft/admin/promote_learner/{id}` — promote a learner to voter
    # - `/raft/admin/transfer_leadership/{id}` — initiate leadership transfer
    # - `-Draft_debug` only: `/raft/admin/pause|resume|partition|heal|reset`
    #
    # Anything else falls through to the next handler in the chain via
    # `call_next`. Pair with `StatusHandler` to expose the read-only surface
    # on a separate endpoint.
    #
    # Concrete (non-generic) — takes any `AdminOps`, which `Node(T)`
    # includes.
    class AdminHandler
      include ::HTTP::Handler

      @ops : AdminOps
      @transport : TCPTransport?

      def initialize(@ops : AdminOps, @transport : TCPTransport? = nil)
      end

      def call(context : ::HTTP::Server::Context)
        method = context.request.method
        path = context.request.path

        if method == "POST" && path.starts_with?("/raft/admin/")
          handle_admin(context, path)
        else
          call_next(context)
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
          if @ops.bootstrap
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
            if @ops.add_server(node_id, address)
              json_response(context, 200, {"status" => "added", "node_id" => node_id.to_s})
            else
              json_response(context, 400, {"error" => "failed to add server"})
            end
          elsif path.starts_with?("/raft/admin/remove_server/")
            node_id = path.split("/").last.to_u64
            if @ops.remove_server(node_id)
              json_response(context, 200, {"status" => "removed", "node_id" => node_id.to_s})
            else
              json_response(context, 400, {"error" => "failed to remove server"})
            end
          elsif path.starts_with?("/raft/admin/promote_learner/")
            node_id = path.split("/").last.to_u64
            if @ops.promote_learner(node_id)
              json_response(context, 200, {"status" => "promoted", "node_id" => node_id.to_s})
            else
              json_response(context, 400, {"error" => "failed to promote learner"})
            end
          elsif path.starts_with?("/raft/admin/transfer_leadership/")
            node_id = path.split("/").last.to_u64
            if @ops.transfer_leadership(to: node_id)
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
            @ops.pause
            json_response(context, 200, {"status" => "paused"})
          when "/raft/admin/resume"
            @ops.resume
            json_response(context, 200, {"status" => "resumed"})
          when "/raft/admin/partition"
            @ops.partition
            json_response(context, 200, {"status" => "partitioned"})
          when "/raft/admin/heal"
            @ops.heal
            json_response(context, 200, {"status" => "healed"})
          when "/raft/admin/reset"
            @ops.reset
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
