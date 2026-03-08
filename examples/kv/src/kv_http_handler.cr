# examples/kv/src/kv_http_handler.cr
require "http/server/handler"
require "json"

class KVHttpHandler
  include HTTP::Handler

  @node : Raft::Node(KVCommand)
  @state_machine : KVStateMachine

  def initialize(@node : Raft::Node(KVCommand), @state_machine : KVStateMachine)
  end

  def call(context : HTTP::Server::Context)
    path = context.request.path
    method = context.request.method

    if path.starts_with?("/kv/")
      key = path[4..]
      case method
      when "GET"
        handle_get(context, key)
      when "PUT"
        handle_put(context, key)
      when "DELETE"
        handle_delete(context, key)
      else
        context.response.status_code = 405
        context.response.print "Method not allowed"
      end
    else
      call_next(context)
    end
  end

  private def handle_get(context, key)
    if value = @state_machine.get(key)
      context.response.content_type = "application/json"
      context.response.print({key: key, value: value}.to_json)
    else
      context.response.status_code = 404
      context.response.content_type = "application/json"
      context.response.print({error: "key not found"}.to_json)
    end
  end

  private def handle_put(context, key)
    unless @node.role == Raft::Role::Leader
      context.response.status_code = 503
      context.response.content_type = "application/json"
      leader = @node.leader_id
      context.response.print({error: "not leader", leader_id: leader}.to_json)
      return
    end

    value = context.request.body.try(&.gets_to_end) || ""
    cmd = KVCommand.new(KVAction::Put, key, value)
    @node.propose(cmd)
    context.response.status_code = 202
    context.response.content_type = "application/json"
    context.response.print({status: "accepted", key: key}.to_json)
  end

  private def handle_delete(context, key)
    unless @node.role == Raft::Role::Leader
      context.response.status_code = 503
      context.response.content_type = "application/json"
      leader = @node.leader_id
      context.response.print({error: "not leader", leader_id: leader}.to_json)
      return
    end

    cmd = KVCommand.new(KVAction::Delete, key)
    @node.propose(cmd)
    context.response.status_code = 202
    context.response.content_type = "application/json"
    context.response.print({status: "accepted", key: key}.to_json)
  end
end
