require "http/server/handler"
require "json"
require "uuid"
require "log"

class QueueHttpHandler
  include HTTP::Handler

  @meta_node : Raft::Node(QueueCommand)
  @meta_sm : MetaStateMachine
  @nodes : Hash(UInt64, Raft::Node(QueueCommand))
  @state_machines : Hash(UInt64, QueueStateMachine)

  def initialize(@meta_node, @meta_sm, @nodes, @state_machines)
  end

  def call(context : HTTP::Server::Context)
    path = context.request.path
    method = context.request.method

    case {method, path}
    when {"GET", "/"}
      handle_web_ui(context)
    when {"GET", "/queues"}
      handle_list_queues(context)
    else
      if path.starts_with?("/queues/")
        rest = path[8..]
        # /queues/{name}            POST → publish, DELETE → delete
        # /queues/{name}/messages   GET  → consume
        # /queues/{name}/events     GET  → SSE
        if idx = rest.index('/')
          name = rest[0...idx]
          tail = rest[(idx + 1)..]
          case {method, tail}
          when {"GET", "messages"}
            handle_consume(context, name)
          when {"GET", "events"}
            handle_events(context, name)
          else
            context.response.status_code = 404
            context.response.print "Not found"
          end
        else
          name = rest
          case method
          when "POST"
            handle_publish(context, name)
          when "DELETE"
            handle_delete(context, name)
          else
            context.response.status_code = 405
            context.response.print "Method not allowed"
          end
        end
      else
        call_next(context)
      end
    end
  end

  private def handle_publish(context, name : String)
    if group_id = @meta_sm.group_for(name)
      if node = @nodes[group_id]?
        unless node.role == Raft::Role::Leader
          return if forward_to_leader(context, node)
          context.response.status_code = 503
          context.response.content_type = "application/json"
          context.response.print({error: "not leader for queue", leader_id: node.leader_id}.to_json)
          return
        end
        body = context.request.body.try(&.gets_to_end) || ""
        node.propose(QueueCommand.new(QueueAction::Publish, name, body: body.to_slice))
        context.response.status_code = 202
        context.response.content_type = "application/json"
        context.response.print({status: "accepted", queue: name}.to_json)
      else
        context.response.status_code = 503
        context.response.content_type = "application/json"
        context.response.print({error: "queue group not loaded on this node"}.to_json)
      end
    else
      # Auto-create via meta consensus, then accept the publish on retry
      unless @meta_node.role == Raft::Role::Leader
        return if forward_to_leader(context, @meta_node)
        context.response.status_code = 503
        context.response.content_type = "application/json"
        context.response.print({error: "not meta leader", leader_id: @meta_node.leader_id}.to_json)
        return
      end
      @meta_node.propose(QueueCommand.new(QueueAction::CreateQueue, name))
      context.response.status_code = 202
      context.response.content_type = "application/json"
      context.response.print({status: "queue_creation_accepted", queue: name}.to_json)
    end
  end

  private def handle_consume(context, name : String)
    group_id = @meta_sm.group_for(name)
    unless group_id
      context.response.status_code = 404
      context.response.content_type = "application/json"
      context.response.print({error: "queue not found"}.to_json)
      return
    end

    node = @nodes[group_id]?
    sm = @state_machines[group_id]?
    unless node && sm
      context.response.status_code = 503
      context.response.content_type = "application/json"
      context.response.print({error: "queue group not loaded on this node"}.to_json)
      return
    end

    unless node.role == Raft::Role::Leader
      return if forward_to_leader(context, node)
      context.response.status_code = 503
      context.response.content_type = "application/json"
      context.response.print({error: "not leader for queue", leader_id: node.leader_id}.to_json)
      return
    end

    req_id = UUID.random.to_s
    ch = Channel(Bytes?).new(1)
    sm.register_request(req_id, ch)
    node.propose(QueueCommand.new(QueueAction::Consume, name, req_id: req_id))

    select
    when result = ch.receive
      sm.cancel_request(req_id) # safe even if already delivered
      if popped = result
        context.response.status_code = 200
        context.response.content_type = "application/octet-stream"
        context.response.write(popped)
      else
        context.response.status_code = 204
      end
    when timeout(5.seconds)
      sm.cancel_request(req_id)
      context.response.status_code = 503
      context.response.content_type = "application/json"
      context.response.print({error: "consume timeout — leader may have changed"}.to_json)
    end
  end

  private def handle_delete(context, name : String)
    context.response.status_code = 501
    context.response.print "Not implemented yet"
  end

  private def handle_list_queues(context)
    context.response.status_code = 501
    context.response.print "Not implemented yet"
  end

  private def handle_events(context, name : String)
    context.response.status_code = 501
    context.response.print "Not implemented yet"
  end

  private def handle_web_ui(context)
    context.response.status_code = 501
    context.response.print "Not implemented yet"
  end

  private def forward_to_leader(context, node : Raft::Node(QueueCommand)) : Bool
    leader_id = node.leader_id
    return false unless leader_id
    peer = node.peers.find { |p| p.id == leader_id }
    return false unless peer && !peer.address.empty?
    host = peer.address.split(":").first
    http_port = 8000 + leader_id
    begin
      client = ::HTTP::Client.new(host, http_port.to_i)
      client.connect_timeout = 2.seconds
      client.read_timeout = 5.seconds
      body = context.request.body.try(&.gets_to_end)
      query = context.request.query
      resource = query ? "#{context.request.path}?#{query}" : context.request.path
      response = client.exec(context.request.method, resource, context.request.headers, body)
      context.response.status_code = response.status_code
      context.response.content_type = response.content_type || "application/json"
      context.response.print response.body
      true
    rescue ex
      Log.warn { "Forward to leader #{leader_id} failed: #{ex.message}" }
      false
    end
  end
end
