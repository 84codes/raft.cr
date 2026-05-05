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
    unless @meta_sm.group_for(name)
      context.response.status_code = 404
      context.response.content_type = "application/json"
      context.response.print({error: "queue not found"}.to_json)
      return
    end
    unless @meta_node.role == Raft::Role::Leader
      return if forward_to_leader(context, @meta_node)
      context.response.status_code = 503
      context.response.content_type = "application/json"
      context.response.print({error: "not meta leader", leader_id: @meta_node.leader_id}.to_json)
      return
    end
    @meta_node.propose(QueueCommand.new(QueueAction::DeleteQueue, name))
    context.response.status_code = 202
    context.response.content_type = "application/json"
    context.response.print({status: "delete_accepted", queue: name}.to_json)
  end

  private def handle_list_queues(context)
    queues = [] of NamedTuple(name: String, group_id: UInt64, depth: Int32, log_last_index: UInt64, is_leader: Bool, leader_id: Raft::NodeID?)
    @meta_sm.all_groups.each do |name, group_id|
      sm = @state_machines[group_id]?
      node = @nodes[group_id]?
      depth = sm.try(&.depth) || 0
      log_last_index = node.try(&.log.last_index) || 0_u64
      is_leader = node.try(&.role) == Raft::Role::Leader
      leader_id = node.try(&.leader_id)
      queues << {name: name, group_id: group_id, depth: depth, log_last_index: log_last_index, is_leader: is_leader, leader_id: leader_id}
    end
    context.response.content_type = "application/json"
    context.response.print queues.to_json
  end

  private def handle_events(context, name : String)
    unless @meta_sm.group_for(name)
      context.response.status_code = 404
      context.response.print "queue not found"
      return
    end
    context.response.headers["Content-Type"] = "text/event-stream"
    context.response.headers["Cache-Control"] = "no-cache"
    context.response.headers["Connection"] = "keep-alive"

    # Naive polling SSE — emits a depth snapshot every 500ms.
    # Sufficient for the PoC; a future iteration can hook into apply().
    last_depth = -1
    deadline_check_interval = 500.milliseconds
    begin
      loop do
        group_id = @meta_sm.group_for(name)
        break unless group_id
        sm = @state_machines[group_id]?
        depth = sm.try(&.depth) || 0
        if depth != last_depth
          context.response << "data: " << {queue: name, depth: depth}.to_json << "\n\n"
          context.response.flush
          last_depth = depth
        end
        sleep deadline_check_interval
      end
    rescue IO::Error
      # client disconnected
    end
  end

  private def handle_web_ui(context)
    context.response.content_type = "text/html"
    context.response << <<-HTML
      <!doctype html>
      <html>
      <head>
        <title>Queue PoC</title>
        <style>
          body { font-family: -apple-system, sans-serif; margin: 2em; }
          h1 { font-size: 18px; }
          table { border-collapse: collapse; margin-top: 1em; }
          th, td { border: 1px solid #ccc; padding: 6px 12px; text-align: left; }
          th { background: #f0f0f0; }
          .leader { color: #060; font-weight: bold; }
          .info { color: #666; font-size: 12px; }
          form { margin-top: 1em; }
          input { padding: 4px; }
          button { padding: 4px 12px; }
        </style>
      </head>
      <body>
        <h1>Queue PoC — live state</h1>
        <p class="info">Each queue is its own Raft group. Watch the gap grow between in-memory depth and on-disk log entries.</p>
        <table id="queues">
          <thead>
            <tr><th>Queue</th><th>Group</th><th>Depth (memory)</th><th>Log entries (disk)</th><th>Leader</th><th>Role here</th></tr>
          </thead>
          <tbody></tbody>
        </table>
        <form id="publish-form" onsubmit="publish(); return false;">
          <input id="qname" placeholder="queue name" required>
          <input id="body" placeholder="message body" required>
          <button>Publish</button>
        </form>
        <form id="consume-form" onsubmit="consume(); return false;">
          <input id="qname-c" placeholder="queue name" required>
          <button>Consume one</button>
        </form>
        <pre id="last-consume"></pre>

        <script>
          async function refresh() {
            const r = await fetch('/queues');
            const list = await r.json();
            const tbody = document.querySelector('#queues tbody');
            function esc(s) { var d = document.createElement('div'); d.textContent = String(s); return d.innerHTML; }
            tbody.innerHTML = list.map(q =>
              '<tr>' +
                '<td>' + esc(q.name) + '</td>' +
                '<td>' + q.group_id + '</td>' +
                '<td>' + q.depth + '</td>' +
                '<td>' + q.log_last_index + '</td>' +
                '<td>' + (q.leader_id || 'unknown') + '</td>' +
                '<td>' + (q.is_leader ? '<span class="leader">leader</span>' : 'follower') + '</td>' +
              '</tr>'
            ).join('');
          }

          async function publish() {
            const name = document.getElementById('qname').value;
            const body = document.getElementById('body').value;
            const r = await fetch('/queues/' + encodeURIComponent(name), {method: 'POST', body: body});
            console.log('publish:', r.status);
            refresh();
          }

          async function consume() {
            const name = document.getElementById('qname-c').value;
            const r = await fetch('/queues/' + encodeURIComponent(name) + '/messages');
            const out = document.getElementById('last-consume');
            if (r.status === 204) {
              out.textContent = '(empty)';
            } else if (r.status === 200) {
              out.textContent = await r.text();
            } else {
              out.textContent = 'error: ' + r.status;
            }
            refresh();
          }

          refresh();
          setInterval(refresh, 1000);
        </script>
      </body>
      </html>
    HTML
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
