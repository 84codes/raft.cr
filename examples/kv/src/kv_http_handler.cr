# examples/kv/src/kv_http_handler.cr
require "http/server/handler"
require "html"
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

    case {method, path}
    when {"GET", "/"}
      handle_web_ui(context)
    when {"GET", "/kv"}
      handle_list_all(context)
    else
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
  end

  private def handle_list_all(context)
    context.response.content_type = "application/json"
    context.response.print @state_machine.all.to_json
  end

  private def handle_web_ui(context)
    is_leader = @node.role == Raft::Role::Leader
    role = @node.role.to_s
    term = @node.current_term
    leader_id = @node.leader_id
    store = @state_machine.all

    table_html = if store.empty?
                   "<p>No data stored.</p>"
                 else
                   String.build do |s|
                     s << "<table><tr><th>Key</th><th>Value</th><th></th></tr>"
                     store.each do |key, value|
                       s << "<tr><td>" << HTML.escape(key) << "</td><td>" << HTML.escape(value) << "</td><td>"
                       if is_leader
                         s << "<button class=\"btn-delete\" onclick=\"deleteKey('" << HTML.escape(key) << "')\">Delete</button>"
                       end
                       s << "</td></tr>"
                     end
                     s << "</table>"
                   end
                 end

    form_html = if is_leader
                  "<form onsubmit=\"putKey(event)\">" \
                  "<input type=\"text\" id=\"key\" placeholder=\"Key\" required>" \
                  "<input type=\"text\" id=\"value\" placeholder=\"Value\" required>" \
                  "<button type=\"submit\" class=\"btn-put\">Put</button>" \
                  "</form>"
                else
                  "<p class=\"not-leader\">Only the leader node can accept writes. Current leader: node #{leader_id || "unknown"}</p>"
                end

    context.response.content_type = "text/html"
    context.response.print <<-HTML
    <!DOCTYPE html>
    <html>
    <head>
      <title>Raft KV Store - Node #{@node.id}</title>
      <style>
        body { font-family: sans-serif; max-width: 800px; margin: 40px auto; padding: 0 20px; }
        h1 { border-bottom: 2px solid #333; padding-bottom: 10px; }
        .status { background: #f0f0f0; padding: 12px; border-radius: 6px; margin-bottom: 20px; }
        .status span { margin-right: 20px; }
        .leader { color: green; font-weight: bold; }
        .follower { color: #666; }
        .candidate { color: orange; }
        table { width: 100%; border-collapse: collapse; margin-bottom: 20px; }
        th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid #ddd; }
        th { background: #f5f5f5; }
        form { background: #f9f9f9; padding: 16px; border-radius: 6px; }
        input[type=text] { padding: 6px 10px; margin-right: 8px; border: 1px solid #ccc; border-radius: 4px; }
        button { padding: 6px 14px; border: none; border-radius: 4px; cursor: pointer; }
        .btn-put { background: #4CAF50; color: white; }
        .btn-delete { background: #f44336; color: white; font-size: 12px; padding: 4px 10px; }
        .btn-put:hover { background: #45a049; }
        .btn-delete:hover { background: #d32f2f; }
        .not-leader { color: #999; font-style: italic; }
      </style>
    </head>
    <body>
      <h1>Raft KV Store</h1>
      <div class="status">
        <span>Node: <b>#{@node.id}</b></span>
        <span>Role: <b class="#{role.downcase}">#{role}</b></span>
        <span>Term: <b>#{term}</b></span>
        <span>Leader: <b>#{leader_id || "unknown"}</b></span>
      </div>

      <h2>Stored Data</h2>
      #{table_html}

      <h2>Add / Update Entry</h2>
      #{form_html}

      <script>
        function putKey(e) {
          e.preventDefault();
          var key = document.getElementById('key').value;
          var value = document.getElementById('value').value;
          fetch('/kv/' + encodeURIComponent(key), { method: 'PUT', body: value })
            .then(function() { setTimeout(function() { location.reload(); }, 300); });
        }
        function deleteKey(key) {
          fetch('/kv/' + encodeURIComponent(key), { method: 'DELETE' })
            .then(function() { setTimeout(function() { location.reload(); }, 300); });
        }
      </script>
    </body>
    </html>
    HTML
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
