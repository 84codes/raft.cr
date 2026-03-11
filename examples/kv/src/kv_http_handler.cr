# examples/kv/src/kv_http_handler.cr
require "http/server/handler"
require "html"
require "json"

class KVHttpHandler
  include HTTP::Handler

  @meta_node : Raft::Node(KVCommand)
  @meta_sm : MetaStateMachine
  @nodes : Hash(UInt64, Raft::Node(KVCommand))
  @value_machines : Hash(UInt64, ValueStateMachine)

  def initialize(@meta_node, @meta_sm, @nodes, @value_machines)
  end

  def call(context : HTTP::Server::Context)
    path = context.request.path
    method = context.request.method

    {% if flag?(:raft_debug) %}
      if method == "POST" && path.starts_with?("/raft/admin/")
        handle_admin(context, path)
        return
      end
    {% end %}

    case {method, path}
    when {"GET", "/"}
      handle_web_ui(context)
    when {"GET", "/kv"}
      handle_list_all(context)
    when {"POST", "/kv/rebalance"}
      handle_rebalance(context)
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
    data = {} of String => String
    @meta_sm.all_groups.each do |key, group_id|
      if vsm = @value_machines[group_id]?
        if v = vsm.value
          data[key] = v
        end
      end
    end
    context.response.content_type = "application/json"
    context.response.print data.to_json
  end

  private def handle_web_ui(context)
    meta_is_leader = @meta_node.role == Raft::Role::Leader
    meta_role = @meta_node.role.to_s
    meta_term = @meta_node.current_term
    meta_leader = @meta_node.leader_id

    # Collect all key-value pairs with their group info
    entries = [] of {String, String?, UInt64, String, Raft::NodeID?}
    @meta_sm.all_groups.each do |key, group_id|
      vsm = @value_machines[group_id]?
      value = vsm.try(&.value)
      node = @nodes[group_id]?
      role = node.try(&.role.to_s) || "unknown"
      leader = node.try(&.leader_id)
      entries << {key, value, group_id, role, leader}
    end

    table_html = if entries.empty?
                   "<p>No data stored.</p>"
                 else
                   String.build do |s|
                     s << "<table><tr><th>Key</th><th>Value</th><th>Group</th><th>Role</th><th>Leader</th><th></th></tr>"
                     entries.each do |key, value, group_id, role, leader|
                       s << "<tr>"
                       s << "<td>" << HTML.escape(key) << "</td>"
                       is_leader = @nodes[group_id]?.try(&.role) == Raft::Role::Leader
                       escaped_key = HTML.escape(key)
                       escaped_value = HTML.escape(value || "")
                       if is_leader
                         s << "<td><input type=\"text\" class=\"inline-edit\" id=\"val-" << escaped_key << "\" value=\"" << escaped_value << "\"></td>"
                       else
                         s << "<td>" << HTML.escape(value || "(nil)") << "</td>"
                       end
                       s << "<td>" << group_id << "</td>"
                       s << "<td class=\"" << role.downcase << "\">" << role << "</td>"
                       s << "<td>" << (leader || "unknown") << "</td>"
                       s << "<td>"
                       if is_leader
                         s << "<button class=\"btn-update\" onclick=\"updateKey('" << escaped_key << "')\">Update</button> "
                         s << "<button class=\"btn-delete\" onclick=\"deleteKey('" << escaped_key << "')\">Delete</button>"
                       end
                       s << "</td></tr>"
                     end
                     s << "</table>"
                   end
                 end

    form_html = if meta_is_leader
                  "<form onsubmit=\"putKey(event)\">" \
                  "<input type=\"text\" id=\"key\" placeholder=\"Key\" required>" \
                  "<input type=\"text\" id=\"value\" placeholder=\"Value\" required>" \
                  "<button type=\"submit\" class=\"btn-put\">Put</button>" \
                  "</form>"
                else
                  "<p class=\"not-leader\">This node is not the meta leader. Meta leader: node #{meta_leader || "unknown"}</p>"
                end

    context.response.content_type = "text/html"
    context.response.print <<-HTML
    <!DOCTYPE html>
    <html>
    <head>
      <title>Multi-Raft KV Store - Node #{@meta_node.id}</title>
      <style>
        body { font-family: sans-serif; max-width: 900px; margin: 40px auto; padding: 0 20px; }
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
        .btn-update { background: #2196F3; color: white; font-size: 12px; padding: 4px 10px; }
        .btn-update:hover { background: #1976D2; }
        .btn-delete { background: #f44336; color: white; font-size: 12px; padding: 4px 10px; }
        .inline-edit { width: 120px; padding: 4px 6px; border: 1px solid #ccc; border-radius: 3px; font-size: 13px; }
        .btn-rebalance { background: #FF9800; color: white; padding: 8px 16px; font-size: 14px; }
        .btn-rebalance:hover { background: #F57C00; }
        #rebalance-result { margin-left: 10px; color: #666; }
        .btn-put:hover { background: #45a049; }
        .btn-delete:hover { background: #d32f2f; }
        .not-leader { color: #999; font-style: italic; }
        .info { background: #e8f4fd; padding: 8px 12px; border-radius: 4px; margin-bottom: 10px; font-size: 13px; }
      </style>
    </head>
    <body>
      <h1>Multi-Raft KV Store</h1>
      <div class="status">
        <span>Node: <b>#{@meta_node.id}</b></span>
        <span>Meta Role: <b class="#{meta_role.downcase}">#{meta_role}</b></span>
        <span>Meta Term: <b>#{meta_term}</b></span>
        <span>Meta Leader: <b>#{meta_leader || "unknown"}</b></span>
        <span>Groups: <b>#{@meta_sm.all_groups.size}</b></span>
      </div>
      <div class="info">Each key is its own Raft group. Different keys can have different leaders.</div>

      <h2>Stored Data</h2>
      #{table_html}

      <h2>Add / Update Entry</h2>
      #{form_html}

      <h2>Cluster Operations</h2>
      <button class="btn-rebalance" onclick="rebalance()">Rebalance Leaders</button>
      <span id="rebalance-result"></span>

      <script>
        function putKey(e) {
          e.preventDefault();
          var key = document.getElementById('key').value;
          var value = document.getElementById('value').value;
          fetch('/kv/' + encodeURIComponent(key), { method: 'PUT', body: value })
            .then(function() { setTimeout(function() { location.reload(); }, 500); });
        }
        function updateKey(key) {
          var value = document.getElementById('val-' + key).value;
          fetch('/kv/' + encodeURIComponent(key), { method: 'PUT', body: value })
            .then(function() { setTimeout(function() { location.reload(); }, 500); });
        }
        function rebalance() {
          fetch('/kv/rebalance', { method: 'POST' })
            .then(function(r) { return r.json(); })
            .then(function(data) {
              document.getElementById('rebalance-result').textContent = data.transfers + ' transfer(s) initiated';
              setTimeout(function() { location.reload(); }, 1000);
            });
        }
        function deleteKey(key) {
          fetch('/kv/' + encodeURIComponent(key), { method: 'DELETE' })
            .then(function() { setTimeout(function() { location.reload(); }, 500); });
        }
      </script>
    </body>
    </html>
    HTML
  end

  private def handle_rebalance(context)
    all_peer_ids = [@meta_node.id] + @meta_node.peers
    groups = @meta_sm.all_groups
    transfers = [] of {String, UInt64, UInt64}

    groups.each_with_index do |(key, group_id), idx|
      target_id = all_peer_ids[idx % all_peer_ids.size]
      if node = @nodes[group_id]?
        if node.role == Raft::Role::Leader && node.id != target_id
          node.transfer_leadership(to: target_id)
          transfers << {key, node.id, target_id}
        end
      end
    end

    context.response.content_type = "application/json"
    context.response.status_code = 200
    result = {
      "status"    => "rebalance_initiated",
      "transfers" => transfers.size,
      "details"   => transfers.map { |key, from, to| {"key" => key, "from" => from, "to" => to} },
    }
    context.response.print result.to_json
  end

  private def handle_get(context, key)
    if group_id = @meta_sm.group_for(key)
      if vsm = @value_machines[group_id]?
        if v = vsm.value
          context.response.content_type = "application/json"
          context.response.print({key: key, value: v}.to_json)
          return
        end
      end
    end
    context.response.status_code = 404
    context.response.content_type = "application/json"
    context.response.print({error: "key not found"}.to_json)
  end

  private def handle_put(context, key)
    value = context.request.body.try(&.gets_to_end) || ""

    if group_id = @meta_sm.group_for(key)
      # Group exists — propose value to data group
      if node = @nodes[group_id]?
        unless node.role == Raft::Role::Leader
          context.response.status_code = 503
          context.response.content_type = "application/json"
          context.response.print({error: "not leader for key", leader_id: node.leader_id}.to_json)
          return
        end
        node.propose(KVCommand.new(KVAction::Put, key, value))
        context.response.status_code = 202
        context.response.content_type = "application/json"
        context.response.print({status: "accepted", key: key}.to_json)
      end
    else
      # Group doesn't exist — create it with initial value via meta consensus
      unless @meta_node.role == Raft::Role::Leader
        context.response.status_code = 503
        context.response.content_type = "application/json"
        context.response.print({error: "not meta leader", leader_id: @meta_node.leader_id}.to_json)
        return
      end
      @meta_node.propose(KVCommand.new(KVAction::CreateGroup, key, value))
      context.response.status_code = 202
      context.response.content_type = "application/json"
      context.response.print({status: "accepted", key: key}.to_json)
    end
  end

  private def handle_delete(context, key)
    unless @meta_sm.group_for(key)
      context.response.status_code = 404
      context.response.content_type = "application/json"
      context.response.print({error: "key not found"}.to_json)
      return
    end
    unless @meta_node.role == Raft::Role::Leader
      context.response.status_code = 503
      context.response.content_type = "application/json"
      context.response.print({error: "not meta leader", leader_id: @meta_node.leader_id}.to_json)
      return
    end
    @meta_node.propose(KVCommand.new(KVAction::DeleteGroup, key))
    context.response.status_code = 202
    context.response.content_type = "application/json"
    context.response.print({status: "accepted", key: key}.to_json)
  end

  {% if flag?(:raft_debug) %}
    private def handle_admin(context, path)
      case path
      when "/raft/admin/pause"
        @meta_node.pause
        @nodes.each_value(&.pause)
        json_response(context, 200, {"status" => "paused", "groups" => @nodes.size + 1})
      when "/raft/admin/resume"
        @meta_node.resume
        @nodes.each_value(&.resume)
        json_response(context, 200, {"status" => "resumed", "groups" => @nodes.size + 1})
      when "/raft/admin/partition"
        @meta_node.partition
        @nodes.each_value(&.partition)
        json_response(context, 200, {"status" => "partitioned", "groups" => @nodes.size + 1})
      when "/raft/admin/heal"
        @meta_node.heal
        @nodes.each_value(&.heal)
        json_response(context, 200, {"status" => "healed", "groups" => @nodes.size + 1})
      else
        context.response.status_code = 404
        context.response.print "Unknown admin action"
      end
    end

    private def json_response(context, status : Int32, data)
      context.response.content_type = "application/json"
      context.response.status_code = status
      context.response.print data.to_json
    end
  {% end %}
end
