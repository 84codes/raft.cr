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
    when {"GET", "/events"}
      handle_events(context)
    when {"GET", "/kv"}
      handle_list_all(context)
    when {"POST", "/kv/rebalance"}
      handle_rebalance(context)
    when {"GET", "/kv/metrics"}
      handle_all_metrics(context)
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
                       escaped_key = HTML.escape(key)
                       escaped_value = HTML.escape(value || "")
                       is_leader = @nodes[group_id]?.try(&.role) == Raft::Role::Leader
                       s << "<tr data-key=\"" << escaped_key << "\">"
                       s << "<td>" << escaped_key << "</td>"
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
      <div class="status" id="status-bar">
        <span>Node: <b id="stat-node">#{@meta_node.id}</b></span>
        <span>Meta Role: <b id="stat-meta-role" class="#{meta_role.downcase}">#{meta_role}</b></span>
        <span>Meta Term: <b id="stat-meta-term">#{meta_term}</b></span>
        <span>Meta Leader: <b id="stat-meta-leader">#{meta_leader || "unknown"}</b></span>
        <span>Groups: <b id="stat-groups">#{@meta_sm.all_groups.size}</b></span>
      </div>
      <div class="info">Each key is its own Raft group. Different keys can have different leaders.</div>

      <h2>Stored Data</h2>
      <div id="table-area">#{table_html}</div>

      <h2>Add / Update Entry</h2>
      <div id="form-area">#{form_html}</div>

      <h2>Cluster Operations</h2>
      <button class="btn-rebalance" onclick="rebalance()">Rebalance Leaders</button>
      <span id="rebalance-result"></span>

      <script>
        function $(id) { return document.getElementById(id); }
        function setText(id, txt) { var el = $(id); if (el) el.textContent = txt; }

        function putKey(e) {
          e.preventDefault();
          var key = $('key').value;
          var value = $('value').value;
          fetch('/kv/' + encodeURIComponent(key), { method: 'PUT', body: value })
            .then(function() { $('key').value = ''; $('value').value = ''; });
        }
        function updateKey(key) {
          var input = $('val-' + key);
          if (!input) return;
          fetch('/kv/' + encodeURIComponent(key), { method: 'PUT', body: input.value });
        }
        function deleteKey(key) {
          fetch('/kv/' + encodeURIComponent(key), { method: 'DELETE' });
        }
        function rebalance() {
          fetch('/kv/rebalance', { method: 'POST' })
            .then(function(r) { return r.json(); })
            .then(function(data) {
              $('rebalance-result').textContent = data.transfers + ' transfer(s) initiated';
            });
        }

        function applySnapshot(snap) {
          var active = document.activeElement;
          var activeInfo = null;
          if (active && active.id) {
            activeInfo = {
              id: active.id,
              value: 'value' in active ? active.value : null,
              selStart: 'selectionStart' in active ? active.selectionStart : null,
              selEnd: 'selectionEnd' in active ? active.selectionEnd : null
            };
          }

          setText('stat-node', snap.node_id);
          var roleEl = $('stat-meta-role');
          if (roleEl) { roleEl.textContent = snap.meta.role; roleEl.className = snap.meta.role.toLowerCase(); }
          setText('stat-meta-term', snap.meta.term);
          setText('stat-meta-leader', snap.meta.leader == null ? 'unknown' : snap.meta.leader);
          setText('stat-groups', snap.entries.length);

          renderTable(snap.entries);
          renderForm(snap.meta.is_leader, snap.meta.leader);

          if (activeInfo) {
            var el = $(activeInfo.id);
            if (el) {
              if (activeInfo.value !== null && 'value' in el) el.value = activeInfo.value;
              try { el.focus(); } catch (e) {}
              if (activeInfo.selStart !== null && el.setSelectionRange) {
                try { el.setSelectionRange(activeInfo.selStart, activeInfo.selEnd); } catch (e) {}
              }
            }
          }
        }

        function renderTable(entries) {
          var area = $('table-area');
          if (entries.length === 0) {
            area.innerHTML = '<p>No data stored.</p>';
            return;
          }
          var table = area.querySelector('table');
          if (!table) {
            area.innerHTML = '';
            table = document.createElement('table');
            var header = document.createElement('tr');
            ['Key', 'Value', 'Group', 'Role', 'Leader', ''].forEach(function(t) {
              var th = document.createElement('th');
              th.textContent = t;
              header.appendChild(th);
            });
            table.appendChild(header);
            area.appendChild(table);
          }
          var existing = {};
          table.querySelectorAll('tr[data-key]').forEach(function(tr) {
            existing[tr.getAttribute('data-key')] = tr;
          });
          var seen = {};
          entries.forEach(function(e) {
            seen[e.key] = true;
            var tr = existing[e.key];
            if (!tr) {
              tr = document.createElement('tr');
              tr.setAttribute('data-key', e.key);
              for (var i = 0; i < 6; i++) tr.appendChild(document.createElement('td'));
              table.appendChild(tr);
            }
            var tds = tr.children;
            tds[0].textContent = e.key;
            if (e.is_leader) {
              var input = tds[1].querySelector('input.inline-edit');
              if (!input) {
                tds[1].innerHTML = '';
                input = document.createElement('input');
                input.type = 'text';
                input.className = 'inline-edit';
                input.id = 'val-' + e.key;
                tds[1].appendChild(input);
              }
              if (document.activeElement !== input) {
                input.value = e.value == null ? '' : e.value;
              }
            } else {
              tds[1].textContent = e.value == null ? '(nil)' : e.value;
            }
            tds[2].textContent = e.group_id;
            tds[3].textContent = e.role;
            tds[3].className = e.role.toLowerCase();
            tds[4].textContent = e.leader == null ? 'unknown' : e.leader;
            if (e.is_leader) {
              if (!tds[5].querySelector('button')) {
                tds[5].innerHTML = '';
                var key = e.key;
                var upd = document.createElement('button');
                upd.className = 'btn-update';
                upd.textContent = 'Update';
                upd.addEventListener('click', function() { updateKey(key); });
                var del = document.createElement('button');
                del.className = 'btn-delete';
                del.textContent = 'Delete';
                del.addEventListener('click', function() { deleteKey(key); });
                tds[5].appendChild(upd);
                tds[5].appendChild(document.createTextNode(' '));
                tds[5].appendChild(del);
              }
            } else if (tds[5].children.length > 0) {
              tds[5].innerHTML = '';
            }
          });
          Object.keys(existing).forEach(function(k) {
            if (!seen[k]) existing[k].remove();
          });
        }

        function renderForm(isLeader, leader) {
          var area = $('form-area');
          if (isLeader) {
            if (!area.querySelector('form')) {
              area.innerHTML = '';
              var form = document.createElement('form');
              form.onsubmit = putKey;
              var k = document.createElement('input');
              k.type = 'text'; k.id = 'key'; k.placeholder = 'Key'; k.required = true;
              var v = document.createElement('input');
              v.type = 'text'; v.id = 'value'; v.placeholder = 'Value'; v.required = true;
              var btn = document.createElement('button');
              btn.type = 'submit'; btn.className = 'btn-put'; btn.textContent = 'Put';
              form.appendChild(k);
              form.appendChild(v);
              form.appendChild(btn);
              area.appendChild(form);
            }
          } else {
            var msg = 'This node is not the meta leader. Meta leader: node ' + (leader == null ? 'unknown' : leader);
            var p = area.querySelector('.not-leader');
            if (!p) {
              area.innerHTML = '';
              p = document.createElement('p');
              p.className = 'not-leader';
              area.appendChild(p);
            }
            p.textContent = msg;
          }
        }

        (function() {
          var es = new EventSource('/events');
          es.onmessage = function(ev) {
            try { applySnapshot(JSON.parse(ev.data)); } catch (e) { console.error(e); }
          };
        })();
      </script>
    </body>
    </html>
    HTML
  end

  private def write_snapshot(io : IO)
    JSON.build(io) do |json|
      json.object do
        json.field "node_id", @meta_node.id
        json.field "meta" do
          json.object do
            json.field "role", @meta_node.role.to_s
            json.field "term", @meta_node.current_term
            json.field "leader", @meta_node.leader_id
            json.field "is_leader", @meta_node.role == Raft::Role::Leader
            json.field "groups", @meta_sm.all_groups.size
          end
        end
        json.field "entries" do
          json.array do
            @meta_sm.all_groups.each do |key, group_id|
              vsm = @value_machines[group_id]?
              value = vsm.try(&.value)
              node = @nodes[group_id]?
              role = node.try(&.role.to_s) || "unknown"
              leader = node.try(&.leader_id)
              is_leader = node.try(&.role) == Raft::Role::Leader
              json.object do
                json.field "key", key
                json.field "value", value
                json.field "group_id", group_id
                json.field "role", role
                json.field "leader", leader
                json.field "is_leader", is_leader
              end
            end
          end
        end
      end
    end
  end

  private def handle_events(context)
    context.response.content_type = "text/event-stream"
    context.response.headers["Cache-Control"] = "no-cache"
    context.response.headers["X-Accel-Buffering"] = "no"

    loop do
      context.response << "data: "
      write_snapshot(context.response)
      context.response << "\n\n"
      context.response.flush
      sleep 1.second
    end
  rescue IO::Error
    # Client disconnected
  end

  private def update_node_gauges(node : Raft::Node(KVCommand))
    if metrics = node.metrics
      metrics.set_gauge("raft_node_role", node.role.value.to_i64)
      metrics.set_gauge("raft_node_term", node.current_term.to_i64)
      metrics.set_gauge("raft_node_commit_index", node.commit_index.to_i64)
      metrics.set_gauge("raft_node_last_log_index", node.log.last_index.to_i64)
      metrics.set_gauge("raft_node_is_leader", node.role == Raft::Role::Leader ? 1_i64 : 0_i64)
      metrics.set_gauge("raft_node_leader_id", (node.leader_id || 0_u64).to_i64)
    end
  end

  private def handle_all_metrics(context)
    context.response.content_type = "text/plain; version=0.0.4"
    context.response.status_code = 200

    node_id = @meta_node.id

    # All groups (meta group 0 is included in @nodes)
    @nodes.each_value do |node|
      update_node_gauges(node)
      if m = node.metrics
        m.to_prometheus(context.response)
      end
    end

    # Active groups from meta state — lets Grafana filter out stale Prometheus data
    @meta_sm.all_groups.each do |key, group_id|
      context.response << "raft_kv_group_active{node_id=\"" << node_id << "\",group_id=\"" << group_id << "\",key=\"" << key << "\"} 1\n"
    end
    # Meta group is always active
    context.response << "raft_kv_group_active{node_id=\"" << node_id << "\",group_id=\"0\",key=\"meta\"} 1\n"

    # Group info with leader and follower node IDs as labels
    @nodes.each do |gid, node|
      all_ids = node.peers.map(&.id).sort
      leader = node.leader_id || 0_u64
      followers = all_ids.reject { |id| id == leader }.join(",")
      context.response << "raft_kv_group_info{node_id=\"" << node_id << "\",group_id=\"" << gid << "\",leader=\"" << leader << "\",followers=\"" << followers << "\"} 1\n"
    end
  end

  private def handle_rebalance(context)
    all_peer_ids = @meta_node.peers.map(&.id).sort
    groups = @meta_sm.all_groups.to_a
    transfers = [] of {String, UInt64, UInt64}

    # Compute desired assignment: round-robin groups across sorted node IDs
    desired = {} of UInt64 => UInt64 # group_id => target_node_id
    groups.each_with_index do |(key, group_id), idx|
      desired[group_id] = all_peer_ids[idx % all_peer_ids.size]
    end

    # Only transfer groups where THIS node is currently the leader
    # (we can only initiate transfer from the leader)
    groups.each do |key, group_id|
      target_id = desired[group_id]
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
      "total_groups" => groups.size,
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

  private def wait_for_commit(node : Raft::Node(KVCommand), timeout : Time::Span = 5.seconds) : Bool
    target_index = node.log.last_index
    deadline = Time.instant + timeout
    while node.commit_index < target_index && Time.instant < deadline
      sleep 10.milliseconds
    end
    node.commit_index >= target_index
  end

  private def handle_put(context, key)
    if group_id = @meta_sm.group_for(key)
      # Group exists — propose value to data group
      if node = @nodes[group_id]?
        unless node.role == Raft::Role::Leader
          return if forward_to_leader(context, node)
          context.response.status_code = 503
          context.response.content_type = "application/json"
          context.response.print({error: "not leader for key", leader_id: node.leader_id}.to_json)
          return
        end
        value = context.request.body.try(&.gets_to_end) || ""
        wait = context.request.query_params.has_key?("wait")
        node.propose(KVCommand.new(KVAction::Put, key, value))
        context.response.content_type = "application/json"
        if wait
          context.response.status_code = wait_for_commit(node) ? 200 : 408
        else
          context.response.status_code = 202
        end
        context.response.print({status: context.response.status_code == 200 ? "committed" : "accepted", key: key}.to_json)
      end
    else
      # Group doesn't exist — create it with initial value via meta consensus
      unless @meta_node.role == Raft::Role::Leader
        return if forward_to_leader(context, @meta_node)
        context.response.status_code = 503
        context.response.content_type = "application/json"
        context.response.print({error: "not meta leader", leader_id: @meta_node.leader_id}.to_json)
        return
      end
      value = context.request.body.try(&.gets_to_end) || ""
      wait = context.request.query_params.has_key?("wait")
      @meta_node.propose(KVCommand.new(KVAction::CreateGroup, key, value))
      context.response.content_type = "application/json"
      if wait
        context.response.status_code = wait_for_commit(@meta_node) ? 200 : 408
      else
        context.response.status_code = 202
      end
      context.response.print({status: context.response.status_code == 200 ? "committed" : "accepted", key: key}.to_json)
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
      return if forward_to_leader(context, @meta_node)
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

  # Forward an HTTP request to the leader node.
  # Derives the leader's HTTP address from its raft address (same host, port 8000 + node_id).
  # Returns true if forwarded, false if leader unknown.
  private def forward_to_leader(context, node : Raft::Node(KVCommand)) : Bool
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
        call_next(context)
      end
    end

    private def json_response(context, status : Int32, data)
      context.response.content_type = "application/json"
      context.response.status_code = status
      context.response.print data.to_json
    end
  {% end %}
end
