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
  @transport : Raft::TCPTransport

  def initialize(@meta_node, @meta_sm, @nodes, @state_machines, @transport)
  end

  def call(context : HTTP::Server::Context)
    path = context.request.path
    method = context.request.method

    case {method, path}
    when {"GET", "/"}
      handle_web_ui(context)
    when {"GET", "/queues"}
      handle_list_queues(context)
    when {"GET", "/raft/metrics"}
      handle_metrics_aggregated(context)
    when {"POST", "/raft/admin/pause"}
      apply_to_all_groups(context, "pause") { |n| {% if flag?(:raft_debug) %} n.pause {% end %} }
    when {"POST", "/raft/admin/resume"}
      apply_to_all_groups(context, "resume") { |n| {% if flag?(:raft_debug) %} n.resume {% end %} }
    when {"POST", "/raft/admin/partition"}
      apply_to_all_groups(context, "partition") { |n| {% if flag?(:raft_debug) %} n.partition {% end %} }
    when {"POST", "/raft/admin/heal"}
      apply_to_all_groups(context, "heal") { |n| {% if flag?(:raft_debug) %} n.heal {% end %} }
    when {"POST", "/raft/admin/reset"}
      apply_to_all_groups(context, "reset") { |n| {% if flag?(:raft_debug) %} n.reset {% end %} }
    when {"POST", "/queue/rebalance"}, {"POST", "/kv/rebalance"}
      handle_rebalance(context)
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
    body = context.request.body.try(&.gets_to_end) || ""

    unless @meta_sm.group_for(name)
      # Auto-create via meta consensus, then fall through to the publish path.
      unless @meta_node.role == Raft::Role::Leader
        return if forward_to_leader(context, @meta_node, body)
        context.response.status_code = 503
        context.response.content_type = "application/json"
        context.response.print({error: "not meta leader", leader_id: @meta_node.leader_id}.to_json)
        return
      end
      @meta_node.propose(QueueCommand.new(QueueAction::CreateQueue, name))
    end

    # Wait for the queue to exist in the meta SM, the local data group to be
    # spun up, and a leader to be known so we can either propose or proxy.
    group_id = nil
    node = nil
    120.times do
      group_id = @meta_sm.group_for(name)
      if group_id
        node = @nodes[group_id]?
        break if node && (node.role == Raft::Role::Leader || !node.leader_id.nil?)
      end
      sleep 25.milliseconds
    end

    unless group_id
      context.response.status_code = 503
      context.response.content_type = "application/json"
      context.response.print({error: "queue creation timed out"}.to_json)
      return
    end

    unless node
      return if forward_to_leader(context, @meta_node, body)
      context.response.status_code = 503
      context.response.content_type = "application/json"
      context.response.print({error: "queue group not loaded on this node"}.to_json)
      return
    end

    unless node.role == Raft::Role::Leader
      return if forward_to_leader(context, node, body)
      context.response.status_code = 503
      context.response.content_type = "application/json"
      context.response.print({error: "not leader for queue", leader_id: node.leader_id}.to_json)
      return
    end

    node.propose(QueueCommand.new(QueueAction::Publish, name, body: body.to_slice))
    context.response.status_code = 202
    context.response.content_type = "application/json"
    context.response.print({status: "accepted", queue: name}.to_json)
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

  # Round-robin distribute group leadership across cluster peer ids. Each peer
  # node runs this independently; only groups currently led by THIS node can be
  # transferred (Raft requires the leader to initiate). Mirrors the KV example.
  private def handle_rebalance(context)
    all_peer_ids = @meta_node.peers.map(&.id).sort
    groups = @meta_sm.all_groups.to_a
    transfers = [] of {String, UInt64, UInt64}

    desired = {} of UInt64 => UInt64
    groups.each_with_index do |(name, group_id), idx|
      desired[group_id] = all_peer_ids[idx % all_peer_ids.size] unless all_peer_ids.empty?
    end

    groups.each do |name, group_id|
      target_id = desired[group_id]?
      next unless target_id
      if (node = @nodes[group_id]?) && node.role.leader? && node.id != target_id
        node.transfer_leadership(to: target_id)
        transfers << {name, node.id, target_id}
      end
    end

    context.response.content_type = "application/json"
    context.response.print({
      status:       "rebalance_initiated",
      transfers:    transfers.size,
      total_groups: groups.size,
      details:      transfers.map { |k, f, t| {key: k, from: f, to: t} },
    }.to_json)
  end

  # Apply a debug action to every Raft group hosted on this node (meta + all
  # data groups). The library's /raft/admin/* endpoints only target the single
  # @meta_node — calling them simulates "pause meta group", not "pause this
  # whole physical node". This wrapper makes the chaos buttons (pause, partition,
  # heal, reset) affect the entire node, which is what TUI demos assume.
  private def apply_to_all_groups(context, action : String, &)
    {% if flag?(:raft_debug) %}
      @nodes.each_value { |node| yield node }
      context.response.content_type = "application/json"
      context.response.print({status: action, scope: "all_groups", count: @nodes.size}.to_json)
    {% else %}
      context.response.status_code = 503
      context.response.content_type = "application/json"
      context.response.print({error: "raft_debug build flag not set"}.to_json)
    {% end %}
  end

  private def handle_metrics_aggregated(context)
    context.response.content_type = "text/plain; version=0.0.4"
    context.response.status_code = 200
    node_id = @meta_node.id
    @nodes.each_value do |node|
      next unless metrics = node.metrics
      metrics.set_gauge("raft_node_role", node.role.value.to_i64)
      metrics.set_gauge("raft_node_term", node.current_term.to_i64)
      metrics.set_gauge("raft_node_commit_index", node.commit_index.to_i64)
      metrics.set_gauge("raft_node_last_log_index", node.log.last_index.to_i64)
      metrics.set_gauge("raft_node_first_log_index", node.log.first_index.to_i64)
      metrics.set_gauge("raft_node_segment_count", node.log.segment_count.to_i64)
      metrics.set_gauge("raft_node_snapshot_index", node.snapshot_index.to_i64)
      metrics.set_gauge("raft_node_snapshot_size_bytes", node.snapshot_size_bytes)
      metrics.set_gauge("raft_node_peers", node.peers.size.to_i64)
      metrics.set_gauge("raft_node_is_leader", node.role.leader? ? 1_i64 : 0_i64)
      metrics.set_gauge("raft_node_leader_id", (node.leader_id || 0_u64).to_i64)
      metrics.set_gauge("raft_node_paused", {% if flag?(:raft_debug) %} (node.paused ? 1_i64 : 0_i64) {% else %} 0_i64 {% end %})
      metrics.set_gauge("raft_node_partitioned", {% if flag?(:raft_debug) %} (node.partitioned ? 1_i64 : 0_i64) {% else %} 0_i64 {% end %})
      metrics.to_prometheus(context.response)
    end

    # Active-group markers and group-info — used by the cluster overview
    # dashboard to filter out stale Prometheus series and render the table.
    context.response << "raft_queue_group_active{node_id=\"" << node_id << "\",group_id=\"0\",key=\"meta\"} 1\n"
    @meta_sm.all_groups.each do |name, group_id|
      context.response << "raft_queue_group_active{node_id=\"" << node_id << "\",group_id=\"" << group_id << "\",key=\"" << name << "\"} 1\n"
    end
    @nodes.each do |gid, node|
      # Skip nodes that don't know who the leader is — they would emit a bogus
      # "followers" label that includes the leader (since reject(0) doesn't match
      # any real peer), polluting the joined Groups Overview table.
      next if (leader = node.leader_id).nil?
      key = gid == 0_u64 ? "meta" : (@meta_sm.all_groups.find { |_, g| g == gid }.try(&.first) || "group-#{gid}")
      all_ids = (node.peers.map(&.id) + [node.id]).uniq.sort
      followers = all_ids.reject { |id| id == leader }.join(",")
      context.response << "raft_queue_group_info{node_id=\"" << node_id << "\",group_id=\"" << gid << "\",key=\"" << key << "\",leader=\"" << leader << "\",followers=\"" << followers << "\"} 1\n"
    end

    @transport.to_prometheus(context.response)
  end

  private def handle_list_queues(context)
    queues = [] of NamedTuple(name: String, group_id: UInt64, depth: Int32, log_last_index: UInt64, log_first_index: UInt64, segment_count: Int32, snapshot_index: UInt64, is_leader: Bool, leader_id: Raft::NodeID?)
    @meta_sm.all_groups.each do |name, group_id|
      sm = @state_machines[group_id]?
      node = @nodes[group_id]?
      depth = sm.try(&.depth) || 0
      log_last_index = node.try(&.log.last_index) || 0_u64
      log_first_index = node.try(&.log.first_index) || 0_u64
      segment_count = node.try(&.log.segment_count) || 0
      snapshot_index = node.try(&.snapshot_index) || 0_u64
      is_leader = node.try(&.role) == Raft::Role::Leader
      leader_id = node.try(&.leader_id)
      queues << {name: name, group_id: group_id, depth: depth, log_last_index: log_last_index, log_first_index: log_first_index, segment_count: segment_count, snapshot_index: snapshot_index, is_leader: is_leader, leader_id: leader_id}
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
    context.response.headers["Content-Type"] = "text/html; charset=utf-8"
    context.response << <<-HTML
      <!doctype html>
      <html>
      <head>
        <meta charset="utf-8">
        <title>Queue PoC</title>
        <style>
          body { font-family: -apple-system, sans-serif; margin: 2em; }
          h1 { font-size: 18px; }
          table { border-collapse: collapse; margin-top: 1em; }
          th, td { border: 1px solid #ccc; padding: 6px 12px; text-align: left; }
          th { background: #f0f0f0; }
          .leader { color: #060; font-weight: bold; }
          .info { color: #666; font-size: 12px; }
          fieldset { margin-top: 1em; padding: 0.75em 1em; border: 1px solid #ddd; max-width: 720px; }
          legend { font-weight: bold; padding: 0 6px; }
          form { margin: 0.25em 0; }
          label { margin-right: 12px; }
          input[type=text], input[type=number] { padding: 4px; }
          button { padding: 4px 12px; }
          button.running { background: #fee; border-color: #c60; color: #c60; }
          .stats { font-family: ui-monospace, monospace; color: #444; margin-left: 8px; }
        </style>
      </head>
      <body>
        <h1>Queue PoC — live state</h1>
        <p class="info">Each queue is its own Raft group. Watch the gap grow between in-memory depth and on-disk log entries.</p>
        <table id="queues">
          <thead>
            <tr><th>Queue</th><th>Group</th><th>Depth (memory)</th><th>Log entries (disk)</th><th>Segments</th><th>Snapshot @</th><th>Leader</th><th>Role here</th></tr>
          </thead>
          <tbody></tbody>
        </table>

        <fieldset>
          <legend>Publish</legend>
          <form onsubmit="publishOne(); return false;">
            <label>Queue <input type="text" id="qname" required></label>
            <label>Body <input type="text" id="body" required></label>
            <button>Publish one</button>
          </form>
          <form onsubmit="publishMany(); return false;">
            <label>Queue <input type="text" id="qname-bulk" required></label>
            <label>Count <input type="number" id="count" value="100" min="1" max="1000000" required></label>
            <button id="bulk-btn">Publish N random</button>
            <span class="stats" id="bulk-stats"></span>
          </form>
        </fieldset>

        <fieldset>
          <legend>Consume</legend>
          <form onsubmit="consumeOne(); return false;">
            <label>Queue <input type="text" id="qname-c" required></label>
            <button>Consume one</button>
          </form>
          <form onsubmit="toggleContinuous(); return false;">
            <label>Queue <input type="text" id="qname-cc" required></label>
            <button id="cc-btn">Start continuous</button>
            <span class="stats" id="cc-stats"></span>
          </form>
          <pre id="last-consume"></pre>
        </fieldset>

        <script>
          function esc(s) { var d = document.createElement('div'); d.textContent = String(s); return d.innerHTML; }

          async function refresh() {
            const r = await fetch('/queues');
            const list = await r.json();
            const tbody = document.querySelector('#queues tbody');
            tbody.innerHTML = list.map(q =>
              '<tr>' +
                '<td>' + esc(q.name) + '</td>' +
                '<td>' + q.group_id + '</td>' +
                '<td>' + q.depth + '</td>' +
                '<td>' + q.log_last_index + '</td>' +
                '<td>' + q.segment_count + '</td>' +
                '<td>' + q.snapshot_index + '</td>' +
                '<td>' + (q.leader_id || 'unknown') + '</td>' +
                '<td>' + (q.is_leader ? '<span class="leader">leader</span>' : 'follower') + '</td>' +
              '</tr>'
            ).join('');
          }

          async function publishOne() {
            const name = document.getElementById('qname').value;
            const body = document.getElementById('body').value;
            await fetch('/queues/' + encodeURIComponent(name), {method: 'POST', body: body});
            refresh();
          }

          async function publishMany() {
            const name = document.getElementById('qname-bulk').value;
            const count = parseInt(document.getElementById('count').value, 10);
            const btn = document.getElementById('bulk-btn');
            const stats = document.getElementById('bulk-stats');
            const url = '/queues/' + encodeURIComponent(name);
            const concurrency = 20;
            let next = 0, done = 0, errors = 0;
            btn.disabled = true;
            const started = performance.now();
            async function worker() {
              while (next < count) {
                const i = next++;
                const body = 'msg-' + i + '-' + Math.random().toString(36).slice(2, 10);
                try {
                  const r = await fetch(url, {method: 'POST', body: body});
                  if (!r.ok) errors++;
                } catch (e) { errors++; }
                done++;
                if (done % 25 === 0 || done === count) {
                  stats.textContent = 'published ' + done + ' / ' + count + (errors ? ' (' + errors + ' errors)' : '');
                }
              }
            }
            await Promise.all(Array.from({length: concurrency}, worker));
            const secs = ((performance.now() - started) / 1000).toFixed(1);
            stats.textContent = 'done: ' + done + ' in ' + secs + 's' + (errors ? ' (' + errors + ' errors)' : '');
            btn.disabled = false;
            refresh();
          }

          async function consumeOne() {
            const name = document.getElementById('qname-c').value;
            const r = await fetch('/queues/' + encodeURIComponent(name) + '/messages');
            const out = document.getElementById('last-consume');
            if (r.status === 204) out.textContent = '(empty)';
            else if (r.status === 200) out.textContent = await r.text();
            else out.textContent = 'error: ' + r.status;
            refresh();
          }

          const cc = { running: false, count: 0 };
          async function toggleContinuous() {
            const btn = document.getElementById('cc-btn');
            const stats = document.getElementById('cc-stats');
            const out = document.getElementById('last-consume');
            if (cc.running) {
              cc.running = false;
              btn.textContent = 'Start continuous';
              btn.classList.remove('running');
              return;
            }
            const name = document.getElementById('qname-cc').value;
            cc.running = true;
            cc.count = 0;
            btn.textContent = 'Stop continuous';
            btn.classList.add('running');
            stats.textContent = 'consumed 0';
            while (cc.running) {
              try {
                const r = await fetch('/queues/' + encodeURIComponent(name) + '/messages');
                if (r.status === 200) {
                  cc.count++;
                  const body = await r.text();
                  out.textContent = '#' + cc.count + ': ' + body;
                  if (cc.count % 10 === 0) stats.textContent = 'consumed ' + cc.count;
                } else if (r.status === 204) {
                  stats.textContent = 'consumed ' + cc.count + ' (queue empty)';
                  await new Promise(r => setTimeout(r, 200));
                } else {
                  stats.textContent = 'consumed ' + cc.count + ' (status ' + r.status + ')';
                  await new Promise(r => setTimeout(r, 500));
                }
              } catch (e) {
                await new Promise(r => setTimeout(r, 500));
              }
            }
            stats.textContent = 'stopped after ' + cc.count;
          }

          refresh();
          setInterval(refresh, 1000);
        </script>
      </body>
      </html>
    HTML
  end

  private def forward_to_leader(context, node : Raft::Node(QueueCommand), body : String? = nil) : Bool
    leader_id = node.leader_id
    return false unless leader_id

    host = nil
    peer = node.peers.find { |p| p.id == leader_id }
    if peer && !peer.address.empty?
      host = peer.address.split(":").first
    elsif addr = @transport.peer_address?(leader_id)
      host = addr[0]
    end
    return false unless host

    http_port = 8000 + leader_id
    begin
      client = ::HTTP::Client.new(host, http_port.to_i)
      client.connect_timeout = 2.seconds
      client.read_timeout = 5.seconds
      forward_body = body || context.request.body.try(&.gets_to_end)
      query = context.request.query
      resource = query ? "#{context.request.path}?#{query}" : context.request.path
      response = client.exec(context.request.method, resource, context.request.headers, forward_body)
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
