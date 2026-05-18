require "http/client"
require "json"

module Raft
  module TUI
    class NodeStatus
      property id : UInt64 = 0_u64
      property role : String = "unknown"
      property term : UInt64 = 0_u64
      property leader_id : UInt64? = nil
      property commit_index : UInt64 = 0_u64
      property last_log_index : UInt64 = 0_u64
      property peers : Array({UInt64, String}) = [] of {UInt64, String}
      property paused : Bool = false
      property reachable : Bool = false
      property address : String = ""
      property raft_address : String = ""

      def initialize(@address : String)
      end
    end

    class Dashboard
      @nodes : Array(NodeStatus)
      @events : Array(String) = [] of String
      @running : Bool = true

      def initialize(addresses : Array(String))
        @nodes = addresses.map { |addr| NodeStatus.new(addr) }
      end

      def run
        print "\e[?25l"   # hide cursor
        print "\e[2J"     # clear screen
        STDIN.raw!

        spawn do
          while @running
            poll_nodes
            render
            sleep 500.milliseconds
          end
        end

        while @running
          handle_input
        end
      ensure
        print "\e[?25h"   # show cursor
        STDIN.cooked!
        STDOUT.puts "\r\nGoodbye."
      end

      private def poll_nodes
        @nodes.each do |node|
          begin
            uri = URI.parse(node.address)
            client = ::HTTP::Client.new(uri)
            client.connect_timeout = 1.second
            client.read_timeout = 1.second
            response = client.get("/raft/status")
            if response.status_code == 200
              data = JSON.parse(response.body)
              old_role = node.role
              node.id = data["id"].as_i64.to_u64
              node.role = data["role"].as_s
              node.term = data["term"].as_i64.to_u64
              node.leader_id = data["leader_id"].as_i64?.try(&.to_u64)
              node.commit_index = data["commit_index"].as_i64.to_u64
              node.last_log_index = data["last_log_index"].as_i64.to_u64
              node.peers = data["peers"].as_a.map { |p| {p["id"].as_i64.to_u64, p["role"].as_s} }
              node.raft_address = data["raft_address"]?.try(&.as_s) || ""
              node.paused = data["paused"]?.try(&.as_bool) || false
              node.reachable = true

              if old_role != "unknown" && old_role != node.role
                add_event("Node #{node.id} became #{node.role} (term #{node.term})")
              end
            else
              node.reachable = false
            end
          rescue ex
            node.reachable = false
          end
        end
      end

      private def render
        buf = String.build do |io|
          io << "\e[H" # cursor to top-left

          line(io, "\e[1m\e[36m── Raft Cluster Dashboard ─────────────────────────────\e[0m")
          line(io, "")

          @nodes.each do |node|
            if !node.reachable
              line(io, "  Node #{node.id} (#{node.address})    \e[31m██ UNREACHABLE\e[0m")
              line(io, "")
            elsif node.paused
              line(io, "  Node #{node.id} (#{node.address})    \e[33m██ PAUSED\e[0m       term: #{node.term}")
              line(io, "  commit: #{node.commit_index}  log: #{node.last_log_index}")
            else
              role_display = case node.role
                             when "leader"    then "\e[32m██ LEADER\e[0m"
                             when "candidate" then "\e[33m░░ CANDIDATE\e[0m"
                             when "follower"
                               if node.peers.empty?
                                 "\e[90m░░ STANDALONE\e[0m"
                               else
                                 "\e[34m░░ FOLLOWER\e[0m"
                               end
                             else "\e[90m░░ STANDALONE\e[0m"
                             end
              line(io, "  Node #{node.id} (#{node.address})    #{role_display}   term: #{node.term}")
              if node.role == "leader"
                line(io, "  commit: #{node.commit_index}  log: #{node.last_log_index}")
              else
                line(io, "  commit: #{node.commit_index}  log: #{node.last_log_index}  leader: #{node.leader_id}")
              end
            end
            line(io, "")
          end

          line(io, "\e[1m\e[36m── Event Log (last 10) ───────────────────────────────\e[0m")
          last_events = @events.last(10)
          last_events.each do |event|
            line(io, "  #{event}")
          end
          (10 - last_events.size).times { line(io, "") }

          line(io, "")
          line(io, "\e[1m\e[36m── Chaos / failure simulation ────────────────────────\e[0m")
          line(io, "  \e[1m[k]\e[0m Kill leader        — pause every Raft group on the meta-leader node.")
          line(io, "                          Watch web UI: each queue's \"Leader\" cell flips within ~1s.")
          line(io, "                          Watch Grafana: orange \"Leader Changes\" annotation, brief commit-lag spike.")
          line(io, "  \e[1m[p]\e[0m Pause node id      — same as [k] but for a specific node (prompts for id).")
          line(io, "  \e[1m[r]\e[0m Resume node id     — undo a previous pause; node rejoins quorum.")
          line(io, "  \e[1m[x]\e[0m Partition node id  — drop all Raft messages to/from a node, but keep it running.")
          line(io, "                          Watch Grafana: \"Commit Lag per Node\" diverges; \"Log Truncations\"")
          line(io, "                          may fire on heal if the partitioned node had uncommitted entries.")
          line(io, "  \e[1m[h]\e[0m Heal node id       — reverse [x]; partitioned node catches up via AppendEntries")
          line(io, "                          (or InstallSnapshot if the leader's log was truncated past it).")
          line(io, "  \e[1m[a]\e[0m Heal all           — resume + heal every node. Cleanup after experiments.")
          line(io, "  \e[1m[d]\e[0m Reset node id      — wipe a node's Raft state. Drastic; expect re-sync via snapshot.")
          line(io, "")
          line(io, "\e[1m\e[36m── Cluster lifecycle ─────────────────────────────────\e[0m")
          line(io, "  \e[1m[f]\e[0m Form cluster       — bootstrap first reachable node + join all the others.")
          line(io, "  \e[1m[B]\e[0m Bootstrap node id  — initialize a single node as the only voter.")
          line(io, "  \e[1m[j]\e[0m Join node id       — add a reachable node as a voter via the current leader.")
          line(io, "  \e[1m[+]\e[0m Add server         — register a new node id with the cluster.")
          line(io, "  \e[1m[-]\e[0m Remove server      — remove a node from the cluster's membership.")
          line(io, "  \e[1m[b]\e[0m Rebalance          — redistribute groups across nodes (KV-style only).")
          line(io, "  \e[1m[q]\e[0m Quit")
        end
        STDOUT.write(buf.to_slice)
      end

      # Write a line with \e[K to clear remainder, \r\n for raw-mode newline
      private def line(io : String::Builder, text : String)
        io << text << "\e[K\r\n"
      end

      private def handle_input
        char = STDIN.read_char
        return unless char

        case char
        when 'q', '\u{3}' # q or Ctrl+C
          @running = false
        when 'k'
          kill_leader
        when 'a'
          heal_all
        when 'p'
          prompt_node("Pause") { |id| pause_node(id) }
        when 'r'
          prompt_node("Resume") { |id| resume_node(id) }
        when 'x'
          prompt_node("Partition") { |id| partition_node(id) }
        when 'h'
          prompt_node("Heal") { |id| heal_node(id) }
        when 'd'
          prompt_node("Reset") { |id| reset_node(id) }
        when 'f'
          form_cluster
        when 'B'
          prompt_node("Bootstrap node") { |id| bootstrap_node(id) }
        when 'j'
          prompt_node("Join node to cluster — node") { |id| join_node(id) }
        when 'b'
          rebalance
        when '+'
          prompt_node("Add server — node id") { |id| add_server(id) }
        when '-'
          prompt_node("Remove server — node id") { |id| remove_server(id) }
        end
      end

      private def prompt_node(action : String, &block : Int32 ->)
        # Write prompt on a status line below controls
        row = @nodes.size * 3 + 14
        STDOUT.write("\e[#{row};1H\e[K  #{action} which node? [1-#{@nodes.size}] ".to_slice)
        if num = STDIN.read_char
          if idx = num.to_i?
            block.call(idx)
          end
        end
      end

      private def kill_leader
        if leader = @nodes.find { |n| n.role == "leader" && n.reachable }
          post_admin(leader.address, "pause")
          add_event("Killed Node #{leader.id} — paused every Raft group on it (simulates crash)")
        else
          add_event("No reachable leader found")
        end
      end

      private def heal_all
        @nodes.each do |node|
          post_admin(node.address, "heal")
          post_admin(node.address, "resume")
        end
        add_event("Healed all nodes")
      end

      private def pause_node(id : Int32)
        if node = @nodes.find { |n| n.id == id.to_u64 }
          post_admin(node.address, "pause")
          add_event("Paused Node #{id} — all groups paused, simulating a crash")
        end
      end

      private def resume_node(id : Int32)
        if node = @nodes.find { |n| n.id == id.to_u64 }
          post_admin(node.address, "resume")
          add_event("Resumed Node #{id} — all groups rejoin quorum")
        end
      end

      private def partition_node(id : Int32)
        if node = @nodes.find { |n| n.id == id.to_u64 }
          post_admin(node.address, "partition")
          add_event("Partitioned Node #{id} — node is running but isolated from Raft traffic")
        end
      end

      private def heal_node(id : Int32)
        if node = @nodes.find { |n| n.id == id.to_u64 }
          post_admin(node.address, "heal")
          add_event("Healed Node #{id} — reconnected; expect AppendEntries / InstallSnapshot catch-up")
        end
      end

      private def reset_node(id : Int32)
        if node = @nodes.find { |n| n.id == id.to_u64 }
          post_admin(node.address, "reset")
          add_event("Reset Node #{id} — wiped Raft state across all groups")
        end
      end

      private def bootstrap_node(id : Int32)
        node = @nodes.find { |n| n.id == id.to_u64 && n.reachable }
        unless node
          # Fall back to positional index (1-based)
          idx = id - 1
          node = @nodes[idx]? if idx >= 0 && idx < @nodes.size
        end
        unless node && node.reachable
          add_event("Node #{id} not found or not reachable")
          return
        end
        begin
          uri = URI.parse(node.address)
          client = ::HTTP::Client.new(uri)
          client.connect_timeout = 2.seconds
          client.read_timeout = 2.seconds
          response = client.post("/raft/admin/bootstrap")
          if response.status_code == 200
            add_event("Bootstrapped Node #{node.id} as leader")
          else
            add_event("Bootstrap failed: #{response.status_code} (already has peers?)")
          end
        rescue ex
          add_event("Bootstrap failed: #{ex.message}")
        end
      end

      private def form_cluster
        reachable = @nodes.select(&.reachable)
        if reachable.empty?
          add_event("No reachable nodes")
          return
        end

        # Bootstrap first reachable node as leader
        first = reachable[0]
        begin
          uri = URI.parse(first.address)
          client = ::HTTP::Client.new(uri)
          client.connect_timeout = 2.seconds
          client.read_timeout = 2.seconds
          response = client.post("/raft/admin/bootstrap")
          if response.status_code == 200
            add_event("Bootstrapped Node (#{first.address}) as leader")
          else
            add_event("Bootstrap failed: #{response.status_code}")
            return
          end
        rescue ex
          add_event("Bootstrap failed: #{ex.message}")
          return
        end

        sleep 500.milliseconds
        poll_nodes # refresh to get raft_addresses and updated roles

        leader = @nodes.find { |n| n.role == "leader" && n.reachable }
        unless leader
          add_event("Bootstrap succeeded but leader not found")
          return
        end

        # Join all other reachable nodes
        others = reachable.reject { |n| n.address == first.address }
        others.each do |node|
          join_node_to_cluster(node, leader)
          wait_for_commit(leader)
        end
      end

      private def join_node(id : Int32)
        node = @nodes.find { |n| n.id == id.to_u64 }
        unless node
          add_event("Node #{id} not found")
          return
        end
        leader = @nodes.find { |n| n.role == "leader" && n.reachable }
        unless leader
          add_event("No reachable leader found")
          return
        end
        join_node_to_cluster(node, leader)
      end

      private def join_node_to_cluster(node : NodeStatus, leader : NodeStatus)
        if node.id == leader.id
          add_event("Cannot join leader to itself (Node #{node.id})")
          return
        end
        # Register transport peers bidirectionally between new node and all cluster members
        cluster_members = @nodes.select { |n| n.reachable && n.address != node.address && !n.peers.empty? }
        cluster_members << leader unless cluster_members.any? { |n| n.address == leader.address }

        cluster_members.each do |member|
          register_transport_peer(member.address, node.id, node.raft_address)
          register_transport_peer(node.address, member.id, member.raft_address)
        end

        # Add server via leader
        begin
          uri = URI.parse(leader.address)
          client = ::HTTP::Client.new(uri)
          client.connect_timeout = 2.seconds
          client.read_timeout = 2.seconds
          body = {address: node.raft_address}.to_json
          response = client.post("/raft/admin/add_server/#{node.id}", body: body, headers: ::HTTP::Headers{"Content-Type" => "application/json"})
          if response.status_code == 200
            add_event("Added Node #{node.id} to cluster via leader (Node #{leader.id})")
          else
            error = begin
              JSON.parse(response.body)["error"]?.try(&.as_s) || response.status_code.to_s
            rescue
              response.status_code.to_s
            end
            add_event("Failed to add Node #{node.id}: #{error}")
          end
        rescue ex
          add_event("Failed to add Node #{node.id}: #{ex.message}")
        end
      end

      # Wait for leader's commit_index to advance (config change committed)
      private def wait_for_commit(leader : NodeStatus, timeout : Time::Span = 3.seconds)
        start_commit = leader.commit_index
        deadline = Time.instant + timeout
        while Time.instant < deadline
          sleep 100.milliseconds
          begin
            uri = URI.parse(leader.address)
            client = ::HTTP::Client.new(uri)
            client.connect_timeout = 1.second
            client.read_timeout = 1.second
            response = client.get("/raft/status")
            if response.status_code == 200
              data = JSON.parse(response.body)
              current_commit = data["commit_index"].as_i64.to_u64
              return if current_commit > start_commit
            end
          rescue
          end
        end
      end

      private def register_transport_peer(target_address : String, peer_id : UInt64, peer_raft_address : String)
        return if peer_raft_address.empty?
        parts = peer_raft_address.split(":")
        host = parts[0]
        port = parts[1]?.try(&.to_i) || 9000
        begin
          uri = URI.parse(target_address)
          client = ::HTTP::Client.new(uri)
          client.connect_timeout = 2.seconds
          client.read_timeout = 2.seconds
          body = {id: peer_id, host: host, port: port}.to_json
          client.post("/raft/admin/register_peer", body: body, headers: ::HTTP::Headers{"Content-Type" => "application/json"})
        rescue
        end
      end

      private def add_server(id : Int32)
        if leader = @nodes.find { |n| n.role == "leader" && n.reachable }
          begin
            uri = URI.parse(leader.address)
            client = ::HTTP::Client.new(uri)
            client.connect_timeout = 2.seconds
            client.read_timeout = 2.seconds
            response = client.post("/raft/admin/add_server/#{id}")
            if response.status_code == 200
              add_event("Added node #{id} as learner via leader (Node #{leader.id})")
            else
              add_event("Failed to add node #{id}: #{response.status_code}")
            end
          rescue ex
            add_event("Failed to add node #{id}: #{ex.message}")
          end
        else
          add_event("No reachable leader found")
        end
      end

      private def remove_server(id : Int32)
        leader = @nodes.find { |n| n.role == "leader" && n.reachable }
        unless leader
          add_event("No reachable leader found")
          return
        end

        # If removing the leader, transfer leadership first
        if leader.id == id.to_u64
          target = @nodes.find { |n| n.reachable && n.id != leader.id && n.peers.any? { |_, role| role == "voter" } }
          unless target
            add_event("No other voter to transfer leadership to")
            return
          end
          begin
            uri = URI.parse(leader.address)
            client = ::HTTP::Client.new(uri)
            client.connect_timeout = 2.seconds
            client.read_timeout = 2.seconds
            response = client.post("/raft/admin/transfer_leadership/#{target.id}")
            if response.status_code == 200
              add_event("Transferring leadership from Node #{leader.id} to Node #{target.id}")
            else
              add_event("Failed to transfer leadership: #{response.status_code}")
              return
            end
          rescue ex
            add_event("Failed to transfer leadership: #{ex.message}")
            return
          end
          # Wait for new leader to emerge
          sleep 2.seconds
          poll_nodes
          leader = @nodes.find { |n| n.role == "leader" && n.reachable && n.id != id.to_u64 }
          unless leader
            add_event("Leadership transfer failed — no new leader found")
            return
          end
          add_event("New leader is Node #{leader.id}")
        end

        begin
          uri = URI.parse(leader.address)
          client = ::HTTP::Client.new(uri)
          client.connect_timeout = 2.seconds
          client.read_timeout = 2.seconds
          response = client.post("/raft/admin/remove_server/#{id}")
          if response.status_code == 200
            add_event("Removed node #{id} via leader (Node #{leader.id})")
          else
            add_event("Failed to remove node #{id}: #{response.status_code}")
          end
        rescue ex
          add_event("Failed to remove node #{id}: #{ex.message}")
        end
      end

      private def rebalance
        total_transfers = 0
        @nodes.each do |node|
          next unless node.reachable
          begin
            uri = URI.parse(node.address)
            client = ::HTTP::Client.new(uri)
            client.connect_timeout = 2.seconds
            client.read_timeout = 2.seconds
            response = client.post("/kv/rebalance")
            if response.status_code == 200
              data = JSON.parse(response.body)
              total_transfers += data["transfers"].as_i
            end
          rescue
          end
        end
        add_event("Rebalance: #{total_transfers} transfer(s) initiated across #{@nodes.count(&.reachable)} nodes")
      end

      private def post_admin(address : String, action : String)
        uri = URI.parse(address)
        client = ::HTTP::Client.new(uri)
        client.connect_timeout = 1.second
        client.read_timeout = 1.second
        client.post("/raft/admin/#{action}")
      rescue ex
        add_event("Failed to #{action}: #{ex.message}")
      end

      private def add_event(message : String)
        timestamp = Time.local.to_s("%H:%M:%S")
        @events << "#{timestamp}  #{message}"
      end
    end
  end
end
