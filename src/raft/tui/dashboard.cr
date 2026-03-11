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
      property peers : Array(UInt64) = [] of UInt64
      property paused : Bool = false
      property reachable : Bool = false
      property address : String = ""

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
                             else                  "\e[34m░░ FOLLOWER\e[0m"
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

          line(io, "\e[1m\e[36m── Event Log ─────────────────────────────────────────\e[0m")
          last_events = @events.last(6)
          last_events.each do |event|
            line(io, "  #{event}")
          end
          (6 - last_events.size).times { line(io, "") }

          line(io, "")
          line(io, "\e[1m\e[36m── Controls ──────────────────────────────────────────\e[0m")
          line(io, "  \e[1m[p]\e[0m Pause  \e[1m[r]\e[0m Resume  \e[1m[x]\e[0m Partition  \e[1m[h]\e[0m Heal")
          line(io, "  \e[1m[k]\e[0m Kill leader  \e[1m[a]\e[0m Heal all  \e[1m[d]\e[0m Reset  \e[1m[b]\e[0m Rebalance  \e[1m[q]\e[0m Quit")
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
        when 'b'
          rebalance
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
          add_event("Paused leader (Node #{leader.id})")
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
          add_event("Paused Node #{id}")
        end
      end

      private def resume_node(id : Int32)
        if node = @nodes.find { |n| n.id == id.to_u64 }
          post_admin(node.address, "resume")
          add_event("Resumed Node #{id}")
        end
      end

      private def partition_node(id : Int32)
        if node = @nodes.find { |n| n.id == id.to_u64 }
          post_admin(node.address, "partition")
          add_event("Partitioned Node #{id}")
        end
      end

      private def heal_node(id : Int32)
        if node = @nodes.find { |n| n.id == id.to_u64 }
          post_admin(node.address, "heal")
          add_event("Healed Node #{id}")
        end
      end

      private def reset_node(id : Int32)
        if node = @nodes.find { |n| n.id == id.to_u64 }
          post_admin(node.address, "reset")
          add_event("Reset Node #{id}")
        end
      end

      private def rebalance
        if leader = @nodes.find { |n| n.role == "leader" && n.reachable }
          begin
            uri = URI.parse(leader.address)
            client = ::HTTP::Client.new(uri)
            client.connect_timeout = 2.seconds
            client.read_timeout = 2.seconds
            response = client.post("/kv/rebalance")
            if response.status_code == 200
              data = JSON.parse(response.body)
              transfers = data["transfers"].as_i
              add_event("Rebalance: #{transfers} transfer(s) initiated")
            else
              add_event("Rebalance failed: HTTP #{response.status_code}")
            end
          rescue ex
            add_event("Rebalance failed: #{ex.message}")
          end
        else
          add_event("No reachable leader for rebalance")
        end
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
