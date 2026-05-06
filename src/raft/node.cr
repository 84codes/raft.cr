module Raft
  class Node(T)
    getter role : Role = Role::Follower
    getter current_term : UInt64 = 0_u64
    getter id : NodeID
    getter voted_for : NodeID? = nil
    getter leader_id : NodeID? = nil
    getter commit_index : UInt64 = 0_u64
    getter last_applied : UInt64 = 0_u64
    getter snapshot_index : UInt64 = 0_u64
    getter snapshot_term : UInt64 = 0_u64
    getter log : Log(T)

    getter peers : Array(Peer)
    getter metrics : Metrics?
    {% if flag?(:raft_debug) %}
      getter paused : Bool = false
      getter partitioned : Bool = false
    {% end %}
    getter group_id : UInt64
    getter address : String = ""
    getter inbox : Channel(Message) = Channel(Message).new(64)
    @config : Config
    @state_machine : StateMachine(T)
    @outbox : Array({NodeID, Message}) = [] of {NodeID, Message}
    @transfer_target : NodeID? = nil
    @election_tick : UInt32 = 0_u32
    @heartbeat_tick : UInt32 = 0_u32
    @election_timeout : UInt32
    @random : Random = Random.new
    @votes_received : Set(NodeID) = Set(NodeID).new
    @pre_votes_received : Set(NodeID) = Set(NodeID).new
    @next_index : Hash(NodeID, UInt64) = {} of NodeID => UInt64
    @match_index : Hash(NodeID, UInt64) = {} of NodeID => UInt64
    @on_configuration_change : Proc(Array(Peer), Nil)?
    @on_configuration_applied : Proc(Array(Peer), Nil)?
    @pending_config_index : UInt64 = 0_u64

    def initialize(@id : NodeID, peers : Array(NodeID), @config : Config, @state_machine : StateMachine(T), @metrics : Metrics? = nil, @group_id : UInt64 = 0_u64, @address : String = "")
      if peers.empty?
        @peers = [] of Peer
      else
        @peers = peers.map { |pid| Peer.new(pid) } + [Peer.new(@id)]
      end
      @log = Log(T).new(@config)
      @election_timeout = random_election_timeout
      recover_state
    end

    {% if flag?(:raft_debug) %}
      def pause
        @paused = true
      end

      def resume
        @paused = false
      end

      def partition
        @partitioned = true
      end

      def heal
        @partitioned = false
      end

      def reset
        @role = Role::Follower
        @current_term = 0_u64
        @voted_for = nil
        @leader_id = nil
        @commit_index = 0_u64
        @last_applied = 0_u64
        @election_tick = 0_u32
        @heartbeat_tick = 0_u32
        @election_timeout = random_election_timeout
        @votes_received = Set(NodeID).new
        @pre_votes_received = Set(NodeID).new
        @next_index.clear
        @match_index.clear
        @outbox.clear
        @paused = false
        @partitioned = false
        @log.reset
        persist_state
      end
    {% end %}

    def tick
      {% if flag?(:raft_debug) %}
        return if @paused
      {% end %}
      case @role
      when Role::Follower
        @election_tick += 1
        start_pre_vote if @election_tick >= @election_timeout
      when Role::Candidate
        @election_tick += 1
        start_pre_vote if @election_tick >= @election_timeout
      when Role::Leader
        @heartbeat_tick += 1
        if @heartbeat_tick >= @config.heartbeat_ticks
          @heartbeat_tick = 0_u32
          advance_commit_index
          send_append_entries
        end
      end
    end

    def step(message : Message)
      {% if flag?(:raft_debug) %}
        return if @paused
        return if @partitioned
      {% end %}

      if message.group_id != @group_id
        ::Log.warn { "Node #{@id}/g#{@group_id}: dropping message with group_id #{message.group_id} (#{message.type} from #{message.from})" }
        return
      end

      # PreVote messages don't cause term changes — they're speculative
      unless message.type == MessageType::PreVote || message.type == MessageType::PreVoteResponse
        if message.term > @current_term
          @metrics.try(&.increment("raft_term_changes_total", {"reason" => "higher_term"}))
          @current_term = message.term
          @voted_for = nil
          become_follower(message.from)
        end
      end

      case message.type
      when MessageType::AppendEntries
        handle_append_entries(message)
      when MessageType::AppendEntriesResponse
        handle_append_entries_response(message)
      when MessageType::RequestVote
        handle_request_vote(message)
      when MessageType::RequestVoteResponse
        handle_request_vote_response(message)
      when MessageType::PreVote
        handle_pre_vote(message)
      when MessageType::PreVoteResponse
        handle_pre_vote_response(message)
      when MessageType::TimeoutNow
        handle_timeout_now(message)
      end
    end

    def propose(data : T) : Bool
      return false unless @role == Role::Leader
      @log.append(term: @current_term, data: data, entry_type: EntryType::Normal)
      @metrics.try(&.increment("raft_proposals_total"))
      advance_commit_index
      send_append_entries
      true
    end

    def transfer_leadership(to target : NodeID) : Bool
      return false unless @role == Role::Leader
      return false unless @peers.any? { |p| p.id == target && p.voter? }
      return false if target == @id
      @transfer_target = target
      @metrics.try(&.increment("raft_leadership_transfers_total", {"result" => "initiated"}))
      send_append_entries # ensure target is caught up
      maybe_send_timeout_now(target)
      true
    end

    # Bootstrap this node as a single-node cluster.
    # Only works when the node has no peers (fresh start).
    def bootstrap : Bool
      return false unless @peers.empty?
      @peers = [Peer.new(@id, address: @address)]
      @current_term = 1_u64
      @role = Role::Leader
      @leader_id = @id
      config_bytes = serialize_peers
      @log.append(term: @current_term, entry_type: EntryType::Configuration, config_data: config_bytes)
      @commit_index = @log.last_index  # single-node cluster, immediately committed
      persist_state
      true
    end

    # Add a new node to the cluster as a learner.
    # Returns false if not leader or node already exists.
    def add_server(node_id : NodeID, address : String = "") : Bool
      return false unless @role == Role::Leader
      return false if @pending_config_index > @commit_index
      return false if @peers.any? { |p| p.id == node_id }
      new_peers = @peers.dup
      new_peers << Peer.new(node_id, Peer::Role::Learner, address)
      append_configuration(new_peers)
      true
    end

    # Remove a node from the cluster.
    # Returns false if not leader, node not found, or trying to remove self.
    def remove_server(node_id : NodeID) : Bool
      return false unless @role == Role::Leader
      return false if @pending_config_index > @commit_index
      return false if node_id == @id
      return false unless @peers.any? { |p| p.id == node_id }
      new_peers = @peers.reject { |p| p.id == node_id }
      # Don't allow removal that would leave zero voters
      return false unless new_peers.any?(&.voter?)
      append_configuration(new_peers)
      true
    end

    # Promote a learner to voter.
    # Returns false if not leader or node is not a learner.
    def promote_learner(node_id : NodeID) : Bool
      return false unless @role == Role::Leader
      return false if @pending_config_index > @commit_index
      return false unless @peers.any? { |p| p.id == node_id && p.learner? }
      new_peers = @peers.map do |p|
        if p.id == node_id
          Peer.new(p.id, Peer::Role::Voter, p.address)
        else
          p
        end
      end
      append_configuration(new_peers)
      true
    end

    def take_messages : Array({NodeID, Message})
      {% if flag?(:raft_debug) %}
        if @partitioned
          @outbox.clear
          return [] of {NodeID, Message}
        end
      {% end %}
      messages = @outbox.map do |target_id, msg|
        msg.group_id = @group_id
        {target_id, msg}
      end
      @outbox.clear
      messages
    end

    def on_configuration_change(&block : Array(Peer) ->)
      @on_configuration_change = block
    end

    # Called whenever the peer list changes — both when config entries are stored
    # in the log (followers) and when they're committed (leader). Use this to
    # register transport peer addresses from config entries.
    def on_configuration_applied(&block : Array(Peer) ->)
      @on_configuration_applied = block
    end

    def close
      @inbox.close
      @log.close
    end

    def voters : Array(Peer)
      @peers.select(&.voter?)
    end

    def learners : Array(Peer)
      @peers.select(&.learner?)
    end

    private def other_peers
      @peers.reject { |p| p.id == @id }
    end

    private def other_voters
      @peers.select { |p| p.id != @id && p.voter? }
    end

    private def quorum_size : Int32
      voters.size // 2 + 1
    end

    private def start_pre_vote
      return if @peers.empty? # standalone node, no elections
      return unless @peers.any? { |p| p.id == @id && p.voter? } # learners don't elect
      @election_tick = 0_u32
      @election_timeout = random_election_timeout
      @pre_votes_received = Set(NodeID).new
      @pre_votes_received.add(@id) # vote for self
      @metrics.try(&.increment("raft_prevote_campaigns_total"))

      other_voters.each do |peer|
        @outbox << {peer.id, Message.new(
          type: MessageType::PreVote,
          from: @id,
          term: @current_term + 1, # proposed term, not yet committed
          last_log_index: @log.last_index,
          last_log_term: @log.last_term,
        )}
      end
    end

    private def become_candidate
      old_role = @role
      @role = Role::Candidate
      @current_term += 1
      @voted_for = @id
      @election_tick = 0_u32
      @election_timeout = random_election_timeout
      @votes_received = Set(NodeID).new
      @votes_received.add(@id)
      persist_state
      @metrics.try(&.increment("raft_elections_total"))
      @metrics.try(&.increment("raft_term_changes_total", {"reason" => "election"}))
      @metrics.try(&.increment("raft_state_transitions_total", {"from" => old_role.to_s.downcase, "to" => "candidate"}))

      other_voters.each do |peer|
        @outbox << {peer.id, Message.new(
          type: MessageType::RequestVote,
          from: @id,
          term: @current_term,
          last_log_index: @log.last_index,
          last_log_term: @log.last_term,
        )}
      end
    end

    private def become_follower(leader : NodeID? = nil)
      old_role = @role
      @role = Role::Follower
      @leader_id = leader
      @election_tick = 0_u32
      @election_timeout = random_election_timeout
      @transfer_target = nil
      persist_state
      @metrics.try(&.increment("raft_state_transitions_total", {"from" => old_role.to_s.downcase, "to" => "follower"}))
    end

    private def become_leader
      @metrics.try(&.increment("raft_state_transitions_total", {"from" => "candidate", "to" => "leader"}))
      @role = Role::Leader
      @leader_id = @id
      @heartbeat_tick = 0_u32
      other_peers.each do |peer|
        @next_index[peer.id] = @log.last_index + 1
        @match_index[peer.id] = 0_u64
      end
      # Append no-op to force log convergence — truncates stale entries on followers
      @log.append(term: @current_term, entry_type: EntryType::Noop)
      advance_commit_index
      send_append_entries
    end

    private def handle_append_entries(msg : Message)
      @metrics.try(&.increment("raft_messages_received_total"))
      @metrics.try(&.increment("raft_heartbeats_received_total")) if msg.entries_count == 0
      @election_tick = 0_u32

      if msg.term < @current_term
        @metrics.try(&.increment("raft_append_entries_rejected_total", {"reason" => "stale_term"}))
        @outbox << {msg.from, Message.new(
          type: MessageType::AppendEntriesResponse,
          from: @id,
          term: @current_term,
          success: false,
          reject_hint: @log.last_index,
        )}
        return
      end

      # Candidate receiving AppendEntries from leader in same term: step down
      if @role == Role::Candidate
        become_follower(msg.from)
      else
        @leader_id = msg.from
      end

      # Check prev_log consistency
      if msg.prev_log_index > 0
        if msg.prev_log_index > @log.last_index
          @metrics.try(&.increment("raft_append_entries_rejected_total", {"reason" => "log_gap"}))
          @outbox << {msg.from, Message.new(
            type: MessageType::AppendEntriesResponse,
            from: @id,
            term: @current_term,
            success: false,
            reject_hint: @log.last_index,
          )}
          return
        end
        if @log.term_at(msg.prev_log_index) != msg.prev_log_term
          @metrics.try(&.increment("raft_append_entries_rejected_total", {"reason" => "term_mismatch"}))
          @outbox << {msg.from, Message.new(
            type: MessageType::AppendEntriesResponse,
            from: @id,
            term: @current_term,
            success: false,
            reject_hint: msg.prev_log_index - 1,
          )}
          return
        end
      end

      # Append new entries
      if msg.entries_count > 0 && msg.entries_data.size > 0
        io = IO::Memory.new(msg.entries_data)
        msg.entries_count.times do
          entry = LogEntry(T).from_io(io)
          if entry.index <= @log.last_index
            if @log.term_at(entry.index) != entry.term
              @metrics.try(&.increment("raft_log_truncations_total"))
              @log.truncate_after(entry.index - 1)
              @log.append(term: entry.term, data: entry.data, entry_type: entry.entry_type, config_data: entry.config_data)
            end
          else
            @log.append(term: entry.term, data: entry.data, entry_type: entry.entry_type, config_data: entry.config_data)
          end
          # Per Raft paper: apply configuration immediately when stored in log,
          # regardless of whether the entry is committed. Process all entries in the
          # batch — a later entry may re-add this node after an earlier one removed it.
          if entry.entry_type == EntryType::Configuration
            apply_configuration_from_entry(entry)
          end
        end
        @metrics.try(&.increment("raft_log_entries_received_total", by: msg.entries_count.to_i64))
      end

      # Update commit index and apply
      if msg.commit_index > @commit_index
        new_commit = Math.min(msg.commit_index, @log.last_index)
        apply_entries(@commit_index + 1, new_commit)
        # If we were removed and log was reset by apply_configuration, skip the update
        unless @peers.empty? && @log.last_index == 0_u64
          @commit_index = new_commit
          @metrics.try(&.increment("raft_commit_advances_total"))
          persist_state
        end
      end

      @outbox << {msg.from, Message.new(
        type: MessageType::AppendEntriesResponse,
        from: @id,
        term: @current_term,
        success: true,
        last_log_index: @log.last_index,
      )}
    end

    private def handle_append_entries_response(msg : Message)
      return unless @role == Role::Leader

      if msg.success
        # Clamp to our own log length — follower may report a higher index
        # from a previous leader's uncommitted entries
        reported_index = Math.min(msg.last_log_index, @log.last_index)
        if reported_index > @match_index.fetch(msg.from, 0_u64)
          @match_index[msg.from] = reported_index
          @next_index[msg.from] = reported_index + 1
        end
        advance_commit_index
        maybe_promote_learner(msg.from)
        if target = @transfer_target
          maybe_send_timeout_now(target) if msg.from == target
        end
      else
        @metrics.try(&.increment("raft_replication_rejections_total"))
        hint = msg.reject_hint
        @next_index[msg.from] = Math.max(hint + 1, 1_u64)
      end
    end

    private def advance_commit_index
      (@commit_index + 1..@log.last_index).reverse_each do |n|
        next unless @log.term_at(n) == @current_term
        replication_count = 0
        voters.each do |peer|
          if peer.id == @id
            replication_count += 1
          elsif @match_index.fetch(peer.id, 0_u64) >= n
            replication_count += 1
          end
        end
        if replication_count >= quorum_size
          apply_entries(@commit_index + 1, n)
          @commit_index = n
          @metrics.try(&.increment("raft_commit_advances_total"))
          persist_state
          break
        end
      end
    end

    private def apply_entries(from : UInt64, to : UInt64)
      start = Math.max(from, @last_applied + 1)
      (start..to).each do |i|
        entry = @log.get(i)
        if entry.entry_type == EntryType::Configuration
          apply_configuration(entry)
          @last_applied = i
          break if @peers.empty? # removed from cluster
        elsif data = entry.data
          @state_machine.apply(data)
          @metrics.try(&.increment("raft_entries_applied_total"))
          @last_applied = i
        else
          @last_applied = i
        end
      end
    end

    private def handle_request_vote(msg : Message)
      vote_granted = false

      # Reject votes from nodes not in our cluster
      unless @peers.any? { |p| p.id == msg.from }
        @outbox << {msg.from, Message.new(
          type: MessageType::RequestVoteResponse,
          from: @id,
          term: @current_term,
          success: false,
        )}
        return
      end

      if msg.term >= @current_term
        if @voted_for.nil? || @voted_for == msg.from
          if msg.last_log_term > @log.last_term ||
             (msg.last_log_term == @log.last_term && msg.last_log_index >= @log.last_index)
            @voted_for = msg.from
            vote_granted = true
            @election_tick = 0_u32
            persist_state
          end
        end
      end

      if vote_granted
        @metrics.try(&.increment("raft_votes_granted_total"))
      else
        @metrics.try(&.increment("raft_votes_denied_total"))
      end

      @outbox << {msg.from, Message.new(
        type: MessageType::RequestVoteResponse,
        from: @id,
        term: @current_term,
        success: vote_granted,
      )}
    end

    private def handle_request_vote_response(msg : Message)
      return unless @role == Role::Candidate
      return unless msg.term == @current_term

      if msg.success
        @votes_received.add(msg.from)
        become_leader if @votes_received.size >= quorum_size
      end
    end

    private def send_append_entries
      other_peers.each do |peer|
        send_append_entries_to(peer.id)
      end
    end

    private def send_append_entries_to(peer_id : NodeID)
      next_idx = @next_index.fetch(peer_id, @log.last_index + 1)
      prev_log_index = next_idx - 1
      prev_log_term = prev_log_index > 0 ? @log.term_at(prev_log_index) : 0_u64

      entries_io = IO::Memory.new
      entries_count = 0_u32
      (next_idx..@log.last_index).each do |i|
        entry_io = IO::Memory.new
        @log.get(i).to_io(entry_io)
        break if entries_count > 0 && entries_io.pos + entry_io.pos > @config.max_append_entries_size
        entries_io.write(entry_io.to_slice)
        entries_count += 1
      end

      @outbox << {peer_id, Message.new(
        type: MessageType::AppendEntries,
        from: @id,
        term: @current_term,
        prev_log_index: prev_log_index,
        prev_log_term: prev_log_term,
        commit_index: @commit_index,
        entries_data: entries_io.to_slice.dup,
        entries_count: entries_count,
      )}
      @metrics.try(&.increment("raft_messages_sent_total"))
      @metrics.try(&.increment("raft_heartbeats_sent_total")) if entries_count == 0
      @metrics.try(&.increment("raft_log_entries_sent_total", by: entries_count.to_i64)) if entries_count > 0
    end

    private def handle_pre_vote(msg : Message)
      vote_granted = false

      # Reject pre-votes from nodes not in our cluster
      unless @peers.any? { |p| p.id == msg.from }
        @outbox << {msg.from, Message.new(
          type: MessageType::PreVoteResponse,
          from: @id,
          term: msg.term,
          success: false,
        )}
        return
      end

      # Grant pre-vote if:
      # 1. The candidate's proposed term is at least as high as ours
      # 2. The candidate's log is at least as up-to-date as ours
      if msg.term >= @current_term
        if msg.last_log_term > @log.last_term ||
           (msg.last_log_term == @log.last_term && msg.last_log_index >= @log.last_index)
          vote_granted = true
        end
      end

      if vote_granted
        @metrics.try(&.increment("raft_prevotes_granted_total"))
      else
        @metrics.try(&.increment("raft_prevotes_denied_total"))
      end

      @outbox << {msg.from, Message.new(
        type: MessageType::PreVoteResponse,
        from: @id,
        term: msg.term,
        success: vote_granted,
      )}
    end

    private def handle_pre_vote_response(msg : Message)
      return unless msg.term == @current_term + 1 # must match our proposed term

      if msg.success
        @pre_votes_received.add(msg.from)
        become_candidate if @pre_votes_received.size >= quorum_size
      end
    end

    private def handle_timeout_now(msg : Message)
      return unless @role == Role::Follower
      return unless msg.term == @current_term
      @metrics.try(&.increment("raft_timeout_now_received_total"))
      # Skip pre-vote, go straight to candidate
      become_candidate
    end

    private def maybe_promote_learner(node_id : NodeID)
      return unless @peers.any? { |p| p.id == node_id && p.learner? }
      match = @match_index.fetch(node_id, 0_u64)
      return unless match >= @log.last_index
      promote_learner(node_id)
    end

    private def maybe_send_timeout_now(target : NodeID)
      match = @match_index.fetch(target, 0_u64)
      return unless match >= @log.last_index
      @transfer_target = nil
      @metrics.try(&.increment("raft_leadership_transfers_total", {"result" => "completed"}))
      @outbox << {target, Message.new(
        type: MessageType::TimeoutNow,
        from: @id,
        term: @current_term,
      )}
    end

    private def append_configuration(new_peers : Array(Peer))
      config_bytes = serialize_peers(new_peers)
      entry = @log.append(term: @current_term, entry_type: EntryType::Configuration, config_data: config_bytes)
      @pending_config_index = entry.index

      # Initialize replication state for any new peers
      new_peers.each do |p|
        next if p.id == @id
        unless @next_index.has_key?(p.id)
          @next_index[p.id] = 1_u64 # send full log to new peers
          @match_index[p.id] = 0_u64
        end
      end

      # Try to commit immediately (single-voter leader can commit without followers)
      advance_commit_index

      # Send to union of old and new peers so both removed and added nodes
      # receive the config entry
      all_peer_ids = Set(NodeID).new
      @peers.each { |p| all_peer_ids << p.id unless p.id == @id }
      new_peers.each { |p| all_peer_ids << p.id unless p.id == @id }
      all_peer_ids.each { |pid| send_append_entries_to(pid) }

      # Track which peers were removed before switching config
      removed = @peers.reject { |p| new_peers.any? { |np| np.id == p.id } }

      # Switch to new configuration
      @peers = new_peers
      persist_state
      @on_configuration_applied.try(&.call(@peers))

      # Clean up tracking for removed peers
      removed.each do |p|
        @next_index.delete(p.id)
        @match_index.delete(p.id)
      end
    end

    private def apply_configuration(entry : LogEntry(T))
      @pending_config_index = 0_u64
      # If removal is now committed, clean up fully
      if @peers.empty?
        @voted_for = nil
        @commit_index = 0_u64
        @last_applied = 0_u64
        @log.reset
        persist_state
      end
      @on_configuration_change.try(&.call(@peers))
    end

    # Apply configuration immediately when stored in log (followers).
    # Per Raft paper: "a server always uses the latest configuration in its log,
    # regardless of whether the entry is committed."
    private def apply_configuration_from_entry(entry : LogEntry(T))
      return if entry.config_data.empty?
      new_peers = deserialize_peers(entry.config_data)
      return if new_peers == @peers

      unless new_peers.any? { |p| p.id == @id }
        if @peers.any?
          # Was a member, now removed — step down but keep log intact.
          # Entry is uncommitted; if leader fails, a new leader may roll it back.
          # Full cleanup (log reset etc.) happens in apply_configuration at commit time.
          @peers = [] of Peer
          @role = Role::Follower
          @leader_id = nil
          persist_state
        end
        # If we were standalone (empty peers), skip — we were never a member.
        # A later config entry in this batch may add us.
        return
      end

      @peers = new_peers
      persist_state
      @on_configuration_applied.try(&.call(@peers))
    end

    # Test helper — drives persist_snapshot from outside while
    # take_snapshot doesn't exist yet (added in Task 3).
    def persist_snapshot_for_test(index : UInt64, term : UInt64)
      persist_snapshot(index, term)
    end

    private def persist_snapshot(index : UInt64, term : UInt64)
      path = File.join(@config.data_dir, "snapshot")
      tmp_path = path + ".tmp"

      File.open(tmp_path, "wb") do |f|
        f.write_bytes(index, IO::ByteFormat::LittleEndian)
        f.write_bytes(term, IO::ByteFormat::LittleEndian)
        peer_bytes = serialize_peers
        f.write_bytes(peer_bytes.size.to_u32, IO::ByteFormat::LittleEndian)
        f.write(peer_bytes)
        @state_machine.snapshot(f)
        f.fsync
      end
      File.rename(tmp_path, path)

      @snapshot_index = index
      @snapshot_term = term
    end

    private def load_snapshot : Bool
      path = File.join(@config.data_dir, "snapshot")
      return false unless File.exists?(path)

      File.open(path, "rb") do |f|
        @snapshot_index = f.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
        @snapshot_term = f.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
        peer_len = f.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
        peer_buf = Bytes.new(peer_len)
        f.read_fully(peer_buf)
        @peers = deserialize_peers(peer_buf)
        @state_machine.restore(f)
      end

      @last_applied = @snapshot_index
      @commit_index = Math.max(@commit_index, @snapshot_index)
      true
    end

    private def random_election_timeout : UInt32
      @random.rand(@config.election_timeout_min_ticks..@config.election_timeout_max_ticks)
    end

    private def persist_state
      path = File.join(@config.data_dir, "raft_meta")
      tmp_path = path + ".tmp"
      File.open(tmp_path, "wb") do |f|
        f.write_bytes(@current_term, IO::ByteFormat::LittleEndian)
        f.write_bytes(@commit_index, IO::ByteFormat::LittleEndian)
        if vf = @voted_for
          f.write_bytes(1_u8, IO::ByteFormat::LittleEndian)
          f.write_bytes(vf, IO::ByteFormat::LittleEndian)
        else
          f.write_bytes(0_u8, IO::ByteFormat::LittleEndian)
        end
        f.write_bytes(@peers.size.to_u32, IO::ByteFormat::LittleEndian)
        @peers.each { |p| p.to_io(f) }
        f.fsync
      end
      File.rename(tmp_path, path)
    end

    private def recover_state
      load_snapshot
      path = File.join(@config.data_dir, "raft_meta")
      tmp_path = path + ".tmp"
      File.delete(tmp_path) if File.exists?(tmp_path)
      return unless File.exists?(path)
      File.open(path, "rb") do |f|
        @current_term = f.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
        @commit_index = f.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
        has_vote = f.read_bytes(UInt8, IO::ByteFormat::LittleEndian)
        @voted_for = has_vote == 1_u8 ? f.read_bytes(UInt64, IO::ByteFormat::LittleEndian) : nil
        peer_count = f.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
        # raft_meta peers may be more recent than the snapshot's; prefer raft_meta.
        @peers = Array(Peer).new(peer_count) { Peer.from_io(f) }
      end
      # Replay any committed log entries past the snapshot
      if @commit_index > @last_applied
        apply_entries(@last_applied + 1, @commit_index)
      end
    end

    private def serialize_peers(peers : Array(Peer) = @peers) : Bytes
      io = IO::Memory.new
      io.write_bytes(peers.size.to_u32, IO::ByteFormat::LittleEndian)
      peers.each { |p| p.to_io(io) }
      io.to_slice.dup
    end

    private def deserialize_peers(data : Bytes) : Array(Peer)
      io = IO::Memory.new(data)
      count = io.read_bytes(UInt32, IO::ByteFormat::LittleEndian)
      Array(Peer).new(count) { Peer.from_io(io) }
    end
  end
end
