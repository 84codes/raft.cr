module Raft
  class Node(T)
    getter role : Role = Role::Follower
    getter current_term : UInt64 = 0_u64
    getter id : NodeID
    getter voted_for : NodeID? = nil
    getter leader_id : NodeID? = nil
    getter commit_index : UInt64 = 0_u64
    getter log : Log(T)

    getter peers : Array(NodeID)
    getter metrics : Metrics?
    {% if flag?(:raft_debug) %}
      getter paused : Bool = false
      getter partitioned : Bool = false
    {% end %}
    getter group_id : UInt64
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

    def initialize(@id : NodeID, @peers : Array(NodeID), @config : Config, @state_machine : StateMachine(T), @metrics : Metrics? = nil, @group_id : UInt64 = 0_u64)
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
          send_append_entries
        end
      end
    end

    def step(message : Message)
      {% if flag?(:raft_debug) %}
        return if @paused
        return if @partitioned
      {% end %}

      # PreVote messages don't cause term changes — they're speculative
      unless message.type == MessageType::PreVote || message.type == MessageType::PreVoteResponse
        if message.term > @current_term
          @current_term = message.term
          become_follower(message.from)
          persist_state
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
      send_append_entries
      true
    end

    def transfer_leadership(to target : NodeID) : Bool
      return false unless @role == Role::Leader
      return false unless @peers.includes?(target)
      return false if target == @id
      @transfer_target = target
      send_append_entries # ensure target is caught up
      maybe_send_timeout_now(target)
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

    def close
      @inbox.close
      @log.close
    end

    private def start_pre_vote
      @election_tick = 0_u32
      @election_timeout = random_election_timeout
      @pre_votes_received = Set(NodeID).new
      @pre_votes_received.add(@id) # vote for self

      @peers.each do |peer|
        @outbox << {peer, Message.new(
          type: MessageType::PreVote,
          from: @id,
          term: @current_term + 1, # proposed term, not yet committed
          last_log_index: @log.last_index,
          last_log_term: @log.last_term,
        )}
      end
    end

    private def become_candidate
      @role = Role::Candidate
      @current_term += 1
      @voted_for = @id
      @election_tick = 0_u32
      @election_timeout = random_election_timeout
      @votes_received = Set(NodeID).new
      @votes_received.add(@id)
      persist_state
      @metrics.try(&.increment("raft_elections_total"))

      @peers.each do |peer|
        @outbox << {peer, Message.new(
          type: MessageType::RequestVote,
          from: @id,
          term: @current_term,
          last_log_index: @log.last_index,
          last_log_term: @log.last_term,
        )}
      end
    end

    private def become_follower(leader : NodeID? = nil)
      @role = Role::Follower
      @leader_id = leader
      @voted_for = nil
      @election_tick = 0_u32
      @election_timeout = random_election_timeout
      @transfer_target = nil
      persist_state
    end

    private def become_leader
      @role = Role::Leader
      @leader_id = @id
      @heartbeat_tick = 0_u32
      @peers.each do |peer|
        @next_index[peer] = @log.last_index + 1
        @match_index[peer] = 0_u64
      end
      # Append no-op to force log convergence — truncates stale entries on followers
      @log.append(term: @current_term, entry_type: EntryType::Noop)
      send_append_entries
    end

    private def handle_append_entries(msg : Message)
      @metrics.try(&.increment("raft_messages_received_total"))
      @metrics.try(&.increment("raft_heartbeats_received_total")) if msg.entries_count == 0
      @election_tick = 0_u32

      if msg.term < @current_term
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
              @log.truncate_after(entry.index - 1)
              @log.append(term: entry.term, data: entry.data, entry_type: entry.entry_type)
            end
          else
            @log.append(term: entry.term, data: entry.data, entry_type: entry.entry_type)
          end
        end
      end

      # Update commit index and apply
      if msg.commit_index > @commit_index
        new_commit = Math.min(msg.commit_index, @log.last_index)
        apply_entries(@commit_index + 1, new_commit)
        @commit_index = new_commit
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
        if target = @transfer_target
          maybe_send_timeout_now(target) if msg.from == target
        end
      else
        hint = msg.reject_hint
        @next_index[msg.from] = Math.max(hint + 1, 1_u64)
      end
    end

    private def advance_commit_index
      (@commit_index + 1..@log.last_index).reverse_each do |n|
        next unless @log.term_at(n) == @current_term
        replication_count = 1 # count self
        @peers.each do |peer|
          replication_count += 1 if @match_index.fetch(peer, 0_u64) >= n
        end
        quorum = (@peers.size + 1) // 2 + 1
        if replication_count >= quorum
          apply_entries(@commit_index + 1, n)
          @commit_index = n
          break
        end
      end
    end

    private def apply_entries(from : UInt64, to : UInt64)
      (from..to).each do |i|
        entry = @log.get(i)
        if data = entry.data
          @state_machine.apply(data)
          @metrics.try(&.increment("raft_entries_applied_total"))
        end
      end
    end

    private def handle_request_vote(msg : Message)
      vote_granted = false

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
        quorum = (@peers.size + 1) // 2 + 1
        become_leader if @votes_received.size >= quorum
      end
    end

    private def send_append_entries
      @peers.each do |peer|
        next_idx = @next_index.fetch(peer, @log.last_index + 1)
        prev_log_index = next_idx - 1
        prev_log_term = prev_log_index > 0 ? @log.term_at(prev_log_index) : 0_u64

        entries_io = IO::Memory.new
        entries_count = 0_u32
        (next_idx..@log.last_index).each do |i|
          @log.get(i).to_io(entries_io)
          entries_count += 1
        end

        @outbox << {peer, Message.new(
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
      end
    end

    private def handle_pre_vote(msg : Message)
      vote_granted = false

      # Grant pre-vote if:
      # 1. The candidate's proposed term is at least as high as ours
      # 2. The candidate's log is at least as up-to-date as ours
      if msg.term >= @current_term
        if msg.last_log_term > @log.last_term ||
           (msg.last_log_term == @log.last_term && msg.last_log_index >= @log.last_index)
          vote_granted = true
        end
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
        quorum = (@peers.size + 1) // 2 + 1
        become_candidate if @pre_votes_received.size >= quorum
      end
    end

    private def handle_timeout_now(msg : Message)
      return unless @role == Role::Follower
      return unless msg.term == @current_term
      # Skip pre-vote, go straight to candidate
      become_candidate
    end

    private def maybe_send_timeout_now(target : NodeID)
      match = @match_index.fetch(target, 0_u64)
      return unless match >= @log.last_index
      @transfer_target = nil
      @outbox << {target, Message.new(
        type: MessageType::TimeoutNow,
        from: @id,
        term: @current_term,
      )}
    end

    private def random_election_timeout : UInt32
      @random.rand(@config.election_timeout_min_ticks..@config.election_timeout_max_ticks)
    end

    private def persist_state
      path = File.join(@config.data_dir, "raft_meta")
      File.open(path, "wb") do |f|
        f.write_bytes(@current_term, IO::ByteFormat::LittleEndian)
        if vf = @voted_for
          f.write_bytes(1_u8, IO::ByteFormat::LittleEndian)
          f.write_bytes(vf, IO::ByteFormat::LittleEndian)
        else
          f.write_bytes(0_u8, IO::ByteFormat::LittleEndian)
        end
      end
    end

    private def recover_state
      path = File.join(@config.data_dir, "raft_meta")
      return unless File.exists?(path)
      File.open(path, "rb") do |f|
        @current_term = f.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
        has_vote = f.read_bytes(UInt8, IO::ByteFormat::LittleEndian)
        @voted_for = has_vote == 1_u8 ? f.read_bytes(UInt64, IO::ByteFormat::LittleEndian) : nil
      end
    end
  end
end
