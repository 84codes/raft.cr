module Raft
  class Node(T)
    getter role : Role = Role::Follower
    getter current_term : UInt64 = 0_u64
    getter id : NodeID
    getter voted_for : NodeID? = nil
    getter leader_id : NodeID? = nil
    getter commit_index : UInt64 = 0_u64
    getter log : Log(T)

    @peers : Array(NodeID)
    @config : Config
    @state_machine : StateMachine(T)
    @outbox : Array(Message) = [] of Message
    @election_tick : UInt32 = 0_u32
    @heartbeat_tick : UInt32 = 0_u32
    @election_timeout : UInt32
    @random : Random = Random.new
    @votes_received : Set(NodeID) = Set(NodeID).new
    @next_index : Hash(NodeID, UInt64) = {} of NodeID => UInt64
    @match_index : Hash(NodeID, UInt64) = {} of NodeID => UInt64

    def initialize(@id : NodeID, @peers : Array(NodeID), @config : Config, @state_machine : StateMachine(T))
      @log = Log(T).new(@config)
      @election_timeout = random_election_timeout
      recover_state
    end

    def tick
      case @role
      when Role::Follower
        @election_tick += 1
        become_candidate if @election_tick >= @election_timeout
      when Role::Candidate
        @election_tick += 1
        become_candidate if @election_tick >= @election_timeout
      when Role::Leader
        @heartbeat_tick += 1
        if @heartbeat_tick >= @config.heartbeat_ticks
          @heartbeat_tick = 0_u32
          send_append_entries
        end
      end
    end

    def step(message : Message)
      # If message has a higher term, step down
      if message.term > @current_term
        @current_term = message.term
        become_follower(message.from)
        persist_state
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
      end
    end

    def propose(data : T) : Bool
      return false unless @role == Role::Leader
      @log.append(term: @current_term, data: data, entry_type: EntryType::Normal)
      send_append_entries
      true
    end

    def take_messages : Array(Message)
      messages = @outbox.dup
      @outbox.clear
      messages
    end

    def close
      @log.close
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

      @peers.each do |peer|
        @outbox << Message.new(
          type: MessageType::RequestVote,
          from: @id,
          term: @current_term,
          last_log_index: @log.last_index,
          last_log_term: @log.last_term,
        )
      end
    end

    private def become_follower(leader : NodeID? = nil)
      @role = Role::Follower
      @leader_id = leader
      @voted_for = nil
      @election_tick = 0_u32
      @election_timeout = random_election_timeout
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
      send_append_entries
    end

    private def handle_append_entries(msg : Message)
      @election_tick = 0_u32

      if msg.term < @current_term
        @outbox << Message.new(
          type: MessageType::AppendEntriesResponse,
          from: @id,
          term: @current_term,
          success: false,
          reject_hint: @log.last_index,
        )
        return
      end

      @leader_id = msg.from

      # Check prev_log consistency
      if msg.prev_log_index > 0
        if msg.prev_log_index > @log.last_index
          @outbox << Message.new(
            type: MessageType::AppendEntriesResponse,
            from: @id,
            term: @current_term,
            success: false,
            reject_hint: @log.last_index,
          )
          return
        end
        if @log.term_at(msg.prev_log_index) != msg.prev_log_term
          @outbox << Message.new(
            type: MessageType::AppendEntriesResponse,
            from: @id,
            term: @current_term,
            success: false,
            reject_hint: msg.prev_log_index - 1,
          )
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

      @outbox << Message.new(
        type: MessageType::AppendEntriesResponse,
        from: @id,
        term: @current_term,
        success: true,
        last_log_index: @log.last_index,
      )
    end

    private def handle_append_entries_response(msg : Message)
      return unless @role == Role::Leader

      if msg.success
        if msg.last_log_index > @match_index.fetch(msg.from, 0_u64)
          @match_index[msg.from] = msg.last_log_index
          @next_index[msg.from] = msg.last_log_index + 1
        end
        advance_commit_index
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
        @state_machine.apply(entry.data)
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

      @outbox << Message.new(
        type: MessageType::RequestVoteResponse,
        from: @id,
        term: @current_term,
        success: vote_granted,
      )
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

        @outbox << Message.new(
          type: MessageType::AppendEntries,
          from: @id,
          term: @current_term,
          prev_log_index: prev_log_index,
          prev_log_term: prev_log_term,
          commit_index: @commit_index,
          entries_data: entries_io.to_slice.dup,
          entries_count: entries_count,
        )
      end
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
