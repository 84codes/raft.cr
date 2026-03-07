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

      # For now, just respond success (replication logic in Task 10)
      @outbox << Message.new(
        type: MessageType::AppendEntriesResponse,
        from: @id,
        term: @current_term,
        success: true,
        last_log_index: @log.last_index,
      )
    end

    private def handle_append_entries_response(msg : Message)
      # Placeholder for Task 10
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
        quorum = (@peers.size + 1) / 2 + 1
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
  end
end
