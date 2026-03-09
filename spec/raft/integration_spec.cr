require "../spec_helper"

# Helper: deliver all pending messages between nodes until no more to deliver
def deliver_all(nodes : Hash(Raft::NodeID, Raft::Node(TestData)))
  loop do
    any_delivered = false
    pending = [] of {Raft::NodeID, Raft::Message}

    nodes.each do |id, node|
      node.take_messages.each do |target_id, msg|
        pending << {target_id, msg}
      end
    end

    pending.each do |target_id, msg|
      if target_node = nodes[target_id]?
        target_node.step(msg)
        any_delivered = true
      end
    end

    break unless any_delivered
  end
end

def make_cluster(election_ticks : UInt32 = 5_u32, heartbeat_ticks : UInt32 = 2_u32) : {Hash(Raft::NodeID, Raft::Node(TestData)), Hash(Raft::NodeID, TestStateMachine)}
  state_machines = {} of Raft::NodeID => TestStateMachine
  nodes = {} of Raft::NodeID => Raft::Node(TestData)

  [1_u64, 2_u64, 3_u64].each do |id|
    sm = TestStateMachine.new
    state_machines[id] = sm
    dir = File.tempname("raft_integration_#{id}")
    Dir.mkdir_p(dir)
    config = Raft::Config.new
    config.data_dir = dir
    config.election_timeout_min_ticks = election_ticks
    config.election_timeout_max_ticks = election_ticks
    config.heartbeat_ticks = heartbeat_ticks
    peers = [1_u64, 2_u64, 3_u64].reject(id)
    nodes[id] = Raft::Node(TestData).new(id: id, peers: peers, config: config, state_machine: sm)
  end

  {nodes, state_machines}
end

describe "Integration: 3-node Raft cluster" do
  it "elects a leader, replicates, commits, and applies" do
    nodes, sms = make_cluster

    # Tick node 1 to trigger election
    5.times { nodes[1_u64].tick }
    deliver_all(nodes)

    nodes[1_u64].role.should eq Raft::Role::Leader

    # Propose entries
    nodes[1_u64].propose(TestData.new("first"))
    nodes[1_u64].propose(TestData.new("second"))
    deliver_all(nodes)

    # Leader should have committed and applied
    sms[1_u64].applied.size.should eq 2
    sms[1_u64].applied[0].value.should eq "first"
    sms[1_u64].applied[1].value.should eq "second"

    # Tick leader to send heartbeat with updated commit_index to followers
    2.times { nodes[1_u64].tick }
    deliver_all(nodes)

    # Followers should have applied
    sms[2_u64].applied.size.should eq 2
    sms[3_u64].applied.size.should eq 2

    nodes.each_value(&.close)
  end

  it "handles leader failure and re-election" do
    nodes, sms = make_cluster

    # Elect node 1
    5.times { nodes[1_u64].tick }
    deliver_all(nodes)
    nodes[1_u64].role.should eq Raft::Role::Leader

    # Kill node 1
    old_leader = nodes.delete(1_u64).not_nil!
    old_leader.close

    # Tick only node 2 past election timeout so it starts pre-vote first
    # (avoids split vote where both candidates vote for themselves)
    5.times { nodes[2_u64].tick }
    # deliver_all handles PreVote → PreVoteResponse → RequestVote → RequestVoteResponse
    deliver_all(nodes)

    # One of the remaining should be leader
    leaders = nodes.values.select { |n| n.role == Raft::Role::Leader }
    leaders.size.should eq 1
    leaders[0].current_term.should be > 1_u64

    nodes.each_value(&.close)
  end

  it "partitioned node does not inflate term (pre-vote)" do
    nodes, sms = make_cluster

    # Elect node 1
    5.times { nodes[1_u64].tick }
    deliver_all(nodes)
    nodes[1_u64].role.should eq Raft::Role::Leader
    term_before = nodes[2_u64].current_term

    # Partition node 2
    nodes[2_u64].partition

    # Tick node 2 through many election timeouts
    50.times { nodes[2_u64].tick }
    nodes[2_u64].take_messages # drain (all dropped)

    # Term should NOT have changed
    nodes[2_u64].current_term.should eq term_before

    # Heal and verify cluster converges
    nodes[2_u64].heal
    2.times { nodes[1_u64].tick } # heartbeat
    deliver_all(nodes)

    nodes[2_u64].role.should eq Raft::Role::Follower
    nodes[2_u64].current_term.should eq term_before

    nodes.each_value(&.close)
  end

  it "uncommitted entries from old leader are overwritten after re-election" do
    nodes, sms = make_cluster(heartbeat_ticks: 100_u32)

    # Elect node 1
    5.times { nodes[1_u64].tick }
    deliver_all(nodes)
    nodes[1_u64].role.should eq Raft::Role::Leader

    # Propose and commit an entry on all nodes
    nodes[1_u64].propose(TestData.new("committed"))
    deliver_all(nodes)
    100.times { nodes[1_u64].tick } # heartbeat to propagate commit
    deliver_all(nodes)

    sms.each_value { |sm| sm.applied.size.should eq 1 }

    # Partition node 1 (leader) from the cluster
    nodes[1_u64].partition

    # Leader proposes entries that won't reach anyone
    nodes[1_u64].propose(TestData.new("uncommitted1"))
    nodes[1_u64].propose(TestData.new("uncommitted2"))
    nodes[1_u64].take_messages # drained by partition

    # Node 1 has extra uncommitted entries
    nodes[1_u64].log.last_index.should be > nodes[2_u64].log.last_index

    # Elect node 2 as new leader
    5.times { nodes[2_u64].tick }
    deliver_all(nodes)
    nodes[2_u64].role.should eq Raft::Role::Leader

    # New leader proposes an entry
    nodes[2_u64].propose(TestData.new("new_leader_entry"))
    deliver_all(nodes)
    100.times { nodes[2_u64].tick } # heartbeat to propagate commit
    deliver_all(nodes)

    # Node 3 should have the new entry (not the old leader's uncommitted ones)
    sms[3_u64].applied.size.should eq 2
    sms[3_u64].applied[1].value.should eq "new_leader_entry"

    # Heal node 1 — it should converge to node 2's log
    nodes[1_u64].heal
    100.times { nodes[2_u64].tick } # heartbeats
    deliver_all(nodes)

    # Node 1's uncommitted entries should be overwritten
    sms[1_u64].applied.size.should eq 2
    sms[1_u64].applied[1].value.should eq "new_leader_entry"

    nodes.each_value(&.close)
  end

  it "committed data survives partition and re-election" do
    nodes, sms = make_cluster

    # Elect node 1, commit some data
    5.times { nodes[1_u64].tick }
    deliver_all(nodes)

    nodes[1_u64].propose(TestData.new("important1"))
    nodes[1_u64].propose(TestData.new("important2"))
    deliver_all(nodes)
    2.times { nodes[1_u64].tick }
    deliver_all(nodes)

    # All nodes have the committed data
    sms.each_value do |sm|
      sm.applied.size.should eq 2
      sm.applied[0].value.should eq "important1"
      sm.applied[1].value.should eq "important2"
    end

    # Kill the leader
    old_leader = nodes.delete(1_u64).not_nil!
    old_leader.close

    # Elect a new leader
    5.times { nodes[2_u64].tick }
    deliver_all(nodes)
    nodes[2_u64].role.should eq Raft::Role::Leader

    # Previously committed data is still there
    sms[2_u64].applied[0].value.should eq "important1"
    sms[2_u64].applied[1].value.should eq "important2"

    # Can continue proposing
    nodes[2_u64].propose(TestData.new("important3"))
    deliver_all(nodes)

    sms[2_u64].applied.size.should eq 3
    sms[2_u64].applied[2].value.should eq "important3"

    nodes.each_value(&.close)
  end
end
