require "../spec_helper"

# Helper: deliver all pending messages between nodes until no more to deliver
def deliver_all(nodes : Hash(Raft::NodeID, Raft::Node(TestData)))
  loop do
    any_delivered = false
    pending = [] of {Raft::NodeID, Raft::Message}

    nodes.each do |id, node|
      node.take_messages.each do |msg|
        nodes.each do |target_id, _|
          next if target_id == id
          pending << {target_id, msg}
        end
      end
    end

    pending.each do |target_id, msg|
      nodes[target_id].step(msg)
      any_delivered = true
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

    # Tick only node 2 past election timeout so it becomes candidate first
    # (avoids split vote where both candidates vote for themselves)
    5.times { nodes[2_u64].tick }
    deliver_all(nodes)

    # One of the remaining should be leader
    leaders = nodes.values.select { |n| n.role == Raft::Role::Leader }
    leaders.size.should eq 1
    leaders[0].current_term.should be > 1_u64

    nodes.each_value(&.close)
  end
end
