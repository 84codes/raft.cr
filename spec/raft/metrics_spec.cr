require "../spec_helper"

describe Raft::Metrics do
  it "tracks gauges" do
    m = Raft::Metrics.new(node_id: 1_u64)
    m.set_gauge("raft_node_term", 5_i64)
    m.get_gauge("raft_node_term").should eq 5_i64
  end

  it "tracks counters" do
    m = Raft::Metrics.new(node_id: 1_u64)
    m.increment("raft_elections_total")
    m.increment("raft_elections_total")
    m.get_counter("raft_elections_total").should eq 2_i64
  end

  it "tracks histograms" do
    m = Raft::Metrics.new(node_id: 1_u64)
    m.observe("raft_append_latency_seconds", 0.005)
    m.observe("raft_append_latency_seconds", 0.0005)
    m.observe("raft_append_latency_seconds", 0.05)
    output = m.to_prometheus
    output.should contain("raft_append_latency_seconds_count{node_id=\"1\",group_id=\"0\"} 3")
  end

  it "outputs prometheus text format" do
    m = Raft::Metrics.new(node_id: 1_u64)
    m.set_gauge("raft_node_term", 5_i64)
    m.increment("raft_elections_total")
    output = m.to_prometheus
    output.should contain("raft_node_term{node_id=\"1\",group_id=\"0\"} 5")
    output.should contain("raft_elections_total{node_id=\"1\",group_id=\"0\"} 1")
  end

  it "supports labeled histograms" do
    m = Raft::Metrics.new(node_id: 1_u64)
    m.observe("raft_replication_latency_seconds", 0.001, labels: {"peer" => "2"})
    output = m.to_prometheus
    output.should contain("peer=\"2\"")
  end
end
