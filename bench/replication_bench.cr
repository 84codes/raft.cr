require "../src/raft"
require "http/client"

struct BenchData
  getter value : String

  def initialize(@value : String)
  end

  def bytesize : Int32
    sizeof(UInt32) + @value.bytesize
  end

  def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian)
    io.write_bytes(@value.bytesize.to_u32, format)
    io.write(@value.to_slice)
  end

  def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
    size = io.read_bytes(UInt32, format)
    slice = Bytes.new(size)
    io.read_fully(slice)
    new(String.new(slice))
  end
end

class BenchStateMachine < Raft::StateMachine(BenchData)
  getter applied_count : Int64 = 0_i64

  def apply(entry : BenchData)
    @applied_count += 1
  end

  def snapshot(io : IO)
  end

  def restore(io : IO)
  end
end

class InProcessCluster
  getter nodes : Hash(Raft::NodeID, Raft::Node(BenchData))
  getter state_machines : Hash(Raft::NodeID, BenchStateMachine)

  def initialize(size : Int32 = 3, election_ticks : UInt32 = 5_u32, heartbeat_ticks : UInt32 = 100_u32)
    @nodes = {} of Raft::NodeID => Raft::Node(BenchData)
    @state_machines = {} of Raft::NodeID => BenchStateMachine
    ids = (1..size).map(&.to_u64)

    ids.each do |id|
      sm = BenchStateMachine.new
      @state_machines[id] = sm
      dir = File.tempname("raft_bench_#{id}")
      Dir.mkdir_p(dir)
      config = Raft::Config.new
      config.data_dir = dir
      config.election_timeout_min_ticks = election_ticks
      config.election_timeout_max_ticks = election_ticks
      config.heartbeat_ticks = heartbeat_ticks
      peers = ids.reject(id)
      @nodes[id] = Raft::Node(BenchData).new(id: id, peers: peers, config: config, state_machine: sm)
    end
  end

  def elect_leader(id : Raft::NodeID = 1_u64)
    5.times { @nodes[id].tick }
    deliver_all
    raise "Node #{id} is not leader" unless @nodes[id].role == Raft::Role::Leader
  end

  def deliver_all
    loop do
      any = false
      pending = [] of {Raft::NodeID, Raft::Message}
      @nodes.each do |_, node|
        node.take_messages.each do |target_id, msg|
          pending << {target_id, msg}
        end
      end
      pending.each do |target_id, msg|
        if target = @nodes[target_id]?
          target.step(msg)
          any = true
        end
      end
      break unless any
    end
  end

  def close
    @nodes.each_value(&.close)
  end
end

def make_payload(size : Int32) : String
  "x" * size
end

def format_bytes(bytes : Int32) : String
  if bytes >= 1024
    "#{"%.1f" % (bytes / 1024.0)}KB"
  else
    "#{bytes}B"
  end
end

def percentile(sorted : Array(Float64), p : Float64) : Float64
  return sorted[0] if sorted.size == 1
  rank = p / 100.0 * (sorted.size - 1)
  lower = rank.floor.to_i
  upper = rank.ceil.to_i
  weight = rank - lower
  sorted[lower] * (1 - weight) + sorted[upper] * weight
end

def compute_stats(latencies : Array(Float64), entries : Int32)
  latencies.sort!
  mean = latencies.sum / latencies.size
  p50 = percentile(latencies, 50)
  p95 = percentile(latencies, 95)
  p99 = percentile(latencies, 99)
  min = latencies.first
  max = latencies.last
  throughput = entries / (latencies.sum / 1_000_000)
  {mean: mean, p50: p50, p95: p95, p99: p99, min: min, max: max, throughput: throughput}
end

def print_results(result)
  puts "  Mean:       #{"%.1f" % result[:mean]} us"
  puts "  P50:        #{"%.1f" % result[:p50]} us"
  puts "  P95:        #{"%.1f" % result[:p95]} us"
  puts "  P99:        #{"%.1f" % result[:p99]} us"
  puts "  Min:        #{"%.1f" % result[:min]} us"
  puts "  Max:        #{"%.1f" % result[:max]} us"
  puts "  Throughput: #{"%.0f" % result[:throughput]} entries/sec"
end

# --- In-process benchmarks ---

def run_replication_bench(entries : Int32, data_size : Int32, cluster_size : Int32 = 3)
  cluster = InProcessCluster.new(size: cluster_size, heartbeat_ticks: 10000_u32)
  cluster.elect_leader
  leader = cluster.nodes[1_u64]
  payload = make_payload(data_size)

  latencies = [] of Float64

  entries.times do
    start = Time.instant
    leader.propose(BenchData.new(payload))
    cluster.deliver_all
    elapsed = Time.instant - start
    latencies << elapsed.total_microseconds
  end

  cluster.close
  compute_stats(latencies, entries)
end

def run_batch_replication_bench(entries : Int32, batch_size : Int32, data_size : Int32, cluster_size : Int32 = 3)
  cluster = InProcessCluster.new(size: cluster_size, heartbeat_ticks: 10000_u32)
  cluster.elect_leader
  leader = cluster.nodes[1_u64]
  payload = make_payload(data_size)

  latencies = [] of Float64
  batches = entries // batch_size

  batches.times do
    start = Time.instant
    batch_size.times { leader.propose(BenchData.new(payload)) }
    cluster.deliver_all
    elapsed = Time.instant - start
    latencies << elapsed.total_microseconds
  end

  cluster.close

  latencies.sort!
  mean = latencies.sum / latencies.size
  total_entries = batches * batch_size
  total_time = latencies.sum / 1_000_000
  throughput = total_entries / total_time
  {mean_per_batch: mean, throughput: throughput, batch_size: batch_size, batches: batches}
end

# --- Remote cluster benchmarks ---

def run_remote_bench(url : String, entries : Int32, data_size : Int32, verify : Bool = false)
  uri = URI.parse(url)
  client = HTTP::Client.new(uri)
  payload = make_payload(data_size)

  # Verify the node is a leader
  status = client.get("/raft/status")
  unless status.body.includes?("Leader")
    STDERR.puts "Error: #{url} is not the leader. Point --remote at the leader node."
    exit 1
  end

  latencies = [] of Float64
  errors = 0

  entries.times do |i|
    key = "bench-#{i}"

    start = Time.instant
    response = client.put("/kv/#{key}", body: payload)
    if verify
      # Poll until the key is readable (committed)
      100.times do
        get_resp = client.get("/kv/#{key}")
        break if get_resp.status_code == 200
        sleep 1.millisecond
      end
    end
    elapsed = Time.instant - start

    if response.status_code == 202
      latencies << elapsed.total_microseconds
    else
      errors += 1
    end
  end

  client.close

  puts "  Errors: #{errors}" if errors > 0
  compute_stats(latencies, latencies.size)
end

def run_remote_batch_bench(url : String, entries : Int32, batch_size : Int32, data_size : Int32)
  uri = URI.parse(url)
  client = HTTP::Client.new(uri)
  payload = make_payload(data_size)

  latencies = [] of Float64
  batches = entries // batch_size
  errors = 0

  batches.times do |b|
    start = Time.instant
    batch_size.times do |i|
      key = "batch-#{b}-#{i}"
      response = client.put("/kv/#{key}", body: payload)
      errors += 1 unless response.status_code == 202
    end
    elapsed = Time.instant - start
    latencies << elapsed.total_microseconds
  end

  client.close

  latencies.sort!
  mean = latencies.sum / latencies.size
  total_entries = batches * batch_size
  total_time = latencies.sum / 1_000_000
  throughput = total_entries / total_time

  puts "  Errors: #{errors}" if errors > 0
  {mean_per_batch: mean, throughput: throughput, batch_size: batch_size, batches: batches}
end

# --- CLI ---

data_size = 64
entries = 10000
cluster_size = 3
remote : String? = nil
verify = false

ARGV.each_with_index do |arg, i|
  case arg
  when "--data-size"
    val = ARGV[i + 1]
    if val.ends_with?("KB") || val.ends_with?("kb")
      data_size = val.rchop("KB").rchop("kb").to_i * 1024
    elsif val.ends_with?("B") || val.ends_with?("b")
      data_size = val.rchop("B").rchop("b").to_i
    else
      data_size = val.to_i
    end
  when "--entries"
    entries = ARGV[i + 1].to_i
  when "--cluster-size"
    cluster_size = ARGV[i + 1].to_i
  when "--remote"
    remote = ARGV[i + 1]
  when "--verify"
    verify = true
  when "--help"
    puts "Usage: replication_bench [options]"
    puts
    puts "In-process mode (default):"
    puts "  --data-size SIZE     Payload size per entry (e.g. 64, 4KB) [default: 64]"
    puts "  --entries N          Number of entries [default: 10000]"
    puts "  --cluster-size N     Number of nodes [default: 3]"
    puts
    puts "Remote cluster mode:"
    puts "  --remote URL         Leader URL (e.g. http://localhost:8001)"
    puts "  --verify             Read back each key to measure commit latency"
    puts "  --data-size SIZE     Payload size per entry [default: 64]"
    puts "  --entries N          Number of entries [default: 10000]"
    exit 0
  end
end

puts "=" * 60
puts "Raft Replication Benchmark"
puts "=" * 60

if url = remote
  puts "  Mode:       remote (#{url})"
  puts "  Data size:  #{format_bytes(data_size)}"
  puts "  Entries:    #{entries}"
  puts "  Verify:     #{verify}"
  puts

  # Warmup
  run_remote_bench(url, Math.min(100, entries), data_size)

  print "Single-entry replication... "
  STDOUT.flush
  result = run_remote_bench(url, entries, data_size, verify)
  puts "done"
  print_results(result)
  puts

  [10, 50].each do |batch|
    print "Batch replication (batch=#{batch})... "
    STDOUT.flush
    result = run_remote_batch_bench(url, entries, batch, data_size)
    puts "done"
    puts "  Mean/batch: #{"%.1f" % result[:mean_per_batch]} us"
    puts "  Throughput: #{"%.0f" % result[:throughput]} entries/sec"
    puts
  end
else
  puts "  Mode:         in-process"
  puts "  Data size:    #{format_bytes(data_size)}"
  puts "  Entries:      #{entries}"
  puts "  Cluster size: #{cluster_size}"
  puts

  # Warmup
  run_replication_bench(100, data_size: data_size, cluster_size: cluster_size)

  print "Single-entry replication... "
  STDOUT.flush
  result = run_replication_bench(entries, data_size: data_size, cluster_size: cluster_size)
  puts "done"
  print_results(result)
  puts

  [10, 50, 100].each do |batch|
    print "Batch replication (batch=#{batch})... "
    STDOUT.flush
    result = run_batch_replication_bench(entries, batch, data_size: data_size, cluster_size: cluster_size)
    puts "done"
    puts "  Mean/batch: #{"%.1f" % result[:mean_per_batch]} us"
    puts "  Throughput: #{"%.0f" % result[:throughput]} entries/sec"
    puts
  end
end
