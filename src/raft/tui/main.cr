require "./dashboard"

addresses = ARGV.to_a
if addresses.empty?
  addresses = [
    "http://127.0.0.1:8001",
    "http://127.0.0.1:8002",
    "http://127.0.0.1:8003",
  ]
  STDERR.puts "No addresses provided, using defaults: #{addresses.join(", ")}"
end

dashboard = Raft::TUI::Dashboard.new(addresses)
dashboard.run
