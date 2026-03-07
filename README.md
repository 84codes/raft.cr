# Raft

A fast, efficient Raft consensus library for Crystal. Generic data types, disk-first persistence, deterministic core. Designed for multi-raft and future `sendfile` zero-copy support.

## Features

- **Deterministic core** — `Node(T)` driven by `tick()` and `step(message)`, no internal IO
- **Generic type `T`** — bring your own data type with `to_io`/`from_io`
- **Segmented disk log** — append-only segments with offset index
- **Multi-raft ready** — multiple raft groups share one transport
- **Abstract transport** — swap TCP for your own (e.g. embed in an AMQP broker)
- **Execution context safe** — build with `-Dpreview_mt -Dexecution_context`

## Usage

```crystal
require "raft"

# Define your data type
struct MyCommand
  getter action : String

  def initialize(@action)
  end

  def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian)
    io.write_bytes(@action.bytesize.to_u32, format)
    io.write(@action.to_slice)
  end

  def self.from_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::LittleEndian) : self
    size = io.read_bytes(UInt32, format)
    slice = Bytes.new(size)
    io.read_fully(slice)
    new(String.new(slice))
  end
end

# Implement your state machine
class MyStateMachine < Raft::StateMachine(MyCommand)
  def apply(entry : MyCommand)
    puts "Applying: #{entry.action}"
  end

  def snapshot(io : IO)
    # serialize state
  end

  def restore(io : IO)
    # restore state
  end
end

# Configure and start
config = Raft::Config.new
config.data_dir = "./raft-data"

sm = MyStateMachine.new
node = Raft::Node(MyCommand).new(
  id: 1_u64,
  peers: [2_u64, 3_u64],
  config: config,
  state_machine: sm
)

# Drive with ticks (deterministic)
node.tick
node.propose(MyCommand.new("set key=value"))
messages = node.take_messages # send these to peers via your transport
```

## Building

```sh
crystal build src/raft.cr -Dpreview_mt -Dexecution_context
```

## Testing

```sh
crystal spec -Dpreview_mt -Dexecution_context
```

## Architecture

See [design document](docs/plans/2026-03-07-raft-consensus-design.md) for full details.

## License

MIT
