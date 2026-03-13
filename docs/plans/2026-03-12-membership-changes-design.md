# Single-Server Membership Changes

## Goal

Enable dynamic cluster membership — adding and removing nodes at runtime using single-server changes (one node at a time), with a learner phase for safe catch-up before promotion to voter.

## Architecture

Peers are tracked as an array of `Peer` structs (id + role), where role is either `Voter` or `Learner`. Self is included in the peers array. Configuration changes are committed as `EntryType::Configuration` log entries, making membership durable and recoverable.

## Data Structures

```crystal
struct Peer
  enum Role
    Voter
    Learner
  end

  getter id : NodeID
  property role : Role
end
```

- `@peers : Array(Peer)` replaces the current `Array(NodeID)`
- Self is included as a `Voter`
- Configuration entries serialize the full peers array

## Operations

### Bootstrap

- First node created with `bootstrap: true`
- Writes config entry `{peers: [{self, Voter}]}`, becomes leader of single-node cluster
- Subsequent restarts recover config from log — no bootstrap flag needed

### AddServer(node_id)

1. Leader appends config entry adding node as `Learner`
2. Leader replicates log to learner
3. Once learner's `match_index` is near leader's `last_index`, leader appends config entry promoting to `Voter`

### RemoveServer(node_id)

1. Must not be the current leader (transfer leadership first)
2. Leader appends config entry removing node from peers
3. Removed node stops receiving replication

## Quorum

Only voters count: `voters.size // 2 + 1` where `voters = @peers.select(&.role.voter?)`. Since self is in `@peers` as a voter, no separate `+1` needed.

## Replication

- **Elections:** send RequestVote/PreVote only to voters, count only voter responses
- **AppendEntries:** send to all peers (voters and learners both need log replication)
- **Quorum checks:** filter to voters only

## Recovery

On startup, replay log to find the latest `EntryType::Configuration` entry. Deserialize it to restore `@peers`. If no config entry exists (fresh node, not bootstrapped), `@peers` starts empty.

## Changes Required

### Peer struct + Role enum
- New file `src/raft/peer.cr`

### Node changes (`src/raft/node.cr`)
- `@peers` type changes from `Array(NodeID)` to `Array(Peer)`
- Self included in `@peers`
- `initialize`: accept `bootstrap` flag; if true, write initial config entry
- Quorum calculation: filter voters, `voters.size // 2 + 1`
- `start_pre_vote`, `become_candidate`: iterate only voters
- `send_append_entries`: iterate all peers
- `advance_commit_index`, `handle_request_vote_response`, `handle_pre_vote_response`: count only voters
- New methods: `add_server(node_id)`, `remove_server(node_id)`, `promote_learner(node_id)`
- `apply_entries`: handle `EntryType::Configuration` by updating `@peers`
- Recovery: scan log for latest config entry on startup

### Server changes (`src/raft/server.cr`)
- Update `add_group` to pass peers as `Array(Peer)` or adapt

### Serialization
- `Peer` needs `to_io`/`from_io` for log entry serialization
- Configuration entry data = serialized `Array(Peer)`
