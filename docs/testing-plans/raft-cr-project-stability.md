# Test Plan: raft-cr-project-stability

**Date:** 2026-05-22
**Plan mode:** project-wide
**SUT:** raft.cr — embeddable Raft consensus library for Crystal (+ KV and Queue example apps)
**Change under test:** N/A — project-wide
**Plan author:** designing-distributed-system-tests skill (Claude)
**Status:** draft

## 0. Architectural summary

`raft.cr` is a Raft consensus library plus two example applications (KV, Queue). The library
is split into a **deterministic core** (no I/O except a single small metadata write and the
snapshot file) and an **I/O substrate** (mmap-backed segmented log, abstract Transport
with TCP + Memory implementations). A consuming app supplies the data type `T`, the
state machine, and drives the loop.

```
   App driver fiber                  TCPTransport
   ┌──────────────┐                  ┌───────────────────────────┐
   │ tick / step  │  outbox          │ accept fiber  ──► dispatch│
   │ take_messages├─────────────────►│ conn fiber(s) ◄──┐ per-peer│
   │ propose      │                  │ inbox channels   │ fibers  │
   └──────┬───────┘                  └──────────────────┴─────────┘
          │                                              │
          ▼ apply (direct virtual dispatch)              │
   ┌──────────────┐    ┌──────────────┐                  │
   │ StateMachine │    │ Node(T)      │                  ▼
   │ (user code)  │◄───┤  - role/term │           single TCP
   └──────────────┘    │  - log       │           connection per peer pair
                       │  - peers     │
                       └──────┬───────┘
                              │ owns
                              ▼
                       ┌──────────────┐    ┌──────────────────────────┐
                       │ Log(T)       ├───►│ Segment (one ::File per  │
                       │ recover seg. │    │   segment; append-only;  │
                       │ truncate     │    │   flush+fsync per entry) │
                       └──────────────┘    └──────────────────────────┘
   raft_meta (tmp+fsync+rename)            │
   snapshot   (tmp+fsync+rename)           ▼
                                       transport_peers (TCPTransport)
```

**Major components.** `Raft::Node(T)` is the deterministic protocol kernel — leader election
with pre-vote, AppendEntries replication with fast `reject_hint`, single-server membership
changes with learners, leadership transfer, snapshot+InstallSnapshot. `Raft::Log(T)`
orchestrates `Raft::Log::Segment(T)` files, each backed by a plain Crystal `::File`
opened append-mode; `Segment#append` calls `flush` + `fsync` per entry (commit dab4cd1
replaced the prior `MFile` mmap implementation). `Raft::Transport` is abstract;
`TCPTransport` multiplexes Raft groups by `group_id` on a single TCP connection per peer
pair; `MemoryTransport` is the in-process fake used in tests. `Raft::Server(T)` is an
optional multi-group lifecycle wrapper. `Raft::Metrics` exposes Prometheus
counters/gauges/histograms.

**Data flow (write).** App calls `node.propose(data)` on the leader → `Log#append` → leader's
outbox grows → driver drains via `take_messages` → transport sends AppendEntries → followers'
`Node#step` → `Log#append` on follower → response → leader updates `match_index` →
`advance_commit_index` (gated by term-safety rule: only entries from `@current_term` are
committable) → `apply_entries` directly invokes `StateMachine#apply`. Followers learn the
commit index from a subsequent AppendEntries (one round-trip lag).

**Where state is durable.**
- `raft_meta` (tmp + fsync + rename) — `current_term`, `commit_index`, `voted_for`, peer list.
- `snapshot` (tmp + fsync + rename) — `[index][term][peer_len][peers][sm_bytes]`.
- `*.log` segments — append-only plain files; `Segment#append` does `flush` + `fsync` per
  entry. `Segment#recover` scans and truncates any partial trailing entry, then `fsync`s.
  `Segment#truncate_to` (used by `Log#truncate_after`) also fsyncs after truncate.
- `transport_peers` — TCP peer host/port registry (TCPTransport only).

**Consensus.** Raft with pre-vote and safety-improved single-server reconfiguration
(Diego Ongaro's PhD thesis). Quorum is `voters // 2 + 1`. Learners receive replication
but don't vote.

**Trust / tenancy.** No multi-tenant model in the core. The example apps treat the
Raft groups (and HTTP endpoints) as a single trust domain. `group_id` is the only
isolation between groups on a shared transport.

**Ordering.** Linearizable per-leader for writes (canonical Raft). Reads are explicitly
**not** linearizable — no `ReadIndex` or leader lease (ARCHITECTURE §6.5).

## 1. Scope

In scope:

- `src/raft/**` — `Node`, `Log`, `Segment`, `MFile`, `Message`, `Peer`, `Config`, `Metrics`,
  `Server`, `Transport` (abstract), `TCPTransport`, `MemoryTransport`, `LogEntry`,
  `StateMachine`.
- The two example apps (`examples/kv/`, `examples/queue/`) treated as integration
  drivers — they exercise the library through realistic application code paths.
- The HTTP admin/status endpoints (`src/raft/http/handler.cr`) used by the integration
  tests, the TUI, and operations.

Out of scope (declared non-goals — see §8):

- TUI dashboard (`src/raft/tui/`) — operator UI, not a correctness surface.
- Grafana dashboards / Prometheus configs under `examples/*/grafana/`.
- Jepsen Clojure stubs at `examples/kv/jepsen/` — incomplete (no workload/checker
  defined yet). This plan recommends implementing them but treats their absence as
  a gap, not an in-scope target.
- LavinMQ integration design doc — exploratory, not yet code.

## 1b. Claims under test

| ID | Claim | Category | Source | Inferred? |
|---|---|---|---|---|
| C1 | A leader only commits entries from its own term (`@current_term`); replicating an older-term entry by majority alone does not commit it. | safety | `node.cr` `advance_commit_index` skip-if-term-mismatch; ARCHITECTURE §3 "Term safety" | no |
| C2 | A vote is granted only if the candidate's log is at least as up-to-date as the voter's (last-term-then-last-index). | safety | `handle_request_vote` / `handle_pre_vote`; ARCHITECTURE §3 "Election restriction" | no |
| C3 | At most one leader is elected in any term. | safety | Raft paper invariant; pre-vote + majority vote enforce | yes (canonical) |
| C4 | If two logs contain an entry with the same index and term, all preceding entries are identical (log matching). | safety | `handle_append_entries` truncate-on-term-mismatch | yes (canonical) |
| C5 | State-machine safety: if a node applies an entry at a given index, no other node applies a different entry at that index. | safety | derives from C1+C2+C4; oracle is the Elle / linearizability checker | yes |
| C6 | Messages with a `group_id` other than the receiving node's are dropped without state mutation. | safety | `node.cr` `step` group-id check; `node_spec.cr` "group_id validation" | no |
| C7 | After `persist_state` returns, `current_term`/`voted_for`/`commit_index`/peers survive a process crash. | durability | tmp+fsync+rename in `persist_state` | no |
| C8 | A crash mid-write of `raft_meta.tmp` leaves the prior committed `raft_meta` intact and the `.tmp` is cleaned up on recovery. | durability | `recover_state` deletes `raft_meta.tmp`; `node_spec.cr` covers happy path | no |
| C9 | Log entries appended to the segment file are durable across crash (`Segment#append` does `flush` + `fsync` per entry), and partial trailing entries (from a crash mid-append) are dropped on recovery. | durability | `Segment#append` flush+fsync; `Segment#recover` truncate-to-valid-end | no (previously partial; tightened in commit dab4cd1) |
| C10 | Snapshot files are replaced atomically (tmp + fsync + rename); a crash leaves either the prior snapshot or no snapshot. | durability | `persist_snapshot` tmp+fsync+rename | no |
| C11 | A snapshot file's leading header (`[index][term][peer_len][peers]`) matches the state-machine bytes; load is all-or-nothing. | durability | `load_snapshot`; `handle_install_snapshot` sanity check | partial |
| C12 | A cluster with a majority of healthy nodes elects a leader within ~`election_timeout_max_ticks × tick_interval` (≤ ~1 s at defaults). | liveness | `random_election_timeout`; defaults: 10–20 ticks × 50 ms | yes |
| C13 | After a leader is elected and the network is stable, every committed entry is eventually replicated to every healthy follower and applied. | liveness | `send_append_entries`, heartbeat retry | yes (canonical) |
| C14 | A learner added by `add_server` is auto-promoted to voter once `match_index == log.last_index`. | liveness | `maybe_promote_learner` | no |
| C15 | A partitioned-then-rejoined node does not force a leader step-down via term inflation (pre-vote gating). | safety / liveness | ARCHITECTURE §3 "PreCandidate"; `start_pre_vote` does not bump term | no |
| C16 | Only one configuration change can be uncommitted at a time; a second `add_server`/`remove_server`/`promote_learner` returns false until the first commits. | membership | `@pending_config_index > @commit_index` guard | no |
| C17 | A configuration entry is applied to a follower's in-memory peer set as soon as it is stored in the log (not when committed), with rollback when the entry is uncommitted and replaced. | membership | `apply_configuration_from_entry`; ARCHITECTURE §3 "Configuration entries applied immediately" | no |
| C18 | `bootstrap` succeeds iff the node has no peers (empty list) and writes a single-entry configuration log; subsequent `bootstrap` calls fail. | operational | `bootstrap` method | no |
| C19 | A node removed by a committed configuration entry resets its log (`log.reset`) and clears `commit_index` / `last_applied`; until commit, the log is intact and the node has stepped down. | membership | `apply_configuration` + `apply_configuration_from_entry` | no |
| C20 | `recover_state` after a crash restores last-persisted `current_term`/`voted_for`/`commit_index`/peers, or starts at term 0 with no vote if no `raft_meta` exists. | durability | `recover_state` | no |
| C21 | A leftover `raft_meta.tmp` on startup is removed and does not corrupt subsequent writes. | durability | `recover_state` deletes `.tmp` | no |
| C22 | A follower rejects AppendEntries whose `prev_log_index`/`prev_log_term` does not match its log, returning a `reject_hint` that drives the leader's `next_index` backward. | ordering | `handle_append_entries` consistency check | no |
| C23 | Entries are applied to the state machine in strictly increasing index order, exactly once per index. | ordering / idempotency | `apply_entries` `start = max(from, @last_applied + 1)` | no |
| C24 | Learners receive AppendEntries but do not start elections and are not counted toward quorum. | membership | `start_pre_vote` voter check; `quorum_size` only counts voters | no |
| C25 | `RequestVote`/`PreVote` from a node not in the local peer set are rejected. | safety | `handle_request_vote`, `handle_pre_vote` non-member check | no |
| C26 | `remove_server` is rejected if it would leave zero voters; `remove_server(self)` is always rejected. | membership | `remove_server` guards | no |
| C27 | A single AppendEntries `entries_data` is ≤ `max_append_entries_size`, except that one entry is always included even if it alone exceeds the limit. | performance | `send_append_entries_to`; `node_spec.cr` batch tests | no |
| C28 | An InstallSnapshot chunk's `entries_data` is ≤ `snapshot_chunk_size`. | performance | `send_install_snapshot_to` | no |
| C29 | Transport per-peer outbox and per-group inbox channels are bounded (size 64) and drop on full; drops are counted in `raft_transport_outbox_drops_total{peer}` / `raft_transport_inbox_drops_total{peer}`. | performance | `TCPTransport` `route_to_peer`, `handle_connection` | no |
| C30 | The metric set documented in ARCHITECTURE §9 is exported by `Raft::Metrics.to_prometheus`, every metric labelled with `node_id` and `group_id`. | observability | `metrics.cr` + ARCHITECTURE §9 | no |
| C31 | `Message.from_io` raises if `entries_data` size exceeds `max_message_payload_bytes` (defends against malformed/malicious senders). | safety | `message.cr` `from_io` payload check | no |
| C32 | TCPTransport persists `(peer_id, host, port)` to `transport_peers` and recovers it on restart. | operational | `persist_peers` / `recover_peers` | no |
| C33 | Per-queue FIFO order: published messages on a single queue group are delivered in the order the Raft log committed them. | ordering (queue example) | `QueueStateMachine#apply` Deque semantics | no |
| C34 | Every successfully published queue message is replicated to every replica's `QueueStateMachine` (via Raft commit + apply). | durability (queue example) | derives from C13 | yes |
| C35 | A queue `Consume` request commits a `Consume` log entry; the result is delivered exactly once via the per-`req_id` channel on the leader; followers silently drop the result. | ordering / idempotency (queue) | `apply` and `register_request`; `queue_state_machine_spec.cr` | no |
| C36 | KV writes to a new key create a value group lazily via the meta group; the meta group's membership defines cluster membership for new value groups. | operational (kv) | `examples/kv/src/main.cr`; `MetaStateMachine` | no |
| C37 | KV HTTP handler forwards writes received by a non-leader to the current leader's HTTP endpoint; the application client is responsible for retry on lost ack. | operational (kv) | `kv_http_handler.cr` proxy logic; ARCHITECTURE §7 | no |
| C38 | Reads from the local node's state machine are best-effort eventually consistent; no `ReadIndex` or leader lease is implemented. | (negative) consistency | ARCHITECTURE §6.5 explicit gap | no |
| C39 | Raft messages between any peer pair travel a single TCP connection (head-of-line ordered at the TCP layer; per-group inbox-channel may drop on full). | ordering | ARCHITECTURE §6 "Single TCP connection per peer pair" | partial — explicit in arch doc, no test |
| C40 | `StateMachine#apply` is invoked synchronously on the driver fiber; blocking I/O in `apply` will stall ticks and message processing for that group. | (negative) operational | ARCHITECTURE §6 "Direct virtual dispatch on apply" | partial |

## 1c. Missing claims discovered

| ID | Behavior the code relies on | Source / evidence | Suggested action |
|---|---|---|---|
| M1 | **`snapshot.tmp` survival across crashes is undefined.** `handle_install_snapshot` opens `snapshot.tmp` in `"ab"` (append) mode when `last_log_index > 0`; if the receiver crashed during a previous transfer and the leader retries from a non-zero offset, stale partial bytes will be appended to. `recover_state` does not delete `snapshot.tmp` (it only cleans `raft_meta.tmp`). | `node.cr` `handle_install_snapshot`, `recover_state` | Either (a) delete `snapshot.tmp` on startup, or (b) always restart InstallSnapshot from offset 0 after recovery, or (c) document the expected behavior. Add a test that crashes a follower mid-InstallSnapshot and verifies the next transfer produces a consistent snapshot. |
| M2 | ~~**Segment writes are not fsynced per append.**~~ **FIXED in commit dab4cd1** — `Segment#append` now calls `@file.flush` + `@file.fsync` after every entry; `Segment#truncate_to` and `Segment#recover` also fsync. The prior `MFile` mmap implementation is gone. The residual concern is **fsync amplification under load** (one fsync per appended entry, no batching) — covered by S16 (perf SLO gate) and by a new followup in §9 about whether to batch fsyncs across an AppendEntries batch. | `src/raft/log/segment.cr` (current code); commit dab4cd1 | Decide whether to batch fsync per `Log#append_batch` for throughput. The correctness gap is closed. |
| M3 | **`take_snapshot` runs synchronously inside `apply_entries`.** A large `StateMachine#snapshot` (e.g. multi-GB) will stall the apply loop, blocking commits, ticks (if the driver is single-fiber), and the inbox drain. | `node.cr` `apply_entries` calls `take_snapshot` inline | Document that `snapshot()` must complete within the heartbeat interval to avoid election timeouts on this node. Add a perf scenario asserting snapshot time stays below a budget. |
| M4 | **Unsynchronized read of `@peers` from outside the dispatch fiber.** `TCPTransport#peer_address?` reads `@peers` directly even though writes happen on the dispatch fiber via `process_command`. A reader may see a torn `Hash` mutation under reconfiguration churn. | `tcp_transport.cr` comment "mostly stable after bootstrap" | Move reads onto the dispatch fiber (via a command), or use an `Atomic` snapshot, or restrict the read to call sites that own the dispatch fiber. |
| M5 | **`raise` inside `handle_install_snapshot` is unhandled.** The header-vs-body sanity check `raise "InstallSnapshot: header (...) != body (...)"` propagates up the `step` call chain; the driver loop does not catch it. Consequences (process exit? fiber death silently? group-dead state?) are not documented. | `node.cr` `handle_install_snapshot` | Replace with a logged error + reject response so the leader retries. Add a fuzz test for the header/body mismatch case. |
| M6 | **`T#from_io` exceptions during AppendEntries deserialization are not handled.** A malformed entry or a custom-type bug causes `Message.entries_data` parsing to raise; the failure is not isolated to a single entry. | `node.cr` `handle_append_entries` | Wrap per-entry parsing in `begin/rescue`; on failure, reject the batch with a typed error. |
| M7 | **`commit_index` is persisted on every advance.** Canonical Raft only requires `current_term` and `voted_for` to be durable; persisting `commit_index` adds disk write traffic and creates a recovery edge case where a persisted `commit_index` could exceed the recovered `log.last_index` (e.g. recover_state reads meta first, then log; if meta has commit_index=N but the log only recovered up to N-1 due to torn-tail loss, the gap is silent). | `node.cr` persists `commit_index` in `persist_state`; `recover_state` does not validate `commit_index <= last_index` | Validate on recovery; clamp `commit_index` to `log.last_index` and log a warning if they diverge. |
| M8 | **No documented backpressure semantics when the per-group inbox is full.** Drops are counted but the behavior at the protocol level (followers missing heartbeats → spurious elections) is not characterized. | `tcp_transport.cr` `handle_connection`; metric `raft_transport_inbox_drops_total` | Add a scenario that saturates a node's inbox under load and measures election rate / commit lag. |
| M9 | **No documented bound on snapshot file size.** Chunks are bounded by `snapshot_chunk_size`, but `Message.from_io`'s `max_message_payload_bytes` (64 MB) check applies per-message, not per-snapshot. A misconfigured chunk size > 64 MB would fail decoding silently (caught only by the connection-fiber's `rescue`). | `config.cr`, `node.cr` `send_install_snapshot_to` | Document/clamp `snapshot_chunk_size ≤ max_message_payload_bytes`. |
| M10 | **Reconfiguration ack semantics for the client are undefined.** When a leader appends a config entry and immediately advances commit (single-voter quorum), the caller observes `true` synchronously; but if the leader fails after appending but before quorum, the client has no signal that the config did not commit. | `add_server`, `remove_server` return `Bool` for "leader accepted", not "committed" | Document the difference between "accepted at the leader" and "committed cluster-wide", or add an async confirmation channel. |

§1c is intentionally exhaustive: every row is a place where docs and code have drifted
or where an implicit assumption isn't tested. These are first-class outputs of the plan.

## 2. SUT model

- **Tenancy / isolation:** None at the library level. Raft groups on the same transport
  are isolated only by `group_id` field in `Message`. Out-of-group messages are dropped
  silently with a log warning. Applications running on the same OS process share fibers,
  memory, and the data directory namespace.
- **Persistence:** Two metadata files (`raft_meta`, `transport_peers`) and one snapshot
  file (`snapshot`) are written tmp+fsync+rename. Log segment files (`*.log`) are plain
  append-only files; `Segment#append` calls `flush` + `fsync` per entry, so log entries
  are durable before the call returns. On crash, recovery scans each segment and truncates
  partial trailing entries (then fsyncs). Prior MFile/mmap path is gone (commit dab4cd1).
- **Replication / consensus:** Standard Raft with pre-vote and safety-improved single-
  server membership changes. Quorum = `voters // 2 + 1`. Multi-raft groups share a
  transport, each with its own log and persistence.
- **Ordering:** Linearizable per-leader (for writes that complete via the canonical
  propose → commit → apply path). Reads have no ordering guarantee beyond "this replica
  applied entries in commit order" — stale reads are explicitly permitted (C38).
- **Network boundaries:** Inbound — TCP socket → `TCPTransport.handle_connection` →
  per-group inbox channel (size 64, drops on full). Outbound — node's outbox →
  shared outbox → per-peer outbox channel (size 64, drops on full) → single TCP conn.
- **Retry / idempotency contract:** Raft retries naturally on every heartbeat or
  election timeout. Application clients must be idempotent themselves (the Queue
  example's per-`req_id` bridge is one such pattern). There is no built-in client-side
  retry library.
- **Observability:** Prometheus metrics per `(node_id, group_id)`; HTTP status/admin
  endpoint at `src/raft/http/handler.cr`; logs via Crystal's `::Log`. The TUI consumes
  the HTTP admin endpoints.

## 3. Existing test inventory

Grouped by subsystem. Source: `spec/`, `examples/kv/spec/`, `examples/queue/spec/`,
`bench/`.

| Test / harness | Subsystem | Invariants pinned | Failure modes it catches |
|---|---|---|---|
| `spec/raft/node_spec.cr` (~40 `it`s) | Node — election, replication, commit, membership, persistence | initial state, election timeout w/ pre-vote, leader heartbeats, AppendEntries batch limit, persistence of term/vote, atomic `persist_state`, all membership ops (add/remove/promote/bootstrap), group_id validation | synchronous protocol bugs; happy-path only — no real network, no real crash |
| `spec/raft/integration_spec.cr` | 3-node cluster lifecycle | end-to-end election, replication, leader failure & re-election, leadership transfer, partition + commit survival, pre-vote term-inflation (debug build) | synthetic isolation via `node.partition` (raft_debug only); no real TCP fault |
| `spec/raft/snapshot_spec.cr` | Snapshot create/restore + InstallSnapshot | persists & reloads, takes snapshot at interval, applies + replays past index, drops compacted segments, chunked snapshot transfer, AppendEntries after InstallSnapshot | happy-path snapshot; no transfer-mid-crash, no corruption injection |
| `spec/raft/log_spec.cr` + `log/segment_spec.cr` | Log + Segment + recovery | append, segment rotation, truncate (within/across segments), recovery of partial trailing entry, multi-segment recovery, append after recovery | tail-truncation recovery (happy path); no torn-write at non-tail; no FS-level fsync-loss; no ENOSPC mid-append |
| `spec/raft/log_entry_spec.cr`, `message_spec.cr` | Serialization round-trips | round-trip stable; `Message.from_io` size check | no fuzz of malformed inputs; no random-byte injection |
| `spec/raft/transport/tcp_transport_spec.cr` | TCPTransport | send/receive, peer registration via command channel, start/stop | basic; no connection drops, no slow peers, no real partition (no iptables/tc), no port exhaustion |
| `spec/raft/transport/memory_transport_spec.cr` | MemoryTransport | message delivery, isolate/heal | only the test fake |
| `spec/raft/server_spec.cr` | Server multi-group | ticker fires, message routing | trivial; no group churn under load |
| `spec/raft/http/handler_spec.cr` | HTTP admin/status | JSON shape, prometheus format, pause/resume (debug) | endpoint syntax; no concurrent admin races |
| `spec/raft/state_machine_spec.cr`, `metrics_spec.cr` | StateMachine, Metrics | snapshot round-trip; metric accumulation/format | unit-level only |
| `examples/queue/spec/queue_integration_spec.cr` | Queue end-to-end | replicates publishes FIFO, consume bridge delivery, empty-queue nil | happy path only |
| `examples/queue/spec/queue_state_machine_spec.cr`, `queue_command_spec.cr`, `meta_state_machine_spec.cr` | Queue state machines + commands | FIFO, snapshot round-trip, command framing, meta-group ID assignment | unit-level only |
| `examples/kv/spec/kv_state_machine_spec.cr` | KV state machine | apply put/get/delete, snapshot round-trip | unit-level only |
| `bench/replication_bench.cr` | Performance benchmark | single-entry latency percentiles, batch throughput, remote-HTTP latency | exploratory only — no SLO gates, no regression baseline |
| `examples/kv/jepsen/project.clj`, `run.sh` | Jepsen harness scaffold | (none — no workload/checker defined) | nothing; Clojure shell only |

**Coverage shape:** strong on synchronous, deterministic-protocol unit tests of the
core; thin on **real-world fault scenarios** (no fsync injection, no real partition,
no clock skew, no Jepsen-style linearizability checker), thin on **soak/perf**
(no SLO gates, no >5 s tests), absent on **fuzzing** (no malformed-message or
random-history exploration).

## 4. Failure-mode hypotheses

Grouped by subsystem. Each row tagged with the claim(s) it could falsify and the
pitfall(s) it instantiates (Pn = entry in `common-distributed-systems-pitfalls.md`).

### Node / protocol core

- **H1. Leader Completeness under partition + crash + re-election.** A leader commits
  entries; a minority partition isolates them; a new leader is elected; the partition
  heals and the old leader returns. Old leader's uncommitted entries on quorum
  followers must be truncated (C4, C22). Could falsify **C1, C4, C5**. Suspected
  because: `advance_commit_index` correctness depends on term-safety; the heal path
  combined with `apply_configuration_from_entry` and `match_index` clamping
  (`Math.min(msg.last_log_index, @log.last_index)`) is intricate. Pitfall: **P3**.
- **H2. State-machine divergence after filesystem-level fsync loss.** `Segment#append`
  now fsyncs per entry (M2 closed in code), but a buggy or misconfigured filesystem /
  block device (or `dm-flakey drop_writes`) can still lose fsynced data. A follower
  acks an AppendEntries, the fsync returns, but the underlying write is lost; on
  recovery its log is shorter than it acknowledged; the leader's `match_index` is now
  ahead of the truth. Could falsify **C5, C9**. Pitfall: **P7**.
- **H3. Linearizability violation under repeated partitions and elections (Elle).**
  Concurrent client writes against a 5-node cluster under randomized partitions could
  surface a history that admits no linearizable explanation. Could falsify **C1–C5**.
  Pitfall: **P3, P4**.
- **H4. Pre-vote term-inflation regression under real TCP partition.** Debug-build
  `node.partition` tests pre-vote; under real `iptables` partition + real TCP
  connection retry, a node may still bump term (or pre-vote may starve under TCP
  RTO). Could falsify **C15**. Pitfall: **P3**.
- **H5. Membership change race with leader crash.** `add_server(4)` is accepted by
  the leader; leader crashes before commit; new leader is elected without the
  uncommitted config; node 4 saw the config entry, applied it (C17), and after
  rollback has stale peer state. Could falsify **C16, C17, C19**. Pitfall: **P6**.
- **H6. Concurrent reconfiguration retry deadlock.** Operator retries `add_server`
  during the commit window; second call returns false; if the original config never
  commits (e.g. quorum loss), the cluster is wedged with no operator-visible
  unblocking path. Could falsify **C16**, surfaces **M10**. Pitfall: **P6**.
- **H7. `commit_index` recovery exceeds log length.** Crash between `persist_state`
  (which wrote `commit_index = N`) and segment append; on recovery `commit_index` may
  exceed `log.last_index`. Could falsify **C20**, surfaces **M7**. Pitfall: **P7**.
- **H8. InstallSnapshot mid-transfer crash → `snapshot.tmp` corruption.** Follower
  receives N-1 chunks, crashes; leader retries from offset N-1 (`"ab"` mode); stale
  partial bytes from prior crash are appended to. Could falsify **C10, C11**,
  surfaces **M1**. Pitfall: **P7**.
- **H9. Apply-loop stall on `take_snapshot` blocks heartbeats.** A leader running
  a multi-GB snapshot stalls inside `apply_entries`; followers time out and start
  pre-vote campaigns; leader steps down. Could falsify **C12** (liveness target),
  surfaces **M3**. Pitfall: **P14**.
- **H10. Removed-self with uncommitted removal then re-add.** Leader removes node 3
  (uncommitted), node 3 sees config, sets peers=[], log intact. Leader fails before
  commit. New leader elected (containing node 3's now-stale state); next config entry
  re-adds node 3. Does node 3 recover correctly? Could falsify **C17, C19**.
  Pitfall: **P6**.
- **H11. `T#from_io` exception aborts AppendEntries handling.** A custom data type
  with a bug in `from_io` raises; `handle_append_entries` does not isolate; entire
  batch fails partway; follower's log is now ambiguous w.r.t. what was applied.
  Surfaces **M6**.

### Log / Segment / MFile

- **H12. Torn write at non-tail position.** Segment recovery only handles partial
  entry at the file end. A corruption mid-file (bit-flip, sector failure between two
  already-fsynced entries) is not detected by the recovery scan — `from_io` will
  either decode garbage as a plausible entry or raise; in the latter case recovery
  silently drops every entry past the corruption. Could falsify **C9**. Pitfall: **P7**.
- **H13. Segment rollover decision under crash.** `Log#append` rolls a new segment
  when `current_segment.size + entry.bytesize > max_segment_size`. A crash between
  `append` returning (entry durable on segment N) and the next call's rollover check
  must not corrupt the on-disk layout: segment N's last entry is durable, segment
  N+1 hasn't been created yet, recovery picks up cleanly. Could falsify **C9**.
- **H14. Truncate-then-append round-trip.** `truncate_after(N)` then `append` (after
  a follower receives a divergent leader) must preserve last_index = N+1, and the
  resulting on-disk state must round-trip across restart. The truncate path calls
  `File#truncate` + `fsync` (commit bdf970c made this crash-safe); the cross-restart
  variant under crash *between* `truncate` and the following `append` is still worth
  exercising. Could falsify **C4, C9**.
- **H15. `ENOSPC` mid-append leaves segment offsets inconsistent.** `Segment#append`
  writes the entry, then `flush` / `fsync`, then mutates `@offsets`/`@size`/`@count`.
  If `flush` or `fsync` raises (e.g. ENOSPC, EIO, NFS server gone), the file may
  contain a partial entry while in-memory state still reflects the pre-append count.
  Could falsify **C9**.

### Transport (TCP + Memory)

- **H16. Head-of-line blocking across raft groups on the shared TCP connection.** A
  large InstallSnapshot on group A stalls heartbeats on group B sharing the connection;
  group B's leader is suspected as dead → election. Could falsify **C13** (under
  multi-raft); confirms the ARCHITECTURE §6 documented cost. Pitfall: **P14, P16**.
- **H17. Inbox-channel saturation triggers spurious elections.** A follower under
  burst load drops AppendEntries on `raft_transport_inbox_drops_total`; election
  timeout fires; pre-vote campaign starts. Could falsify **C13**, surfaces **M8**.
  Pitfall: **P16**.
- **H18. `Message.from_io` denial-of-service via oversized payload.** A malicious or
  buggy peer sends an `entries_data` size of (max_payload − 1), repeated, exhausting
  memory. `from_io` allocates `Bytes.new(data_size)` after the size check passes.
  Could falsify **C31** (in the sense that the bound exists but the per-connection
  burst is unbounded). Pitfall: outside catalog (DoS).
- **H19. Peer registry torn-read under reconfiguration churn.** A reader of
  `@peers` via `peer_address?` races a dispatcher write of a new peer entry; the
  Crystal `Hash` is not lock-free. Surfaces **M4**.
- **H20. Connection re-establish on flap.** A peer connection drops; outbox fiber
  attempts to send; `get_connection` re-dials. Under repeated flap, do we leak
  TCPSockets, leak fibers, or thrash? Could regress **C29** (drop counting).

### Snapshot + InstallSnapshot

- **H21. Snapshot interval triggers snapshot during membership change.** Crossing
  `snapshot_interval_entries` while a configuration entry sits uncommitted leads to
  a snapshot that captures the new peers (because `apply_configuration_from_entry`
  ran) but the entry may roll back. Could falsify **C11, C17**. Pitfall: **P7**.
- **H22. Two concurrent InstallSnapshot streams from the same leader to different
  followers race on the on-disk file.** The leader reads `File.open(path).seek(offset)`
  for each chunk; if a new snapshot is taken between chunks, the file content for
  one in-flight stream diverges from its already-sent prefix. Could falsify **C11**.

### Membership

- **H23. Removed leader keeps replicating until commit.** A leader removes itself
  (rejected at the API per C26 — but a re-added node could be removed mid-leader). The
  protocol's transition path needs verifying under real fault.
- **H24. Learner auto-promotion under partial connectivity.** Learner matches leader's
  index but the response is lost; `maybe_promote_learner` only fires when the leader
  observes `match_index >= log.last_index`. Could falsify **C14**.

### Multi-raft (Server)

- **H25. Multi-raft tick fairness under heavy single-group load.** `Server#start_ticker`
  ticks every group serially in one fiber; a slow `apply` on one group delays ticks on
  all others, causing election timeouts. Could falsify **C12** across groups, surfaces
  **M3**. Pitfall: **P14**.
- **H26. Group churn (add/remove groups) under traffic.** No existing test;
  `Server#add_group` is not idempotent and has no atomicity with respect to the
  ticker fiber. Could falsify operational soundness.

### Queue example

- **H27. Queue consume request loss across leader handoff.** A `Consume` is proposed;
  the bridge `pending[req_id]` lives on the leader's heap; leader fails mid-commit;
  new leader's bridge has no entry; HTTP client times out though the Consume committed.
  Could falsify **C35**. Pitfall: **P10, P5**.
- **H28. Per-queue FIFO violation under split-brain heal.** A network partition during
  a publish burst; followers' QueueStateMachines initially diverge from leader's; after
  heal, do all replicas converge to the same FIFO order? Should hold by C33+C13, but
  worth a test. Pitfall: **P3**.
- **H29. Meta-group group_id reuse after restart.** `MetaStateMachine` assigns
  increasing group IDs; on restart it must restore the counter from snapshot/log.
  Existing test verifies snapshot round-trip but not partition + restart of meta
  group. Could falsify **C36**. Pitfall: **P9**.

### KV example

- **H30. KV write proxy to old leader after failover.** Non-leader proxies write to
  the leader at the *currently-cached* address; if leadership has just changed, the
  proxy goes to the old leader who returns "not leader" — client sees 5xx without a
  typed retry hint. Could falsify nothing strict (C38 admits this) but observes the
  client-side gap. Pitfall: **P5**.

### Performance / SLO

- **H31. p99 commit latency regression.** A change anywhere in the hot path can
  blow up p99 without breaking a unit test. Need a regression baseline. Could
  falsify implicit SLO claim.
- **H32. Memory leak under long-running multi-raft.** Cumulative segment
  files, fibers, mmap regions could accumulate during long uptime.

### Configuration

- **H33. Pathological timeouts.** `election_timeout_min_ticks > election_timeout_max_ticks`,
  or `heartbeat_ticks ≥ election_timeout_min_ticks`, should be rejected at config
  time or behave gracefully. Currently no validation.
- **H34. Single-node cluster + `bootstrap` + crash before first heartbeat.** A
  bootstrapped node writes a config entry, advances commit, persists state, then
  crashes. On recovery is it still leader (term 1) with a valid log? Could falsify
  **C18, C20**.

### Idempotency / replay

- **H35. Replay across snapshot boundary.** `recover_state` calls `load_snapshot`
  then `apply_entries(@last_applied + 1, @commit_index)`. The state machine must
  receive entries in `(snapshot_index, commit_index]`, exactly once. Could falsify
  **C23**.

## 5. Coverage matrix

Two-table split because there are 40 claims × 35 hypotheses. The summary gives
the at-a-glance view; the detail keeps the gap-kind granularity.

### 5a. Per-claim summary

| Claim | Status today | Worst-case gap | Top scenario(s) to close |
|---|---|---|---|
| C1 (term safety on commit) | partial (synthetic only) | no real-network + crash variant | S01 |
| C2 (election restriction) | partial | no fuzzed-log scenarios | S03 |
| C3 (at-most-one-leader) | not directly tested | no oracle counts leaders | S03 |
| C4 (log matching) | partial | no real-network truncate scenario | S01 |
| C5 (state-machine safety) | **not covered** | no Elle/linearizability checker | **S03** |
| C6 (group_id isolation) | covered (unit) | — | — |
| C7 (persistent state durability) | partial | no fsync-loss / crash injection | **S05** |
| C8 (no torn meta) | covered (happy-path) | no real crash | S05 |
| C9 (log entry durability) | code-side fixed (per-append fsync, commit dab4cd1); FS-level fsync-loss and mid-file torn writes still untested | no FS fault injection; no mid-file corruption injection | **S06** |
| C10 (snapshot atomicity) | partial | no mid-write crash | S07 |
| C11 (snapshot self-consistency) | partial | no header/body mismatch fuzz | S07 |
| C12 (election liveness bound) | partial | no real-net measurement | S03 |
| C13 (replication liveness) | partial | no real-net under fault | S03, S08 |
| C14 (learner auto-promote) | covered (synthetic) | no real-net partial fail | S09 |
| C15 (pre-vote term-inflation prevention) | covered (synthetic) | no real-TCP scenario | S04 |
| C16 (one-config-at-a-time) | covered (unit) | — | — |
| C17 (config applied at log-store) | covered (unit) | no rollback test under real fault | S09 |
| C18 (bootstrap exclusivity) | covered (unit) | — | — |
| C19 (removed-self stepdown / commit-time log reset) | covered (unit) | no crash variant | S09 |
| C20 (recover_state) | partial | no crash mid-persist | S05 |
| C21 (raft_meta.tmp cleanup) | covered (unit) | — | — |
| C22 (AppendEntries consistency check) | covered (unit) | — | — |
| C23 (apply-once, in-order) | covered (unit) | replay-across-snapshot-boundary not directly | S10 |
| C24 (learners don't vote) | covered (unit) | — | — |
| C25 (no vote to non-members) | covered (unit) | — | — |
| C26 (cannot remove only voter / self) | covered (unit) | — | — |
| C27 (AppendEntries size cap) | covered (unit) | — | — |
| C28 (snapshot chunk size cap) | partial (indirectly via tests) | — | — |
| C29 (bounded channels + drop metrics) | partial | no saturation scenario | S11 |
| C30 (metrics export) | partial | no end-to-end Prom scrape under load | S15 |
| C31 (max payload check) | covered (unit) | no fuzz | S12 |
| C32 (transport_peers persistence) | not tested | no restart variant | S05 |
| C33 (queue FIFO) | covered (happy-path) | no partition variant | S13 |
| C34 (queue replication) | covered (happy-path) | no fault variant | S13 |
| C35 (queue Consume bridge exactly-once) | covered (unit) | no leader-handoff test | S13 |
| C36 (KV lazy group creation) | covered (unit) | no fault variant | S14 |
| C37 (KV leader proxy) | not tested under fault | no failover during proxy | S14 |
| C38 (eventual consistency for reads — negative) | (negative claim, not testable as a guarantee) | clarify in docs | — |
| C39 (single TCP connection per peer-pair, HOL ordered) | not tested | no HOL scenario | S11 |
| C40 (apply() blocking blocks group — negative) | not tested | demonstrate; document | S15 |

### 5b. Per-hypothesis detail

| H | Claims | Existing test | Verdict | Gap kind | Scenario |
|---|---|---|---|---|---|
| H1 | C1,C4,C5 | integration_spec partition tests (debug-only) | partial | no fault-injection variant | S01 |
| H2 | C5,C9 | none | not covered | no FS-level fsync-loss test | S06 |
| H3 | C1–C5 | none | not covered | no oracle (Elle/Knossos) | S03 |
| H4 | C15 | node_spec pre-vote (debug-only) | partial | no fault-injection variant | S04 |
| H5 | C16,C17,C19 | node_spec rejects concurrent | partial | no leader-crash variant | S09 |
| H6 | C16 (M10) | none | not covered | shallow / no fault | S09 |
| H7 | C20 (M7) | persistence spec (happy) | partial | no crash-mid-persist | S05 |
| H8 | C10,C11 (M1) | snapshot_spec happy path | partial | no crash-mid-InstallSnapshot | S07 |
| H9 | C12 (M3) | none | not covered | no scale variant | S15 |
| H10 | C17,C19 | node_spec removed-node | partial | no crash mid-rollback | S09 |
| H11 | M6 | none | not covered | no fuzz | S12 |
| H12 | C9 | segment_spec tail-recovery | partial | no non-tail corruption | S06 |
| H13 | C9 | log_spec rotation (happy) | partial | shallow | S06 |
| H14 | C4,C9 | log_spec truncate (in-process) | partial | no cross-restart | S06 |
| H15 | C9 | none | not covered | shallow | S06 (property) |
| H16 | C13 | none | not covered | no scale variant | S11 |
| H17 | C13 (M8) | none | not covered | no fault-injection variant | S11 |
| H18 | C31 | message_spec size check | partial | no DoS/fuzz | S12 |
| H19 | M4 | none | not covered | no concurrency stress | S11 |
| H20 | C29 | none | not covered | no scale variant | S11 |
| H21 | C11,C17 | none | not covered | no fault | S07 |
| H22 | C11 | none | not covered | no concurrency | S07 |
| H23 | C17 | node_spec removed-node | partial | no crash variant | S09 |
| H24 | C14 | node_spec auto-promotion | partial | no partial-connectivity variant | S09 |
| H25 | C12 (M3) | server_spec ticks (trivial) | partial | no scale variant | S15 |
| H26 | (operational) | none | not covered | no test | S15 |
| H27 | C35 | queue_state_machine_spec (unit) | partial | no leader-handoff | S13 |
| H28 | C33 | queue_integration_spec (happy) | partial | no partition variant | S13 |
| H29 | C36 | meta_state_machine_spec (unit) | partial | no restart variant | S13 |
| H30 | C37 | none (kv proxy not tested under fault) | not covered | shallow | S14 |
| H31 | (perf SLO) | bench (no gate) | partial | no SLO gate | S16 |
| H32 | (perf) | none | not covered | no soak | S17 |
| H33 | (config) | none | not covered | no test | S12 (property) |
| H34 | C18,C20 | node_spec bootstrap | partial | no crash variant | S05 |
| H35 | C23 | snapshot_spec apply-past-index | partial | combined with crash recovery | S05, S10 |

Verdict legend: covered / partial / not covered.
Gap-kind legend: no test / shallow test / oracle too weak / no fault-injection variant /
no scale variant / no oracle.

## 6. Technique selection

Seven techniques in combination, chosen for the project's surface area.

### T1. Jepsen + Elle (linearizability + cycle detection)
- **Hypotheses addressed:** H1, H3, H5, H6, H10, H27, H28, H29.
- **What it catches that other techniques miss:** randomized concurrent client histories
  against a real-network multi-node cluster with nemesis-driven partitions/crashes,
  checked against a linearizability oracle (Knossos / Porcupine register or Elle for
  list/map). The existing test suite cannot produce this kind of finding — no oracle.
- **Reference:** `references/jepsen-and-elle.md`.
- **Cost:** ~2–3 weeks to author the Clojure workload + checker; ~10–30 min per run
  in CI; multi-hour for serious soak.

### T2. Crash-recovery + upgrade
- **Hypotheses addressed:** H2, H7, H8, H21, H34, H35; and the fsync-loss / torn-write
  surface in general.
- **What it catches:** durability bugs that survive happy-path tests because the OS
  page cache delivers; mmap/segment recovery edge cases at non-tail positions;
  `snapshot.tmp` survival semantics; `commit_index > last_index` recovery edge.
- **Reference:** `references/crash-recovery-and-upgrade.md`.
- **Cost:** ~1 week to add `dm-flakey` / `libfaketime` / kill-9 harness;
  scenarios run in 1–5 min each.

### T3. Chaos / fault injection on real transport
- **Hypotheses addressed:** H1, H4, H16, H17, H20, H22, H30.
- **What it catches:** TCP-level behaviors invisible at the `MemoryTransport`
  level — connection RTOs vs. heartbeat period, asymmetric partitions, slow-network
  HOL on shared TCP connection, peer-fiber leak on flap.
- **Reference:** `references/chaos-and-fault-injection.md`.
- **Cost:** runs inside the Jepsen Docker harness — reuses T1's plumbing.

### T4. Deterministic simulation of the protocol core
- **Hypotheses addressed:** H1, H3, H5, H7, H8, H10, H11, H21, H22, H29.
- **What it catches:** because `Node(T)` is already a step/tick state machine with no
  internal I/O, scenarios can be replayed bit-exact from a seed. Schedules of
  message delivery + crash points + reconfigurations + snapshot triggers can be
  generated and minimized. This is the highest leverage technique for this codebase
  — the library is *already designed* for deterministic simulation; nothing in the
  catalog matches it better.
- **Reference:** `references/deterministic-simulation.md`.
- **Cost:** ~1–2 weeks to author the simulator (workload + scheduler + checker);
  millions of seeds in minutes.

### T5. Property + metamorphic
- **Hypotheses addressed:** H12, H13, H14, H15, H18, H19, H33; serialization round-trips.
- **What it catches:** invariants like "every entry that is in segment N is at offset
  `@offsets[index - first_index]`", "round-trip via `to_io`/`from_io` is identity",
  "after `truncate_after(N); append(X)`, `get(N+1) == X`", "configuration space
  validation rejects nonsensical timeouts".
- **Reference:** `references/property-and-metamorphic.md`.
- **Cost:** continuous; runs as part of `crystal spec`.

### T6. Performance + benchmarking with SLO gates
- **Hypotheses addressed:** H31, H32; supports H9, H16, H25.
- **What it catches:** regressions in commit latency, throughput, memory, segment-file
  count growth that don't break unit tests.
- **Reference:** `references/performance-and-benchmarking.md`.
- **Cost:** baseline run + CI gate; soak runs nightly.

### T7. Fuzzing
- **Hypotheses addressed:** H11, H18, H15 (input-space exhaustion).
- **What it catches:** `Message.from_io` and `LogEntry.from_io` paths under random
  bytes (malformed wire data); state-machine command serialization under random
  bytes; segment recovery under random bit-flip injection.
- **Reference:** `references/fuzzing.md`.
- **Cost:** one-time harness; CPU-bounded thereafter.

## 6b. Environment requirements

| Requirement | Version floor | Used by scenarios | Install hint |
|---|---|---|---|
| Crystal compiler | 1.10+ (`-Dpreview_mt -Dexecution_context`) | all | from `crystal-lang.org` or `asdf` |
| Crystal `-Draft_debug` flag (for `pause`/`resume`/`partition`/`heal`/`reset`) | — | S01, S04, S08 (synthetic variants), S15 | already conditionally compiled |
| docker + docker compose | 2.20+ | S01, S03, S04, S07, S08, S11, S13, S14, S16, S17 | `apt-get install docker.io docker-compose-plugin` |
| Clojure + Leiningen (Jepsen) | Clojure 1.11, lein 2.10, Jepsen 0.3.5 | S01, S03, S07, S13 | `lein` from leiningen.org; Jepsen as project dep |
| iptables + tc/netem | any modern Linux | S03, S04, S08, S11 | usually pre-installed; needs `CAP_NET_ADMIN` |
| libfaketime | any | S15 (clock skew under multi-raft tick fairness) | `apt-get install libfaketime` |
| dm-flakey (device-mapper) | any modern Linux kernel | S06 (fsync loss) | requires root + loopback device |
| Pumba / Toxiproxy | latest | S08 (alternative to tc when running without root) | docker images |
| Loopback fs / `O_DIRECT` capable | — | S06 alternative | brought up by SUT scripts |
| Prometheus + Grafana | bundled in `examples/*/docker-compose.yml` | S03, S15, S16 | already in the example compose stacks |
| Crystal `Spec` runner | bundled | S05 (in-process), S10, S12, S07 simulator variant | — |
| Python 3.10+ (Hypothesis or shrinking helper) | optional | S12 (property variants), S10 (history shrinker) | `pip install hypothesis` |

For each, mark "brought up by SUT" if it's already wired into the example compose
stacks; the executing skill should prefer that path.

## 7. Scenarios

Sixteen scenarios. Each closes one or more rows in §5. Names encode the claim each
scenario targets.

---

### S01: linearizable_writes_under_partition_and_crash

- **Falsifies if it FAILs:** C1, C4, C5, C13.
- **Closes:** H1.
- **Technique:** T1 (Jepsen + Elle).
- **Workload:** 5-node KV cluster (`examples/kv`) with 8 concurrent clients, mixed
  read/write/cas to a single register key. Rate ~200 ops/s/client. Duration 5 min.
- **Faults:** Jepsen nemesis schedule — random partitions (majority/minority,
  asymmetric) interleaved with kill-9 + restart of one node every 30–60 s.
- **Oracle:** Knossos linearizability checker over the recorded history (single
  register model). Secondary: every replica's KV state machine identical at shutdown.
- **Observability required:** Per-op invoke/complete timestamps, op result, node
  contacted; cluster snapshot at shutdown.
- **Exit criteria:** PASS if checker accepts the history *and* state machines
  converge after partition heal + 10 s settle. FAIL on any non-linearizable history.
  INCONCLUSIVE if the workload never reaches >100 ops/s sustained (env issue).
- **Target test file:** `examples/kv/jepsen/src/raft_cr_kv/workload.clj` +
  `examples/kv/jepsen/src/raft_cr_kv/nemesis.clj` (new — currently stub).
- **Skeleton language:** clojure
- **Skeleton:**
  ```clojure
  ;; AUTO-GENERATED from test plan: docs/testing-plans/raft-cr-project-stability.md
  ;; Scenario: S01 linearizable_writes_under_partition_and_crash
  ;; Falsifies: C1, C4, C5, C13 (see §1b of the plan)
  ;;
  ;; REVIEW BEFORE TREATING AS A PERMANENT REGRESSION:
  ;; this file was generated by executing-distributed-system-tests
  ;; in author mode.
  (ns raft-cr-kv.workload
    (:require [jepsen [client :as client] [checker :as checker]
               [generator :as gen] [nemesis :as nemesis]]
              [jepsen.checker [linearizable :as lin]]
              [knossos.model :as model]))
  (defn workload [opts]
    {:client     ;; TODO: HTTP client against examples/kv on port 8001-8005
                 nil
     :nemesis    ;; TODO: partition-random-halves + kill-restart, mixed
                 nil
     :generator  ;; TODO: mixed r/w/cas on single key, gen/stagger 5ms
                 nil
     :checker    (checker/compose
                   {:lin    (lin/checker (model/register))
                    :stats  (checker/stats)})})
  ```

#### §7.M — Model / history / checker discipline
- **Model under test:** register (single key, read/write/cas).
- **Operation history:** default 11 fields; `node seen` records which of the 5 nodes
  the client connected to; recording mechanism = in-process Jepsen client +
  server-side audit via the HTTP `/raft/status` snapshot at shutdown.
- **Checker:** Knossos linearizability checker on the register model
  (`references/oracle-patterns.md` checker picker: model=register × claim=linearizability
  → Knossos).
- **Nemesis + landing evidence:** Jepsen `partition-random-halves` (iptables-based)
  + Jepsen `kill` (SIGKILL on `raft-cr` process). Landing evidence — iptables
  packet-drop counter on each victim node ≥ 1 during the partition window; the
  killed process's PID disappears in `pgrep` output.
- **Ambiguous outcomes:** HTTP 5xx during partition → `timeout_marker = true`,
  `complete_ts = null`; duplicate responses (HTTP retry) → recorder error.
- **Reduction plan:** if FAIL, minimize to 3 nodes, single partition window, single
  client, deterministic Jepsen seed; then classify SUT / harness / checker / env.
  An SUT-classified FAIL on a 5-second history is a P0 finding.

---

### S02: at_most_one_leader_per_term_under_split_vote

- **Falsifies if it FAILs:** C2, C3, C15.
- **Closes:** H3 (subset), H4.
- **Technique:** T4 (deterministic simulation).
- **Workload:** 5-node cluster, no client writes; just election timers firing under
  controlled message scheduling.
- **Faults:** Simulator induces split-vote conditions (election timers fire near-
  simultaneously, votes arrive in different orders to different candidates). Then
  delivers a pre-vote campaign by a partitioned-and-rejoined node 100 elections later.
- **Oracle:** Invariant — `forall term, |{n : n.role = Leader && n.current_term = term}| ≤ 1`,
  checked after every message delivery. Secondary: a partitioned-then-rejoined node
  never causes another node to step down via `term > @current_term`.
- **Observability required:** Per-step state dump (term, role, voted_for) of every
  node.
- **Exit criteria:** PASS if 100 000 simulated seeds yield no violation. FAIL on any
  seed that produces two leaders in one term. Each seed runs in <1 s.
- **Target test file:** `spec/raft/simulation/at_most_one_leader_spec.cr` (new).
- **Skeleton language:** crystal
- **Skeleton:**
  ```crystal
  # AUTO-GENERATED from test plan: docs/testing-plans/raft-cr-project-stability.md
  # Scenario: S02 at_most_one_leader_per_term_under_split_vote
  # Falsifies: C2, C3, C15
  require "../spec_helper"
  describe "S02: at_most_one_leader_per_term_under_split_vote" do
    it "never elects two leaders in one term across 100k random schedules" do
      100_000.times do |seed|
        # TODO: instantiate 5 Node(TestData) with deterministic Random(seed)
        # TODO: schedule_messages_with_random_delay(rng)
        # TODO: assert leaders_per_term invariant after each step
      end
    end
  end
  ```

#### §7.M
- **Model under test:** membership-table (the leaders-per-term set).
- **Operation history:** Per-step records `(step_id, node_id, role, term, voted_for)`;
  in-process recorder.
- **Checker:** custom invariant — `forall term, ≤ 1 leader`; an Elle/Knossos checker
  is not needed because this is a structural invariant over state, not over op history.
- **Nemesis:** none — adversarial scheduling is the fault. Landing evidence: scheduler
  log shows the targeted interleaving was produced.
- **Ambiguous outcomes:** none — simulator is deterministic.
- **Reduction plan:** failing seed shrunk by `crystal spec --error-trace`; min-seed
  is the canonical reproducer. Classify SUT vs. harness via "does the same seed fail
  on the simulator's reference Raft (TLA+ translation) too?".

---

### S03: linearizable_appends_with_random_faults_elle

- **Falsifies if it FAILs:** C1, C2, C3, C4, C5.
- **Closes:** H3.
- **Technique:** T1 (Jepsen + Elle) + T3 (chaos).
- **Workload:** 5-node KV cluster; 10 concurrent clients append unique tokens to a
  list-valued key (one key, many writes); read returns full list. Duration 10 min.
- **Faults:** Combination — partitions every 20 s, kill+restart every 60 s,
  packet loss 5% (tc/netem) every 10 s.
- **Oracle:** Elle's list-append checker over the recorded history. Looks for
  G0/G1a/G1b/G1c/G2 cycles. Secondary: post-run, every replica's list is bitwise-
  equal to the leader's at quiescence.
- **Observability required:** Elle history file; per-node `/raft/status` snapshot
  every 1 s.
- **Exit criteria:** PASS if Elle reports no anomaly. FAIL otherwise.
- **Target test file:** `examples/kv/jepsen/src/raft_cr_kv/append_workload.clj` (new).
- **Skeleton:** as S01 with Elle's `:append` workload.

#### §7.M
- **Model under test:** log (list-append).
- **Operation history:** Elle's standard schema (process, type, fn, value).
- **Checker:** Elle, list-append profile.
- **Nemesis + landing evidence:** Jepsen `partition-random-halves` + `kill` + tc
  netem; landing — tc reports configured loss profile in `tc -s qdisc`; iptables
  packet counter for the partition variant.
- **Ambiguous outcomes:** Elle classifies timeouts as `:info` (unknown outcome);
  duplicates are an error in the recorder.
- **Reduction plan:** reduce to 3 nodes, single-key single-fault, deterministic Jepsen
  seed. Classify per executing-skill rubric.

---

### S04: pre_vote_prevents_term_inflation_under_tcp_partition

- **Falsifies if it FAILs:** C15.
- **Closes:** H4.
- **Technique:** T3.
- **Workload:** 5-node KV cluster, idle (no client writes), just heartbeats.
- **Faults:** iptables drops *outbound* packets from node 5 to the rest of the
  cluster for 60 s, while inbound is permitted (asymmetric partition that classic
  Raft is famously vulnerable to). Repeat 10×.
- **Oracle:** During the 60-s window, node 5's `current_term` must not advance (pre-
  vote denies). After heal, no other node's term increased due to node 5's rejoin.
- **Observability required:** `raft_term_changes_total{reason}` time series.
- **Exit criteria:** PASS if `raft_term_changes_total{reason="higher_term"}` on
  nodes 1–4 is 0 across all partitions. FAIL if any term advanced because of node 5.
- **Target test file:** `examples/kv/jepsen/src/raft_cr_kv/prevote_workload.clj`.

#### §7.M
- **Model under test:** membership-table (which terms exist).
- **Operation history:** per-node `(timestamp, current_term, role)` 1 Hz polling
  via `/raft/status`.
- **Checker:** custom — assert `term_delta(node 1..4) == 0` across partition window.
- **Nemesis + landing evidence:** asymmetric iptables drop. Landing — iptables
  `OUTPUT` chain `DROP` rule counter ≥ 1 during the window; verified by `iptables -L -v -n`.
- **Ambiguous outcomes:** if a non-victim node's term advances for an unrelated
  reason (e.g. real election due to load), classify as INCONCLUSIVE and retry.
- **Reduction plan:** if FAIL, isolate to a 3-node cluster, single asymmetric
  partition window, no client load.

---

### S05: durability_survives_crash_mid_persist_state

- **Falsifies if it FAILs:** C7, C8, C18, C20, C32.
- **Closes:** H7, H34, partial H35.
- **Technique:** T2 (crash-recovery).
- **Workload:** Single-node bootstrap; loop — propose 100 KV writes; SIGKILL the
  process; restart; verify `current_term`, `voted_for`, `commit_index`, peer list
  match the most recent committed state. Run 1000 iterations.
- **Faults:** SIGKILL at a uniformly random point in each iteration (sub-µs
  granularity controlled by `setitimer`).
- **Oracle:** After every restart: `recover_state` succeeds; recovered `commit_index
  ≤ log.last_index`; recovered `current_term ≥` previously-observed term; no
  `raft_meta.tmp` exists; if `transport_peers` was persisted, peers are recovered.
- **Observability required:** Pre-kill state snapshot via debug API; post-restart
  snapshot.
- **Exit criteria:** PASS if 1000/1000 iterations recover consistently. FAIL on
  any inconsistency (`commit_index > log.last_index`, lost vote, peer list
  truncated, leftover `.tmp`).
- **Target test file:** `spec/raft/crash_recovery/persist_state_durability_spec.cr` (new).
- **Skeleton:** Crystal driver + bash supervisor.

#### §7.M
- **Model under test:** register (the persistent metadata).
- **Operation history:** `(iter_id, kill_offset_ns, pre_state, post_state)` recorded
  by the supervisor.
- **Checker:** structural invariants on `post_state` (see Oracle).
- **Nemesis + landing evidence:** SIGKILL via `kill -9`. Landing — `wait()` returns
  status code SIGKILL.
- **Ambiguous outcomes:** if `raft_meta` doesn't exist post-restart (kill was before
  the first fsync), this is the valid empty-state path and not a failure.
- **Reduction plan:** if FAIL, narrow the kill offset to the failing μs-range with
  bisection; classify SUT vs env.

---

### S06: log_durability_under_fsync_loss_and_torn_writes

- **Falsifies if it FAILs:** C9, C5.
- **Closes:** H2, H12, H13, H14, H15.
- **Technique:** T2.
- **Background:** `Segment#append` now calls `flush` + `fsync` per entry (commit
  dab4cd1), so application-level "we forgot to fsync" is closed. This scenario
  therefore targets **filesystem / block-device-level fsync loss** (rarer, but
  the property that matters under partial-disk-failure recovery) plus mid-file
  torn writes and ENOSPC handling.
- **Workload:** 3-node cluster on `dm-flakey` device-mapper devices. Leader writes
  10 000 entries; followers ack after each batch. Periodically the device is set to
  "drop_writes" for 5 s windows (fsync returns success, but bytes are lost).
- **Faults:**
  - (a) `dm-flakey drop_writes` simulates a filesystem/disk that loses post-fsync
    writes.
  - (b) After workload, SIGKILL all nodes; restart; verify state-machine equivalence.
  - (c) Bit-flip injection at random offsets in random non-tail positions of one
    `.log` file; verify `Segment#recover` either detects the corruption or
    deterministically reports the corrupted index.
  - (d) ENOSPC variant — mount the data dir on a tight loopback fs; fill it; observe
    `Segment#append`'s behavior when `flush`/`fsync` raises mid-append (H15).
- **Oracle:** After restart, every node's `last_applied` ≤ leader's commit_index at
  kill time; every applied entry on any node matches the entry from the leader's
  log; for variant (c), corruption either detected (preferred) or — if not detected —
  surfaces in inter-replica divergence; for variant (d), `@offsets` / `@size` /
  `@count` stay consistent with the on-disk file after the failed append.
- **Observability required:** Per-node log dumps; `dmsetup status` for dm-flakey
  state.
- **Exit criteria:** PASS if no replica diverges semantically after fsync-loss heals.
  FAIL on divergence. INCONCLUSIVE if dm-flakey isn't available on the test host
  (mark M2 as unverified-this-run).
- **Target test file:** `spec/raft/crash_recovery/log_durability_spec.cr` driving a
  shell harness under `tools/dm-flakey-driver.sh`.

#### §7.M
- **Model under test:** log (list-append, per-replica).
- **Operation history:** `(entry_index, entry_term, entry_bytes_hash)` per replica.
- **Checker:** custom — entries with matching `(index, term)` must have matching
  payload bytes across replicas (log-matching at byte granularity).
- **Nemesis + landing evidence:** dm-flakey `drop_writes` window. Landing —
  `dmsetup status flakey-disk` reports `drop_writes` mode; subsequent `fsync`
  syscalls return success but writes are lost (validated by writing a sentinel byte
  and observing it absent after kill+restart).
- **Ambiguous outcomes:** writes during the drop window are "unknown" — followers
  may or may not have them on restart; both outcomes are consistent with Raft as
  long as inter-replica matching holds.
- **Reduction plan:** if FAIL, isolate to single-segment workload (entries × ≤ N
  to stay below `max_segment_size`); minimize drop window length. Classify SUT
  (raft.cr durability) vs env (filesystem peculiarity).

---

### S07: snapshot_transfer_consistent_under_crash_and_concurrent_take

- **Falsifies if it FAILs:** C10, C11, C13, C17.
- **Closes:** H8, H21, H22. Surfaces M1, M9.
- **Technique:** T1 + T2 (chaos within a Jepsen run + targeted crash injection).
- **Workload:** 3-node cluster; node 3 is wedged offline. Leader accumulates >> 
  `snapshot_interval_entries` writes (KV puts to 1 000 keys). Leader takes snapshot.
  Node 3 comes back online; InstallSnapshot begins.
  - Variant A: kill node 3 mid-transfer (after 50% of chunks); restart; observe
    the next transfer.
  - Variant B: trigger a new snapshot on the leader during in-flight transfer to
    node 3.
- **Faults:** Jepsen kill + restart at controlled offsets in the snapshot transfer;
  controlled trigger of a new leader snapshot.
- **Oracle:** Post-transfer, node 3's state-machine SHA256 matches the leader's at
  the snapshot's index. No `snapshot.tmp` left on disk after recovery. No raised
  exception from `handle_install_snapshot` propagates out of the driver loop.
- **Observability required:** `raft_install_snapshot_sent_total`, log lines for
  each chunk, on-disk file list before/after kill.
- **Exit criteria:** PASS if SHA256 matches in both variants. FAIL on mismatch,
  leftover `.tmp`, or driver-loop crash.
- **Target test file:** `examples/kv/jepsen/src/raft_cr_kv/snapshot_workload.clj` +
  helper Crystal driver.

#### §7.M
- **Model under test:** log (the snapshot is a log-prefix surrogate).
- **Operation history:** chunk-level events: `(node, chunk_offset, chunk_size,
  is_last, success)`; final SHA256 of state machine bytes.
- **Checker:** SHA256 equality — strong oracle. No standard checker (Elle/Knossos)
  applicable; the structural invariant suffices.
- **Nemesis + landing evidence:** Jepsen `kill` at offset within InstallSnapshot
  stream. Landing — the killed process's last log line shows `handle_install_snapshot`
  was active mid-chunk; file size of `snapshot.tmp` is between 0 and total snapshot
  size.
- **Ambiguous outcomes:** if a new snapshot rolls in (variant B) before node 3
  completes, node 3 should restart the transfer from offset 0 with the new
  index/term; if it instead appends to the old `.tmp`, that's a FAIL.
- **Reduction plan:** if FAIL, reduce to a 2-node cluster (1 leader + 1 catching-up
  follower), single chunk size, single kill offset. Classify SUT.

---

### S08: liveness_under_repeated_leader_kill

- **Falsifies if it FAILs:** C12, C13.
- **Closes:** part of H1 (liveness arm), supports H17.
- **Technique:** T3 + T6 (perf-style measurement).
- **Workload:** 5-node KV cluster, 1 client at 100 ops/s on a single key.
- **Faults:** Every 30 s, SIGKILL the current leader; wait 5 s; restart. Run 30 min.
- **Oracle:** End-to-end client commit latency p99 stays bounded — at most 1.5 ×
  `election_timeout_max_ticks × tick_interval` (≈ 1.5 s at defaults). At least 95 %
  of operations succeed; the cluster always recovers a leader within 5 s of each kill.
- **Observability required:** Per-op latency histogram, `raft_state_transitions_total`,
  `raft_elections_total`, time-to-recover.
- **Exit criteria:** PASS if oracle holds across the 30-min window. FAIL if any
  kill window leaves the cluster leaderless > 5 s, or if p99 > budget.
- **Target test file:** `examples/kv/jepsen/src/raft_cr_kv/liveness_workload.clj`.

#### §7.M
§7.M: not applicable (no gated claim category falsified — this is a liveness/SLO
scenario, the threats are time-window and rate, oracle is an SLO threshold not a
history check).

---

### S09: membership_change_safe_under_leader_crash

- **Falsifies if it FAILs:** C16, C17, C19.
- **Closes:** H5, H6, H10, H23, H24.
- **Technique:** T1 + T2 + T4 (deterministic-simulator variant for shrinking).
- **Workload:** 5-node cluster + 1 candidate node; operator loops `add_server(N)` /
  `remove_server(N)` / `promote_learner(N)` on the leader at 1 Hz; concurrent client
  writes at 50 ops/s.
- **Faults:** SIGKILL the leader at a uniformly random point during each
  reconfiguration. Pause the candidate node briefly (`raft_debug` builds — use
  `node.pause`/`resume`) during learner-catchup window.
- **Oracle:**
  - After heal: every committed configuration entry is present on every replica.
  - No uncommitted configuration entry leaves a node with stale `peers` and no
    way to recover.
  - Acked client writes remain committed (durable).
- **Observability required:** Per-node peer list snapshot every 1 s; `raft_log_truncations_total`.
- **Exit criteria:** PASS if 1000 reconfiguration ops under fault yield no
  invariant violation. FAIL on any violation.
- **Target test file:** `examples/kv/jepsen/src/raft_cr_kv/membership_workload.clj` +
  `spec/raft/simulation/membership_under_crash_spec.cr`.

#### §7.M
- **Model under test:** membership-table.
- **Operation history:** `(timestamp, node, peers_hash, role, term)` 1 Hz polling
  + every `on_configuration_change` callback fires an event.
- **Checker:** custom — final-state convergence + invariant "every committed config
  entry appears on every replica's persisted state".
- **Nemesis + landing evidence:** SIGKILL of leader, `node.pause` of candidate;
  landing — leader PID disappears; paused candidate has `paused == true` visible at
  `/raft/status`.
- **Ambiguous outcomes:** an `add_server` call that returns `true` but is killed
  before committing is "unknown"; the post-recovery replicas must show the entry
  either everywhere or nowhere.
- **Reduction plan:** if FAIL, replay the schedule in the deterministic simulator
  (T4) to shrink to minimal seed; classify SUT.

---

### S10: apply_once_in_order_across_snapshot_boundary

- **Falsifies if it FAILs:** C23.
- **Closes:** H35.
- **Technique:** T5 (property test in-process).
- **Workload:** In-process; a `StateMachine` that records every `(index, payload)`
  it sees on `apply`. Drive proposals, force snapshots, restart from snapshot.
- **Faults:** None — controlled test asserting the invariant.
- **Oracle:** The recorded sequence at the recovered SM matches the leader's
  committed sequence exactly, in order, with no gaps and no duplicates.
- **Observability required:** SM's recorded `(index, payload)` list.
- **Exit criteria:** PASS if 100 randomized sequences (different snapshot points,
  different commit/apply timings) all show in-order, exactly-once apply.
- **Target test file:** `spec/raft/apply_invariants_spec.cr`.

#### §7.M
- **Model under test:** log (apply order).
- **Operation history:** `(index, payload, when_applied)`.
- **Checker:** structural — strictly increasing index, no duplicates, matches
  expected.
- **Nemesis:** none. §7.M reduction: not applicable (no fault).

---

### S11: tcp_transport_under_chaos_no_unbounded_state

- **Falsifies if it FAILs:** C13, C29, C39.
- **Closes:** H16, H17, H19, H20. Surfaces M4, M8.
- **Technique:** T3 + T6.
- **Workload:** 5-node KV cluster, 200 ops/s, 3 raft groups (KV: meta + 2 value
  groups).
- **Faults:** Toxiproxy/Pumba — alternating connection drops, 30% packet loss for
  10-s windows, 200 ms latency injection. Concurrently, `add_server`/`remove_server`
  every 10 s on group 0 (stresses M4 — peer registry concurrent read).
- **Oracle:**
  - Cluster makes progress (commit rate ≥ 50 ops/s averaged over each minute).
  - `raft_transport_outbox_drops_total` / `raft_transport_inbox_drops_total`
    are bounded relative to load (drops are expected under chaos, but the cluster
    must not stall).
  - No fiber/thread leak (process RSS bounded over the run); no goroutine-like
    fiber explosion in `pmap` of the process.
  - No assertion failure or unhandled exception in any node's logs.
  - No segfault from `peer_address?` torn-read (M4).
- **Observability required:** Prometheus full scrape, process metrics
  (`ps -o rss,nlwp`).
- **Exit criteria:** PASS if all four oracle clauses hold over 30 min. FAIL on any
  stall, leak, or crash.
- **Target test file:** `examples/kv/jepsen/src/raft_cr_kv/chaos_workload.clj`.

#### §7.M
§7.M: not applicable (no gated claim category falsified directly; this is a
liveness + resource-leak scenario, oracle is SLO + process invariants).

---

### S12: serialization_and_message_parser_fuzz

- **Falsifies if it FAILs:** C31. Surfaces M6, M9.
- **Closes:** H11, H15, H18, H33 (config-space property variant).
- **Technique:** T7 (fuzzing) + T5.
- **Workload:** Run libFuzzer or Crystal-side random byte generator against:
  - `Message.from_io` with random byte sequences up to 1 MB.
  - `LogEntry.from_io` with random bytes.
  - `Peer.from_io` with random bytes.
  - Custom-type `T#from_io` for KV and Queue commands.
  - `Config` with random valid-shape but pathological values (e.g.
    `election_timeout_min > election_timeout_max`).
- **Faults:** None — purely input-driven.
- **Oracle:** No panic, no infinite loop, no `OOM` for any byte sequence ≤ payload
  max; every parser either succeeds with a valid value or raises a typed error;
  `Config` validation is added (currently absent — see H33).
- **Observability required:** Crash backtraces; coverage map.
- **Exit criteria:** PASS if 24 CPU-hours of fuzzing yield no new crash. FAIL on
  any unexpected panic.
- **Target test file:** `spec/fuzz/message_from_io_fuzz.cr` (+ shell harness).

#### §7.M
- **Model under test:** other(parser).
- **Operation history:** input bytes → output value / error.
- **Checker:** "no crash, only typed errors" invariant — sufficient on its own.
- **Nemesis:** input space (no runtime fault).
- **Ambiguous outcomes:** N/A.
- **Reduction plan:** libFuzzer-style minimization on each crashing input.

---

### S13: queue_per_key_fifo_under_partition_and_handoff

- **Falsifies if it FAILs:** C33, C34, C35, C36.
- **Closes:** H27, H28, H29.
- **Technique:** T1.
- **Workload:** 3-node `examples/queue` cluster. 3 clients publish unique tagged
  bodies to queue `q1` at 50 ops/s each (total 150). 1 client consumes from `q1`
  at 100 ops/s. Duration 3 min.
- **Faults:** Partition + leader kill every 30 s, focused on the queue-`q1`'s
  raft group leader.
- **Oracle:**
  - Every successfully-published body appears in some consumed result exactly once
    (no duplicates, no losses for acked publishes).
  - The sequence of consumed bodies, when projected to the order the producer
    issued them, is monotonically non-decreasing on each producer (FIFO per-
    producer; cross-producer order matches per-queue commit order).
  - HTTP `200`/`204` from `/queues/q1/messages` distinguishes "delivered" vs
    "empty" reliably; no `5xx` is misinterpreted as success.
- **Observability required:** Producer log of every issued tag + ack status;
  consumer log of every received body + status.
- **Exit criteria:** PASS if oracle holds. FAIL on duplicate, loss, or out-of-order
  delivery per producer.
- **Target test file:** `examples/queue/jepsen/src/raft_cr_queue/workload.clj` (new).

#### §7.M
- **Model under test:** queue.
- **Operation history:** producer + consumer events, default 11 fields.
- **Checker:** Elle's queue checker (or a custom Knossos queue model since
  raft.cr's queue is per-producer FIFO with global per-queue commit order).
- **Nemesis + landing evidence:** Jepsen partition-random-halves + kill; landing
  evidence as in S01.
- **Ambiguous outcomes:** HTTP `5xx` during fault → `timeout_marker = true`;
  client must not assume "published" without `200`.
- **Reduction plan:** reduce to 2 producers + 1 consumer, single partition window.

---

### S14: kv_leader_proxy_under_failover

- **Falsifies if it FAILs:** C37.
- **Closes:** H30.
- **Technique:** T3.
- **Workload:** 3-node `examples/kv` cluster. Client writes to node 2 (a follower)
  at 50 ops/s; node 2 proxies to the leader.
- **Faults:** SIGKILL the leader during steady-state.
- **Oracle:**
  - Client never sees a stale 5xx that masks a committed write (lost-ack pitfall
    — track every proposed key/value pair via a deterministic generator and
    verify post-recovery state).
  - Within 5 s of failover, the proxy redirects to the new leader.
  - The proxy never forwards to a node that is not the current leader for more
    than one round-trip.
- **Observability required:** Proxy log lines; `/raft/status` history.
- **Exit criteria:** PASS if oracle holds across 100 failovers. FAIL if a
  committed write is observably lost or if proxy retargeting takes > 5 s.

#### §7.M
- **Model under test:** register (per-key write).
- **Operation history:** per-key sequence of attempted writes, with the node
  contacted at each attempt + the final acknowledged value (or `unknown`).
- **Checker:** custom — committed writes must be readable post-failover; a key
  the client got 5xx for can be either present or absent, but no key the client
  got 200 for may be absent.
- **Nemesis + landing evidence:** SIGKILL of leader; landing — leader PID gone.
- **Ambiguous outcomes:** 5xx during failover window = `timeout_marker = true`.
- **Reduction plan:** reduce to 2 nodes + 1 client + single failover.

---

### S15: multi_raft_tick_fairness_and_apply_blocking

- **Falsifies if it FAILs:** C12, C40 (negative claim demonstrated).
- **Closes:** H9, H25, H26. Surfaces M3.
- **Technique:** T6 + T3.
- **Workload:** 3-node cluster running `Raft::Server` with 20 raft groups. One
  group's `StateMachine#apply` sleeps for 200 ms on every entry (simulating a slow
  application). Other groups have trivial apply.
- **Faults:** No external fault — the slow apply is itself the perturbation.
- **Oracle:**
  - Other groups maintain liveness: `raft_state_transitions_total{to="leader"}`
    on healthy groups does not increase beyond a baseline (no spurious elections).
  - The slow group's commit lag is observable and bounded.
  - If oracle fails (other groups elect spuriously), the failure demonstrates C40
    and confirms M3 — this is *expected behavior*; the scenario's value is making
    it explicit and *documented*.
- **Observability required:** Per-group commit-lag histograms,
  `raft_state_transitions_total`, `raft_elections_total`.
- **Exit criteria:** PASS if either (a) other groups stay healthy *or* (b) the
  failure mode is reproducible and matches the documented behavior in `ARCHITECTURE.md`
  §6 "Direct virtual dispatch on apply". A passing run also writes the observed
  budget for `apply` work as a new (now-explicit) claim.
- **Target test file:** `spec/raft/server_fairness_spec.cr`.

#### §7.M
§7.M: not applicable (no gated claim category falsified; demonstrates a
documented operational constraint).

---

### S16: replication_perf_slo_gate

- **Falsifies if it FAILs:** (implicit performance SLO claim — not yet documented;
  see Open Questions).
- **Closes:** H31.
- **Technique:** T6.
- **Workload:** Run `bench/replication_bench.cr` against a stable, low-noise
  environment (3 nodes, MemoryTransport for the in-process variant; TCP transport
  for the multi-process variant).
- **Faults:** None.
- **Oracle:**
  - In-process: median commit latency ≤ baseline + 10 %; p99 ≤ baseline + 20 %.
  - Multi-process (TCP local): median ≤ baseline + 15 %; p99 ≤ baseline + 25 %.
  - Throughput at 1 MB AppendEntries cap ≥ baseline − 5 %.
- **Observability required:** Bench output JSON.
- **Exit criteria:** PASS if all three within budget. FAIL otherwise; the run
  becomes the new baseline only on explicit baseline-update commit.
- **Target test file:** `bench/replication_bench.cr` + CI gate in
  `.github/workflows/ci.yml`.

#### §7.M
§7.M: not applicable (perf-SLO scenario).

---

### S17: soak_24h_multi_raft_no_leak

- **Falsifies if it FAILs:** (implicit operational soundness — see Open Questions).
- **Closes:** H32.
- **Technique:** T6 + T3.
- **Workload:** 3-node KV cluster with 10 keys spread across 10 value groups; 50
  ops/s for 24 hours; random partition/kill schedule (sparse — every 30 min).
- **Faults:** Background chaos as above.
- **Oracle:**
  - Process RSS at hour 24 ≤ 2 × RSS at hour 1.
  - Open file count bounded.
  - Mmap region count bounded.
  - No leaked `.log` segment files (segments older than snapshot_index are
    truncated_before).
  - No new compiler warnings or assertion failures in 24 h of logs.
- **Observability required:** Hourly `ps`, `lsof`, `ls data/raft/group-*/`,
  Prometheus scrape.
- **Exit criteria:** PASS if all bounds hold. FAIL on leak or unbounded growth.

#### §7.M
§7.M: not applicable.

---

## 7b. Coverage adequacy argument

For each claim from §1b, the argument that the chosen scenarios — *taken together*
— would falsify the claim if it were violated.

| Claim | Threat model | Scenarios | Why sufficient |
|---|---|---|---|
| C1 (term safety on commit) | (a) split leader commits old term entry by quorum, (b) crash before commit, (c) divergent followers | S01 (real fault), S03 (random fault + Elle), S02 (adversarial scheduling) | Three threat axes; Elle/Knossos would flag any non-linearizable history, the simulator catches adversarial interleavings. |
| C2 (election restriction) | candidate with shorter log requests vote | S02 (deterministic schedules), S03 (Elle catches resulting cycle) | Simulator explores stale-log candidate cases; Elle catches the consequence if it slipped through. |
| C3 (at-most-one-leader-per-term) | split vote yielding two leaders | S02 directly | Direct structural invariant in 100k seeds. |
| C4 (log matching) | divergent tails after partition heal | S01, S03 | Both Jepsen scenarios force partition + heal; post-heal SM equivalence checked. |
| C5 (state-machine safety) | divergent apply | S01, S03, S06 (durability variant) | Three independent threat angles cover this. |
| C6 (group_id isolation) | misrouted message in multi-raft | S11 + existing unit test | Combined coverage. |
| C7–C8 (persistent state durability + no torn meta) | crash mid-`persist_state` | S05 (1000 random kill offsets) | Direct test of the failure mode. |
| C9 (log entry durability) | FS-level fsync loss, mid-file torn writes, ENOSPC mid-append | S06 (dm-flakey + bit-flip + ENOSPC variants) | Per-append fsync is in code (dab4cd1); S06 exercises the remaining threats. |
| C10–C11 (snapshot atomicity + self-consistency) | crash mid-InstallSnapshot, concurrent take | S07 (variants A + B) | Both threats covered. |
| C12 (election liveness) | repeated leader kills | S08, S15 (apply-blocking variant) | Direct latency budget oracle. |
| C13 (replication liveness) | partition, drops, slow peers | S08, S11 | Two threat angles. |
| C14 (learner auto-promote) | partial connectivity during catch-up | S09 (concurrent reconfig + fault) | Combined under fault. |
| C15 (pre-vote anti-inflation) | asymmetric partition + rejoin | S04 directly | Direct test. |
| C16–C17 (one-config + applied-at-log-store) | reconfiguration race with leader crash | S09 | Both directly exercised. |
| C18 (bootstrap exclusivity) | crash after bootstrap, before first heartbeat | S05 (bootstrap variant) | Covered. |
| C19 (removed-self stepdown / commit-time reset) | crash during rollback path | S09 | Directly exercised. |
| C20–C21 (recover_state + tmp cleanup) | leftover `.tmp`, crash before fsync | S05 | Directly exercised. |
| C22 (AE consistency check) | divergent log heads | S01, S03 (via natural disagreement) | Indirect — the AE rejection path is exercised whenever followers disagree under fault. |
| C23 (apply-once, in-order) | replay across snapshot boundary | S10, S07 | Two angles. |
| C24–C26 (learner/non-member/voter guards) | malicious or buggy non-member | S03 (with Jepsen client that joins/leaves) + existing unit tests | Combined. |
| C27–C28 (size caps) | huge entries, huge snapshots | S12 (fuzz) + existing unit tests | Both. |
| C29 (bounded channels, drops counted) | inbox saturation | S11 directly | Direct. |
| C30 (metric set) | metric scrape under load | S11 + S08 + S16 | Indirect — every scenario reads metrics; if a metric is missing, scenarios that depend on it FAIL with INCONCLUSIVE. |
| C31 (max payload check) | malformed message | S12 (fuzz) directly | Direct. |
| C32 (transport_peers persistence) | restart with peer registry | S05 (variant covering `transport_peers`) | Direct. |
| C33–C35 (queue: FIFO, replication, bridge exactly-once) | partition + handoff during publish/consume | S13 | Direct, with Elle queue checker. |
| C36 (KV lazy value-group creation) | partition during first write | S14 + S13 (analogous) | Direct + analogous. |
| C37 (KV leader proxy) | failover during proxied write | S14 | Direct. |
| C38 (eventual consistency for reads — negative) | stale read after recent write | none — claim is *negative*; the library documents the gap | Out of scope: this claim says we do NOT guarantee linearizable reads, so testing for failure of linearizable reads is not a finding. The claim's "you must read through the leader" caveat is exercised by C37 (S14). |
| C39 (single TCP conn HOL ordering) | large message stalls heartbeat | S11 directly + S07 (snapshot transfer × heartbeats) | Two angles. |
| C40 (apply blocks group — negative claim) | slow apply | S15 directly | Demonstrates the documented limitation. |

A reviewer should read this table row by row. If a row's "Why sufficient" is not
convincing for some claim, the scenarios in §7 are inadequate for that claim and
should be supplemented or the limitation declared in §7c.

## 7c. Residual uncertainty

| Uncertainty | Why uncovered | Why acceptable today | When to revisit |
|---|---|---|---|
| **Byzantine faults** — node returns malformed messages or lies about its log. | Out of scope by Raft's assumptions; no plan to add BFT. | Not in the Raft threat model. | Only if a BFT variant is on the roadmap. |
| **Cross-OS portability** — fsync semantics differ on macOS/BSD/Linux. | dm-flakey is Linux-only. | Target environment is Linux. | When supporting non-Linux production deployment. |
| **Real disk hardware faults** (silent bitrot, sector-level read errors) — distinct from S06's bit-flip injection in that the OS doesn't surface them. | Requires faulty-disk hardware or block-layer fault-injection beyond dm-flakey's scope. | Production rests on RAID/ZFS for these; library does not claim per-block checksums. | If raft.cr ships its own checksums, retest. |
| **Cross-DC / WAN latency profiles** | bench/perf focuses on LAN; no WAN injection beyond tc/netem 200 ms. | Library is targeted at intra-DC consensus per ARCHITECTURE. | When multi-region usage is considered. |
| **Multi-Crystal-version compatibility** | Only one Crystal version tested. | Single language; downstream consumers pin Crystal. | When supporting LavinMQ across multiple Crystal versions. |
| **`Time.instant` vs. `Time.monotonic` divergence** under leap seconds, NTP adjustments | No specific scenario. | The library's only time use is tick-counter-based; wall clock is not in safety path. | If wall-clock leaves logical-clock domain. |
| **Adversarial transport-layer DoS** beyond the per-message size check (e.g., 100 connections each opening sockets at max rate) | No scenario; H18 covers single-conn payload DoS but not connection-storm. | Transport doesn't expose ACLs; assumed to be deployed behind a trusted boundary. | When raft.cr is exposed across a trust boundary. |
| **Determinism of `Crystal::Spec` ordering under `-Dexecution_context`** for the in-process spec runs | Not in scope. | Specs are designed to be order-independent. | If flake observed. |

## 7d. Confidence statement

If every scenario in §7 passes against `main` at the validation sha, a reviewer
should believe: (a) raft.cr honors C1, C2, C3, C4, C5 under partitioned, crashed,
and adversarially-scheduled real-network conditions to the precision of the Elle
/ Knossos / simulator oracles (S01, S02, S03); (b) C7, C8, C9, C10, C11 hold
under controlled crash and fsync-loss injection (S05, S06, S07) — the prior
gap M2 (segment writes not fsynced per append) is closed in code (commit dab4cd1
adds per-entry `flush` + `fsync`); residual concern is filesystem-/block-level
fsync-loss, which S06 exercises directly via dm-flakey; (c) C12, C13,
C14, C15 hold under repeated leader kills, asymmetric partitions, and
reconfiguration churn (S04, S08, S09); (d) C16–C19, C23, C29, C31, C32 hold
under their stated threat models; (e) the queue and KV examples preserve their
documented per-queue / per-key semantics under partition and handoff (S13, S14);
(f) the library does not leak resources or stall under 24 h of multi-raft chaos
(S11, S17). The reviewer **should not** believe this run validates the items
in §7c — Byzantine faults, hardware bitrot, cross-DC, multi-Crystal-version
compatibility — nor the explicit non-goals in §8. Performance regressions are
gated by S16 against a recorded baseline; a passing S16 does not certify
absolute throughput, only the absence of regression. Failures classified as
"FAIL-reproducible" by the executing skill carry the most weight; "INCONCLUSIVE"
verdicts indicate the harness, not the SUT.

The plan's most actionable single output, even before any scenario runs, is
§1c (Missing claims): M1, M5, M6, M7, M10 are likely-bug-shaped findings that
warrant code or doc fixes regardless of test outcome. (M2 was in this list at
the time of writing but is now closed by commit dab4cd1.)

## 8. What this plan does NOT cover

- **The TUI** (`src/raft/tui/`) — purely an operator interface; correctness
  surface is the `/raft/status` HTTP endpoint, which is tested.
- **Grafana dashboards / Prometheus alerting rules** — visual/operational, not
  protocol.
- **LavinMQ integration design** (`docs/plans/2026-03-09-lavinmq-raft-integration-design.md`)
  — exploratory; nothing to test.
- **Zero-copy / `sendfile` replication** — explicitly listed as not-yet in
  ARCHITECTURE §6.
- **Joint consensus (multi-server membership change)** — explicitly not implemented.
- **`ReadIndex` / leader lease / linearizable reads** — explicit non-goal per
  ARCHITECTURE §6.5; not tested.
- **Multi-Crystal-version testing** — fix one Crystal version per CI run.
- **Cross-platform behavior on macOS/BSD** — Linux-only testing.
- **Byzantine faults** — not in Raft's threat model.

## 9. Open questions / followups

- Should the project adopt explicit perf SLOs (commit p50 / p99 / throughput
  floor) so S16's "baseline + N %" can be replaced with absolute numbers?
  Owner: maintainer, before next release.
- Should `Config` validate that `election_timeout_min_ticks < election_timeout_max_ticks`
  and `heartbeat_ticks ≤ election_timeout_min_ticks // 2`? H33 implies yes.
  Owner: maintainer, before S12 runs in CI.
- Per-append fsync (commit dab4cd1) closes the correctness gap but means one
  fsync per appended entry. Should `Log` expose a batched-append API that
  fsyncs once per AppendEntries batch (leader-side: per `propose` batch;
  follower-side: per `handle_append_entries` batch)? Trade-off: fewer fsyncs
  vs. larger window of "in-memory but not yet durable" data; the
  Raft-correctness requirement is only that data be durable before the
  AppendEntriesResponse is sent, which is compatible with batching. Owner:
  maintainer; S16 (perf SLO) should track the per-entry-fsync overhead so
  this decision can be data-driven.
- Should `handle_install_snapshot`'s mismatch case `rescue` rather than `raise`?
  M5 — surface as a typed reject. Owner: maintainer, follow-up PR.
- Should the Jepsen Clojure project be filled in (it's currently a stub)?
  Multiple scenarios (S01, S03, S04, S07, S13) depend on it. Owner: someone with
  Clojure + Jepsen experience; ~2 weeks of work.
- Should a deterministic simulator be implemented in Crystal natively, or as
  a separate Rust/Go tool that reads the wire format? The library is uniquely
  positioned to be simulated — this is the single highest-leverage investment
  in the plan and is currently absent. Owner: maintainer + interested
  contributor.
- Should `MetaStateMachine` (queue example) persist the group-id counter via
  snapshot only, or also via a synced-on-create marker? Bears on H29.
