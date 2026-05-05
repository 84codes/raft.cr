# Queue PoC

A multi-raft queue example demonstrating the unbounded-log problem from `ARCHITECTURE.md` §6.5.

Each queue is its own Raft group. A meta group (group 0) maps `queue_name → group_id`.
The HTTP API supports publish + one-shot consume; consume uses a request-id bridge to
deliver the popped value back to the HTTP handler (see `docs/plans/2026-05-04-queue-poc-design.md` §4).

## Run locally with Docker Compose

```bash
docker compose -f examples/queue/docker-compose.yml up --build
```

This starts 3 nodes (HTTP on 8001, 8002, 8003), Prometheus, and Grafana.

Bootstrap the cluster:
```bash
curl -X POST http://localhost:8001/raft/admin/bootstrap
curl -X POST -d 'node-2:9000' http://localhost:8001/raft/admin/register_peer/2
curl -X POST -d 'node-3:9000' http://localhost:8001/raft/admin/register_peer/3
curl -X POST http://localhost:8001/raft/admin/add_server/2
curl -X POST http://localhost:8001/raft/admin/add_server/3
```

(Or use the TUI in `bin/raft-tui` if available — see top-level README.)

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/queues/{name}` | Publish — body is the message bytes. Auto-creates the queue. |
| `GET` | `/queues/{name}/messages` | Consume one message. 200 + body, or 204 if empty. |
| `GET` | `/queues` | List queue names + depth + log_last_index. |
| `DELETE` | `/queues/{name}` | Delete the queue and its Raft group. |
| `GET` | `/queues/{name}/events` | SSE stream of depth changes. |
| `GET` | `/` | Web UI showing live queue state. |

## What to look for

After publishing then draining N messages, inspect each node's `data/raft/group-*/`
directory: the on-disk segments hold ~2N entries (publish + consume) even though every
queue is empty in memory. This is the unbounded-log problem the PoC was built to surface.
Compaction is tracked separately.
