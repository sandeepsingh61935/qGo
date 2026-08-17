# Job Queue

Lightweight async job queue in Go with Redis backend. Built for learning/resume — demonstrates durability, visibility timeouts, idempotency, retries/DLQ, and Prometheus metrics.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  API        │     │  Redis      │     │  Workers    │
│  (enqueue)  │────▶│  Lists      │────▶│  (process)  │
└─────────────┘     │  + Lua      │     └─────────────┘
                    │  + Keys     │            ▲
                    └─────────────┘            │
                          │                    │
                          ▼                    │
                    ┌─────────────┐     ┌─────────────┐
                    │  DLQ        │     │  Reaper     │
                    │  (failed)   │◀────│  (stale)    │
                    └─────────────┘     └─────────────┘
```

## Quick Start

```bash
# Local (requires Redis on localhost:6379)
go run ./cmd/jobqueue -mode api
go run ./cmd/jobqueue -mode worker

# Docker
docker-compose up --build
```

## API

### Enqueue Job
```bash
curl -X POST http://localhost:8080/enqueue \
  -H "Content-Type: application/json" \
  -d '{"type":"email.send","payload":{"to":"user@example.com"},"priority":5}'
# {"job_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","status":"queued"}
```

### Health
```bash
curl http://localhost:8080/health
# {"status":"healthy","queue_depth":2,"processing_depth":1,"oldest_job_age_seconds":0.3,"dlq_depth":0}
```

### Metrics (Prometheus)
```bash
curl http://localhost:8080/metrics
```

## Metrics Exposed

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `jobs_enqueued_total` | Counter | type, queue | Jobs accepted |
| `jobs_processed_total` | Counter | type, queue | Jobs completed |
| `jobs_failed_total` | Counter | type, queue | Jobs moved to DLQ |
| `jobs_retried_total` | Counter | type, queue | Retry attempts |
| `jobs_reaped_total` | Counter | — | Stale jobs requeued |
| `queue_depth` | Gauge | — | Main queue length |
| `processing_depth` | Gauge | — | In-flight jobs |
| `dlq_depth` | Gauge | — | Dead-letter count |
| `job_duration_seconds` | Histogram | type, queue | Processing latency |
| `queue_lag_seconds` | Gauge | — | Oldest job age |

## Design Decisions & Trade-offs

1. **Redis lists + Lua vs Postgres SKIP LOCKED** — Chose Redis for sub-ms latency and simpler ops. Trade-off: no ACID durability without replicas; at-least-once requires idempotent handlers.
2. **At-least-once + idempotency vs exactly-once** — Idempotency key checked atomically in dequeue Lua script. Simpler than distributed consensus; shifts burden to handler author.
3. **No delayed/priority jobs in v1** — Scope cut for 3h build. `priority` field exists in payload for future weighted-fair-queuing; `run_at` would need a scheduler (sorted set + single poller).

## Running the Demo Worker

Register a handler in `cmd/jobqueue/main.go`:
```go
w.RegisterHandler("email.send", &EmailHandler{})
```

## License

MIT