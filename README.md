# Job Queue Service

A production-quality REST job-processing service written in Go, built to demonstrate:

- **10-goroutine worker pool** backed by a **100-slot buffered channel**
- **HTTP 429** backpressure when the queue is at capacity
- **Exponential-backoff retry** (up to 3 attempts, base 200 ms, capped 5 s)
- **Bounded TCP connection pool** (5 reusable connections) to a mock downstream server
- **Graceful shutdown** via `context` + `sync.WaitGroup`
- **Shared-state protection** with `sync.RWMutex`
- **Table-driven tests** + **race detector** at ≥ 80 % coverage
- **Multi-stage Docker build** (scratch runtime image)

---

## Architecture

```
┌───────────────────────────────────────────────────────────┐
│                      HTTP Server (:8080)                  │
│  POST /jobs  GET /jobs  GET /jobs/:id  GET /health        │
└────────────────────────┬──────────────────────────────────┘
                         │ enqueue (buffered chan, cap 100)
                         ▼
┌───────────────────────────────────────────────────────────┐
│              Worker Pool  (10 goroutines)                 │
│  • dequeue jobs                                           │
│  • retry up to 3× with exponential backoff               │
│  • update job status in the in-memory store              │
└────────────────────────┬──────────────────────────────────┘
                         │ Get / Put
                         ▼
┌───────────────────────────────────────────────────────────┐
│           Bounded TCP Pool  (5 reusable conns)            │
│  • PING/PONG health-check before reuse                   │
│  • dead-connection eviction + fresh dial                  │
└────────────────────────┬──────────────────────────────────┘
                         │ JSON over TCP
                         ▼
┌───────────────────────────────────────────────────────────┐
│         Mock Downstream Server (:9090)                    │
│  • PING → PONG   (health-check)                          │
│  • JSON job → OK / ERR   (configurable fail rate)        │
└───────────────────────────────────────────────────────────┘
```

---

## Project Layout

```
job-queue-service/
├── cmd/
│   ├── server/       # HTTP server entrypoint
│   └── mockserver/   # Standalone mock downstream TCP server
├── internal/
│   ├── job/          # Job type, status constants, CanRetry helper
│   ├── store/        # Thread-safe in-memory job registry (sync.RWMutex)
│   ├── pool/         # Bounded TCP connection pool with health-checks
│   ├── worker/       # 10-goroutine pool, backpressure, retry logic
│   ├── handler/      # HTTP routes: POST/GET /jobs, GET /health
│   └── mockserver/   # Reusable mock TCP server (used in tests too)
├── Dockerfile        # Multi-stage build → scratch runtime image
├── docker-compose.yml
├── Makefile
└── go.mod
```

---

## Quick Start

### Prerequisites
- Go 1.23+
- Docker & Docker Compose (for containerised runs)

### Run locally (two terminals)

**Terminal 1 — mock downstream:**
```bash
make run-mock
# listening on :9090 (5 % random failure rate for retry demo)
```

**Terminal 2 — HTTP server:**
```bash
make run-server
# listening on :8080
```

### Run with Docker Compose

```bash
make docker-up
```

---

## API Reference

### `POST /jobs`
Enqueue a new job.

```bash
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"email","payload":{"to":"user@example.com"}}'
```

**Response `202 Accepted`:**
```json
{
  "id": "20241015120000.123456789",
  "type": "email",
  "status": "pending",
  "retries": 0,
  "max_retries": 3,
  ...
}
```

**Response `429 Too Many Requests`** (queue at capacity):
```json
{ "error": "server is busy, queue at capacity — retry later" }
```

### `GET /jobs`
List all jobs.

```bash
curl http://localhost:8080/jobs
```

### `GET /jobs/{id}`
Fetch a single job by ID.

```bash
curl http://localhost:8080/jobs/20241015120000.123456789
```

### `GET /health`
Liveness check + metrics.

```bash
curl http://localhost:8080/health
```
```json
{
  "status": "ok",
  "queue":    { "length": 3,  "capacity": 100 },
  "tcp_pool": { "idle": 4,    "in_flight": 1, "total": 5 },
  "jobs_stored": 42
}
```

---

## Testing

```bash
# Run all tests (verbose)
make test

# Run with the race detector (recommended)
make race

# Generate HTML coverage report
make cover
```

Coverage is verified to be ≥ 80 % across all packages.

---

## Key Design Decisions

| Concern | Solution |
|---|---|
| Backpressure | Non-blocking `select` on buffered channel; `ErrQueueFull` → HTTP 429 |
| Retry | Exponential backoff `base × 2^(n-1)`, capped at 5 s, respects shutdown ctx |
| Connection reuse | Bounded channel-semaphore pool; PING/PONG before handing out each conn |
| Dead conn eviction | `MarkBad()` flag on return path; pool dials a fresh replacement |
| Graceful shutdown | `context.Cancel` → workers finish current job → `WaitGroup.Wait()` |
| Data-race safety | `sync.RWMutex` in Store; pool guarded by `sync.Mutex` + channel semaphore |
| Observability | Structured `log.Printf` with `[component]` prefixes throughout |
