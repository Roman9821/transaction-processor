# Transaction Processor

An asynchronous event-processing service for financial transactions. Transactions
are ingested over HTTP, fanned out through a **Redis Streams** queue, then processed
by a worker that **deduplicates**, **converts every amount to USD**, and persists to
**PostgreSQL**. The service is designed not to lose events when the database or rate
source is temporarily unavailable.

```
            POST /transactions                 XREADGROUP (consumer group)
 client  ────────────────────▶  FastAPI  ──XADD──▶  Redis Stream  ──────────────▶  Worker
                                  │                   ("transactions")               │
                                  │                                                  │ dedup (PK)
   GET /users/{id}/summary        │                                                  │ FX -> USD
   GET /transactions  ◀───────────┴──────────────  PostgreSQL  ◀─────────────────────┘ persist + XACK
```

## Quick start

```bash
docker compose up --build
```

This launches `postgres`, `redis`, the `api` (port **8000**), and the `worker`.
Then send some traffic:

```bash
# one transaction
curl -X POST localhost:8000/transactions -H 'content-type: application/json' -d '{
  "id": "tx-1", "user_id": "alice", "amount": "100.50",
  "currency": "EUR", "timestamp": "2026-06-17T10:00:00Z"
}'

# or seed a batch (needs httpx: pip install httpx)
python scripts/seed.py 100
```

Read it back:

```bash
curl localhost:8000/users/alice/summary
curl "localhost:8000/users/alice/transactions?page=1&page_size=20"
curl "localhost:8000/users/alice/transactions?from=2026-06-01T00:00:00Z&to=2026-06-30T23:59:59Z"
curl localhost:8000/metrics         # Prometheus metrics
```

Interactive API docs: <http://localhost:8000/docs>

## API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/transactions` | Ingest a transaction `{id, user_id, amount, currency, timestamp}`. Returns `202 Accepted` and enqueues it. |
| `GET`  | `/users/{user_id}/summary` | Total USD amount and transaction count for a user. |
| `GET`  | `/users/{user_id}/transactions` | Paginated history for a user. Filters: `from`, `to` (ISO 8601); pagination: `page`, `page_size`. |
| `GET`  | `/metrics` | Prometheus metrics. |
| `GET`  | `/health` | Liveness check. |

## Tests

Unit tests cover the two pieces of core logic the spec calls out — **deduplication**
and **currency conversion** — and run without any infrastructure (in-memory SQLite):

```bash
pip install -r requirements.txt
pytest
```

---

## Design notes

### Why Redis Streams for the queue

I evaluated Redis Streams, RabbitMQ, and Kafka. **Redis Streams won** for this
workload because it offers the three queue properties the task actually needs —
**consumer groups, explicit acknowledgement, and a pending-message list for
recovery** — at the lowest operational cost.

- **It is a real queue, not pub/sub.** Consumer groups distribute load across
  workers, `XACK` gives explicit at-least-once acknowledgement, and the **Pending
  Entries List (PEL)** plus `XAUTOCLAIM` let a healthy worker reclaim messages
  that a crashed worker never acknowledged. That is exactly what "do not lose
  events" requires.
- **It matches the scale.** ~100 events/s with bursts to ~1,000/s is trivial for
  Redis; a single stream handles far more. Kafka's partition-based throughput and
  replay are real advantages, but they don't pay off until well beyond this scale,
  and Kafka's footprint (broker + coordination) makes the project slower to run
  and review.
- **Lowest friction.** One lightweight container, boots in seconds. RabbitMQ is the
  close runner-up and would also be a fine choice, but it adds an extra broker to
  operate for guarantees Redis Streams already provides here.

### Delivery semantics: at-least-once + idempotent storage

The system is **at-least-once at the queue** and **idempotent at the database**,
which yields **effectively exactly-once *persistence*** — the property that matters
for financial correctness.

- A message is `XACK`'d **only after** its row is committed to Postgres. If the
  worker dies between processing and ack, the message stays in the PEL and is
  redelivered.
- Redelivery is safe because the **transaction `id` is the primary key**. A repeat
  insert raises a conflict, is caught, and counts as a duplicate (`events_duplicate_total`)
  rather than creating a second row. No double-counting in summaries.

Achieving true exactly-once *delivery* end-to-end would require distributed
transactions across Redis and Postgres; idempotent writes give the same business
outcome far more simply.

### Reliability / recovery ("do not lose events")

- **Transient failures** (DB down, rate source unreachable) are retried with
  **exponential backoff** (`app/retry.py`). `RateUnavailableError` and DB
  connection errors are treated as retryable.
- If retries are exhausted, the message is **left unacknowledged** so it remains in
  the PEL and is redelivered later — nothing is dropped.
- **Poison messages** (permanent errors like an unsupported currency, or a message
  redelivered more than `MAX_DELIVERIES` times) are moved to a **dead-letter
  stream** (`transactions:dead`) and acked, so one bad event can't wedge the group.
- **Crashed-worker recovery**: every loop, `XAUTOCLAIM` reclaims messages idle for
  longer than `CLAIM_MIN_IDLE_MS` from any consumer that stopped acking.

### Observability

Prometheus metrics are exposed per process (the API and worker run separately, so
each owns its counters):

| Metric | Where | Meaning |
|--------|-------|---------|
| `events_ingested_total` | API `:8000/metrics` | accepted by the API (ingestion throughput) |
| `events_processed_total` | worker `:9100` | successfully persisted |
| `events_duplicate_total` | worker `:9100` | duplicates skipped by dedup |
| `events_failed_total` | worker `:9100` | dead-lettered after exhausting retries |
| `queue_lag_messages` | worker `:9100` | pending (delivered-but-unacked) messages — the key backpressure signal |

```bash
curl localhost:8000/metrics   # API: ingestion throughput
curl localhost:9100/metrics   # worker: processing, duplicates, failures, queue lag
```

A Prometheus deployment scrapes both targets; splitting metrics by process is the
standard pattern (in-process counters don't cross process boundaries).

### One architectural trade-off I made

**HTTP ingestion returns `202 Accepted` the moment the event is on the stream,
before it is persisted.** This decouples ingestion from processing: the API stays
fast and absorbs bursts (Redis is the buffer), and the worker drains at its own
pace. The cost is that a successful `POST` means "durably queued," not "stored and
queryable" — there's a small window where a just-accepted transaction isn't yet in
a summary. For an async processing service that's the right trade (throughput and
burst tolerance over read-your-write immediacy); a client that needs confirmation
can poll the history endpoint or we could add a status webhook.

### Scaling to 10x (~1,000 sustained / ~10,000 burst)

1. **Add workers.** They already share one consumer group, so scaling is
   `docker compose up --scale worker=N` — Redis partitions delivery across them
   with no code change.
2. **Postgres write path.** Batch inserts per poll (`XREADGROUP count=...` already
   reads in batches), add a connection pooler (PgBouncer), and partition the
   `transactions` table by time. Move summaries to an incrementally-maintained
   rollup table if aggregate queries get hot.
3. **Redis.** Cap stream length (`XADD MAXLEN ~`) so memory stays bounded once
   events are persisted; shard into multiple streams (e.g. by `user_id` hash) if a
   single stream becomes the bottleneck.
4. **Rate lookups.** Cache FX rates in-process with a short TTL (they change
   slowly) to remove that call from the hot path.
5. **Watch `queue_lag_messages`** — sustained growth is the signal to add workers;
   it's the natural autoscaling trigger.

## Project layout

```
app/
  main.py        FastAPI app + endpoints (ingest, summary, history, metrics)
  worker.py      async consumer: reclaim -> read -> dedup/convert/persist -> ack
  queue.py       Redis Streams helpers (group creation, publish)
  rates.py       currency -> USD conversion (with transient-failure simulation)
  repository.py  DB queries (idempotent insert, summary, paginated history)
  retry.py       exponential-backoff retry helper
  models.py      SQLAlchemy ORM (transaction id = primary key = dedup key)
  schemas.py     Pydantic request/response models
  db.py          async engine / session / table creation
  config.py      env-driven settings
tests/
  test_dedup.py     deduplication + summary correctness
  test_currency.py  conversion, rounding, error handling
scripts/seed.py     send sample traffic for a manual demo
```
