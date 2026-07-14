# Stage 7 — Live demo control UI + metrics API

The recruiter-facing surface: one page to *drive* the pipeline and *watch* it
react in real time. Two cleanly separated parts.

## 7a. control-api ([control-api/app.py](../control-api/app.py))

FastAPI backend. **Controls** proxy to the load generator / mock services;
**metrics** are aggregated from whichever system owns each truth:

| Metric | Source | How |
|---|---|---|
| Kafka consumer lag | Kafka **AdminClient** | Σ(end offset − committed offset) per partition |
| Ingestion rate (logs/s) | Kafka | Δ(end offsets) / Δt |
| Flink processing rate, slots, state | Flink **REST API** | `/overview`, `/jobs/:id` (Δ read-records) |
| Error rate / avg latency | **TimescaleDB** | latest `sliding_1m_10s` row per service |
| Log tail / correlation-id trace | **Elasticsearch** | `_search` sorted by time / `term` query |

Exposed as `/api/metrics` (JSON snapshot) and `/api/stream` (**SSE**, pushes
every ~1.5s). Controls: `POST /api/rps`, `/api/burst`, `/api/inject-errors`,
plus `/api/trace/{correlation_id}`. Every source is wrapped defensively — a
down backend returns `null`, the rest of the dashboard keeps working.

## 7b. UI ([ui/index.html](../ui/index.html))

Single file, Chart.js from CDN, same-origin calls to 7a. Traffic slider, burst
button, inject-errors toggle; live charts (ingest vs Flink throughput, Kafka
lag, per-service error rate, processing lag); an animated flow diagram that
lights up under load; a log tail; and a correlation-id trace box (click any id
in the tail to trace it across all four services).

### Verified
- control-api serves the UI (`GET /` → 200) and `/api/metrics` returns a live
  snapshot: **TimescaleDB metrics present**, Kafka/Flink/ES gracefully `null`
  when down — the defensive design works.
- UI renders with a **live SSE connection** (green "● live"), all panels present.

## The demo story (why each control maps to a visible effect)
- **Slide rps 1k→8k** → ingestion throughput climbs; if Flink can't keep up,
  Kafka **consumer lag** rises then **drains** as it catches up (the buffer
  story), while end-to-end latency stays bounded.
- **Burst** → a short spike Kafka absorbs; you watch lag spike and recover.
- **Inject errors** → mock services emit ERROR bursts → per-service error-rate
  chart jumps → the Grafana `>2% for 5m` alert fires.
- **Paste a correlation_id** → see that one request across all four services.

## Interview Q&A

**Q1. Why SSE instead of polling or WebSockets?**
The dashboard is one-directional (server→browser) and periodic. SSE gives that
over a single long-lived HTTP connection with auto-reconnect built into the
browser's `EventSource`, no extra protocol. WebSockets would be overkill
(bidirectional); naive polling wastes requests and races. Controls, which are
occasional and client→server, stay as plain POSTs.

**Q2. How is Kafka consumer lag actually computed, and why does it prove the buffer story?**
Lag = (log-end-offset − committed-offset) summed over partitions, read via the
Kafka AdminClient/consumer. It's literally "how many messages are produced but
not yet consumed." Under a burst, producers outrun Flink so lag grows; when
Flink catches up it shrinks. Watching it rise-then-drain is a direct, visual
proof that Kafka decouples producers from consumers and absorbs spikes.

**Q3. Where do the Flink numbers come from?**
The Flink REST API on the JobManager (`:8081`): `/overview` gives task-slot
totals/availability (parallelism utilization) and `/jobs/:id` gives per-vertex
record counts, from which we derive a processing rate by differencing over time.
This is the same API Flink's own web UI uses.

**Q4. Why aggregate in a separate service instead of the UI calling each system?**
Separation of concerns + security: the browser shouldn't hold Kafka/DB
credentials or reach internal services directly. The control-api is the single
trusted aggregation point; the UI only speaks to it. It also lets us normalize
five very different sources into one clean JSON/SSE contract and degrade
gracefully when any one is down.
