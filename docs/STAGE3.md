# Stage 3 — Apache Flink stream processing (Java) — the core

Consumes the Protobuf log stream from Kafka and turns it into (a) searchable
enriched logs, (b) time-series metrics, and (c) an archival Parquet lake.

## Topology

```
Kafka "logs" (Protobuf bytes)
   │  BytesDeserializationSchema (defer parsing so bad records don't kill the source)
   ▼  LogParseFunction: parse + validate ──(malformed / bad ts / bad level)──► side output ──► Kafka "logs.dlq"
EnrichedLog (POJO)
   │  EnrichmentFunction: status_class (2xx..5xx), processing_ts, region default
   ▼  filter: keep INFO/WARN/ERROR; sample ~10% DEBUG
assignTimestampsAndWatermarks (event-time, bounded out-of-orderness 5s, idleness 15s)
   ├────────────────────────► Elasticsearch  (daily index logs-yyyy.MM.dd, idempotent id)
   ├─ map→Avro GenericRecord ► S3/MinIO       (hourly Parquet, rolls on checkpoint)
   ├─ keyBy(service) TUMBLING 1m       ─ aggregate ─► TimescaleDB (upsert)
   └─ keyBy(service) SLIDING 1m/10s    ─ aggregate ─► TimescaleDB (moving error-rate)
                       (late data past watermark+30s ─► logs.dlq)
```

Files: [StreamingJob.java](../flink-job/src/main/java/com/observability/StreamingJob.java)
(topology), `parse/`, `enrich/`, `metrics/`, `sink/`. Built via multi-stage
[Dockerfile](../flink-job/Dockerfile) (Maven → Flink app-mode image, S3 plugin enabled).

## Key concepts (interview-ready)

### Event-time vs processing-time, and watermarks
- **Processing-time** = the wall clock when Flink handles a record. Simple but
  non-deterministic (results depend on machine speed / delays) and wrong for
  windowed metrics if data is delayed.
- **Event-time** = when the event actually happened (our `timestamp` field). We
  use it so a "1-minute error rate" means one minute of *real* time regardless
  of when logs arrive.
- **Watermark** = Flink's assertion "I believe I've seen all events up to time
  T." We use `forBoundedOutOfOrderness(5s)`: watermark = maxEventTimeSeen − 5s.
  A window `[t0, t0+1m)` fires when the watermark passes `t0+1m`. Events later
  than the watermark are **late**; `allowedLateness(30s)` lets them still update
  a fired window, and anything later than that is routed to a side output
  (→ `logs.dlq`) instead of being silently dropped. `withIdleness(15s)` stops a
  quiet partition from stalling the watermark for everyone.

### keyBy + windows + state
- `keyBy(serviceName)` partitions the stream by key so each service aggregates
  independently and in parallel. State (the window accumulator) is **per key**.
- **Tumbling 1m**: fixed, non-overlapping buckets → "requests/errors per minute".
- **Sliding 1m/10s**: a 1-minute window emitted every 10s → a smooth *moving*
  error rate that reacts quickly (each event belongs to multiple windows).
- We aggregate with an **AggregateFunction** (incremental fold into a tiny
  accumulator) + a **ProcessWindowFunction** (stamps window metadata at close).
  This keeps only the accumulator in state, not every buffered event.

### Checkpointing + exactly-once
- `enableCheckpointing(30s, EXACTLY_ONCE)` snapshots all operator state + source
  offsets via the Chandy-Lamport barrier algorithm. On failure Flink restarts
  from the last checkpoint: Kafka offsets rewind and window state is restored.
- **Exactly-once vs at-least-once**: exactly-once means each record affects state
  exactly once even across failures. Flink gives this internally for state. For
  *sinks* it needs cooperation: the Parquet FileSink commits files only on
  checkpoint (2-phase), the ES sink uses a deterministic doc id (retries
  overwrite, not duplicate), and the TimescaleDB sink upserts on a unique key.
  The DLQ Kafka sink is at-least-once (fine for an audit topic).

### Backpressure
- If a sink (e.g. ES) slows down, its input buffers fill; Flink propagates this
  upstream through the network stack until the Kafka source simply reads slower.
  No data loss — the backlog accumulates in Kafka (visible as consumer lag) and
  drains when the sink recovers. This is the behaviour the Stage 7 UI visualizes.

## Interview Q&A

**Q1. Why event-time here, and what do watermarks actually do?**
Metrics like "error rate per minute" must be defined over when events *happened*,
not when they arrived, or a network hiccup would smear counts across minutes.
Watermarks are how Flink decides a window is complete despite out-of-order
arrival: `forBoundedOutOfOrderness(5s)` trades 5s of latency for tolerance to 5s
of reordering. Later events are handled by allowed-lateness then a side output.

**Q2. AggregateFunction vs ProcessWindowFunction — why combine them?**
A pure ProcessWindowFunction buffers every element until the window closes → O(n)
state. An AggregateFunction folds each element into a small accumulator → O(1)
state, but can't see window metadata. Combining them gives incremental
aggregation *and* the window start/end at close — cheap and complete.

**Q3. How does Flink achieve exactly-once, and where is it only at-least-once here?**
Distributed snapshots (checkpoint barriers) capture consistent state + source
offsets; recovery replays from there. Internal state is exactly-once. End-to-end
requires idempotent/transactional sinks: Parquet commits on checkpoint, ES uses
stable ids, Timescale upserts. The `logs.dlq` sink is at-least-once by choice.

**Q4. What is the dead-letter pattern and why not just skip bad records?**
Malformed/invalid records are routed to a side output → `logs.dlq` instead of
throwing (which would fail the job) or dropping silently (which loses data and
hides bugs). The DLQ is durable and replayable, so you can inspect why records
failed and reprocess after a fix. We use it for parse failures, validation
failures, and excessively-late events.
