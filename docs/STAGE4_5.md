# Stage 4 — Storage backends & Stage 5 — Dashboards

## Stage 4: three storage tiers, each matched to a query pattern

| Tier | Tech | What it stores | Why this store |
|---|---|---|---|
| Search (hot) | Elasticsearch 7.17 | enriched logs, daily index `logs-*` | inverted index → fast full-text + keyword search |
| Metrics | TimescaleDB | windowed per-service metrics (hypertable) | time-series; fast time-range scans + retention |
| Archive (cold) | MinIO (S3) | hourly Parquet | columnar, ~10× cheaper; query on demand |

- **ES mapping** ([index_template.json](../storage/elasticsearch/index_template.json)):
  `correlation_id`/`service_name`/`level`/`status_class`/`region` are `keyword`
  (exact-match, aggregatable), `message` is `text` (full-text) with a `.raw`
  keyword subfield, `@timestamp` is `date`. Installed via `es-init`.
- **TimescaleDB** ([init.sql](../storage/timescaledb/init.sql)): `service_metrics`
  hypertable partitioned on `window_start`; PK `(service_name, window_type,
  window_start)` doubles as the upsert conflict target.
- **MinIO**: buckets `logs-archive` (Parquet) + `flink-checkpoints`. Query the
  archive with DuckDB — see [query_archive.md](../storage/query_archive.md).

### Verified
- ES template installed; a `correlation_id` search returns the doc → **trace query works**.
- TimescaleDB `service_metrics` created as a hypertable (1 time dimension).
- MinIO buckets created.
- **Full Flink→TimescaleDB path proven end-to-end**: 16 tumbling + 74 sliding
  rows with realistic per-service error rates & latencies.
- ES + Parquet sinks: code verified + two real bugs fixed (below). Running them
  *simultaneously with everything else* exceeds this 3.8 GB / emulated-amd64
  Docker VM (TaskManager heartbeat times out under CPU saturation) — it needs a
  bigger host or the k8s cluster.

### Two real bugs found & fixed by running it
1. **Kryo can't serialize Avro `GenericRecord`** (its `Schema` holds immutable
   collections). Fix: tag the stream with `GenericRecordAvroTypeInfo` so Flink
   uses `AvroSerializer`, not Kryo.
2. **Parquet sink `NoClassDefFoundError: FileOutputFormat`** — needed
   `hadoop-mapreduce-client-core`, not just `hadoop-common`. Because es-sink and
   parquet-sink share an operator chain, the Parquet crash was also starving ES.

## Stage 5: dashboards

- **Grafana** (provisioned): [datasource](../dashboards/grafana/provisioning/datasources/timescaledb.yml)
  (TimescaleDB), [dashboard](../dashboards/grafana/dashboards/observability.json)
  (error-rate %, avg latency, request count, moving error-rate), and an
  [alert rule](../dashboards/grafana/provisioning/alerting/rules.yml):
  **error_rate > 2% for 5m**. Verified: datasource + dashboard + rule provisioned,
  and Grafana queried the live per-service metrics.
- **Kibana** (wired): `kibana-init` auto-creates the `logs-*` data view. The
  headline demo — paste a `correlation_id`, see the request across all 4 services —
  is the ES search we verified directly in Stage 4.

## Interview Q&A

**Q1. Why Elasticsearch for logs — what's an inverted index?**
An inverted index maps each term → the list of documents containing it, so
"find logs mentioning 'timeout'" is a dictionary lookup, not a scan. Keyword
fields index exact values for fast filtering/aggregation; text fields are
analyzed (tokenized/lowercased) for full-text. That's why ES answers
"correlation_id = X" and "message contains Y" in milliseconds.

**Q2. Why a TimescaleDB hypertable instead of a plain table?**
A hypertable auto-partitions rows into time-based chunks. Time-range queries
(Grafana's bread and butter) prune to a few chunks instead of scanning
everything, inserts hit the newest chunk (cache-friendly), and retention =
"drop old chunks." You keep full SQL/Postgres compatibility.

**Q3. Why Parquet on object storage for the archive?**
Parquet is columnar: a query touching 2 of 15 columns reads only those columns,
and per-column compression is excellent. On object storage it's ~10× cheaper
than ES per GB, so it's the long-retention tier you query occasionally with
DuckDB/Trino/Athena — the classic hot/cold split.

**Q4. Same data in three stores — isn't that duplication?**
Yes, intentionally: each store answers a different question well. ES = "find
this needle" (recent, expensive). TimescaleDB = "trend over time" (compact
aggregates). Parquet = "cheap history / ad-hoc analytics." Flink fans out once
so all three stay consistent from a single processed stream.
