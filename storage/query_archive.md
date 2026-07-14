# Querying the archived Parquet lake (the "query cold data" story)

Flink writes enriched logs as hourly-partitioned Parquet to MinIO
(`s3a://logs-archive/enriched/yyyy-MM-dd--HH/`). This is the cheap, durable
archive tier — you keep it for months/years and query it on demand, the same
pattern as S3 + Athena/Trino in production.

Locally, **DuckDB** plays the Athena role. It reads Parquet directly from MinIO
over the S3 API:

```sql
INSTALL httpfs; LOAD httpfs;
SET s3_endpoint='localhost:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;
SET s3_url_style='path';

-- error rate by service over the whole archive
SELECT service_name,
       count(*)                                   AS total,
       sum(CASE WHEN level='ERROR' THEN 1 ELSE 0 END) AS errors,
       round(100.0*sum(CASE WHEN level='ERROR' THEN 1 ELSE 0 END)/count(*), 2) AS err_pct
FROM read_parquet('s3://logs-archive/enriched/**/*.parquet', hive_partitioning=1)
GROUP BY service_name
ORDER BY err_pct DESC;

-- trace one request across services, straight from cold storage
SELECT timestamp, service_name, level, message
FROM read_parquet('s3://logs-archive/enriched/**/*.parquet')
WHERE correlation_id = '<paste-id>'
ORDER BY event_time_ms;
```

Why this tier exists: Elasticsearch is fast but expensive per GB, so you keep
only recent hot data there; Parquet on object storage is ~10× cheaper and its
columnar layout means a query touching 2 columns reads only those columns.
Trino/Athena would run the identical SQL over the same files at scale.
