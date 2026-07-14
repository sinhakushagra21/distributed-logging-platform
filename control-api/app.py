"""control-api — the backend for the live demo UI (Stage 7).

Two jobs:
  1. CONTROLS: change the load generator's rps, fire a burst, and toggle error
     injection on the mock services — so a recruiter can *drive* the pipeline.
  2. METRICS: aggregate the numbers the UI plots, each pulled from the system
     that actually owns that truth:
       - Kafka consumer lag  -> Kafka AdminClient (end offset - committed offset)
       - ingestion rate      -> delta of Kafka end offsets over time
       - Flink processing     -> Flink REST API (/jobs, /overview)
       - error rate / latency -> TimescaleDB (the aggregates Flink wrote)
       - log tail / trace     -> Elasticsearch search API

Everything is defensive: if a backend is down, that metric returns null and the
rest of the dashboard keeps working. Metrics are exposed both as a plain JSON
snapshot (/api/metrics) and as a Server-Sent Events stream (/api/stream), which
is the simplest way to push ~1/s updates to a browser over one HTTP connection.
"""

from __future__ import annotations

import asyncio
import json
import os
import time

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

# --- config ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "logs")
FLINK_GROUP = os.getenv("FLINK_GROUP", "flink-log-processor")
FLINK_URL = os.getenv("FLINK_URL", "http://flink-jobmanager:8081")
ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
TS_DSN = os.getenv("TIMESCALE_DSN", "host=timescaledb port=5432 dbname=metrics user=postgres password=postgres")
LOADGEN_URL = os.getenv("LOADGEN_URL", "http://loadgen:9100")
SERVICE_URLS = os.getenv("SERVICE_URLS",
    "http://api-gateway:8000,http://auth:8001,http://trip:8002,http://payments:8003").split(",")

app = FastAPI(title="control-api")

# Rolling state for rate calculations (offsets/records seen last tick).
_state = {"kafka_end": None, "kafka_ts": None, "flink_in": None, "flink_ts": None}


# --------------------------------------------------------------------------- #
# Metric sources (each defensive: return None on failure)
# --------------------------------------------------------------------------- #
def kafka_metrics() -> dict:
    """Consumer lag + ingestion rate from Kafka directly."""
    try:
        from confluent_kafka import Consumer, TopicPartition
        c = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": FLINK_GROUP,
                      "enable.auto.commit": False})
        md = c.list_topics(KAFKA_TOPIC, timeout=5)
        parts = [TopicPartition(KAFKA_TOPIC, p) for p in md.topics[KAFKA_TOPIC].partitions]
        committed = c.committed(parts, timeout=5)
        committed_by_p = {tp.partition: (tp.offset if tp.offset >= 0 else 0) for tp in committed}
        total_end = 0
        lag = 0
        for tp in parts:
            lo, hi = c.get_watermark_offsets(tp, timeout=5)
            total_end += hi
            lag += max(0, hi - committed_by_p.get(tp.partition, 0))
        c.close()

        # ingestion rate = delta of end offsets / elapsed
        now = time.time()
        rate = None
        if _state["kafka_end"] is not None and _state["kafka_ts"] is not None:
            dt = now - _state["kafka_ts"]
            if dt > 0:
                rate = max(0.0, (total_end - _state["kafka_end"]) / dt)
        _state["kafka_end"], _state["kafka_ts"] = total_end, now
        return {"consumer_lag": lag, "ingest_rate": round(rate, 1) if rate is not None else None,
                "total_messages": total_end}
    except Exception as e:
        return {"consumer_lag": None, "ingest_rate": None, "error": str(e)[:120]}


async def flink_metrics(client: httpx.AsyncClient) -> dict:
    """Job state, task-slot utilization, and processing rate from Flink REST."""
    try:
        ov = (await client.get(f"{FLINK_URL}/overview", timeout=4)).json()
        jobs = (await client.get(f"{FLINK_URL}/jobs", timeout=4)).json()["jobs"]
        if not jobs:
            return {"job_state": "NO_JOB", "slots_total": ov.get("slots-total"),
                    "slots_available": ov.get("slots-available"), "processing_rate": None}
        jid = jobs[0]["id"]
        detail = (await client.get(f"{FLINK_URL}/jobs/{jid}", timeout=4)).json()
        total_in = sum(v["metrics"].get("read-records", 0) for v in detail["vertices"])
        now = time.time()
        rate = None
        if _state["flink_in"] is not None and _state["flink_ts"] is not None:
            dt = now - _state["flink_ts"]
            if dt > 0:
                rate = max(0.0, (total_in - _state["flink_in"]) / dt)
        _state["flink_in"], _state["flink_ts"] = total_in, now
        return {
            "job_state": detail["state"],
            "slots_total": ov.get("slots-total"),
            "slots_available": ov.get("slots-available"),
            "processing_rate": round(rate, 1) if rate is not None else None,
            "records_processed": total_in,
        }
    except Exception as e:
        return {"job_state": None, "processing_rate": None, "error": str(e)[:120]}


def timescale_metrics() -> dict:
    """Latest per-service error rate + avg latency (from the moving windows)."""
    try:
        import psycopg2
        conn = psycopg2.connect(TS_DSN, connect_timeout=4)
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ON (service_name) service_name, error_rate, avg_latency_ms
            FROM service_metrics WHERE window_type='sliding_1m_10s'
            ORDER BY service_name, window_start DESC
        """)
        rows = cur.fetchall()
        cur.close(); conn.close()
        return {"per_service": [
            {"service": r[0], "error_rate": round(r[1], 4), "avg_latency_ms": round(r[2], 1)}
            for r in rows]}
    except Exception as e:
        return {"per_service": [], "error": str(e)[:120]}


async def es_metrics(client: httpx.AsyncClient) -> dict:
    """Indexed doc count + processing lag (processing_ts - event_time) from ES."""
    try:
        cnt = (await client.get(f"{ES_URL}/logs-*/_count", timeout=4)).json().get("count")
        q = {"size": 1, "sort": [{"@timestamp": "desc"}],
             "_source": ["event_time_ms", "processing_ts_ms"]}
        r = (await client.post(f"{ES_URL}/logs-*/_search", json=q, timeout=4)).json()
        hits = r.get("hits", {}).get("hits", [])
        lag = None
        if hits:
            s = hits[0]["_source"]
            if s.get("processing_ts_ms") and s.get("event_time_ms"):
                lag = s["processing_ts_ms"] - s["event_time_ms"]
        return {"indexed_docs": cnt, "processing_lag_ms": lag}
    except Exception as e:
        return {"indexed_docs": None, "processing_lag_ms": None, "error": str(e)[:120]}


async def snapshot(client: httpx.AsyncClient) -> dict:
    flink, es = await asyncio.gather(flink_metrics(client), es_metrics(client))
    return {
        "ts": int(time.time() * 1000),
        "kafka": kafka_metrics(),
        "flink": flink,
        "timescale": timescale_metrics(),
        "elasticsearch": es,
    }


# --------------------------------------------------------------------------- #
# Metrics endpoints
# --------------------------------------------------------------------------- #
@app.get("/api/metrics")
async def metrics():
    async with httpx.AsyncClient() as client:
        return await snapshot(client)


@app.get("/api/stream")
async def stream(request: Request):
    """Server-Sent Events: push a metrics snapshot every ~1.5s."""
    async def gen():
        async with httpx.AsyncClient() as client:
            while True:
                if await request.is_disconnected():
                    break
                data = await snapshot(client)
                yield f"data: {json.dumps(data)}\n\n"
                await asyncio.sleep(1.5)
    return StreamingResponse(gen(), media_type="text/event-stream")


# --------------------------------------------------------------------------- #
# Control endpoints (proxy to loadgen / mock services)
# --------------------------------------------------------------------------- #
@app.get("/api/rps")
async def get_rps():
    async with httpx.AsyncClient() as c:
        try:
            return (await c.get(f"{LOADGEN_URL}/rps", timeout=4)).json()
        except Exception as e:
            return {"error": str(e)[:120]}


@app.post("/api/rps")
async def set_rps(value: float):
    async with httpx.AsyncClient() as c:
        return (await c.post(f"{LOADGEN_URL}/rps", params={"value": value}, timeout=4)).json()


@app.post("/api/burst")
async def burst(multiplier: float = 6.0, duration_s: float = 15.0):
    async with httpx.AsyncClient() as c:
        return (await c.post(f"{LOADGEN_URL}/burst",
                params={"multiplier": multiplier, "duration_s": duration_s}, timeout=4)).json()


@app.post("/api/inject-errors")
async def inject_errors(enabled: bool = True, duration_s: float = 120.0):
    """Fan out an error-mode toggle to every mock service."""
    results = {}
    async with httpx.AsyncClient() as c:
        for url in SERVICE_URLS:
            try:
                r = await c.post(f"{url.strip()}/admin/error-mode",
                                 params={"enabled": enabled, "duration_s": duration_s}, timeout=4)
                results[url] = r.json()
            except Exception as e:
                results[url] = {"error": str(e)[:80]}
    return {"injected": enabled, "services": results}


# --------------------------------------------------------------------------- #
# Log tail + correlation-id trace (from Elasticsearch)
# --------------------------------------------------------------------------- #
@app.get("/api/logs/tail")
async def logs_tail(size: int = 30):
    q = {"size": size, "sort": [{"@timestamp": "desc"}]}
    async with httpx.AsyncClient() as c:
        try:
            r = (await c.post(f"{ES_URL}/logs-*/_search", json=q, timeout=5)).json()
            return [h["_source"] for h in r.get("hits", {}).get("hits", [])]
        except Exception as e:
            return {"error": str(e)[:120]}


@app.get("/api/trace/{correlation_id}")
async def trace(correlation_id: str):
    """Return one request's path across all services, ordered by event time."""
    q = {"size": 200, "query": {"term": {"correlation_id": correlation_id}},
         "sort": [{"event_time_ms": "asc"}]}
    async with httpx.AsyncClient() as c:
        try:
            r = (await c.post(f"{ES_URL}/logs-*/_search", json=q, timeout=5)).json()
            hits = [h["_source"] for h in r.get("hits", {}).get("hits", [])]
            return {"correlation_id": correlation_id, "span_count": len(hits),
                    "services": sorted({h["service_name"] for h in hits}), "events": hits}
        except Exception as e:
            return {"error": str(e)[:120]}


@app.get("/api/health")
async def health():
    return {"status": "ok"}


# Serve the single-page UI at "/".
_UI_DIR = os.getenv("UI_DIR", "/app/ui")
if os.path.isdir(_UI_DIR):
    @app.get("/")
    async def index():
        return FileResponse(os.path.join(_UI_DIR, "index.html"))
    app.mount("/static", StaticFiles(directory=_UI_DIR), name="static")
