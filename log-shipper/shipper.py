"""log-shipper — the JSON -> Protobuf -> Kafka bridge.

Fluent Bit POSTs batches of JSON log records to /ingest. For each record we:
  1. build a `LogEvent` Protobuf message (typed core + string `extra` map),
  2. serialize it to bytes,
  3. produce it to the Kafka `logs` topic, KEYED BY correlation_id.

Why key by correlation_id?
  Kafka guarantees ordering only *within a partition*. Hashing the key to a
  partition means every log for a given request lands on the same partition and
  is therefore consumed in order — so a request's trace is never reordered.
  It also spreads load across partitions (different requests -> different
  partitions -> parallel consumers) without hot-spotting on any single service.

Delivery semantics: acks=all + retries + idempotent producer gives us
effectively-once production to Kafka under normal failures; combined with
Fluent Bit's HTTP retries, the path from file to Kafka is at-least-once.
"""

from __future__ import annotations

import os
import time

from confluent_kafka import Producer
from fastapi import FastAPI, Request

import log_event_pb2  # generated from proto/log_event.proto at image build time

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "logs")

# Fields we promote to typed Protobuf columns; everything else goes to `extra`.
_CORE_STR = {"timestamp", "service_name", "level", "correlation_id", "message",
             "region", "endpoint", "user_id", "host", "collector"}
_RESERVED = _CORE_STR | {"status_code", "latency_ms"}

app = FastAPI(title="log-shipper")


def _make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",                 # wait for the (single) in-sync replica
        "enable.idempotence": True,    # no duplicate/reordered produces on retry
        "linger.ms": 20,               # small batching window for throughput
        "compression.type": "lz4",     # cheaper network + disk for log data
        "client.id": "log-shipper",
    })


@app.on_event("startup")
async def _startup():
    app.state.producer = _make_producer()
    app.state.produced = 0
    app.state.errors = 0


@app.on_event("shutdown")
async def _shutdown():
    # Block until everything queued is actually delivered.
    app.state.producer.flush(10)


def _to_int(v, default=0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _to_float(v, default=0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def build_event(record: dict) -> "log_event_pb2.LogEvent":
    """Map one JSON log record to a LogEvent protobuf message."""
    evt = log_event_pb2.LogEvent()
    evt.timestamp = str(record.get("timestamp", ""))
    evt.service_name = str(record.get("service_name", ""))
    evt.level = str(record.get("level", ""))
    evt.correlation_id = str(record.get("correlation_id", "-"))
    evt.message = str(record.get("message", ""))
    evt.status_code = _to_int(record.get("status_code"))
    evt.latency_ms = _to_float(record.get("latency_ms"))
    evt.region = str(record.get("region", ""))
    evt.endpoint = str(record.get("endpoint", ""))
    evt.user_id = str(record.get("user_id", ""))
    evt.host = str(record.get("host", ""))
    evt.collector = str(record.get("collector", ""))
    evt.ingest_ts_ms = int(time.time() * 1000)

    # Anything not promoted to a typed field is preserved as a string in `extra`
    # (trip_id, payment_id, driver_id, downstream, ...). Fluent Bit's own `date`
    # field is dropped — we keep our ISO `timestamp` instead.
    for k, v in record.items():
        if k in _RESERVED or k == "date":
            continue
        evt.extra[k] = str(v)
    return evt


@app.post("/ingest")
async def ingest(request: Request):
    """Receive a Fluent Bit HTTP batch (JSON array) and produce to Kafka."""
    records = await request.json()
    if isinstance(records, dict):  # single record edge case
        records = [records]

    producer: Producer = app.state.producer
    accepted = 0
    for record in records:
        try:
            evt = build_event(record)
            producer.produce(
                TOPIC,
                key=evt.correlation_id.encode("utf-8"),
                value=evt.SerializeToString(),
            )
            accepted += 1
        except BufferError:
            # Local produce queue full -> flush and retry once (backpressure).
            producer.flush(5)
            producer.produce(TOPIC, key=evt.correlation_id.encode("utf-8"),
                             value=evt.SerializeToString())
            accepted += 1
        except Exception:
            app.state.errors += 1

    # Serve delivery callbacks without blocking the request path.
    producer.poll(0)
    app.state.produced += accepted
    return {"accepted": accepted, "total_produced": app.state.produced}


@app.get("/admin/health")
async def health():
    return {"status": "ok", "produced": app.state.produced,
            "errors": app.state.errors, "topic": TOPIC}
