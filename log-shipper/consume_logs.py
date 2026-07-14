"""Verification consumer: read Protobuf LogEvents back out of Kafka.

Proves the round-trip works end to end: bytes on the `logs` topic decode into
structured LogEvent messages, correlation ids are intact, and (as a bonus) all
records for one correlation id share a partition.

Run inside the compose network against kafka:9092, e.g.:
    docker compose run --rm --no-deps --entrypoint \
      "python consume_logs.py --max 20" log-shipper
"""

from __future__ import annotations

import argparse
import collections
import os

from confluent_kafka import Consumer

import log_event_pb2


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "logs"))
    ap.add_argument("--max", type=int, default=20, help="messages to read")
    ap.add_argument("--timeout", type=float, default=15.0)
    args = ap.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": f"verify-{os.getpid()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([args.topic])

    seen = 0
    by_partition = collections.Counter()
    by_service = collections.Counter()
    cid_partition = {}  # correlation_id -> set of partitions (should be size 1)
    import time
    deadline = time.time() + args.timeout
    try:
        while seen < args.max and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("consumer error:", msg.error())
                continue
            evt = log_event_pb2.LogEvent()
            evt.ParseFromString(msg.value())
            seen += 1
            by_partition[msg.partition()] += 1
            by_service[evt.service_name] += 1
            cid_partition.setdefault(evt.correlation_id, set()).add(msg.partition())
            if seen <= 8:
                print(f"[p{msg.partition()}] {evt.service_name:12} {evt.level:5} "
                      f"cid={evt.correlation_id[:8]} status={evt.status_code} "
                      f"lat={evt.latency_ms:.1f}ms :: {evt.message} "
                      f"| extra={dict(evt.extra)}")
    finally:
        consumer.close()

    print(f"\ndecoded {seen} protobuf LogEvents")
    print("by partition:", dict(by_partition))
    print("by service:  ", dict(by_service))
    multi = {c: p for c, p in cid_partition.items() if len(p) > 1}
    print("correlation_ids spanning >1 partition (should be 0):", len(multi))


if __name__ == "__main__":
    main()
