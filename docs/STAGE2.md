# Stage 2 — Log shipping to Kafka (Protobuf, KRaft)

Turns the fleet's log *files* into a durable, replayable Protobuf stream on Kafka.

```
services → /var/log/fleet/*.log → Fluent Bit → (HTTP JSON) → log-shipper
        → Protobuf → Kafka topic "logs" (3 partitions, keyed by correlation_id)
```

## Components

| Path | Role |
|---|---|
| [proto/log_event.proto](../proto/log_event.proto) | Shared wire schema (Python + Java generate from it) |
| [fluent-bit/fluent-bit.conf](../fluent-bit/fluent-bit.conf) | Tail + enrich + mask + HTTP output |
| [fluent-bit/mask.lua](../fluent-bit/mask.lua) | Redacts `user_id` digits at the edge |
| [log-shipper/shipper.py](../log-shipper/shipper.py) | JSON → Protobuf → Kafka (keyed by correlation_id) |
| [log-shipper/consume_logs.py](../log-shipper/consume_logs.py) | Verification consumer (decodes Protobuf) |
| [docker-compose.yml](../docker-compose.yml) | Kafka (KRaft) + kafka-init + fleet + Fluent Bit + shipper |

## Run + verify

```bash
docker compose up -d --build
# push traffic from the host (keep it modest on Docker Desktop's VM)
python services/loadgen/load_generator.py --rps 20 --duration 10 --target http://localhost:8000

# topics + per-partition offsets
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic logs

# decode Protobuf back out of Kafka (proves the round-trip + partition ordering)
docker compose run --rm --no-deps --entrypoint python log-shipper consume_logs.py --max 30
docker compose down
```

## Why these choices

- **Protobuf over JSON:** a shared, versioned schema (producer & consumer can't
  silently disagree), 3–10× smaller on the wire, and much faster to (de)serialize
  than parsing JSON text. Field tags give forward/backward compatibility.
- **Key by `correlation_id`:** Kafka orders only *within* a partition. Hashing the
  key routes all of one request's logs to one partition → its trace stays ordered,
  while different requests spread across partitions for parallelism.
- **3 partitions (lite):** upper bound on consumer parallelism; small enough for a
  laptop. Full profile raises this (Stage 6).
- **KRaft (no ZooKeeper):** one process is both broker and controller; simpler,
  fewer moving parts, the modern Kafka default.
- **Fluent Bit → shipper bridge:** Fluent Bit can't emit our custom Protobuf, so it
  does tail/enrich/mask and the shipper owns Protobuf + Kafka. HTTP retries + an
  idempotent producer give at-least-once file→Kafka delivery.
- **Masking at the edge:** PII is redacted before logs leave the collector, so raw
  sensitive data never lands in Kafka/Elasticsearch.
