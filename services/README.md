# Stage 1 — Mock microservices + load generator

A mock ride-hailing backend that produces realistic, correlation-id-tagged
structured logs. This is the **data source** for the whole pipeline.

## Components

```
services/
├── common/              # shared, so all services behave identically
│   ├── tracing.py       #   correlation-id ContextVar + pure-ASGI middleware
│   ├── log_setup.py     #   JSON log formatter (core schema + context fields)
│   ├── runtime.py       #   error-injection state + /admin router
│   ├── sim.py           #   latency jitter + weighted level outcomes
│   └── app.py           #   FastAPI app factory (wires the above)
├── api_gateway/  (:8000)  front door; mints correlation id; fans out ↓
├── auth/         (:8001)  validates the user
├── trip/         (:8002)  creates trip + assigns driver
├── payments/     (:8003)  authorizes payment
└── loadgen/               configurable load generator (CLI + control daemon)
```

Request flow: `client → api-gateway → auth → trip → payments`. The gateway
propagates one `X-Correlation-ID` header down every hop.

## Run locally (no Docker)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
./run_local.sh                      # starts all four services

# fire one request; the response echoes the correlation id header
curl -s -D - -XPOST localhost:8000/rides/request \
  -H 'content-type: application/json' \
  -d '{"user_id":"u1","pickup":"soma","dropoff":"airport"}'

tail -f logs/*.log                  # structured JSON logs
./run_local.sh stop
```

## Trace one request across all services

```bash
CID=$(curl -s -D - -o /dev/null -XPOST localhost:8000/rides/request \
      -H 'content-type: application/json' -d '{"user_id":"u1"}' \
      | awk 'tolower($1)=="x-correlation-id:"{print $2}' | tr -d '\r')
grep -h "$CID" logs/*.log            # same id in api-gateway, auth, trip, payments
```

## Generate load

```bash
# one-shot: 200 rps for 10s, prints achieved throughput + p50/p95
python loadgen/load_generator.py --rps 200 --duration 10 --target http://localhost:8000

# daemon with control API (used by the Stage 7 UI)
python loadgen/load_generator.py --serve --port 9100 --target http://localhost:8000 --rps 50
curl -XPOST 'localhost:9100/rps?value=800'
curl -XPOST 'localhost:9100/burst?multiplier=6&duration_s=15'
curl        'localhost:9100/stats'
```

## Inject an error spike (demo the error-rate story later)

```bash
for p in 8000 8001 8002 8003; do
  curl -s -XPOST "localhost:$p/admin/error-mode?enabled=true&duration_s=30" >/dev/null
done
```

## Log schema

Every line is one JSON object:

```json
{"timestamp":"2026-07-14T06:33:28.370094Z","service_name":"api-gateway",
 "level":"INFO","correlation_id":"081d559b-...","message":"ride request completed",
 "endpoint":"/rides/request","user_id":"user-33554","status_code":200,
 "latency_ms":444.82,"trip_id":"trip-...","payment_id":"pay-..."}
```

Core fields (`timestamp, service_name, level, correlation_id, message`) are on
every record; contextual fields vary by call site.
