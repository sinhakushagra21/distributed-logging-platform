# Build context is the repo root (so we can COPY the shared proto/ dir).
FROM python:3.12-slim

WORKDIR /app

COPY log-shipper/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Generate the Python protobuf bindings from the SHARED schema at build time,
# so the image always matches proto/log_event.proto (no checked-in _pb2.py to
# drift). grpc_tools bundles protoc, so no separate protoc install is needed.
COPY proto/log_event.proto ./proto/log_event.proto
RUN python -m grpc_tools.protoc -I./proto --python_out=. ./proto/log_event.proto

COPY log-shipper/shipper.py log-shipper/consume_logs.py ./

EXPOSE 8080
CMD ["uvicorn", "shipper:app", "--host", "0.0.0.0", "--port", "8080", "--log-level", "warning"]
