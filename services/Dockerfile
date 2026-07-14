# One image for the whole Python fleet (all 4 services + load generator).
# They share the same dependencies and the `common` package, so a single image
# with a per-container CMD override (set in docker-compose) is DRY and keeps the
# build cache warm. Build context is the `services/` directory.
FROM python:3.12-slim

WORKDIR /app

# Install deps first for layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the shared package, the service apps, and the load generator.
COPY common/ ./common/
COPY api_gateway/ ./api_gateway/
COPY auth/ ./auth/
COPY trip/ ./trip/
COPY payments/ ./payments/
COPY loadgen/ ./loadgen/

# Logs are written here; compose/k8s mount a volume so Fluent Bit can tail them.
ENV LOG_DIR=/var/log/fleet
RUN mkdir -p /var/log/fleet

# Default command runs the gateway; every other service overrides `command:`
# in docker-compose (e.g. uvicorn auth.main:app --port 8001).
EXPOSE 8000
CMD ["uvicorn", "api_gateway.main:app", "--host", "0.0.0.0", "--port", "8000"]
