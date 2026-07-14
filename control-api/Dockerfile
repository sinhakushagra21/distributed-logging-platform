# Build context is the repo root (so we can COPY the ui/ directory).
FROM python:3.12-slim
WORKDIR /app

COPY control-api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY control-api/app.py .
COPY ui/ ./ui/

ENV UI_DIR=/app/ui
EXPOSE 8090
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8090", "--log-level", "warning"]
