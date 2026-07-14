"""Correlation-ID propagation for distributed tracing.

The whole point of a correlation id is that ONE incoming user request produces
log lines in every service it touches, and all of those lines carry the *same*
id. Later (Kafka -> Flink -> Elasticsearch) we can type that id into Kibana and
reconstruct the request's entire path across the fleet.

How it flows:

  client --> api-gateway            (gateway mints a UUID4 if none supplied)
              |  X-Correlation-ID: <uuid>
              v
            auth / trip / payments  (read the header, reuse the same id)

We store the id in a `contextvars.ContextVar`. A ContextVar is the async-safe
equivalent of thread-local storage: each concurrently-handled request gets its
own isolated value, even though they all run on the same event loop. The JSON
log formatter (see log_setup.py) reads this ContextVar at format time, so every
`logger.info(...)` call inside a request automatically gets the right id with
no extra plumbing at each call site.

IMPORTANT design note: we use a *pure ASGI* middleware rather than Starlette's
`BaseHTTPMiddleware`. BaseHTTPMiddleware runs the endpoint in a separate task,
which breaks ContextVar propagation (the value set in the middleware would not
be visible inside the endpoint). A pure ASGI middleware runs in the same task,
so the ContextVar set here is visible everywhere downstream in the request.
"""

from __future__ import annotations

import uuid
from contextvars import ContextVar

import httpx

# Canonical header name used to pass the id between services.
CORRELATION_ID_HEADER = "x-correlation-id"

# Per-request storage. Default "-" means "no correlation id in scope" (e.g. a
# log emitted at startup, outside any request).
_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="-")


def get_correlation_id() -> str:
    """Return the correlation id bound to the current request context."""
    return _correlation_id.get()


def set_correlation_id(value: str):
    """Bind a correlation id to the current context; returns a reset token."""
    return _correlation_id.set(value)


def new_correlation_id() -> str:
    """Generate a fresh correlation id (only the gateway should need this)."""
    return str(uuid.uuid4())


class CorrelationIdMiddleware:
    """Pure ASGI middleware that binds a correlation id for each HTTP request.

    - If the incoming request already has an X-Correlation-ID header (i.e. it
      came from an upstream service), we reuse it -> the id survives the hop.
    - Otherwise we mint a new UUID4. In practice only the api-gateway hits this
      branch, because it is the fleet's front door.
    - We also echo the id back on the response header, which is handy for the
      load generator and for manual `curl` debugging.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        # Only HTTP requests carry correlation ids; pass through websockets etc.
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers") or [])
        raw = headers.get(CORRELATION_ID_HEADER.encode())
        cid = raw.decode() if raw else new_correlation_id()

        token = set_correlation_id(cid)

        async def send_with_header(message):
            if message["type"] == "http.response.start":
                # Copy headers and append ours so clients can see the id.
                message.setdefault("headers", [])
                message["headers"].append(
                    (CORRELATION_ID_HEADER.encode(), cid.encode())
                )
            await send(message)

        try:
            await self.app(scope, receive, send_with_header)
        finally:
            # Reset so the ContextVar does not leak into an unrelated context.
            _correlation_id.reset(token)


def outbound_headers(extra: dict | None = None) -> dict:
    """Headers to attach to a downstream call so the id propagates.

    Call this whenever a service makes an HTTP call to another service.
    """
    headers = {CORRELATION_ID_HEADER: get_correlation_id()}
    if extra:
        headers.update(extra)
    return headers


def make_async_client(timeout: float = 5.0) -> httpx.AsyncClient:
    """A shared httpx client factory.

    We create one long-lived client per service (connection pooling) rather
    than one per request. The correlation id is added per-call via
    `outbound_headers`, not baked into the client, because it changes every
    request.
    """
    return httpx.AsyncClient(timeout=timeout)
