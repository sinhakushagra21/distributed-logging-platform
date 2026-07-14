"""FastAPI app factory shared by all services.

Guarantees every service is wired up identically: JSON logging, the pure-ASGI
correlation-id middleware, and the /admin router (health + error-mode toggle).
Each service then just adds its own business endpoints.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI

from .runtime import admin_router
from .tracing import CorrelationIdMiddleware


def create_service(service_name: str, logger: logging.Logger) -> FastAPI:
    app = FastAPI(title=service_name)

    # Pure-ASGI middleware (added via add_middleware wraps the whole app) so the
    # correlation-id ContextVar is bound in the same task the endpoints run in.
    app.add_middleware(CorrelationIdMiddleware)
    app.include_router(admin_router())

    @app.on_event("startup")
    async def _startup():
        logger.info(f"{service_name} started", extra={"event": "startup"})

    return app
