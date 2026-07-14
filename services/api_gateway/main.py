"""api-gateway — the fleet's front door and the origin of every correlation id.

Responsibilities:
  1. Accept the user's "request a ride" call.
  2. Because it is the entry point, this is where a brand-new correlation id is
     minted (by CorrelationIdMiddleware, if the client did not supply one).
  3. Fan out to the downstream services IN ORDER: auth -> trip -> payments,
     propagating the SAME correlation id on every hop via `outbound_headers()`.
  4. Aggregate the results (or short-circuit on failure) and return to the user.

Every log line here, and every log line the downstream services emit for this
request, carries the identical correlation id. That is the thread we later pull
in Kibana to see one request's full journey.
"""

from __future__ import annotations

import os
import time

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from common.app import create_service
from common.log_setup import setup_logging
from common.tracing import make_async_client, outbound_headers

SERVICE_NAME = "api-gateway"
logger = setup_logging(SERVICE_NAME)
app: FastAPI = create_service(SERVICE_NAME, logger)

# Downstream locations are configurable so the same code runs under uvicorn
# (localhost ports) and under docker-compose / k8s (service DNS names).
AUTH_URL = os.getenv("AUTH_URL", "http://localhost:8001")
TRIP_URL = os.getenv("TRIP_URL", "http://localhost:8002")
PAYMENTS_URL = os.getenv("PAYMENTS_URL", "http://localhost:8003")


@app.on_event("startup")
async def _open_client():
    # One pooled client for the process; correlation id is added per-call.
    app.state.client = make_async_client(timeout=5.0)


@app.on_event("shutdown")
async def _close_client():
    await app.state.client.aclose()


class RideRequest(BaseModel):
    user_id: str = "user-anon"
    pickup: str = "downtown"
    dropoff: str = "airport"


async def _call(method: str, url: str, json: dict) -> httpx.Response:
    """Make a downstream call, propagating the correlation id header."""
    client: httpx.AsyncClient = app.state.client
    return await client.request(method, url, json=json, headers=outbound_headers())


@app.post("/rides/request")
async def request_ride(req: RideRequest):
    started = time.perf_counter()
    base_ctx = {"endpoint": "/rides/request", "user_id": req.user_id}
    logger.info("received ride request", extra=base_ctx)

    # --- Hop 1: auth ---------------------------------------------------------
    try:
        auth_resp = await _call("POST", f"{AUTH_URL}/auth/validate",
                                 {"user_id": req.user_id})
    except httpx.HTTPError as exc:
        logger.error("auth call failed (network)", extra={**base_ctx, "status_code": 502,
                     "downstream": "auth", "error": str(exc)})
        return JSONResponse(status_code=502, content={"error": "auth unreachable"})

    if auth_resp.status_code != 200:
        logger.warning("auth rejected request", extra={**base_ctx,
                       "status_code": auth_resp.status_code, "downstream": "auth"})
        return JSONResponse(status_code=auth_resp.status_code,
                            content={"error": "authentication failed"})

    # --- Hop 2: trip ---------------------------------------------------------
    try:
        trip_resp = await _call("POST", f"{TRIP_URL}/trips",
                                {"user_id": req.user_id, "pickup": req.pickup,
                                 "dropoff": req.dropoff})
    except httpx.HTTPError as exc:
        logger.error("trip call failed (network)", extra={**base_ctx, "status_code": 502,
                     "downstream": "trip", "error": str(exc)})
        return JSONResponse(status_code=502, content={"error": "trip unreachable"})

    if trip_resp.status_code != 200:
        logger.error("trip creation failed", extra={**base_ctx,
                     "status_code": trip_resp.status_code, "downstream": "trip"})
        return JSONResponse(status_code=trip_resp.status_code,
                            content={"error": "could not create trip"})

    trip = trip_resp.json()

    # --- Hop 3: payments -----------------------------------------------------
    try:
        pay_resp = await _call("POST", f"{PAYMENTS_URL}/payments/authorize",
                               {"user_id": req.user_id, "trip_id": trip["trip_id"],
                                "amount": trip.get("fare_estimate", 20.0)})
    except httpx.HTTPError as exc:
        logger.error("payments call failed (network)", extra={**base_ctx, "status_code": 502,
                     "downstream": "payments", "error": str(exc)})
        return JSONResponse(status_code=502, content={"error": "payments unreachable"})

    total_ms = round((time.perf_counter() - started) * 1000, 2)

    if pay_resp.status_code != 200:
        logger.warning("payment not authorized", extra={**base_ctx,
                       "status_code": pay_resp.status_code, "downstream": "payments",
                       "latency_ms": total_ms, "trip_id": trip["trip_id"]})
        return JSONResponse(status_code=pay_resp.status_code,
                            content={"error": "payment failed", "trip_id": trip["trip_id"]})

    payment = pay_resp.json()
    logger.info("ride request completed", extra={**base_ctx, "status_code": 200,
                "latency_ms": total_ms, "trip_id": trip["trip_id"],
                "payment_id": payment.get("payment_id")})

    return {
        "status": "confirmed",
        "trip": trip,
        "payment": payment,
        "gateway_latency_ms": total_ms,
    }
