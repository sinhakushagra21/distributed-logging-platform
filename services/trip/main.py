"""trip service — creates a trip/order and assigns a driver.

Second downstream hop. Slightly higher base latency than auth (it does more
work: driver matching). Emits a couple of INFO breadcrumbs per request so a
single trace shows meaningful internal steps, plus occasional "no drivers
nearby" WARNs.
"""

from __future__ import annotations

import random
import uuid

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from common.app import create_service
from common.log_setup import setup_logging
from common.runtime import STATE
from common.sim import decide_outcome, pick_region, should_sample_debug, simulate_latency

SERVICE_NAME = "trip"
logger = setup_logging(SERVICE_NAME)
app: FastAPI = create_service(SERVICE_NAME, logger)


class TripRequest(BaseModel):
    user_id: str
    pickup: str = "unknown"
    dropoff: str = "unknown"


@app.post("/trips")
async def create_trip(req: TripRequest):
    region = pick_region()
    trip_id = f"trip-{uuid.uuid4().hex[:12]}"
    base_ctx = {
        "endpoint": "/trips",
        "region": region,
        "user_id": req.user_id,
        "trip_id": trip_id,
    }

    if should_sample_debug(STATE.debug_sample_rate):
        logger.debug("matching nearby drivers", extra={**base_ctx, "radius_km": 3})

    latency = await simulate_latency(base_ms=35, jitter_ms=15)
    outcome = decide_outcome(STATE.error_rate, STATE.warn_rate)
    ctx = {**base_ctx, "latency_ms": latency}

    if outcome == "error":
        ctx["status_code"] = 500
        logger.error("driver matching service crashed", extra=ctx)
        return JSONResponse(status_code=500, content={"error": "trip creation failed"})

    if outcome == "warn":
        ctx["status_code"] = 200
        logger.warning("no drivers nearby, widening search", extra=ctx)
        # Still succeeds, just slower / degraded.

    driver_id = f"driver-{random.randint(1000, 9999)}"
    ctx.setdefault("status_code", 200)
    ctx["driver_id"] = driver_id
    logger.info("trip created and driver assigned", extra=ctx)
    return {
        "trip_id": trip_id,
        "driver_id": driver_id,
        "eta_min": random.randint(2, 12),
        "fare_estimate": round(random.uniform(8, 45), 2),
    }
