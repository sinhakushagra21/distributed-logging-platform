"""auth service — validates a user's session token.

In the ride-hailing flow this is the first downstream hop the gateway makes.
It demonstrates: reusing the inbound correlation id (never minting a new one),
jittered latency, and a realistic level mix. Auth failures surface as 401 (a
client error, logged WARN) while dependency-style failures surface as 500
(logged ERROR).
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from common.app import create_service
from common.log_setup import setup_logging
from common.runtime import STATE
from common.sim import decide_outcome, pick_region, should_sample_debug, simulate_latency

SERVICE_NAME = "auth"
logger = setup_logging(SERVICE_NAME)
app: FastAPI = create_service(SERVICE_NAME, logger)


class ValidateRequest(BaseModel):
    user_id: str
    token: str = "session-token"


@app.post("/auth/validate")
async def validate(req: ValidateRequest):
    region = pick_region()
    latency = await simulate_latency(base_ms=12, jitter_ms=5)
    outcome = decide_outcome(STATE.error_rate, STATE.warn_rate)

    ctx = {
        "endpoint": "/auth/validate",
        "region": region,
        "latency_ms": latency,
        "user_id": req.user_id,
    }

    if should_sample_debug(STATE.debug_sample_rate):
        logger.debug("token lookup in session store", extra={**ctx, "cache": "miss"})

    if outcome == "error":
        # e.g. session store unreachable -> we cannot authenticate anyone.
        ctx["status_code"] = 503
        logger.error("session store unavailable", extra=ctx)
        return JSONResponse(status_code=503, content={"error": "auth backend down"})

    if outcome == "warn":
        # e.g. token expired -> legitimate 401, worth a WARN not an ERROR.
        ctx["status_code"] = 401
        logger.warning("token expired or invalid", extra=ctx)
        return JSONResponse(status_code=401, content={"authorized": False})

    ctx["status_code"] = 200
    logger.info("token validated", extra=ctx)
    return {"authorized": True, "user_id": req.user_id}
