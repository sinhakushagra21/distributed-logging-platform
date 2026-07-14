"""payments service — authorizes payment for a trip.

Final downstream hop and the most latency-heavy (talks to an external "payment
processor"). Has its own domain-specific outcomes: declines (WARN, 402) vs
processor outages (ERROR, 502). This variety makes per-service error-rate charts
in Stage 5 look realistic rather than uniform.
"""

from __future__ import annotations

import uuid

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from common.app import create_service
from common.log_setup import setup_logging
from common.runtime import STATE
from common.sim import decide_outcome, pick_region, should_sample_debug, simulate_latency

SERVICE_NAME = "payments"
logger = setup_logging(SERVICE_NAME)
app: FastAPI = create_service(SERVICE_NAME, logger)


class PaymentRequest(BaseModel):
    user_id: str
    trip_id: str
    amount: float = 20.0


@app.post("/payments/authorize")
async def authorize(req: PaymentRequest):
    region = pick_region()
    payment_id = f"pay-{uuid.uuid4().hex[:12]}"
    ctx = {
        "endpoint": "/payments/authorize",
        "region": region,
        "user_id": req.user_id,
        "trip_id": req.trip_id,
        "payment_id": payment_id,
        "amount": req.amount,
    }

    if should_sample_debug(STATE.debug_sample_rate):
        logger.debug("contacting payment processor", extra={**ctx, "processor": "stripe-sim"})

    latency = await simulate_latency(base_ms=60, jitter_ms=25, slow_tail_prob=0.08)
    outcome = decide_outcome(STATE.error_rate, STATE.warn_rate)
    ctx["latency_ms"] = latency

    if outcome == "error":
        ctx["status_code"] = 502
        logger.error("payment processor timeout", extra=ctx)
        return JSONResponse(status_code=502, content={"error": "payment processor error"})

    if outcome == "warn":
        ctx["status_code"] = 402
        logger.warning("card declined", extra=ctx)
        return JSONResponse(status_code=402, content={"authorized": False, "reason": "declined"})

    ctx["status_code"] = 200
    logger.info("payment authorized", extra=ctx)
    return {"authorized": True, "payment_id": payment_id, "amount": req.amount}
