"""Per-service runtime state + a small admin API.

This exists mainly to support the Stage 7 demo controls: a recruiter clicks
"inject errors" and the mock services start emitting a burst of ERROR logs so
the error-rate charts and the Grafana alert visibly react.

We keep it deliberately tiny: a module-level singleton holding a couple of
tunables, plus a FastAPI router that lets an operator (or the control-api in
Stage 7) toggle error mode over HTTP.
"""

from __future__ import annotations

import os
import time

from fastapi import APIRouter


class RuntimeState:
    """Mutable knobs that influence how a service behaves at runtime.

    error_rate is the probability that a given request produces an ERROR-level
    outcome. Normally low (baseline noise); when error_mode is on it jumps so
    the effect is obvious in dashboards. error_mode can also auto-expire so a
    "spike" is naturally transient (mirrors a real incident being mitigated).
    """

    def __init__(self):
        self.base_error_rate = float(os.getenv("BASE_ERROR_RATE", "0.02"))   # ~2%
        self.warn_rate = float(os.getenv("WARN_RATE", "0.08"))               # ~8%
        self.error_mode_rate = float(os.getenv("ERROR_MODE_RATE", "0.35"))   # ~35%
        self.debug_sample_rate = float(os.getenv("DEBUG_SAMPLE_RATE", "0.3"))
        self._error_mode_until: float = 0.0  # epoch seconds; 0 == off

    @property
    def error_mode(self) -> bool:
        return time.time() < self._error_mode_until

    @property
    def error_rate(self) -> float:
        return self.error_mode_rate if self.error_mode else self.base_error_rate

    def enable_error_mode(self, duration_s: float = 60.0):
        self._error_mode_until = time.time() + duration_s

    def disable_error_mode(self):
        self._error_mode_until = 0.0


# One shared instance per process.
STATE = RuntimeState()


def admin_router() -> APIRouter:
    """Router mounted by every service at /admin for demo control + health."""
    router = APIRouter(prefix="/admin", tags=["admin"])

    @router.get("/health")
    async def health():
        return {"status": "ok"}

    @router.get("/error-mode")
    async def get_error_mode():
        return {
            "error_mode": STATE.error_mode,
            "effective_error_rate": STATE.error_rate,
        }

    @router.post("/error-mode")
    async def set_error_mode(enabled: bool = True, duration_s: float = 60.0):
        if enabled:
            STATE.enable_error_mode(duration_s)
        else:
            STATE.disable_error_mode()
        return {"error_mode": STATE.error_mode}

    return router
