"""Configurable load generator for the mock ride-hailing fleet.

Two modes, one engine:

  * One-shot CLI (demo scale from the terminal):
        python load_generator.py --rps 200 --duration 10 \
            --target http://localhost:8000

  * Long-lived daemon with a control API (used by the Stage 7 UI so a slider
    can change the rate live and a button can fire a burst):
        python load_generator.py --serve --port 9100 \
            --target http://localhost:8000 --rps 50
    then:  curl -XPOST 'localhost:9100/rps?value=800'
           curl -XPOST 'localhost:9100/burst?multiplier=6&duration_s=15'
           curl        'localhost:9100/stats'

Pacing model
------------
We slice each second into `TICKS_PER_SEC` ticks and, on every tick, launch
`target_rps / TICKS_PER_SEC` fire-and-forget requests. In-flight requests are
bounded by a semaphore: if the fleet can't keep up (backpressure), we simply
fail to reach the target rate rather than pile up unbounded memory. The stats
therefore report the *achieved* throughput, not just the requested one — which
is exactly the honest number that belongs on a resume.
"""

from __future__ import annotations

import argparse
import asyncio
import random
import time
from collections import deque

import httpx

TICKS_PER_SEC = 20  # 50ms granularity: smooth enough, cheap enough

PICKUPS = ["downtown", "airport", "mission", "soma", "midtown", "harbor"]
DROPOFFS = ["airport", "downtown", "uptown", "stadium", "pier", "campus"]


class LoadEngine:
    """Drives traffic at an adjustable requests-per-second target."""

    def __init__(self, target: str, rps: float, concurrency: int = 2000):
        self.target = target.rstrip("/")
        self.target_rps = float(rps)
        self._burst_until = 0.0
        self._burst_multiplier = 1.0

        self._sem = asyncio.Semaphore(concurrency)
        self._client: httpx.AsyncClient | None = None
        self._running = False
        self._tasks: set[asyncio.Task] = set()  # in-flight request tasks

        # Stats.
        self.sent = 0
        self.completed = 0
        self.status_buckets = {"2xx": 0, "4xx": 0, "5xx": 0, "error": 0}
        self._latencies: deque[float] = deque(maxlen=5000)   # recent, for pXX
        self._completions: deque[float] = deque(maxlen=50000)  # timestamps, for rps

    # ---- live controls (also exposed over HTTP in daemon mode) -------------
    @property
    def effective_rps(self) -> float:
        if time.time() < self._burst_until:
            return self.target_rps * self._burst_multiplier
        return self.target_rps

    def set_rps(self, value: float):
        self.target_rps = max(0.0, float(value))

    def burst(self, multiplier: float, duration_s: float):
        self._burst_multiplier = max(1.0, float(multiplier))
        self._burst_until = time.time() + float(duration_s)

    def throughput_rps(self, window_s: float = 2.0) -> float:
        """Achieved completions/sec over a trailing window."""
        now = time.time()
        cutoff = now - window_s
        while self._completions and self._completions[0] < cutoff:
            self._completions.popleft()
        return round(len(self._completions) / window_s, 1)

    def percentiles(self) -> dict:
        if not self._latencies:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0}
        data = sorted(self._latencies)

        def pct(p):
            idx = min(len(data) - 1, int(round((p / 100.0) * (len(data) - 1))))
            return round(data[idx], 1)

        return {"p50": pct(50), "p95": pct(95), "p99": pct(99)}

    def snapshot(self) -> dict:
        return {
            "target_rps": self.target_rps,
            "effective_rps": self.effective_rps,
            "achieved_rps": self.throughput_rps(),
            "sent": self.sent,
            "completed": self.completed,
            "in_flight": self.sent - self.completed,
            "status": dict(self.status_buckets),
            "latency_ms": self.percentiles(),
            "bursting": time.time() < self._burst_until,
        }

    # ---- request firing -----------------------------------------------------
    async def _fire_one(self):
        payload = {
            "user_id": f"user-{random.randint(1, 100_000)}",
            "pickup": random.choice(PICKUPS),
            "dropoff": random.choice(DROPOFFS),
        }
        t0 = time.perf_counter()
        async with self._sem:
            try:
                resp = await self._client.post(
                    f"{self.target}/rides/request", json=payload
                )
                bucket = f"{resp.status_code // 100}xx"
                self.status_buckets[bucket] = self.status_buckets.get(bucket, 0) + 1
            except Exception:
                # Timeouts / connection refused count as errors (fleet overloaded).
                self.status_buckets["error"] += 1
            finally:
                self._latencies.append((time.perf_counter() - t0) * 1000.0)
                self.completed += 1
                self._completions.append(time.time())

    async def run(self):
        """Main pacing loop. Runs until stop() is called."""
        self._running = True
        self._client = httpx.AsyncClient(
            timeout=10.0, limits=httpx.Limits(max_connections=None)
        )
        tick = 1.0 / TICKS_PER_SEC
        # Carry fractional requests across ticks so low rps values are honoured.
        carry = 0.0
        try:
            while self._running:
                start = time.perf_counter()
                want = self.effective_rps / TICKS_PER_SEC + carry
                n = int(want)
                carry = want - n
                for _ in range(n):
                    self.sent += 1
                    t = asyncio.create_task(self._fire_one())
                    # Track tasks so we can drain them cleanly on stop (otherwise
                    # closing the client under in-flight requests warns noisily).
                    self._tasks.add(t)
                    t.add_done_callback(self._tasks.discard)
                # Sleep the remainder of the tick to hold the cadence.
                elapsed = time.perf_counter() - start
                await asyncio.sleep(max(0.0, tick - elapsed))
        finally:
            # Let in-flight requests finish (bounded) before tearing down.
            if self._tasks:
                await asyncio.wait(set(self._tasks), timeout=11.0)
            await self._client.aclose()

    def stop(self):
        self._running = False


# --------------------------------------------------------------------------- #
# One-shot CLI mode
# --------------------------------------------------------------------------- #
async def run_oneshot(engine: LoadEngine, duration: float):
    task = asyncio.create_task(engine.run())
    end = time.time() + duration
    print(f"[loadgen] firing ~{engine.target_rps:.0f} rps at {engine.target} "
          f"for {duration:.0f}s ...")
    while time.time() < end:
        await asyncio.sleep(1.0)
        s = engine.snapshot()
        print(f"[loadgen] achieved={s['achieved_rps']:.0f}/s "
              f"sent={s['sent']} completed={s['completed']} "
              f"2xx={s['status']['2xx']} 4xx={s['status']['4xx']} "
              f"5xx={s['status']['5xx']} err={s['status']['error']} "
              f"p50={s['latency_ms']['p50']}ms p95={s['latency_ms']['p95']}ms")
    engine.stop()
    await task
    print("\n[loadgen] FINAL:", engine.snapshot())


# --------------------------------------------------------------------------- #
# Daemon mode (control API for the Stage 7 UI)
# --------------------------------------------------------------------------- #
def build_app(engine: LoadEngine):
    from fastapi import FastAPI

    app = FastAPI(title="load-generator")

    @app.on_event("startup")
    async def _start():
        app.state.task = asyncio.create_task(engine.run())

    @app.on_event("shutdown")
    async def _stop():
        engine.stop()

    @app.get("/admin/health")
    async def health():
        return {"status": "ok"}

    @app.get("/rps")
    async def get_rps():
        return {"target_rps": engine.target_rps, "effective_rps": engine.effective_rps}

    @app.post("/rps")
    async def set_rps(value: float):
        engine.set_rps(value)
        return {"target_rps": engine.target_rps}

    @app.post("/burst")
    async def burst(multiplier: float = 5.0, duration_s: float = 15.0):
        engine.burst(multiplier, duration_s)
        return {"bursting": True, "multiplier": multiplier, "duration_s": duration_s}

    @app.get("/stats")
    async def stats():
        return engine.snapshot()

    return app


def main():
    parser = argparse.ArgumentParser(description="Load generator for the mock fleet")
    parser.add_argument("--target", default="http://localhost:8000",
                        help="api-gateway base URL")
    parser.add_argument("--rps", type=float, default=100.0,
                        help="initial/target requests per second")
    parser.add_argument("--duration", type=float, default=15.0,
                        help="(one-shot mode) seconds to run")
    parser.add_argument("--concurrency", type=int, default=2000,
                        help="max in-flight requests (backpressure bound)")
    parser.add_argument("--serve", action="store_true",
                        help="run as a daemon with a control API instead of one-shot")
    parser.add_argument("--port", type=int, default=9100,
                        help="(daemon mode) control API port")
    args = parser.parse_args()

    engine = LoadEngine(args.target, args.rps, concurrency=args.concurrency)

    if args.serve:
        import uvicorn
        uvicorn.run(build_app(engine), host="0.0.0.0", port=args.port,
                    log_level="warning")
    else:
        asyncio.run(run_oneshot(engine, args.duration))


if __name__ == "__main__":
    main()
