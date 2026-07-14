"""Traffic + behaviour simulation helpers.

These make the mock fleet *look* like a real ride-hailing backend: variable
latency with occasional slow tails, a realistic mix of log levels (mostly INFO,
some WARN, occasional ERROR), and region tagging. Centralised so all services
feel consistent and so the level distribution is easy to reason about.
"""

from __future__ import annotations

import asyncio
import random

# A handful of regions so logs/metrics can be sliced by geography later.
REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-south-1"]


def pick_region() -> str:
    return random.choice(REGIONS)


async def simulate_latency(base_ms: float, jitter_ms: float,
                           slow_tail_prob: float = 0.05,
                           slow_tail_mult: float = 6.0) -> float:
    """Sleep for a realistic amount of time and return the latency in ms.

    Models a normal-ish latency around `base_ms` plus a heavy tail: with
    probability `slow_tail_prob` the request is much slower (GC pause, cold
    cache, a slow dependency) -> this is what makes p95/p99 interesting.
    """
    latency = max(1.0, random.gauss(base_ms, jitter_ms))
    if random.random() < slow_tail_prob:
        latency *= random.uniform(1.5, slow_tail_mult)
    await asyncio.sleep(latency / 1000.0)
    return round(latency, 2)


def decide_outcome(error_rate: float, warn_rate: float) -> str:
    """Classify a request outcome into 'error' | 'warn' | 'ok'.

    Errors take priority over warns. The caller maps this to a status code and
    a log level. Keeping the decision here (not scattered in each service) means
    the fleet-wide error rate is a single, tunable number.
    """
    r = random.random()
    if r < error_rate:
        return "error"
    if r < error_rate + warn_rate:
        return "warn"
    return "ok"


def should_sample_debug(sample_rate: float) -> bool:
    """Whether to emit a DEBUG line for this request.

    In production you rarely keep every DEBUG log at high volume; you sample.
    Stage 3's Flink job will also drop most DEBUG, but sampling at the source
    keeps file/Kafka volume sane during the scale demo.
    """
    return random.random() < sample_rate
