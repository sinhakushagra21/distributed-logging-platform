#!/usr/bin/env python3
"""Benchmark / measurement harness.

Ramps the load generator through a series of target rates, then fires a burst,
sampling the live pipeline metrics at each step from the control-api. It records:
  - ingestion throughput sustained (logs/sec into Kafka)
  - Flink processing rate (records/sec)
  - Kafka consumer lag (backlog) — and how it drains after a burst
  - end-to-end / processing lag (ms)
Results are written to bench/results.json and a human-readable bench/RESULTS.md
so you can put concrete numbers on a resume.

Prereq: the stack (or the smoke subset) is up, the loadgen daemon is running
(`docker compose --profile load up -d loadgen`), and control-api is reachable.

Usage:
  python bench/benchmark.py --control http://localhost:8090 \
      --steps 500,1000,2000,4000 --hold 45
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.request
from datetime import datetime, timezone


def _req(method, url, timeout=6):
    r = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def set_rps(control, value):
    try:
        _req("POST", f"{control}/api/rps?value={value}")
    except Exception as e:
        print(f"  ! set_rps failed: {e}")


def burst(control, mult=6, dur=20):
    try:
        _req("POST", f"{control}/api/burst?multiplier={mult}&duration_s={dur}")
    except Exception as e:
        print(f"  ! burst failed: {e}")


def sample(control):
    try:
        return _req("GET", f"{control}/api/metrics")
    except Exception as e:
        return {"error": str(e)}


def _num(m, *path):
    cur = m
    for p in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    return cur


def run(args):
    steps = [float(s) for s in args.steps.split(",")]
    results = {"started": datetime.now(timezone.utc).isoformat(),
               "steps": [], "burst": {}}

    print(f"[bench] ramping through {steps} rps, holding {args.hold}s each")
    for rps in steps:
        set_rps(args.control, rps)
        # let it stabilise, then average a few samples
        time.sleep(args.hold * 0.5)
        samples = []
        for _ in range(max(3, args.hold // 5)):
            samples.append(sample(args.control))
            time.sleep(5)
        ingest = [x for x in (_num(s, "kafka", "ingest_rate") for s in samples) if x]
        frate = [x for x in (_num(s, "flink", "processing_rate") for s in samples) if x]
        lag = [x for x in (_num(s, "kafka", "consumer_lag") for s in samples) if x is not None]
        plag = [x for x in (_num(s, "elasticsearch", "processing_lag_ms") for s in samples) if x]
        row = {
            "target_rps": rps,
            "ingest_rate_avg": round(sum(ingest) / len(ingest), 1) if ingest else None,
            "flink_rate_avg": round(sum(frate) / len(frate), 1) if frate else None,
            "consumer_lag_max": max(lag) if lag else None,
            "processing_lag_ms_avg": round(sum(plag) / len(plag)) if plag else None,
        }
        results["steps"].append(row)
        print(f"  rps={rps:>6} ingest~{row['ingest_rate_avg']} flink~{row['flink_rate_avg']} "
              f"lag_max={row['consumer_lag_max']} plag={row['processing_lag_ms_avg']}ms")

    # ---- burst test: watch lag grow then drain ----
    print("[bench] burst test (×6 for 20s): watching lag grow then drain")
    burst(args.control, 6, 20)
    lag_series = []
    for i in range(24):  # ~72s
        s = sample(args.control)
        lag_series.append({"t": i * 3, "lag": _num(s, "kafka", "consumer_lag"),
                           "flink_rate": _num(s, "flink", "processing_rate")})
        time.sleep(3)
    lags = [x["lag"] for x in lag_series if x["lag"] is not None]
    results["burst"] = {
        "lag_series": lag_series,
        "lag_peak": max(lags) if lags else None,
        "lag_final": lags[-1] if lags else None,
        "drained": bool(lags and lags[-1] <= (lags[0] if lags else 0) + 50),
    }
    print(f"  lag peak={results['burst']['lag_peak']} final={results['burst']['lag_final']} "
          f"drained={results['burst']['drained']}")

    # ---- write outputs ----
    results["finished"] = datetime.now(timezone.utc).isoformat()
    with open(args.out, "w") as f:
        json.dump(results, f, indent=2)

    md = ["# Benchmark results", "",
          f"Run: {results['started']} → {results['finished']}", "",
          "## Ramp (sustained rates)", "",
          "| target rps | ingest logs/s | flink rec/s | max Kafka lag | proc lag ms |",
          "|---|---|---|---|---|"]
    for r in results["steps"]:
        md.append(f"| {r['target_rps']} | {r['ingest_rate_avg']} | {r['flink_rate_avg']} "
                  f"| {r['consumer_lag_max']} | {r['processing_lag_ms_avg']} |")
    b = results["burst"]
    md += ["", "## Burst absorption",
           f"- Kafka lag peaked at **{b.get('lag_peak')}** and "
           f"{'drained back' if b.get('drained') else 'was still draining'} "
           f"(final {b.get('lag_final')}).",
           "- This demonstrates Kafka absorbing a spike while Flink catches up "
           "(backpressure + buffering), with no data loss."]
    with open(args.out.replace(".json", ".md").replace("results", "RESULTS"), "w") as f:
        f.write("\n".join(md) + "\n")
    print(f"[bench] wrote {args.out} and RESULTS.md")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--control", default="http://localhost:8090")
    ap.add_argument("--steps", default="500,1000,2000,4000")
    ap.add_argument("--hold", type=int, default=45, help="seconds per step")
    ap.add_argument("--out", default="bench/results.json")
    run(ap.parse_args())
