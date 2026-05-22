#!/usr/bin/env python3
"""Aggregate KPIs from a realistic-50 worker bench raw log.

Usage:
    analyze_realistic50.py <raw.log>

Prints:
    duration_secs, n_workers
    pubs_total, pubs_per_sec
    subs_received_total, msg_loss_pct, sub_rx_per_sec
    queries_sent, queries_success, queries_timeout, queries_error,
        query_success_pct, query_timeout_pct
    sub_latency_p50/p95/p99 (us), get_latency_p50/p95/p99 (us)
    worker_startup p99 (ms): open + declare summed
"""

from __future__ import annotations

import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path


METRIC_RE = re.compile(r"metric (.+)")


def parse_metric(line: str) -> dict[str, str] | None:
    m = METRIC_RE.search(line)
    if not m:
        return None
    parts = m.group(1).split()
    kv = {}
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k] = v
    return kv


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(len(s) * pct / 100)) - 1))
    return s[k]


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    log_path = Path(argv[1])
    if not log_path.exists():
        print(f"missing log: {log_path}", file=sys.stderr)
        return 2

    worker_finals: dict[str, dict[str, str]] = {}
    worker_opens: dict[str, dict[str, str]] = {}
    sub_lat_p50, sub_lat_p95, sub_lat_p99 = [], [], []
    get_lat_p50, get_lat_p95, get_lat_p99 = [], [], []
    pub_put_p99 = []
    qbl_handled_total = 0
    pub_put_count_lines = 0
    pub_put_total_count = 0

    for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
        kv = parse_metric(line)
        if not kv:
            continue
        role = kv.get("role", kv.get("name", ""))
        if role == "worker":
            worker_opens[kv.get("index", "?")] = kv
        elif role == "worker_final":
            worker_finals[kv.get("index", "?")] = kv
        elif kv.get("name") == "worker_sub_latency":
            if "p50_us" in kv:
                sub_lat_p50.append(float(kv["p50_us"]))
                sub_lat_p95.append(float(kv["p95_us"]))
                sub_lat_p99.append(float(kv["p99_us"]))
        elif kv.get("name") == "worker_get_latency":
            if "p50_us" in kv:
                get_lat_p50.append(float(kv["p50_us"]))
                get_lat_p95.append(float(kv["p95_us"]))
                get_lat_p99.append(float(kv["p99_us"]))
        elif kv.get("name") == "worker_pub_put":
            if "p99_us" in kv:
                pub_put_p99.append(float(kv["p99_us"]))
                pub_put_count_lines += 1
                pub_put_total_count += int(kv.get("count", "0") or 0)

    n_workers = len(worker_finals)
    if n_workers == 0:
        print("no worker_final lines found", file=sys.stderr)
        return 2

    total_sub = sum(int(w["sub_count"]) for w in worker_finals.values())
    total_sent = sum(int(w["get_sent"]) for w in worker_finals.values())
    total_success = sum(int(w["get_success"]) for w in worker_finals.values())
    total_timeout = sum(int(w["get_timeouts"]) for w in worker_finals.values())
    total_reply_err = sum(int(w["get_reply_errors"]) for w in worker_finals.values())
    total_err = sum(int(w["get_errors"]) for w in worker_finals.values())
    qbl_handled_total = sum(int(w["qbl_handled"]) for w in worker_finals.values())

    # Recover duration from the last worker_pub_put line.
    # If unavailable, default 1.
    duration_s = 1
    last_pub_put_elapsed = []
    for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "worker_pub_put" in line and "elapsed_ms=" in line:
            m = re.search(r"elapsed_ms=(\d+)", line)
            if m:
                last_pub_put_elapsed.append(int(m.group(1)))
    if last_pub_put_elapsed:
        duration_s = max(last_pub_put_elapsed) / 1000.0
        duration_s = max(duration_s, 1)

    # Expected publish count = workers * n_pubs_per_worker * duration / put_period.
    # We can read n_publishers from the open lines.
    sample_open = next(iter(worker_opens.values()), {})
    n_pubs_per_worker = int(sample_open.get("n_publishers", "0") or 0)
    n_subs_per_worker = int(sample_open.get("n_subscribers", "0") or 0)
    n_queryables_per_worker = int(sample_open.get("n_queryables", "0") or 0)
    total_queryables = int(sample_open.get("total_queryables", "0") or 0)

    # Pub rate inferred from publisher metric counts:
    # pub_put_total_count = sum of (count from each worker_pub_put print) which is
    # only the *current period*. So we use it as an instantaneous rate
    # rather than total. Reconstruct total puts: workers × pubs × duration /
    # put_period.
    # For loss-pct, we need expected vs received. Subscribers wildcard the
    # whole tree, so each sub receives all (n_workers × n_pubs) pubs every
    # put-period. Expected = n_workers × n_pubs × duration / period × n_subs_per_worker.
    # We don't know put_period_ms here unless we parse out the supervisor
    # command. Hard-code conservative default 1000 ms.
    # Caller can adjust via env var if needed.
    import os

    put_period_ms = int(os.environ.get("PUT_PERIOD_MS", "1000"))
    get_period_ms = int(os.environ.get("GET_PERIOD_MS", "1000"))

    pubs_per_worker_per_sec = (
        n_pubs_per_worker * (1000.0 / put_period_ms) if put_period_ms else 0
    )
    total_pubs_per_sec = n_workers * pubs_per_worker_per_sec
    expected_per_sub_per_sec = total_pubs_per_sec
    expected_sub_total = (
        expected_per_sub_per_sec * duration_s * (n_subs_per_worker * n_workers)
    )
    msg_loss_pct = 0.0
    if expected_sub_total > 0:
        msg_loss_pct = max(0.0, (expected_sub_total - total_sub) / expected_sub_total * 100.0)

    expected_getter_per_sec = (
        n_workers * (n_queryables_per_worker if total_queryables else 0)
        if get_period_ms == 0
        else n_workers * 2 * (1000.0 / get_period_ms)  # 2 getters/worker default
    )

    print(f"n_workers                          : {n_workers}")
    print(f"duration_secs                      : {duration_s:.1f}")
    print(
        f"declare_pubs/declare_subs/declare_qbls (sample worker): "
        f"{sample_open.get('declare_pubs_ms')}/"
        f"{sample_open.get('declare_subs_ms')}/"
        f"{sample_open.get('declare_qbls_ms')} ms"
    )
    print(
        f"per_worker mix                     : pubs={n_pubs_per_worker} "
        f"subs={n_subs_per_worker} qbls={n_queryables_per_worker} "
        f"total_queryables={total_queryables}"
    )
    print(
        f"pub put rate (expected)            : {pubs_per_worker_per_sec:.1f}/sec/worker × "
        f"{n_workers} workers = {total_pubs_per_sec:.0f}/sec system"
    )
    print()
    print("--- Pub/Sub (msg) ---")
    print(f"subs received total                : {total_sub}")
    print(
        f"sub recv rate                      : {total_sub / max(duration_s, 1):.1f} /sec system  "
        f"({total_sub / max(duration_s, 1) / max(n_workers * n_subs_per_worker, 1):.1f} /sec per subscriber)"
    )
    print(f"expected sub total                 : {expected_sub_total:.0f}")
    print(f"msg loss vs expected               : {msg_loss_pct:.3f} %")
    print(
        f"sub latency p50/p95/p99 median (us): "
        f"{statistics.median(sub_lat_p50) if sub_lat_p50 else 0:.0f} / "
        f"{statistics.median(sub_lat_p95) if sub_lat_p95 else 0:.0f} / "
        f"{statistics.median(sub_lat_p99) if sub_lat_p99 else 0:.0f}"
    )
    print(
        f"sub latency p99 max                : "
        f"{max(sub_lat_p99) if sub_lat_p99 else 0:.0f} us  (across all 1-sec samples)"
    )
    print()
    print("--- Query/Service ---")
    print(f"queries sent (total over run)      : {total_sent}")
    print(f"queries success                    : {total_success}")
    print(f"queries timeout                    : {total_timeout}")
    print(f"queries reply_error                : {total_reply_err}")
    print(f"queries error                      : {total_err}")
    if total_sent > 0:
        print(f"query success pct                  : {100.0 * total_success / total_sent:.3f} %")
        print(f"query timeout pct                  : {100.0 * total_timeout / total_sent:.3f} %")
    print(f"qbl total handled (sanity)         : {qbl_handled_total} (should ≈ queries_success)")
    print(
        f"get latency p50/p95/p99 median (us): "
        f"{statistics.median(get_lat_p50) if get_lat_p50 else 0:.0f} / "
        f"{statistics.median(get_lat_p95) if get_lat_p95 else 0:.0f} / "
        f"{statistics.median(get_lat_p99) if get_lat_p99 else 0:.0f}"
    )
    print(
        f"get latency p99 max                : "
        f"{max(get_lat_p99) if get_lat_p99 else 0:.0f} us  (across all 1-sec samples)"
    )
    print()
    print("--- Pub put hold ---")
    print(
        f"pub_put latency p99 median (us)    : "
        f"{statistics.median(pub_put_p99) if pub_put_p99 else 0:.0f}"
    )
    print(f"pub_put p99 max (us)               : {max(pub_put_p99) if pub_put_p99 else 0:.0f}")

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
