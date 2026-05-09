#!/usr/bin/env python3
"""
Aggregate topology-comparison benchmark results into a comparison report.

Reads `summary.json` from a topology-preset run directory and prints a
side-by-side comparison of client vs peer mode across all configured
scenarios. Use this to drive the perf_topology_report.md content.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


def fmt(v):
    if v is None or v == "n/a":
        return "n/a"
    if isinstance(v, float):
        return f"{v:.1f}"
    return str(v)


def collect_scenario(out_dir: Path, scenario_name: str) -> dict:
    """Aggregate per-scenario routing-layer max metrics across runs."""
    metrics = {
        "wtables_peer_init_max": 0,
        "ctrl_lock_ntu_wait_max": 0,
        "ntu_wallclock_max": 0,
        "decl_key_and_send_max": 0,
        "wtables_flush_peer_init_max": 0,
        "rtables_publisher_hold_max": 0,
        "first_sample_p99_ms": None,
    }
    fs_vals = []
    for run_log in sorted(out_dir.glob(f"{scenario_name}_run*.log")):
        text = run_log.read_text(errors="ignore")
        for line in text.splitlines():
            if "name=wtables_diag " in line and "site=peer_init" in line:
                m = re.search(r"hold_max_us=(\d+)", line)
                if m:
                    metrics["wtables_peer_init_max"] = max(
                        metrics["wtables_peer_init_max"], int(m.group(1))
                    )
            if (
                "name=ctrl_lock_diag " in line
                and "site=new_transport_unicast" in line
            ):
                m = re.search(r"acquire_wait_max_us=(\d+)", line)
                if m:
                    metrics["ctrl_lock_ntu_wait_max"] = max(
                        metrics["ctrl_lock_ntu_wait_max"], int(m.group(1))
                    )
            if "name=new_transport_unicast_diag" in line:
                m = re.search(r"max_us=(\d+)", line)
                if m:
                    metrics["ntu_wallclock_max"] = max(
                        metrics["ntu_wallclock_max"], int(m.group(1))
                    )
            if (
                "name=repropagate_subs_step_diag" in line
                and "step=decl_key_and_send" in line
            ):
                m = re.search(r"max_us=(\d+)", line)
                if m:
                    metrics["decl_key_and_send_max"] = max(
                        metrics["decl_key_and_send_max"], int(m.group(1))
                    )
            if (
                "name=wtables_flush_diag" in line
                and "site=peer_init" in line
            ):
                m = re.search(r"elapsed_max_us=(\d+)", line)
                if m:
                    metrics["wtables_flush_peer_init_max"] = max(
                        metrics["wtables_flush_peer_init_max"], int(m.group(1))
                    )
            if (
                "name=rtables_diag" in line
                and "role=publisher" not in line
            ):
                # rtables_diag doesn't carry a role tag; just take max hold_max_us.
                m = re.search(r"hold_max_us=(\d+)", line)
                if m:
                    metrics["rtables_publisher_hold_max"] = max(
                        metrics["rtables_publisher_hold_max"], int(m.group(1))
                    )
            if "first_sample_ms" in line:
                m = re.search(r"first_sample_ms=(\d+)", line)
                if m:
                    fs_vals.append(int(m.group(1)))
    if fs_vals:
        fs_vals.sort()
        metrics["first_sample_p99_ms"] = fs_vals[
            min(len(fs_vals) * 99 // 100, len(fs_vals) - 1)
        ]
        metrics["first_sample_max_ms"] = fs_vals[-1]
    return metrics


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True, help="benchmark output dir")
    args = parser.parse_args()

    out_dir = Path(args.dir)
    summary_path = out_dir / "summary.json"
    if not summary_path.exists():
        print(f"missing {summary_path}", file=sys.stderr)
        return 2

    summary = json.loads(summary_path.read_text())
    by_scenario: dict[str, list] = {}
    for r in summary["results"]:
        by_scenario.setdefault(r["scenario"]["name"], []).append(r)

    print("=" * 100)
    print("Topology comparison: per-scenario aggregated results")
    print("=" * 100)
    print()
    print(
        f"{'scenario':<38}{'rx_p99_ms':>10}{'fst_p99_ms':>11}"
        f"{'miss/zero':>11}{'cls_p99_ms':>11}"
    )
    print("-" * 100)
    for name, runs in by_scenario.items():
        # Pull aggregated stats from the first run summary block (per-run summary.json structure)
        # Actually the issue_comment.md aggregator runs across all runs; use that style.
        rx_p99s = []
        first_p99s = []
        miss = 0
        zero = 0
        close_p99s = []
        for r in runs:
            s = r.get("summary", {})
            rx = s.get("subscriber_latency_steady", {})
            if rx.get("p99") is not None:
                rx_p99s.append(rx["p99"] / 1000.0)
            fs = s.get("first_sample_ms", {})
            if fs.get("p99") is not None:
                first_p99s.append(fs["p99"])
            miss += s.get("missing_first_samples", 0)
            zero += s.get("zero_sample_subscribers", 0)
            cc = s.get("churn_close_ms", {})
            if cc.get("p99") is not None:
                close_p99s.append(cc["p99"])
        rx_p99 = max(rx_p99s) if rx_p99s else None
        first_p99 = max(first_p99s) if first_p99s else None
        close_p99 = max(close_p99s) if close_p99s else None
        print(
            f"{name:<38}"
            f"{fmt(rx_p99):>10}"
            f"{fmt(first_p99):>11}"
            f"{miss:>5}/{zero:<5}"
            f"{fmt(close_p99):>11}"
        )

    print()
    print("=" * 100)
    print("Routing-layer max metrics per scenario (microseconds)")
    print("=" * 100)
    print()
    print(
        f"{'scenario':<38}{'wt_pi_hold':>11}{'cl_ntu_wait':>13}"
        f"{'ntu_wc':>10}{'decl_key':>10}{'wt_flush':>10}"
    )
    print("-" * 100)
    for name in by_scenario:
        m = collect_scenario(out_dir, name)
        print(
            f"{name:<38}"
            f"{m['wtables_peer_init_max']:>11}"
            f"{m['ctrl_lock_ntu_wait_max']:>13}"
            f"{m['ntu_wallclock_max']:>10}"
            f"{m['decl_key_and_send_max']:>10}"
            f"{m['wtables_flush_peer_init_max']:>10}"
        )

    print()
    print("=" * 100)
    print("Blast-radius analysis: rx_p99 per 5-s bucket for restart scenarios")
    print("=" * 100)
    print(
        "For each `*_restart` scenario, separates subscribers into "
        "'restarted' (idx < restart_count) and 'normal' (idx >= "
        "restart_count) and prints the rx_p99 per 5-second bucket."
    )
    print()
    blast_radius_report(out_dir, summary)
    return 0


def blast_radius_report(out_dir: Path, summary: dict) -> None:
    """For every `*_restart` scenario, bucket per-sub rx p99 by 5-s window
    and split by restarted vs not. Prints a table per scenario.
    """
    import re
    from collections import defaultdict

    # Pull restart_count per scenario from summary.json command lines.
    restart_count = {}
    for r in summary["results"]:
        name = r["scenario"]["name"]
        cmd = r.get("command", [])
        if "--restart-count" in cmd:
            i = cmd.index("--restart-count")
            try:
                restart_count[name] = int(cmd[i + 1])
            except (IndexError, ValueError):
                pass

    BUCKET_MS = 5000
    for name, k in sorted(restart_count.items()):
        if k == 0:
            continue
        # Aggregate by (group, bucket) -> [p99 samples]
        buckets_restart = defaultdict(list)
        buckets_normal = defaultdict(list)
        for run in (1, 2, 3):
            log = out_dir / f"{name}_run{run}.log"
            if not log.exists():
                continue
            for line in log.read_text(errors="ignore").splitlines():
                m = re.search(
                    r"name=subscriber_latency index=(\d+) elapsed_ms=(\d+).*?p99_us=(\d+)",
                    line,
                )
                if not m:
                    continue
                idx = int(m.group(1))
                t = int(m.group(2))
                p99 = int(m.group(3))
                bucket = (t // BUCKET_MS) * BUCKET_MS
                if idx < k:
                    buckets_restart[bucket].append(p99)
                else:
                    buckets_normal[bucket].append(p99)
        if not (buckets_restart or buckets_normal):
            continue
        print(f"\n  {name} (restart {k} subs):")
        print(
            f"    {'bucket (s)':>12}  "
            f"{'restarted_p99 (ms)':>20}  "
            f"{'normal_p99 (ms)':>20}"
        )
        all_buckets = sorted(set(buckets_restart) | set(buckets_normal))
        for b in all_buckets:
            r_vals = buckets_restart.get(b, [])
            n_vals = buckets_normal.get(b, [])
            if not r_vals and not n_vals:
                continue
            r_p99 = (
                sorted(r_vals)[len(r_vals) * 99 // 100] / 1000.0 if r_vals else 0.0
            )
            n_p99 = (
                sorted(n_vals)[len(n_vals) * 99 // 100] / 1000.0 if n_vals else 0.0
            )
            print(
                f"    {b // 1000:>12}  "
                f"{r_p99:>20.1f}  "
                f"{n_p99:>20.1f}"
            )


if __name__ == "__main__":
    raise SystemExit(main())
