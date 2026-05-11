#!/usr/bin/env python3
#
# Copyright (c) 2026 ZettaScale Technology
#
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
# which is available at https://www.apache.org/licenses/LICENSE-2.0.
#
# SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
#

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import platform
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import mean
from typing import Any


EXAMPLE = "z_p2p_routing_perf"


@dataclass(frozen=True)
class Scenario:
    name: str
    hub_mode: str
    leaf_mode: str
    idle_peers: int
    subscribers: int
    churners: int
    publishers: int = 1
    topics: int = 1
    restart_at_secs: int = 0
    restart_count: int = 0


@dataclass
class RunResult:
    scenario: dict[str, Any]
    run_index: int
    port: int
    command: list[str]
    raw_log: str
    return_code: int | None
    timed_out: bool
    summary: dict[str, Any]


def main() -> int:
    args = parse_args()
    if args.nofile_limit is not None:
        set_nofile_limit(args.nofile_limit)
    repo = Path(__file__).resolve().parents[2]
    out_dir = output_dir(repo, args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    runs = args.runs
    if runs is None:
        runs = 1 if args.preset == "quick" else 3
    duration_secs = args.duration_secs
    if duration_secs is None:
        duration_secs = 8 if args.preset == "quick" else 30

    scenarios = scenario_matrix(args.preset, args.include_200)
    env = os.environ.copy()
    env["RUST_LOG"] = args.rust_log

    if not args.no_build:
        run_checked(build_command(args.profile), repo, env, out_dir / "build.log")

    binary = example_binary(repo, args.profile)
    if not binary.exists():
        raise SystemExit(f"missing benchmark binary: {binary}")

    results: list[RunResult] = []
    for scenario_idx, scenario in enumerate(scenarios):
        for run_idx in range(runs):
            port = args.start_port + scenario_idx * runs + run_idx
            raw_log = out_dir / f"{scenario.name}_run{run_idx + 1}.log"
            command = supervisor_command(
                binary,
                scenario,
                port,
                duration_secs,
                args.startup_delay_ms,
                args.grace_secs,
                args.put_period_ms,
                args.payload_size,
                args.churn_hold_ms,
                args.churn_idle_ms,
                args.wait_declares,
                args.cfg,
            )
            print(f"[bench] running {scenario.name} run {run_idx + 1}/{runs} on port {port}")
            sample_path = out_dir / f"{scenario.name}_run{run_idx + 1}.proc.csv"
            completed = run_benchmark(
                command,
                repo,
                env,
                raw_log,
                timeout_secs=(
                    duration_secs
                    + args.grace_secs
                    + 2 * args.startup_delay_ms / 1000
                    + 20
                ),
                sample_path=sample_path,
                sample_interval_s=args.sample_interval_s,
            )
            metrics = parse_metrics(raw_log.read_text(errors="replace").splitlines())
            summary = summarize_run(metrics, scenario, args.startup_window_ms)
            results.append(
                RunResult(
                    scenario=asdict(scenario),
                    run_index=run_idx + 1,
                    port=port,
                    command=[str(part) for part in command],
                    raw_log=str(raw_log.relative_to(out_dir)),
                    return_code=completed.returncode,
                    timed_out=completed.timed_out,
                    summary=summary,
                )
            )

    report = {
        "environment": environment(repo),
        "parameters": {
            "preset": args.preset,
            "profile": args.profile,
            "runs": runs,
            "duration_secs": duration_secs,
            "include_200": args.include_200,
            "nofile_limit": args.nofile_limit,
            "startup_delay_ms": args.startup_delay_ms,
            "startup_window_ms": args.startup_window_ms,
            "grace_secs": args.grace_secs,
            "put_period_ms": args.put_period_ms,
            "payload_size": args.payload_size,
            "churn_hold_ms": args.churn_hold_ms,
            "churn_idle_ms": args.churn_idle_ms,
            "wait_declares": args.wait_declares,
            "rust_log": args.rust_log,
            "cfg": args.cfg,
        },
        "results": [asdict(result) for result in results],
        "scenario_summary": summarize_scenarios(results),
    }
    (out_dir / "summary.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (out_dir / "issue_comment.md").write_text(render_issue_comment(report), encoding="utf-8")
    print(f"[bench] wrote {out_dir / 'summary.json'}")
    print(f"[bench] wrote {out_dir / 'issue_comment.md'}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run p2p routing stress benchmarks and generate a GitHub issue comment."
    )
    parser.add_argument(
        "--preset",
        choices=["quick", "issue", "topology", "restart-sweep", "n-sweep"],
        default="quick",
    )
    parser.add_argument("--runs", type=int)
    parser.add_argument("--duration-secs", type=int)
    parser.add_argument("--include-200", action="store_true")
    parser.add_argument("--profile", choices=["dev", "fast", "release"], default="fast")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--output-dir")
    parser.add_argument("--nofile-limit", type=int)
    parser.add_argument("--start-port", type=int, default=17540)
    parser.add_argument("--startup-delay-ms", type=int, default=1000)
    parser.add_argument("--startup-window-ms", type=int, default=5000)
    parser.add_argument("--grace-secs", type=int, default=20)
    parser.add_argument("--put-period-ms", type=int, default=2)
    parser.add_argument("--payload-size", type=int, default=64)
    parser.add_argument("--churn-hold-ms", type=int, default=20)
    parser.add_argument("--churn-idle-ms", type=int, default=20)
    parser.add_argument(
        "--wait-declares",
        default=True,
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument("--rust-log", default="off")
    parser.add_argument("--cfg", action="append", default=[])
    parser.add_argument(
        "--sample-interval-s",
        type=float,
        default=1.0,
        help="Sampling cadence for the per-run process-tree RSS/CPU CSV.",
    )
    return parser.parse_args()


def output_dir(repo: Path, requested: str | None) -> Path:
    if requested:
        return Path(requested).resolve()
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    return repo / "target" / "p2p-routing-bench" / stamp


def build_command(profile: str) -> list[str]:
    command = ["cargo", "build", "-p", "zenoh-examples", "--example", EXAMPLE]
    if profile != "dev":
        command += ["--profile", profile]
    return command


def example_binary(repo: Path, profile: str) -> Path:
    target_profile = "debug" if profile == "dev" else profile
    extension = ".exe" if os.name == "nt" else ""
    return repo / "target" / target_profile / "examples" / f"{EXAMPLE}{extension}"


def scenario_matrix(preset: str, include_200: bool) -> list[Scenario]:
    if preset == "quick":
        scenarios = [
            Scenario("baseline_peer", "peer", "peer", 0, 1, 0),
            Scenario("p2p_20_idle", "peer", "peer", 20, 5, 0),
            Scenario("p2p_20_churn", "peer", "peer", 20, 5, 4),
            Scenario("client_20_churn", "router", "client", 20, 5, 4),
        ]
    elif preset == "n-sweep":
        # Phase 1 of the optimization roadmap: characterize the cliff.
        # Sweep total session count N across {50, 75, 100, 125, 150, 175,
        # 200, 250} for both peer and client modes, with a fixed
        # 1pub/20sub workload. idle_peers is N - publishers - subscribers,
        # so the harness ends up with N session-bearing processes
        # (excluding the supervisor itself).
        scenarios = []
        for n in (50, 75, 100, 125, 150, 175, 200, 250):
            idle = n - 1 - 20
            if idle < 0:
                continue
            scenarios.append(
                Scenario(
                    f"n{n:03d}_p2p_1pub_20sub", "peer", "peer", idle, 20, 0,
                    publishers=1, topics=1,
                )
            )
            scenarios.append(
                Scenario(
                    f"n{n:03d}_cli_1pub_20sub", "router", "client", idle, 20, 0,
                    publishers=1, topics=1,
                )
            )
        return scenarios
    elif preset == "restart-sweep":
        # Steady-state with small individual peer restart events.
        # Same 100-session 1pub/20sub topology as topology preset, but
        # vary restart_count over {0, 1, 3, 5, 6} to isolate the impact
        # of small-N restart events on never-restarted peers' rx tail
        # latency. Expected reading: blast-radius severity scales
        # sub-linearly with restart count; the 1-restart case is the
        # most realistic "rolling update" event.
        scenarios = []
        for k in (0, 1, 3, 5, 6):
            tag = "steady" if k == 0 else f"restart{k}"
            scenarios.append(
                Scenario(
                    f"100_p2p_1pub_20sub_{tag}", "peer", "peer", 79, 20, 0,
                    publishers=1, topics=1,
                    restart_at_secs=0 if k == 0 else 15,
                    restart_count=k,
                )
            )
            scenarios.append(
                Scenario(
                    f"100_cli_1pub_20sub_{tag}", "router", "client", 79, 20, 0,
                    publishers=1, topics=1,
                    restart_at_secs=0 if k == 0 else 15,
                    restart_count=k,
                )
            )
        return scenarios
    elif preset == "topology":
        # Topology comparison: client/peer × pub-count × restart variants.
        # idle_peers field becomes the "filler" sessions to push the total
        # session count to the target, on top of pub + sub + hub.
        # 100-session scenarios. total = 1 hub + N pub + M sub + idle.
        # Restart fires at 15s into a 30s run (mid-run), restarts 30% of subs.
        scenarios = [
            # 100 sessions, single publisher, shared topic — baseline shape
            Scenario("100_p2p_1pub_20sub", "peer", "peer", 79, 20, 0, publishers=1, topics=1),
            Scenario("100_cli_1pub_20sub", "router", "client", 79, 20, 0, publishers=1, topics=1),
            # 100 sessions, 5 publishers, 5 distinct topics, 30 subs (sub uses wildcard)
            Scenario("100_p2p_5pub_30sub_5tpc", "peer", "peer", 64, 30, 0, publishers=5, topics=5),
            Scenario("100_cli_5pub_30sub_5tpc", "router", "client", 64, 30, 0, publishers=5, topics=5),
            # Restart variants: same topology, restart 30% of subs at t=15s
            Scenario("100_p2p_1pub_20sub_restart", "peer", "peer", 79, 20, 0,
                     publishers=1, topics=1, restart_at_secs=15, restart_count=6),
            Scenario("100_cli_1pub_20sub_restart", "router", "client", 79, 20, 0,
                     publishers=1, topics=1, restart_at_secs=15, restart_count=6),
            Scenario("100_p2p_5pub_30sub_5tpc_restart", "peer", "peer", 64, 30, 0,
                     publishers=5, topics=5, restart_at_secs=15, restart_count=10),
            Scenario("100_cli_5pub_30sub_5tpc_restart", "router", "client", 64, 30, 0,
                     publishers=5, topics=5, restart_at_secs=15, restart_count=10),
        ]
        if include_200:
            scenarios += [
                Scenario("200_p2p_5pub_60sub_5tpc", "peer", "peer", 134, 60, 0,
                         publishers=5, topics=5),
                Scenario("200_cli_5pub_60sub_5tpc", "router", "client", 134, 60, 0,
                         publishers=5, topics=5),
                Scenario("200_p2p_5pub_60sub_5tpc_restart", "peer", "peer", 134, 60, 0,
                         publishers=5, topics=5, restart_at_secs=15, restart_count=20),
                Scenario("200_cli_5pub_60sub_5tpc_restart", "router", "client", 134, 60, 0,
                         publishers=5, topics=5, restart_at_secs=15, restart_count=20),
            ]
        return scenarios
    else:
        scenarios = [
            Scenario("baseline_peer", "peer", "peer", 0, 1, 0),
            Scenario("p2p_50_idle", "peer", "peer", 50, 10, 0),
            Scenario("p2p_50_churn", "peer", "peer", 50, 10, 8),
            Scenario("p2p_100_idle", "peer", "peer", 100, 20, 0),
            Scenario("p2p_100_churn", "peer", "peer", 100, 20, 16),
            Scenario("client_100_idle", "router", "client", 100, 20, 0),
            Scenario("client_100_churn", "router", "client", 100, 20, 16),
        ]
    if include_200:
        scenarios += [
            Scenario("p2p_200_idle", "peer", "peer", 200, 40, 0),
            Scenario("p2p_200_churn", "peer", "peer", 200, 40, 24),
            Scenario("client_200_churn", "router", "client", 200, 40, 24),
        ]
    return scenarios


def supervisor_command(
    binary: Path,
    scenario: Scenario,
    port: int,
    duration_secs: int,
    startup_delay_ms: int,
    grace_secs: int,
    put_period_ms: int,
    payload_size: int,
    churn_hold_ms: int,
    churn_idle_ms: int,
    wait_declares: bool,
    cfg: list[str],
) -> list[str]:
    command = [
        str(binary),
        "supervisor",
        "--endpoint",
        f"tcp/127.0.0.1:{port}",
        "--hub-mode",
        scenario.hub_mode,
        "--leaf-mode",
        scenario.leaf_mode,
        "--duration-secs",
        str(duration_secs),
        "--startup-delay-ms",
        str(startup_delay_ms),
        "--grace-secs",
        str(grace_secs),
        "--idle-peers",
        str(scenario.idle_peers),
        "--publishers",
        str(scenario.publishers),
        "--subscribers",
        str(scenario.subscribers),
        "--churners",
        str(scenario.churners),
        "--topics",
        str(scenario.topics),
        "--restart-at-secs",
        str(scenario.restart_at_secs),
        "--restart-count",
        str(scenario.restart_count),
        "--payload-size",
        str(payload_size),
        "--put-period-ms",
        str(put_period_ms),
        "--churn-hold-ms",
        str(churn_hold_ms),
        "--churn-idle-ms",
        str(churn_idle_ms),
        "--wait-declares",
        str(wait_declares).lower(),
    ]
    for item in cfg:
        command += ["--cfg", item]
    return command


@dataclass
class Completed:
    returncode: int | None
    timed_out: bool


def run_checked(command: list[str], cwd: Path, env: dict[str, str], log_path: Path) -> None:
    print(f"[bench] {' '.join(command)}")
    with log_path.open("w", encoding="utf-8") as log:
        completed = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    if completed.returncode != 0:
        raise SystemExit(f"command failed with code {completed.returncode}: {' '.join(command)}")


def run_benchmark(
    command: list[str],
    cwd: Path,
    env: dict[str, str],
    log_path: Path,
    timeout_secs: float,
    sample_path: Path | None = None,
    sample_interval_s: float = 1.0,
) -> Completed:
    """Runs the supervisor as a child process while a background thread
    samples the resulting process tree at ``sample_interval_s`` cadence.

    When ``sample_path`` is provided, the samples are written as CSV with
    one row per tick. The CSV schema is documented in
    ``sample_proc_tree``.
    """
    with log_path.open("w", encoding="utf-8") as log:
        proc = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        stop_event = threading.Event()
        sampler_thread = None
        if sample_path is not None:
            sampler_thread = threading.Thread(
                target=_sample_loop,
                args=(proc.pid, sample_path, sample_interval_s, stop_event),
                daemon=True,
            )
            sampler_thread.start()
        try:
            try:
                proc.wait(timeout=timeout_secs)
                timed_out = False
            except subprocess.TimeoutExpired:
                log.write(
                    f"\nmetric role=runner_timeout timeout_secs={timeout_secs:.1f}\n"
                )
                proc.kill()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    pass
                cleanup_orphans()
                timed_out = True
        finally:
            stop_event.set()
            if sampler_thread is not None:
                sampler_thread.join(timeout=5)
        returncode = None if timed_out else proc.returncode
        return Completed(returncode, timed_out)


def _sample_loop(
    supervisor_pid: int,
    csv_path: Path,
    interval_s: float,
    stop_event: threading.Event,
) -> None:
    """Polls the process tree every ``interval_s`` seconds until ``stop_event``
    is set or the supervisor exits. Writes one CSV row per tick.
    """
    start = time.monotonic()
    fields = [
        "t_ms",
        "child_count",
        "supervisor_rss_kb",
        "supervisor_cpu_pct",
        "tree_total_rss_kb",
        "tree_max_rss_kb",
        "tree_total_cpu_pct",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        while not stop_event.is_set():
            sample = sample_proc_tree(supervisor_pid)
            if sample is None:
                # supervisor exited; stop sampling
                return
            t_ms = int((time.monotonic() - start) * 1000)
            writer.writerow(
                {
                    "t_ms": t_ms,
                    "child_count": sample["child_count"],
                    "supervisor_rss_kb": sample["supervisor_rss_kb"],
                    "supervisor_cpu_pct": f"{sample['supervisor_cpu_pct']:.2f}",
                    "tree_total_rss_kb": sample["total_rss_kb"],
                    "tree_max_rss_kb": sample["max_rss_kb"],
                    "tree_total_cpu_pct": f"{sample['total_cpu_pct']:.2f}",
                }
            )
            f.flush()
            stop_event.wait(timeout=interval_s)


def sample_proc_tree(supervisor_pid: int) -> dict[str, Any] | None:
    """Samples the whole process tree rooted at ``supervisor_pid`` using
    ``ps``. Returns aggregate RSS (KB), aggregate CPU %, and child count,
    or ``None`` if the supervisor is no longer alive.

    RSS comes from ``ps -o rss`` (KB on Linux and macOS); CPU is the
    instantaneous ``%CPU`` value reported by ``ps`` (which on Linux is a
    cumulative average over the process lifetime, and on macOS is an
    instantaneous sample — caveat lector when comparing across OSes).
    """
    try:
        out = subprocess.check_output(
            ["ps", "-axo", "pid=,ppid=,rss=,pcpu="],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    procs: dict[int, tuple[int, int, float]] = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
            rss = int(parts[2])
            cpu = float(parts[3])
        except ValueError:
            continue
        procs[pid] = (ppid, rss, cpu)
    if supervisor_pid not in procs:
        return None
    descendants = {supervisor_pid}
    changed = True
    while changed:
        changed = False
        for pid, (ppid, _, _) in procs.items():
            if ppid in descendants and pid not in descendants:
                descendants.add(pid)
                changed = True
    rss_values = [procs[p][1] for p in descendants]
    cpu_values = [procs[p][2] for p in descendants]
    return {
        "child_count": len(descendants),
        "supervisor_rss_kb": procs[supervisor_pid][1],
        "supervisor_cpu_pct": procs[supervisor_pid][2],
        "total_rss_kb": sum(rss_values),
        "max_rss_kb": max(rss_values, default=0),
        "total_cpu_pct": sum(cpu_values),
    }


def cleanup_orphans() -> None:
    if shutil.which("pkill") is None:
        return
    subprocess.run(
        ["pkill", "-f", EXAMPLE],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


def parse_metrics(lines: list[str]) -> list[dict[str, Any]]:
    metrics: list[dict[str, Any]] = []
    for line in lines:
        if not line.startswith("metric "):
            continue
        metric: dict[str, Any] = {}
        for token in line[len("metric ") :].split():
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            metric[key] = parse_value(value)
        metrics.append(metric)
    return metrics


def parse_value(value: str) -> Any:
    if value == "true":
        return True
    if value == "false":
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def summarize_run(
    metrics: list[dict[str, Any]], scenario: Scenario, startup_window_ms: int
) -> dict[str, Any]:
    startup_put = named_period(
        metrics, ["publisher_put", "publisher_put_final"], startup_window_ms, le=True
    )
    steady_put = named_period(
        metrics, ["publisher_put", "publisher_put_final"], startup_window_ms, le=False
    )
    startup_rx_latency = named_period(
        metrics, ["subscriber_latency"], startup_window_ms, le=True
    )
    steady_rx_latency = named_period(
        metrics, ["subscriber_latency"], startup_window_ms, le=False
    )
    close_metrics = named_any(metrics, ["churn_close", "churn_close_final"])

    subscriber_finals = [
        metric
        for metric in metrics
        if metric.get("role") == "subscriber_rx_final" and "count" in metric
    ]
    first_samples = [
        metric["first_sample_ms"]
        for metric in metrics
        if metric.get("role") == "subscriber" and "first_sample_ms" in metric
    ]

    return {
        "open_ms": role_field_summary(metrics, "open_ms"),
        "declare_ms": role_field_summary(metrics, "declare_ms"),
        "first_sample_ms": number_summary(first_samples),
        "missing_first_samples": max(0, scenario.subscribers - len(first_samples)),
        "zero_sample_subscribers": sum(1 for metric in subscriber_finals if metric["count"] == 0),
        "subscriber_final_count": number_summary(
            [metric["count"] for metric in subscriber_finals]
        ),
        "subscriber_final_rate": number_summary(
            [metric["rate"] for metric in subscriber_finals if "rate" in metric]
        ),
        "publisher_put_startup_us": period_summary(startup_put),
        "publisher_put_steady_us": period_summary(steady_put),
        "subscriber_latency_startup_us": period_summary(startup_rx_latency),
        "subscriber_latency_steady_us": period_summary(steady_rx_latency),
        "churn_close_us": period_summary(close_metrics),
        "churn_cycles": sum(
            metric["cycles"]
            for metric in metrics
            if metric.get("role") == "churn_final" and "cycles" in metric
        ),
        "churn_close_errors": sum(
            1 for metric in metrics if metric.get("role") == "churn_close_error"
        ),
        "churn_open_errors": sum(
            1 for metric in metrics if metric.get("role") == "churn_open_error"
        ),
        "child_timeouts": [
            {
                "role": metric.get("target_role"),
                "index": metric.get("index"),
            }
            for metric in metrics
            if metric.get("role") == "child_timeout"
        ],
        "child_failures": [
            {
                "role": metric.get("target_role"),
                "index": metric.get("index"),
                "code": metric.get("code"),
            }
            for metric in metrics
            if metric.get("role") == "child_exit" and metric.get("success") is False
        ],
        "runner_timeout": any(metric.get("role") == "runner_timeout" for metric in metrics),
    }


def named_any(metrics: list[dict[str, Any]], names: list[str]) -> list[dict[str, Any]]:
    return [metric for metric in metrics if metric.get("name") in names]


def named_period(
    metrics: list[dict[str, Any]], names: list[str], startup_window_ms: int, le: bool
) -> list[dict[str, Any]]:
    selected = []
    for metric in named_any(metrics, names):
        elapsed = metric.get("elapsed_ms")
        if not isinstance(elapsed, int):
            continue
        if (elapsed <= startup_window_ms) == le:
            selected.append(metric)
    return selected


def role_field_summary(metrics: list[dict[str, Any]], field: str) -> dict[str, Any]:
    by_role: dict[str, list[float]] = {}
    for metric in metrics:
        role = metric.get("role")
        value = metric.get(field)
        if role is None or not isinstance(value, (int, float)):
            continue
        by_role.setdefault(str(role), []).append(float(value))
    return {role: number_summary(values) for role, values in sorted(by_role.items())}


def period_summary(metrics: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "buckets": len(metrics),
        "count": sum(metric.get("count", 0) for metric in metrics),
        "avg_us": number_summary([metric["avg_us"] for metric in metrics if "avg_us" in metric]),
        "p95_us": number_summary([metric["p95_us"] for metric in metrics if "p95_us" in metric]),
        "p99_us": number_summary([metric["p99_us"] for metric in metrics if "p99_us" in metric]),
        "max_us": number_summary([metric["max_us"] for metric in metrics if "max_us" in metric]),
    }


def number_summary(values: list[int | float]) -> dict[str, Any]:
    if not values:
        return {"count": 0}
    sorted_values = sorted(values)
    return {
        "count": len(sorted_values),
        "avg": mean(sorted_values),
        "p50": percentile(sorted_values, 50),
        "p95": percentile(sorted_values, 95),
        "p99": percentile(sorted_values, 99),
        "max": sorted_values[-1],
    }


def percentile(values: list[int | float], pct: int) -> int | float:
    if not values:
        raise ValueError("empty values")
    index = (len(values) - 1) * pct // 100
    return values[index]


def summarize_scenarios(results: list[RunResult]) -> list[dict[str, Any]]:
    by_name: dict[str, list[RunResult]] = {}
    for result in results:
        by_name.setdefault(result.scenario["name"], []).append(result)

    summaries = []
    for name, scenario_results in by_name.items():
        scenario = scenario_results[0].scenario
        summaries.append(
            {
                "name": name,
                "hub_mode": scenario["hub_mode"],
                "leaf_mode": scenario["leaf_mode"],
                "idle_peers": scenario["idle_peers"],
                "subscribers": scenario["subscribers"],
                "churners": scenario["churners"],
                "runs": len(scenario_results),
                "open_max_ms": max_nested(scenario_results, "open_ms", "max"),
                "first_sample_p99_ms": max_path(
                    scenario_results, ["first_sample_ms", "p99"]
                ),
                "first_sample_max_ms": max_path(
                    scenario_results, ["first_sample_ms", "max"]
                ),
                "missing_first_samples": sum_path(
                    scenario_results, ["missing_first_samples"]
                ),
                "zero_sample_subscribers": sum_path(
                    scenario_results, ["zero_sample_subscribers"]
                ),
                "put_startup_p99_ms": us_to_ms(
                    max_path(
                        scenario_results, ["publisher_put_startup_us", "p99_us", "max"]
                    )
                ),
                "put_startup_max_ms": us_to_ms(
                    max_path(
                        scenario_results, ["publisher_put_startup_us", "max_us", "max"]
                    )
                ),
                "put_steady_p99_ms": us_to_ms(
                    max_path(
                        scenario_results, ["publisher_put_steady_us", "p99_us", "max"]
                    )
                ),
                "rx_startup_p99_ms": us_to_ms(
                    max_path(
                        scenario_results,
                        ["subscriber_latency_startup_us", "p99_us", "max"],
                    )
                ),
                "rx_startup_max_ms": us_to_ms(
                    max_path(
                        scenario_results,
                        ["subscriber_latency_startup_us", "max_us", "max"],
                    )
                ),
                "rx_steady_p99_ms": us_to_ms(
                    max_path(
                        scenario_results,
                        ["subscriber_latency_steady_us", "p99_us", "max"],
                    )
                ),
                "churn_close_p99_ms": us_to_ms(
                    max_path(scenario_results, ["churn_close_us", "p99_us", "max"])
                ),
                "churn_close_max_ms": us_to_ms(
                    max_path(scenario_results, ["churn_close_us", "max_us", "max"])
                ),
                "churn_cycles": sum_path(scenario_results, ["churn_cycles"]),
                "churn_close_errors": sum_path(scenario_results, ["churn_close_errors"]),
                "churn_open_errors": sum_path(scenario_results, ["churn_open_errors"]),
                "child_timeouts": sum(
                    len(result.summary["child_timeouts"]) for result in scenario_results
                ),
                "child_failures": sum(
                    len(result.summary["child_failures"]) for result in scenario_results
                ),
                "runner_timeouts": sum(
                    1 for result in scenario_results if result.timed_out
                ),
            }
        )
    return summaries


def max_nested(results: list[RunResult], key: str, field: str) -> float | None:
    values = []
    for result in results:
        for summary in result.summary[key].values():
            value = summary.get(field)
            if isinstance(value, (int, float)):
                values.append(float(value))
    return max(values) if values else None


def max_path(results: list[RunResult], path: list[str]) -> float | None:
    values = []
    for result in results:
        current: Any = result.summary
        for item in path:
            if not isinstance(current, dict) or item not in current:
                current = None
                break
            current = current[item]
        if isinstance(current, (int, float)):
            values.append(float(current))
    return max(values) if values else None


def sum_path(results: list[RunResult], path: list[str]) -> int:
    total = 0
    for result in results:
        current: Any = result.summary
        for item in path:
            if not isinstance(current, dict) or item not in current:
                current = 0
                break
            current = current[item]
        if isinstance(current, (int, float)):
            total += int(current)
    return total


def us_to_ms(value: float | None) -> float | None:
    if value is None:
        return None
    return value / 1000


def environment(repo: Path) -> dict[str, Any]:
    status = command_output(["git", "status", "--short"], repo)
    return {
        "timestamp": dt.datetime.now().isoformat(timespec="seconds"),
        "system": platform.platform(),
        "machine": platform.machine(),
        "cpu": platform.processor(),
        "python": sys.version.split()[0],
        "git_branch": command_output(["git", "branch", "--show-current"], repo),
        "git_commit": command_output(["git", "rev-parse", "HEAD"], repo),
        "working_tree_dirty": bool(status),
        "nofile_limit": nofile_limit(),
        "rustc": command_output(["rustc", "--version"], repo),
        "cargo": command_output(["cargo", "--version"], repo),
    }


def command_output(command: list[str], cwd: Path) -> str:
    try:
        return subprocess.check_output(
            command,
            cwd=cwd,
            stderr=subprocess.STDOUT,
            text=True,
        ).strip()
    except (OSError, subprocess.CalledProcessError) as error:
        return f"unavailable: {error}"


def nofile_limit() -> str:
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        return f"soft={soft}, hard={hard}"
    except (ImportError, OSError, ValueError) as error:
        return f"unavailable: {error}"


def set_nofile_limit(requested: int) -> None:
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if requested <= soft:
            return
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(requested, hard), hard))
    except (ImportError, OSError, ValueError) as error:
        raise SystemExit(f"failed to set NOFILE limit to {requested}: {error}")


def render_issue_comment(report: dict[str, Any]) -> str:
    env = report["environment"]
    params = report["parameters"]
    scenario_summary = report["scenario_summary"]
    lines = [
        "### p2p routing benchmark update",
        "",
        "I extended the reproducer from a publisher `put()` stall check into a broader routing stress benchmark. It measures publisher stalls, subscriber receive latency, subscriber discovery/startup delay, churn close latency, and child-process timeouts under many peer links.",
        "",
        "#### Method",
        "",
        "- One hub listens on TCP. Leaf sessions connect to the hub.",
        "- `peer/peer` scenarios use a peer hub plus peer leaves. `router/client` scenarios keep the same load but connect clients to a router, matching the workaround where switching some nodes to clients reduces symptoms.",
        "- Idle peers only keep sessions open to increase p2p link/routing state.",
        "- One publisher uses `CongestionControl::Block` and publishes every "
        f"{params['put_period_ms']} ms. The payload embeds a local timestamp so subscribers can report end-to-end local latency.",
        "- Subscribers report `open`, `declare`, first-sample delay, receive rate, and per-second latency buckets.",
        "- Churners repeatedly open a session, declare a subscriber, undeclare it, close the session, and sleep "
        f"{params['churn_hold_ms']}/{params['churn_idle_ms']} ms.",
        "- Multicast scouting is disabled and `open/return_conditions/declares` is "
        f"`{str(params['wait_declares']).lower()}`.",
        "",
        "#### Environment",
        "",
        f"- Date: `{env['timestamp']}`",
        f"- System: `{env['system']}` `{env['machine']}`",
        f"- Git: `{env['git_branch']}` `{env['git_commit'][:12]}` dirty=`{str(env['working_tree_dirty']).lower()}`",
        f"- NOFILE limit: `{env['nofile_limit']}`",
        f"- Rust: `{env['rustc']}` / `{env['cargo']}`",
        f"- Preset: `{params['preset']}`, runs per scenario: `{params['runs']}`, duration: `{params['duration_secs']}s`, startup bucket: `{params['startup_window_ms']}ms`",
        "",
        "#### Startup and data path",
        "",
        "| scenario | topology | idle | subs | churn | first sample p99/max ms | put startup p99/max ms | rx startup p99/max ms | rx steady p99 ms | missing/zero subs |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in scenario_summary:
        lines.append(
            "| {name} | {hub}/{leaf} | {idle} | {subs} | {churn} | {first_p99}/{first_max} | {put_p99}/{put_max} | {rx_p99}/{rx_max} | {rx_steady} | {missing}/{zero} |".format(
                name=item["name"],
                hub=item["hub_mode"],
                leaf=item["leaf_mode"],
                idle=item["idle_peers"],
                subs=item["subscribers"],
                churn=item["churners"],
                first_p99=fmt(item["first_sample_p99_ms"]),
                first_max=fmt(item["first_sample_max_ms"]),
                put_p99=fmt(item["put_startup_p99_ms"]),
                put_max=fmt(item["put_startup_max_ms"]),
                rx_p99=fmt(item["rx_startup_p99_ms"]),
                rx_max=fmt(item["rx_startup_max_ms"]),
                rx_steady=fmt(item["rx_steady_p99_ms"]),
                missing=item["missing_first_samples"],
                zero=item["zero_sample_subscribers"],
            )
        )
    lines += [
        "",
        "#### Churn and shutdown path",
        "",
        "| scenario | churners | cycles | close p99/max ms | open/close errors | child timeouts | child failures | runner timeouts |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in scenario_summary:
        lines.append(
            "| {name} | {churners} | {cycles} | {close_p99}/{close_max} | {open_errors}/{close_errors} | {timeouts} | {failures} | {runner_timeouts} |".format(
                name=item["name"],
                churners=item["churners"],
                cycles=item["churn_cycles"],
                close_p99=fmt(item["churn_close_p99_ms"]),
                close_max=fmt(item["churn_close_max_ms"]),
                open_errors=item["churn_open_errors"],
                close_errors=item["churn_close_errors"],
                timeouts=item["child_timeouts"],
                failures=item["child_failures"],
                runner_timeouts=item["runner_timeouts"],
            )
        )

    lines += render_observations(scenario_summary)
    lines += [
        "",
        "#### Artifacts",
        "",
        "The benchmark writes one raw log per scenario/run plus `summary.json`. Each raw log contains the original `metric ...` lines emitted by the harness, so the table above is reproducible from the artifacts.",
        "",
        "Command used:",
        "",
        "```bash",
        "python3 examples/scripts/p2p_routing_benchmark.py "
        f"--preset {params['preset']} --runs {params['runs']} --duration-secs {params['duration_secs']} "
        f"--profile {params['profile']} --put-period-ms {params['put_period_ms']} --startup-window-ms {params['startup_window_ms']}"
        + (f" --nofile-limit {params['nofile_limit']}" if params["nofile_limit"] else "")
        + (" --include-200" if params["include_200"] else ""),
        "```",
        "",
    ]
    return "\n".join(lines)


def render_observations(summaries: list[dict[str, Any]]) -> list[str]:
    by_name = {item["name"]: item for item in summaries}
    observations = ["", "#### Observations", ""]
    baseline = by_name.get("baseline_peer")
    p2p_churn = next(
        (
            item
            for item in summaries
            if item["leaf_mode"] == "peer" and item["churners"] > 0
        ),
        None,
    )
    client_pairs = []
    for p2p_item in summaries:
        if p2p_item["leaf_mode"] != "peer" or p2p_item["churners"] == 0:
            continue
        for client_item in summaries:
            if (
                client_item["leaf_mode"] == "client"
                and client_item["idle_peers"] == p2p_item["idle_peers"]
                and client_item["churners"] == p2p_item["churners"]
            ):
                client_pairs.append((p2p_item, client_item))
    matched_p2p_churn, matched_client_churn = (
        max(client_pairs, key=lambda pair: pair[0]["idle_peers"])
        if client_pairs
        else (None, None)
    )
    if baseline and p2p_churn:
        observations.append(
            "- Subscriber startup is the easiest receiver-side signal to reproduce: baseline first-sample max was "
            f"`{fmt(baseline['first_sample_max_ms'])} ms`, while `{p2p_churn['name']}` reached "
            f"`{fmt(p2p_churn['first_sample_max_ms'])} ms`."
        )
    if p2p_churn:
        observations.append(
            "- Churn stresses the declaration/undeclaration and session close paths: "
            f"`{p2p_churn['name']}` reported close p99/max `{fmt(p2p_churn['churn_close_p99_ms'])}/{fmt(p2p_churn['churn_close_max_ms'])} ms`, "
            f"`{p2p_churn['churn_open_errors']}` open errors, `{p2p_churn['churn_close_errors']}` close errors, and `{p2p_churn['child_timeouts']}` child timeouts."
        )
    if matched_p2p_churn and matched_client_churn:
        observations.append(
            "- The router/client comparison keeps the publisher/subscriber/churn workload shape but removes the peer leaf routing fanout. "
            f"`{matched_p2p_churn['name']}` vs `{matched_client_churn['name']}` is the matched-size comparison for separating p2p routing-state cost from application workload cost."
        )
    client_failures = sum(item["child_failures"] for item in summaries if item["leaf_mode"] == "client")
    if client_failures:
        observations.append(
            "- Router/client rows also expose startup connection-storm behavior in this process-heavy benchmark: "
            f"the client scenarios reported `{client_failures}` child failures, so those rows should be read together with the failure columns rather than as a pure latency-only comparison."
        )
    observations.append(
        "- These measurements point at routing-state critical sections rather than a pure transport throughput limit: the symptoms appear in `put()`, subscriber first-sample/latency, and churn close/discovery behavior."
    )
    return observations


def fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.1f}"
    return str(value)


if __name__ == "__main__":
    raise SystemExit(main())
