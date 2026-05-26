# 4-quadrant cloud/x5 flap, zenoh 1.8.0

Topology: one process, three router instances (cloud :17501, x5 :17502, s100 :17503),
x5 connects to both. Eight client sessions, four pairs:

| pair | pub side | probe/sub side | what it tests |
|---|---|---|---|
| intra_ros   | client→s100 | client→s100   | s100-internal query (baseline control) |
| cross_ros   | client→x5   | client→s100   | query across router-router link |
| intra_aorta | client→s100 | client→s100   | s100-internal pub/sub |
| cross_aorta | client→cloud| client→s100   | pub/sub across two router-router links |

Probe rate 4 Hz, 1500 ms timeout. Aorta pub rate 30 Hz × M publishers.

## Run A — `--flap-which x5 --n 5000 --m-pub 100`, 3 flaps × 3000ms in 130s

| axis | result |
|---|---|
| intra_ros   | ok=845000 to=0  **0.00% timeout** |
| cross_ros   | ok=688722 to=156278 **18.49% timeout**  max latency **1437 ms** |
| intra_aorta | recv=405200 miss=0 **0.00% loss** |
| cross_aorta | recv=370505 miss=33495 **8.29% loss** |

Latency spike happened during *recovery*, not the flap window itself
(tick 49, max_lat=965ms while x5 had just re-opened and both sides were
redeclaring entities).

## Run B — `--flap-which cloud --n 500 --m-pub 10`, 4 flaps × 3000ms in 130s

(this matches the original regions A/B test's numerical workload)

| axis | result |
|---|---|
| intra_ros   | ok=256000 to=0 **0.00% timeout** |
| cross_ros   | ok=256000 to=0 **0.00% timeout**  max latency **39 ms** |
| intra_aorta | recv=67510 miss=0 **0.00% loss** |
| cross_aorta | recv=53270 miss=14230 **21.08% loss** |

## What this proves and what it doesn't

**Proven**:

- Router-router drift exists and stalls cross-router RPC when the entity
  count is high enough. N=5000 + x5 flap → 18.5% query timeout, peak
  1437 ms latency. N=500 + cloud flap → only 39 ms peak (storm too small).
- The drift cost concentrates in the *recovery* phase after the link
  comes back, when both sides redeclare entities into each other's
  router_subs/router_qabls. The flap window itself just loses physical
  bytes.
- Latency scales with entity count visible across the link — consistent
  with the SPF + tree_change iterating over `router_subs.len()`.

**Not proven** (and the user's fleet symptom does include this):

- Storm at x5 does NOT, in this localhost single-process repro,
  propagate into intra-router clients on s100. `intra_ros` and
  `intra_aorta` stayed 0% even during the heavy N=5000 storm.

The gap between "stall at x5 only" (what we measure) and "every message
on the robot drops" (what the fleet sees) needs at least one of:

1. rmw_zenoh humble `graph_cache` — no reconcile path; one declare storm
   leaves `service_is_ready()` permanently wrong until restart.
2. Order-of-magnitude more entities (real fleet has rmw_zenoh + cloud_agent
   stacks; total entity count is likely 10k–50k, not 5k).
3. CPU constraint — S100 is an embedded ARM, the storm work that finishes
   in ms on M3 takes seconds on-target and starves other threads.
4. Real network RTT — localhost amplifies nothing; on-robot Ethernet adds
   per-declare ack latency.

The user's empirical fix ("kill x5 router process → problem disappears")
is consistent with x5 being the storm source. This repro confirms x5
generates the storm. It does NOT confirm the storm reaches s100-local
traffic via Tables lock contention alone; that part requires either
graph_cache instrumentation or a higher-fidelity test environment.
