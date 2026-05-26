# Cloud-flap A/B/C: 1.8.0 vs 1.9-track auto vs 1.9-track + explicit regions

All three runs use the same workload knobs:
- N=500 (queryable + liveliness + subscriber pairs)
- M=10 data publishers @ 50 Hz from cloud → s100 on `aorta/data/{i}`
- ROS probe (ros_probe → ros_pub, both on s100) at 4 Hz, 800 ms timeout
- Flap: cloud router DOWN 3000 ms every 25 s, 4 flaps total, 130 s run

| Metric | 1.8.0, no regions | 1.9 hardening, regions OFF | 1.9 hardening, regions ON |
|---|---|---|---|
| ROS probe timeout rate | 0.00% | 0.00% | 0.00% |
| Longest ROS outage | 0.00 s | 0.00 s | 0.00 s |
| Aorta sample loss | 21.05% | 21.05% | 21.04% |
| Longest aorta outage | 7.07 s | 7.07 s | 7.07 s |
| ros-only outage ticks | 0 | 0 | 0 |
| aorta-only outage ticks | 108 | 108 | 108 |
| both-down ticks | 0 | 0 | 0 |

## Conclusions (corrected)

1. **The ROS-vs-Aorta isolation observed in the regions A/B test exists
   identically in 1.8.0 with no regions configured.** It is not 1.9-specific
   and not gated by `region_name`/`gateway.south`.

2. **Where the isolation actually comes from in 1.8.0:**
   - The ROS probe path is `ros_probe → s100 router → ros_pub`, all three
     processes live on the same s100 router. Cloud router flap never crosses
     this path physically.
   - In `zenoh::net::routing::hat::router::HatTables`, `router_subs` holds
     entities learned from remote *routers* (cloud-side aorta entities, via
     x5 router). `ros_pub` is a *client*, its entity is stored in the s100
     face's `local_subs`, not in `router_subs`.
   - Cloud flap → x5 sends a LinkStateList update → s100's `routers_net`
     loses the cloud node → `routers_trees_worker` runs SPF + tree_change
     iterating only over `router_subs`. With our N=500 + M=10 aorta keys the
     write lock is held briefly enough that ros_probe's 800 ms timeout
     window always finds a read lock.

3. **What 1.9-track `RegionMap<HatTablesData>` actually buys:**
   For a topology with multiple north routers OR multiple south routers,
   each region gets its own `router_subs`/`routers_net`/SPF worker, so a
   flap on one north link only touches that region's worker. For the vita
   topology (single cloud router + single x5 bridge + single s100), the
   1.8.0 single-`HatTables` and the 1.9 single-`HAT[North]` cases are
   structurally equivalent and behave identically (as measured).

4. **Implications for vita-robot:**
   - Upgrading 1.8.0 → 1.9 routing-stalls-hardening does NOT meaningfully
     reduce M3 cloud-flap outage on this topology.
   - Adding explicit `region_name` + `gateway.south` does NOT help either.
   - The actionable mitigation remains the application-layer one we already
     shipped in vita-robot PR #2035: tighten the ACL so each router-router
     link only sees its intended keys, shrinking the `router_subs` set that
     SPF + tree_change iterates over.
   - The minute-scale outages observed in fleet journals are dominated by
     rmw_zenoh humble's graph_cache having no reconcile path (a previously
     established orthogonal failure mode), not by zenoh SPF cost.
