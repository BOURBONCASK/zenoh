// Minimal reproduction of router-router linkstate drift behavior on zenoh 1.8.0.
//
// Background
// ----------
// When two zenohd instances run in `mode: router` and are connected, the
// router HAT (zenoh/src/net/routing/hat/router/mod.rs) forces full linkstate
// routing (`router_full_linkstate = true`, line 341 — no config knob to
// disable). Every router-router link event triggers a chain:
//
//   close_face / new_transport_unicast_face / handle_oam(OAM_LINKSTATE)
//      → routers_net.remove_link/add_link/link_states
//      → schedule_compute_trees     (100 ms debounce, TREES_COMPUTATION_DELAY_MS)
//      → TreesComputationWorker:
//          zwrite!(tables_ref.tables)              ← writers locked out
//          Network::compute_trees()                 ← N × Bellman-Ford SPF
//          pubsub_tree_change(); queries_tree_change(); token_tree_change()
//                                                   ← O(N × M × children)
//                                                     declare re-emit per
//                                                     subscriber/queryable/token
//
// As the entity table M grows (rmw_zenoh humble typically lands at
// 500-2000 liveliness tokens per robot), the writer lock held by the trees
// worker can grow to seconds and effectively freezes the whole router for
// that duration. Anything attempting `declare_*` / queryable matching / data
// forwarding on either router blocks behind that write lock.
//
// This binary reproduces the scenario in a single OS process with four
// in-memory zenoh sessions:
//
//     router_a (mode=router, listen tcp/127.0.0.1:RA)
//        ▲
//        │ TCP linkstate (router-router)
//        │
//     router_b (mode=router, listen tcp/127.0.0.1:RB, connect router_a)
//        ▲
//        │ ┌─ client_pub  (mode=client, connect router_a) — declares N entities
//        └─┤
//          └─ client_sub  (mode=client, connect router_b) — probes the entities
//
// `client_pub` mimics the side that owns ROS entities (rmw_zenoh sessions
// on S100 in our deployment). `client_sub` mimics a service-call consumer
// reaching across the router-router boundary (`app_agent::fsm_client_` in
// our deployment).
//
// To trigger the drift symptom, the example periodically drops the
// `router_b` Session (the Session::Drop closes all its transports, which
// is the same code path as a TCP RST). The recovery window — when the
// trees worker re-runs SPF + tree_change for every entity — is what shows
// up as a multi-second probe outage.
//
// Usage
// -----
//   cargo run --release --example z_router_drift_repro -- --n 100
//   cargo run --release --example z_router_drift_repro -- --n 1000 \
//          --probe-rate-hz 10 --flap-interval-secs 30 --duration-secs 240
//
// Output format (per probe tick):
//
//   [HH:MM:SS.mmm] tick=NNN N=NNN ok=NNN timeout=NNN p50=Xms p99=Yms STATE
//
// where STATE is one of:
//   STEADY   — all probes succeeded last tick
//   FLAP     — link is currently torn down by this binary
//   RECOVER  — link is up again but probes still failing/timing out
//
// Expected scaling:
//
//   * N=10:    STEADY ≈ a couple ms, RECOVER ≈ <100 ms
//   * N=100:   STEADY ≈ a couple ms, RECOVER ≈ 100-500 ms
//   * N=1000:  STEADY ≈ a couple ms, RECOVER ≈ multi-second to >10s — and
//              CPU spikes on the router_b session while compute_trees +
//              tree_change rerun even after probes succeed again.

use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use zenoh::config::{Config, WhatAmI};
use zenoh::query::{ConsolidationMode, QueryTarget};

#[derive(Parser, Debug, Clone)]
#[command(about = "Minimal router-router linkstate drift reproducer for zenoh 1.8.0")]
struct Args {
    /// Number of queryable + liveliness token entities declared by client_pub.
    /// Vary this to see how SPF + tree_change cost scales.
    #[arg(long, default_value_t = 100)]
    n: usize,

    /// Probe rate in Hz (probes per second from client_sub).
    #[arg(long, default_value_t = 10)]
    probe_rate_hz: u32,

    /// Wall-clock seconds between link flaps. Set to 0 to disable flapping.
    #[arg(long, default_value_t = 30)]
    flap_interval_secs: u64,

    /// Duration of each link tear-down, in milliseconds.
    #[arg(long, default_value_t = 3000)]
    flap_down_ms: u64,

    /// Total experiment duration in seconds.
    #[arg(long, default_value_t = 120)]
    duration_secs: u64,

    /// TCP port for router_a's listener (router_b connects here).
    #[arg(long, default_value_t = 17447)]
    port_a: u16,

    /// TCP port for router_b's listener (clients connect here).
    #[arg(long, default_value_t = 17448)]
    port_b: u16,

    /// Per-probe timeout in milliseconds.
    #[arg(long, default_value_t = 500)]
    probe_timeout_ms: u64,

    /// Enable zenoh trace logging. Loud but useful to see compute_trees scheduling.
    #[arg(long, default_value_t = false)]
    trace: bool,
}

fn ts_now() -> String {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    let ms = dur.subsec_millis();
    format!("{h:02}:{m:02}:{s:02}.{ms:03}")
}

fn make_router_config(listen: &str, connect: Option<&str>) -> Config {
    let mut c = Config::default();
    c.set_mode(Some(WhatAmI::Router)).unwrap();
    c.scouting.multicast.set_enabled(Some(false)).unwrap();
    c.listen
        .endpoints
        .set(vec![listen.parse().unwrap()])
        .unwrap();
    if let Some(ce) = connect {
        c.connect.endpoints.set(vec![ce.parse().unwrap()]).unwrap();
    }
    c
}

fn make_client_config(connect: &str) -> Config {
    let mut c = Config::default();
    c.set_mode(Some(WhatAmI::Client)).unwrap();
    c.scouting.multicast.set_enabled(Some(false)).unwrap();
    c.connect.endpoints.set(vec![connect.parse().unwrap()]).unwrap();
    c
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();

    if args.trace {
        zenoh::init_log_from_env_or("zenoh=trace");
    } else {
        zenoh::init_log_from_env_or("zenoh=info");
    }

    let router_a_listen = format!("tcp/127.0.0.1:{}", args.port_a);
    let router_b_listen = format!("tcp/127.0.0.1:{}", args.port_b);

    println!(
        "[setup] router_a listen={router_a_listen}  router_b listen={router_b_listen} connect={router_a_listen}"
    );
    println!("[setup] N={} probe_rate_hz={}", args.n, args.probe_rate_hz);

    // router_a (the "S100 router" side — also where the publisher attaches)
    let router_a = zenoh::open(make_router_config(&router_a_listen, None)).await?;
    println!("[setup] router_a zid={}", router_a.zid());

    // router_b (the "X5 router" side — connects to router_a)
    let mut router_b: Option<zenoh::Session> = Some(
        zenoh::open(make_router_config(
            &router_b_listen,
            Some(&router_a_listen),
        ))
        .await?,
    );
    println!(
        "[setup] router_b zid={}",
        router_b.as_ref().unwrap().zid()
    );

    // Wait for router-router transport to come up before clients attach.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // client_pub attaches to router_a — it's the side that "owns" entities.
    let client_pub = zenoh::open(make_client_config(&router_a_listen)).await?;
    println!("[setup] client_pub zid={}", client_pub.zid());

    // client_sub attaches to router_b — queries traverse the router-router bridge.
    let client_sub = zenoh::open(make_client_config(&router_b_listen)).await?;
    println!("[setup] client_sub zid={}", client_sub.zid());

    // Declare N queryables and liveliness tokens from client_pub.
    let n = args.n;
    println!("[setup] client_pub declaring {n} queryables + {n} liveliness tokens");
    let mut _queryables = Vec::with_capacity(n);
    let mut _tokens = Vec::with_capacity(n);
    let callback_counter = Arc::new(AtomicU64::new(0));
    for i in 0..n {
        let key = format!("repro/q/{i}");
        let c = callback_counter.clone();
        let key_for_reply = key.clone();
        let q = client_pub
            .declare_queryable(&key)
            .callback(move |query| {
                let c = c.clone();
                let key = key_for_reply.clone();
                tokio::spawn(async move {
                    c.fetch_add(1, Ordering::Relaxed);
                    let _ = query.reply(key, "ok").await;
                });
            })
            .await?;
        _queryables.push(q);

        let tok = client_pub
            .liveliness()
            .declare_token(format!("repro/l/{i}"))
            .await?;
        _tokens.push(tok);
    }

    // Settle: let linkstate propagate queryables router_a → router_b
    println!("[setup] waiting 3s for linkstate to propagate {n} queryables");
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!("[setup] ready — beginning probe loop\n");

    // Probe loop on client_sub. Each tick fires N parallel get()s.
    let probe_period = Duration::from_millis(1000 / args.probe_rate_hz.max(1) as u64);
    let probe_timeout = Duration::from_millis(args.probe_timeout_ms);
    let total_duration = Duration::from_secs(args.duration_secs);
    let flap_interval = if args.flap_interval_secs == 0 {
        Duration::from_secs(u64::MAX / 2)
    } else {
        Duration::from_secs(args.flap_interval_secs)
    };
    let flap_down = Duration::from_millis(args.flap_down_ms);

    let mut next_flap = Instant::now() + flap_interval;
    let mut next_recovery_end: Option<Instant> = None;
    let mut state: &'static str = "STEADY";
    let mut tick = 0u64;
    let started = Instant::now();

    while started.elapsed() < total_duration {
        tick += 1;
        let tick_start = Instant::now();

        // State machine: STEADY → FLAP → RECOVER → STEADY
        if args.flap_interval_secs > 0 && tick_start >= next_flap && state == "STEADY" {
            println!(
                "\n[flap {}] tick={tick} dropping router_b session for {} ms",
                ts_now(),
                args.flap_down_ms
            );
            // Dropping the Session closes its TransportUnicast, which on
            // the router_a side fires close_face. This reproduces what a
            // TCP RST from a flaky USB tether or QUIC migration looks like
            // at the routing layer.
            router_b = None;
            state = "FLAP";
            next_recovery_end = Some(tick_start + flap_down);
        }
        if state == "FLAP" {
            if let Some(end) = next_recovery_end {
                if tick_start >= end {
                    println!(
                        "[flap {}] tick={tick} re-opening router_b — will trigger new_transport_unicast_face → schedule_compute_trees on both sides",
                        ts_now()
                    );
                    router_b = Some(
                        zenoh::open(make_router_config(
                            &router_b_listen,
                            Some(&router_a_listen),
                        ))
                        .await?,
                    );
                    state = "RECOVER";
                    next_flap = tick_start + flap_interval;
                }
            }
        }

        // Fire N parallel probes from client_sub.
        let mut futs = Vec::with_capacity(n);
        for i in 0..n {
            let s = client_sub.clone();
            let key = format!("repro/q/{i}");
            futs.push(tokio::spawn(async move {
                let p_start = Instant::now();
                let res = tokio::time::timeout(probe_timeout, async move {
                    let replies = s
                        .get(&key)
                        .target(QueryTarget::All)
                        .consolidation(ConsolidationMode::None)
                        .await?;
                    while replies.recv_async().await.is_ok() {
                        return Ok::<bool, Box<dyn Error + Send + Sync>>(true);
                    }
                    Ok::<bool, Box<dyn Error + Send + Sync>>(false)
                })
                .await;
                let latency = p_start.elapsed();
                let ok = matches!(res, Ok(Ok(true)));
                (ok, latency)
            }));
        }

        let mut ok = 0u32;
        let mut timeout = 0u32;
        let mut latencies = Vec::with_capacity(n);
        for f in futs {
            match f.await {
                Ok((true, lat)) => {
                    ok += 1;
                    latencies.push(lat);
                }
                _ => timeout += 1,
            }
        }

        latencies.sort();
        let p50 = latencies
            .get(latencies.len() / 2)
            .copied()
            .unwrap_or_default();
        let p99 = latencies
            .get(latencies.len() * 99 / 100)
            .copied()
            .unwrap_or_default();

        if state == "RECOVER" && timeout == 0 {
            state = "STEADY";
        }

        println!(
            "[{}] tick={tick:4} N={n:4} ok={ok:4} timeout={timeout:4} p50={:>5}ms p99={:>5}ms {state}",
            ts_now(),
            p50.as_millis(),
            p99.as_millis()
        );

        let elapsed = tick_start.elapsed();
        if elapsed < probe_period {
            tokio::time::sleep(probe_period - elapsed).await;
        }
    }

    let total_calls = callback_counter.load(Ordering::Relaxed);
    println!("\n[done] queryable callbacks served by client_pub: {total_calls}");

    // Keep router_a alive until end so Drop ordering is clean.
    drop(client_sub);
    drop(client_pub);
    drop(router_b);
    drop(router_a);

    Ok(())
}
