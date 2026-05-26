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
// As the entity table size grows (rmw_zenoh humble typically lands at
// 500-2000 liveliness tokens per robot), the writer lock held by the trees
// worker can grow to seconds and effectively freezes the whole router for
// that duration. Anything attempting `declare_*` / queryable matching / data
// forwarding on either router blocks behind that write lock.
//
// Topology (in-process)
// ---------------------
//
//     router_a (mode=router, listen tcp/127.0.0.1:RA)
//        ▲                   ▲
//        │                   │ client_pub probes / publishes from here
//        │                   │
//        │ TCP linkstate     ├── client_pub  (mode=client, connect router_a)
//        │ (router-router)   │     · declares N queryables + N liveliness tokens
//        │                   │     · publishes M topics at PUB_HZ
//        │
//     router_b (mode=router, listen tcp/127.0.0.1:RB, connect router_a)
//        ▲
//        │
//        └── client_sub  (mode=client, connect router_b)
//             · probes every queryable through the router-router bridge
//             · subscribes to every topic and counts received vs. expected
//
// `client_pub` mimics the side that owns ROS entities (rmw_zenoh sessions on
// S100). `client_sub` mimics a service-call consumer + data-topic subscriber
// reaching across the router-router boundary (think `app_agent::fsm_client_`
// + `/joy` subscriber on the X5 side).
//
// To trigger the drift symptom, the example periodically drops the
// `router_b` Session (Session::Drop closes the TransportUnicast, which is
// the same code path as a TCP RST). The recovery window — when the trees
// worker re-runs SPF + tree_change for every entity — is what shows up as
// a multi-second probe outage AND a data-topic gap.
//
// Usage
// -----
//   cargo run --release --example z_router_drift_repro -- --n 500 --m-pub 10
//
//   cargo run --release --example z_router_drift_repro -- \
//       --n 1000 --m-pub 20 --pub-rate-hz 50 \
//       --probe-rate-hz 5 --flap-interval-secs 45 --flap-down-ms 4000 \
//       --duration-secs 600
//
// Each tick prints:
//
//   [HH:MM:SS.mmm] tick=NNN N=NNN ok=NNN to=NNN p99=Yms  pub_recv/s=X  miss/s=Y  STATE
//
// where:
//   N         — declared queryables + liveliness tokens
//   ok / to   — probe success / timeout out of N
//   p99       — 99th percentile probe latency (steady state)
//   pub_recv  — total data samples received from M publishers (sum over all M)
//   miss/s    — detected sample gaps per second (loss)
//   STATE     — STEADY / FLAP / RECOVER

use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

    /// Number of high-rate data publishers / subscribers (separate from N).
    /// Simulates ROS topics like /joy or /odometry that publish steadily.
    #[arg(long, default_value_t = 10)]
    m_pub: usize,

    /// Per-publisher publish rate in Hz.
    #[arg(long, default_value_t = 50)]
    pub_rate_hz: u32,

    /// Probe rate in Hz (probes per second from client_sub).
    #[arg(long, default_value_t = 5)]
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
    c.connect
        .endpoints
        .set(vec![connect.parse().unwrap()])
        .unwrap();
    c
}

/// Per-topic subscriber stats (kept on the consumer side).
#[derive(Default)]
struct SubStats {
    /// Total samples received so far.
    received: AtomicU64,
    /// Number of sequence-number gaps detected so far.
    missed: AtomicU64,
    /// Last seen sequence number, or u64::MAX if none yet.
    last_seq: AtomicU64,
}

impl SubStats {
    fn new() -> Self {
        Self {
            received: AtomicU64::new(0),
            missed: AtomicU64::new(0),
            last_seq: AtomicU64::new(u64::MAX),
        }
    }
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
    println!(
        "[setup] N={}  M_pub={}@{}Hz  probe={}Hz",
        args.n, args.m_pub, args.pub_rate_hz, args.probe_rate_hz
    );

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

    // Declare N queryables and N liveliness tokens from client_pub.
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

    // Declare M data subscribers on client_sub. Each subscriber expects a
    // monotonic u64 sequence number in the payload from its paired publisher.
    let m = args.m_pub;
    println!("[setup] client_sub declaring {m} data subscribers");
    let mut sub_stats: Vec<Arc<SubStats>> = Vec::with_capacity(m);
    let mut _subs = Vec::with_capacity(m);
    for j in 0..m {
        let stats = Arc::new(SubStats::new());
        sub_stats.push(stats.clone());
        let key = format!("repro/data/{j}");
        let sub = client_sub
            .declare_subscriber(&key)
            .callback(move |sample| {
                let payload = sample.payload().to_bytes();
                if payload.len() >= 8 {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&payload[..8]);
                    let seq = u64::from_le_bytes(buf);
                    stats.received.fetch_add(1, Ordering::Relaxed);
                    let prev = stats.last_seq.swap(seq, Ordering::Relaxed);
                    if prev != u64::MAX && seq > prev + 1 {
                        stats.missed.fetch_add(seq - prev - 1, Ordering::Relaxed);
                    }
                }
            })
            .await?;
        _subs.push(sub);
    }

    // Spawn M publisher tasks on client_pub. Each task owns its own
    // Publisher handle (declared inside the spawn) so the handle's lifetime
    // is tied to the task running; when we set `stop=true` the task exits
    // and the publisher is dropped.
    let stop = Arc::new(AtomicBool::new(false));
    for j in 0..m {
        let key = format!("repro/data/{j}");
        let session = client_pub.clone();
        let stop = stop.clone();
        let pub_rate = args.pub_rate_hz.max(1);
        let period_us = 1_000_000 / pub_rate as u64;
        tokio::spawn(async move {
            let publisher = match session.declare_publisher(&key).await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("declare_publisher({key}) failed: {e}");
                    return;
                }
            };
            let mut seq: u64 = 0;
            let period = Duration::from_micros(period_us);
            let mut next = Instant::now() + period;
            while !stop.load(Ordering::Relaxed) {
                let payload = seq.to_le_bytes().to_vec();
                let _ = publisher.put(payload).await;
                seq = seq.wrapping_add(1);
                let now = Instant::now();
                if next > now {
                    tokio::time::sleep(next - now).await;
                }
                next += period;
            }
        });
    }
    println!(
        "[setup] {} publishers started, total nominal data rate = {} samples/s",
        m,
        m as u32 * args.pub_rate_hz
    );

    // Settle: let linkstate propagate before probing.
    println!("[setup] waiting 3s for linkstate to propagate");
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

    // For per-tick delta of data samples
    let mut last_total_received: u64 = sub_stats.iter().map(|s| s.received.load(Ordering::Relaxed)).sum();
    let mut last_total_missed: u64 = sub_stats.iter().map(|s| s.missed.load(Ordering::Relaxed)).sum();
    let mut last_tick_at = Instant::now();

    // Track longest continuous outage of data flow (any sub seeing no
    // samples for longer than ~5× pub_period).
    let mut current_outage_start: Option<Instant> = None;
    let mut max_outage = Duration::ZERO;

    // Stats summary
    let mut flap_count = 0u64;
    let mut total_probes_ok = 0u64;
    let mut total_probes_to = 0u64;

    while started.elapsed() < total_duration {
        tick += 1;
        let tick_start = Instant::now();

        // State machine: STEADY → FLAP → RECOVER → STEADY
        if args.flap_interval_secs > 0 && tick_start >= next_flap && state == "STEADY" {
            println!(
                "\n[flap {}] tick={tick} dropping router_b session for {} ms (flap #{})",
                ts_now(),
                args.flap_down_ms,
                flap_count + 1,
            );
            // Dropping the Session closes its TransportUnicast, which on
            // the router_a side fires close_face. This reproduces what a
            // TCP RST from a flaky tether or QUIC migration looks like
            // at the routing layer.
            router_b = None;
            state = "FLAP";
            next_recovery_end = Some(tick_start + flap_down);
            flap_count += 1;
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
        let mut timeout_n = 0u32;
        let mut latencies = Vec::with_capacity(n);
        for f in futs {
            match f.await {
                Ok((true, lat)) => {
                    ok += 1;
                    latencies.push(lat);
                }
                _ => timeout_n += 1,
            }
        }
        total_probes_ok += ok as u64;
        total_probes_to += timeout_n as u64;

        latencies.sort();
        let p50 = latencies.get(latencies.len() / 2).copied().unwrap_or_default();
        let p99 = latencies.get(latencies.len() * 99 / 100).copied().unwrap_or_default();

        // Pub/sub data-flow stats
        let total_received: u64 = sub_stats.iter().map(|s| s.received.load(Ordering::Relaxed)).sum();
        let total_missed: u64 = sub_stats.iter().map(|s| s.missed.load(Ordering::Relaxed)).sum();
        let now = Instant::now();
        let dt = now.duration_since(last_tick_at).as_secs_f64().max(0.001);
        let recv_per_s = ((total_received - last_total_received) as f64 / dt) as u64;
        let miss_per_s = ((total_missed - last_total_missed) as f64 / dt) as u64;
        last_total_received = total_received;
        last_total_missed = total_missed;
        last_tick_at = now;

        // Outage tracking: data is "out" if recv_per_s drops below 10% of nominal.
        let nominal = m as u64 * args.pub_rate_hz as u64;
        let is_outage = recv_per_s < nominal / 10;
        if is_outage {
            if current_outage_start.is_none() {
                current_outage_start = Some(tick_start);
            }
        } else if let Some(start) = current_outage_start.take() {
            let dur = tick_start - start;
            if dur > max_outage {
                max_outage = dur;
            }
        }

        if state == "RECOVER" && timeout_n == 0 && !is_outage {
            state = "STEADY";
        }

        println!(
            "[{}] t={tick:5} N={n:5} ok={ok:4} to={timeout_n:4} p50={:>5}ms p99={:>5}ms  pub_recv/s={recv_per_s:>6} miss/s={miss_per_s:>5}  {state}",
            ts_now(),
            p50.as_millis(),
            p99.as_millis()
        );

        let elapsed = tick_start.elapsed();
        if elapsed < probe_period {
            tokio::time::sleep(probe_period - elapsed).await;
        }
    }

    // Close any outage still open
    if let Some(start) = current_outage_start.take() {
        let dur = started.elapsed() - (start - started);
        if dur > max_outage {
            max_outage = dur;
        }
    }

    stop.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let total_calls = callback_counter.load(Ordering::Relaxed);
    let total_recv: u64 = sub_stats.iter().map(|s| s.received.load(Ordering::Relaxed)).sum();
    let total_miss: u64 = sub_stats.iter().map(|s| s.missed.load(Ordering::Relaxed)).sum();

    println!("\n========== SUMMARY ==========");
    println!("duration:                    {:>8} s", args.duration_secs);
    println!("N (queryables + tokens):     {:>8}", args.n);
    println!("M (publishers/subscribers):  {:>8}  @ {} Hz", args.m_pub, args.pub_rate_hz);
    println!("flaps triggered:             {:>8}", flap_count);
    println!("probes ok:                   {:>8}", total_probes_ok);
    println!("probes timeout:              {:>8}", total_probes_to);
    println!("probe timeout rate:          {:>7.2}%",
        100.0 * total_probes_to as f64 / (total_probes_ok + total_probes_to).max(1) as f64);
    println!("queryable callbacks served:  {:>8}", total_calls);
    println!("data samples received:       {:>8}", total_recv);
    println!("data samples missed (gap):   {:>8}", total_miss);
    println!("data loss:                   {:>7.2}%",
        100.0 * total_miss as f64 / (total_recv + total_miss).max(1) as f64);
    println!("longest continuous outage:   {:>8.2} s", max_outage.as_secs_f64());
    println!("===============================");

    drop(client_sub);
    drop(client_pub);
    drop(router_b);
    drop(router_a);

    Ok(())
}
