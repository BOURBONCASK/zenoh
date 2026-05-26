// Regions-aware variant of z_router_drift_repro.
//
// This example mirrors the production vita-robot topology — a 3-router chain
// with two independent traffic axes — and lets you flip between
// 1.8.0-style "one big HAT" routing and 1.9-track regions-partitioned
// routing on the X5 bridge. The point is to demonstrate that with regions,
// a link flap on one axis does NOT storm the other axis, even though
// 1.8.0-style routing makes them collide on the same writer lock.
//
// Topology
// --------
//
//     router_cloud (mode=router, region_name="cloud-aorta",  listen :RC)
//        ▲
//        │ TCP linkstate (acts as "cloud QUIC" in the vita deployment)
//        │
//     router_x5    (mode=router, region_name="x5-bridge",    listen :RX)
//        ▲                       gateway.south = (when --use-regions):
//        │                          [{filters:[{region_names:["cloud-aorta"]}]},
//        │                           {filters:[{region_names:["robot-local"]}]}]
//        │
//        │ TCP linkstate (acts as "X5↔S100 Ethernet")
//        │
//     router_s100  (mode=router, region_name="robot-local",  listen :RS)
//        ▲          (gateway.south = [{filters:[{region_names:["x5-bridge"]}]}]
//        │           when --use-regions)
//        │
//        ├── ros_pub      (client → router_s100)  — declares N ROS-like
//        │                                          queryables + tokens
//        │
//        ├── ros_probe    (client → router_s100)  — probes ros queryables.
//        │                                          Query stays in-region
//        │                                          on s100; never crosses
//        │                                          the s100↔x5 boundary.
//        │
//        └── aorta_sub    (client → router_s100)  — subscribes Aorta data
//                                                  published by aorta_pub.
//
//     router_cloud
//        └── aorta_pub    (client → router_cloud) — publishes M topics at
//                                                  PUB_HZ. Samples traverse
//                                                  cloud → x5 → s100 → aorta_sub.
//
// What the experiment shows
// -------------------------
// The flap_which CLI selects which router-router link to tear down each
// flap_interval:
//
//   --flap-which s100   only drops the s100 router (s100↔x5 link gone)
//   --flap-which cloud  only drops the cloud router (cloud↔x5 link gone)
//   --flap-which both   alternates: odd flap = s100, even flap = cloud
//
// Without regions (--use-regions=false), each router runs ONE HAT, so a
// flap on either link triggers a full-table compute_trees +
// {pubsub,queries,token}_tree_change storm under the tables writer lock
// on every router. The s100 router's writer lock is held while it
// re-emits declarations for every entity it knows about — including the
// N ROS entities native to s100 — even though those entities never
// touched the link that broke. ros_probe times out during the storm.
//
// With regions (--use-regions=true), router_s100 splits its routing
// state into HAT[North] (local ROS sessions) and HAT[South{x5-bridge}]
// (the x5 bridge face). A s100↔x5 link flap only schedules
// HAT[South{x5-bridge}].compute_trees; HAT[North] is untouched. The ROS
// queryables that ros_probe targets are in HAT[North], so the probes
// remain answerable while the bridge HAT re-stabilizes.
//
// Usage
// -----
//   cargo run --release --example z_router_drift_repro_regions -- \
//       --use-regions \
//       --n 500 --m-pub 10 --pub-rate-hz 50 \
//       --flap-which s100 --flap-interval-secs 25 --flap-down-ms 3000 \
//       --duration-secs 180
//
// Output (per tick):
//
//   [HH:MM:SS.mmm] t=NNN
//       ros:    ok/to + p99 latency
//       aorta:  recv/s + miss/s
//       state:  STEADY / FLAP / RECOVER  (state derives from ROS path)

use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use zenoh::config::{Config, WhatAmI};
use zenoh::query::{ConsolidationMode, QueryTarget};

#[derive(Parser, Debug, Clone)]
#[command(
    about = "Regions-aware router-router drift reproducer. Toggle --use-regions to see how 1.9-track regions partition the SPF storm."
)]
struct Args {
    /// Enable regions partitioning on the bridge router (x5) and the
    /// edge router (s100). When false, the topology is the classic
    /// 1.8.0-style flat router-router-router chain.
    /// Pass like: --use-regions=true or --use-regions=false (with an
    /// explicit value).
    #[arg(long, action = clap::ArgAction::Set, default_value_t = true)]
    use_regions: bool,

    /// Number of ROS-like queryables + liveliness tokens on router_s100.
    /// These live in HAT[North] on s100 when --use-regions is on.
    #[arg(long, default_value_t = 500)]
    n: usize,

    /// Number of Aorta-like high-rate publishers on router_cloud (and
    /// matching subscribers attached to router_s100). These cross the
    /// cloud → x5 → s100 region boundary.
    #[arg(long, default_value_t = 10)]
    m_pub: usize,

    /// Per-publisher publish rate in Hz.
    #[arg(long, default_value_t = 50)]
    pub_rate_hz: u32,

    /// ROS probe rate in Hz.
    #[arg(long, default_value_t = 5)]
    probe_rate_hz: u32,

    /// Wall-clock seconds between link flaps. 0 = no flapping.
    #[arg(long, default_value_t = 25)]
    flap_interval_secs: u64,

    /// Duration of each link tear-down, in milliseconds.
    #[arg(long, default_value_t = 3000)]
    flap_down_ms: u64,

    /// Which link to flap each cycle: `s100`, `cloud`, or `both`
    /// (`both` alternates).
    #[arg(long, default_value = "s100")]
    flap_which: String,

    /// Total experiment duration in seconds.
    #[arg(long, default_value_t = 180)]
    duration_secs: u64,

    /// TCP ports for the three routers.
    #[arg(long, default_value_t = 17501)]
    port_cloud: u16,
    #[arg(long, default_value_t = 17502)]
    port_x5: u16,
    #[arg(long, default_value_t = 17503)]
    port_s100: u16,

    /// Per-probe timeout in milliseconds.
    #[arg(long, default_value_t = 600)]
    probe_timeout_ms: u64,

    /// Enable zenoh trace logging.
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

fn make_router_config(
    listen: &str,
    connect: &[&str],
    region: Option<&str>,
    gateway: Option<&str>,
) -> Config {
    let mut c = Config::default();
    c.set_mode(Some(WhatAmI::Router)).unwrap();
    c.scouting.multicast.set_enabled(Some(false)).unwrap();
    c.insert_json5(
        "listen/endpoints",
        &serde_json::to_string(&[listen]).unwrap(),
    )
    .unwrap();
    if !connect.is_empty() {
        c.insert_json5(
            "connect/endpoints",
            &serde_json::to_string(connect).unwrap(),
        )
        .unwrap();
    }
    if let Some(name) = region {
        c.insert_json5("region_name", &format!("\"{name}\"")).unwrap();
    }
    if let Some(g) = gateway {
        c.insert_json5("gateway", g).unwrap();
    }
    c
}

fn make_client_config(connect: &str) -> Config {
    let mut c = Config::default();
    c.set_mode(Some(WhatAmI::Client)).unwrap();
    c.scouting.multicast.set_enabled(Some(false)).unwrap();
    c.insert_json5(
        "connect/endpoints",
        &serde_json::to_string(&[connect]).unwrap(),
    )
    .unwrap();
    c
}

#[derive(Default)]
struct SubStats {
    received: AtomicU64,
    missed: AtomicU64,
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

async fn open_cloud_router(args: &Args) -> Result<zenoh::Session, Box<dyn Error + Send + Sync>> {
    let listen = format!("tcp/127.0.0.1:{}", args.port_cloud);
    let region = if args.use_regions { Some("cloud-aorta") } else { None };
    let cfg = make_router_config(&listen, &[], region, None);
    Ok(zenoh::open(cfg).await?)
}

async fn open_x5_router(args: &Args) -> Result<zenoh::Session, Box<dyn Error + Send + Sync>> {
    let listen = format!("tcp/127.0.0.1:{}", args.port_x5);
    let connect_cloud = format!("tcp/127.0.0.1:{}", args.port_cloud);
    let connect_s100 = format!("tcp/127.0.0.1:{}", args.port_s100);
    let region = if args.use_regions { Some("x5-bridge") } else { None };
    // With regions: cloud is south[0], s100 is south[1] from x5's view.
    let gateway = if args.use_regions {
        Some(
            r#"{
                south: [
                    {filters: [{region_names: ["cloud-aorta"]}]},
                    {filters: [{region_names: ["robot-local"]}]}
                ]
            }"#,
        )
    } else {
        None
    };
    let cfg = make_router_config(
        &listen,
        &[&connect_cloud, &connect_s100],
        region,
        gateway,
    );
    Ok(zenoh::open(cfg).await?)
}

async fn open_s100_router(args: &Args) -> Result<zenoh::Session, Box<dyn Error + Send + Sync>> {
    let listen = format!("tcp/127.0.0.1:{}", args.port_s100);
    let region = if args.use_regions { Some("robot-local") } else { None };
    // s100 (leaf router) uses the auto-preset gateway. Its `region_name`
    // lets the bridge router (x5) filter it into a specific south subregion
    // on x5's side. From s100's own perspective the incoming x5 connection
    // is sent as "remote South" by x5, so s100 puts x5 in its North region
    // via the (None, Some(South)) arm of compute_region_of — which is the
    // arm responsible for confining the storm to s100's "north"
    // (= "the cloud bridge link") instead of mixing with HAT[North_main]
    // where the ROS clients live.
    let cfg = make_router_config(&listen, &[], region, None);
    Ok(zenoh::open(cfg).await?)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();

    if args.trace {
        zenoh::init_log_from_env_or("zenoh=trace");
    } else {
        zenoh::init_log_from_env_or("zenoh=info");
    }

    println!(
        "[setup] use_regions={}  ports: cloud={} x5={} s100={}",
        args.use_regions, args.port_cloud, args.port_x5, args.port_s100
    );
    println!(
        "[setup] N={}  M_pub={}@{}Hz  probe={}Hz  flap_which={}",
        args.n, args.m_pub, args.pub_rate_hz, args.probe_rate_hz, args.flap_which
    );

    // Start the three routers.
    let mut router_cloud: Option<zenoh::Session> = Some(open_cloud_router(&args).await?);
    println!(
        "[setup] router_cloud zid={}",
        router_cloud.as_ref().unwrap().zid()
    );
    let mut router_s100: Option<zenoh::Session> = Some(open_s100_router(&args).await?);
    println!(
        "[setup] router_s100 zid={}",
        router_s100.as_ref().unwrap().zid()
    );
    // x5 connects to both — start it after the listeners are up.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut router_x5: Option<zenoh::Session> = Some(open_x5_router(&args).await?);
    println!(
        "[setup] router_x5    zid={}",
        router_x5.as_ref().unwrap().zid()
    );

    // Settle the router-router transports.
    tokio::time::sleep(Duration::from_millis(700)).await;

    // ros_pub + ros_probe attach to router_s100. They never cross a region boundary.
    let s100_listen = format!("tcp/127.0.0.1:{}", args.port_s100);
    let ros_pub = zenoh::open(make_client_config(&s100_listen)).await?;
    let ros_probe = zenoh::open(make_client_config(&s100_listen)).await?;
    println!(
        "[setup] ros_pub zid={}  ros_probe zid={}",
        ros_pub.zid(),
        ros_probe.zid()
    );

    // aorta_pub attaches to router_cloud; aorta_sub attaches to router_s100.
    // Aorta samples therefore cross cloud → x5 → s100.
    let cloud_listen = format!("tcp/127.0.0.1:{}", args.port_cloud);
    let aorta_pub = zenoh::open(make_client_config(&cloud_listen)).await?;
    let aorta_sub = zenoh::open(make_client_config(&s100_listen)).await?;
    println!(
        "[setup] aorta_pub zid={}  aorta_sub zid={}",
        aorta_pub.zid(),
        aorta_sub.zid()
    );

    // Declare N ROS-like queryables + tokens on ros_pub.
    let n = args.n;
    println!("[setup] ros_pub declaring {n} queryables + {n} liveliness tokens");
    let mut _qbls = Vec::with_capacity(n);
    let mut _toks = Vec::with_capacity(n);
    for i in 0..n {
        let key = format!("ros/q/{i}");
        let key_for_reply = key.clone();
        let q = ros_pub
            .declare_queryable(&key)
            .callback(move |query| {
                let key = key_for_reply.clone();
                tokio::spawn(async move {
                    let _ = query.reply(key, "ok").await;
                });
            })
            .await?;
        _qbls.push(q);
        let tok = ros_pub
            .liveliness()
            .declare_token(format!("ros/l/{i}"))
            .await?;
        _toks.push(tok);
    }

    // M aorta-style data subscribers on aorta_sub (s100-attached).
    let m = args.m_pub;
    println!("[setup] aorta_sub declaring {m} data subscribers");
    let mut sub_stats: Vec<Arc<SubStats>> = Vec::with_capacity(m);
    let mut _subs = Vec::with_capacity(m);
    for j in 0..m {
        let stats = Arc::new(SubStats::new());
        sub_stats.push(stats.clone());
        let key = format!("aorta/pub/{j}");
        let sub = aorta_sub
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

    // Spawn M publisher tasks on aorta_pub (cloud-attached).
    let stop = Arc::new(AtomicBool::new(false));
    for j in 0..m {
        let key = format!("aorta/pub/{j}");
        let session = aorta_pub.clone();
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
        "[setup] {} aorta publishers started, nominal {} samples/s",
        m,
        m as u32 * args.pub_rate_hz
    );

    // Settle linkstate / declare propagation across all three routers.
    println!("[setup] waiting 5s for linkstate to propagate across cloud↔x5↔s100");
    tokio::time::sleep(Duration::from_secs(5)).await;
    println!("[setup] ready — beginning probe loop\n");

    let probe_period = Duration::from_millis(1000 / args.probe_rate_hz.max(1) as u64);
    let probe_timeout = Duration::from_millis(args.probe_timeout_ms);
    let total_duration = Duration::from_secs(args.duration_secs);
    let flap_interval = if args.flap_interval_secs == 0 {
        Duration::from_secs(60 * 60 * 24 * 30) // 30 days, effectively disabled
    } else {
        Duration::from_secs(args.flap_interval_secs)
    };
    let flap_down = Duration::from_millis(args.flap_down_ms);

    let mut next_flap = Instant::now() + flap_interval;
    let mut next_recovery_end: Option<Instant> = None;
    let mut current_flap_target: &'static str = "s100";
    let mut state: &'static str = "STEADY";
    let mut tick = 0u64;
    let started = Instant::now();
    let mut flap_count = 0u64;

    let mut last_recv: u64 = sub_stats.iter().map(|s| s.received.load(Ordering::Relaxed)).sum();
    let mut last_miss: u64 = sub_stats.iter().map(|s| s.missed.load(Ordering::Relaxed)).sum();
    let mut last_tick_at = Instant::now();

    let mut ros_outage_start: Option<Instant> = None;
    let mut max_ros_outage = Duration::ZERO;
    let mut aorta_outage_start: Option<Instant> = None;
    let mut max_aorta_outage = Duration::ZERO;
    let mut total_probes_ok = 0u64;
    let mut total_probes_to = 0u64;
    let mut ros_only_outage_ticks = 0u64;
    let mut aorta_only_outage_ticks = 0u64;
    let mut both_outage_ticks = 0u64;

    while started.elapsed() < total_duration {
        tick += 1;
        let tick_start = Instant::now();

        if args.flap_interval_secs > 0 && tick_start >= next_flap && state == "STEADY" {
            // Pick which link to flap this cycle.
            let target = match args.flap_which.as_str() {
                "s100" => "s100",
                "cloud" => "cloud",
                "both" => {
                    if flap_count % 2 == 0 {
                        "s100"
                    } else {
                        "cloud"
                    }
                }
                _ => "s100",
            };
            current_flap_target = target;
            flap_count += 1;
            println!(
                "\n[flap {}] tick={tick} dropping router_{target} for {} ms (flap #{})",
                ts_now(),
                args.flap_down_ms,
                flap_count
            );
            match target {
                "s100" => router_s100 = None,
                "cloud" => {
                    // Drop the cloud router. From x5's perspective this is
                    // identical to losing the QUIC link to cloud (TCP RST);
                    // x5↔s100 stays alive throughout, so we cleanly isolate
                    // "cloud-side jitter" from "local-side jitter". aorta_pub
                    // is attached to cloud and will reconnect when cloud
                    // re-opens; its publisher resets monotonic seq to 0.
                    router_cloud = None;
                }
                _ => unreachable!(),
            }
            state = "FLAP";
            next_recovery_end = Some(tick_start + flap_down);
        }

        if state == "FLAP" {
            if let Some(end) = next_recovery_end {
                if tick_start >= end {
                    println!(
                        "[flap {}] tick={tick} re-opening router_{}",
                        ts_now(),
                        current_flap_target
                    );
                    match current_flap_target {
                        "s100" => {
                            router_s100 = Some(open_s100_router(&args).await?);
                        }
                        "cloud" => {
                            router_cloud = Some(open_cloud_router(&args).await?);
                        }
                        _ => unreachable!(),
                    }
                    state = "RECOVER";
                    next_flap = tick_start + flap_interval;
                }
            }
        }

        // ROS probes (s100-internal path)
        let mut ros_futs = Vec::with_capacity(n);
        for i in 0..n {
            let s = ros_probe.clone();
            let key = format!("ros/q/{i}");
            ros_futs.push(tokio::spawn(async move {
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
                (matches!(res, Ok(Ok(true))), latency)
            }));
        }

        let mut ros_ok = 0u32;
        let mut ros_to = 0u32;
        let mut ros_latencies = Vec::with_capacity(n);
        for f in ros_futs {
            match f.await {
                Ok((true, lat)) => {
                    ros_ok += 1;
                    ros_latencies.push(lat);
                }
                _ => ros_to += 1,
            }
        }
        ros_latencies.sort();
        let ros_p99 = ros_latencies
            .get(ros_latencies.len() * 99 / 100)
            .copied()
            .unwrap_or_default();
        total_probes_ok += ros_ok as u64;
        total_probes_to += ros_to as u64;

        // Aorta data-flow stats
        let total_recv: u64 = sub_stats.iter().map(|s| s.received.load(Ordering::Relaxed)).sum();
        let total_miss: u64 = sub_stats.iter().map(|s| s.missed.load(Ordering::Relaxed)).sum();
        let now = Instant::now();
        let dt = now.duration_since(last_tick_at).as_secs_f64().max(0.001);
        let recv_per_s = ((total_recv - last_recv) as f64 / dt) as u64;
        let miss_per_s = ((total_miss - last_miss) as f64 / dt) as u64;
        last_recv = total_recv;
        last_miss = total_miss;
        last_tick_at = now;

        let nominal_aorta = m as u64 * args.pub_rate_hz as u64;
        let aorta_outage = recv_per_s < nominal_aorta / 10;
        let ros_outage = ros_to > 0;

        // Track per-axis outage durations
        if ros_outage {
            ros_outage_start.get_or_insert(tick_start);
        } else if let Some(s) = ros_outage_start.take() {
            let d = tick_start - s;
            if d > max_ros_outage {
                max_ros_outage = d;
            }
        }
        if aorta_outage {
            aorta_outage_start.get_or_insert(tick_start);
        } else if let Some(s) = aorta_outage_start.take() {
            let d = tick_start - s;
            if d > max_aorta_outage {
                max_aorta_outage = d;
            }
        }
        match (ros_outage, aorta_outage) {
            (true, true) => both_outage_ticks += 1,
            (true, false) => ros_only_outage_ticks += 1,
            (false, true) => aorta_only_outage_ticks += 1,
            _ => {}
        }

        if state == "RECOVER" && !ros_outage && !aorta_outage {
            state = "STEADY";
        }

        println!(
            "[{}] t={tick:5}  ros: ok={ros_ok:4} to={ros_to:4} p99={:>4}ms   aorta: recv/s={recv_per_s:>5} miss/s={miss_per_s:>5}   {state}",
            ts_now(),
            ros_p99.as_millis()
        );

        let elapsed = tick_start.elapsed();
        if elapsed < probe_period {
            tokio::time::sleep(probe_period - elapsed).await;
        }
    }

    if let Some(s) = ros_outage_start.take() {
        let d = Instant::now() - s;
        if d > max_ros_outage {
            max_ros_outage = d;
        }
    }
    if let Some(s) = aorta_outage_start.take() {
        let d = Instant::now() - s;
        if d > max_aorta_outage {
            max_aorta_outage = d;
        }
    }

    stop.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let total_recv: u64 = sub_stats.iter().map(|s| s.received.load(Ordering::Relaxed)).sum();
    let total_miss: u64 = sub_stats.iter().map(|s| s.missed.load(Ordering::Relaxed)).sum();

    println!("\n========== SUMMARY ==========");
    println!("use_regions:                       {}", args.use_regions);
    println!("duration:                       {:>5} s", args.duration_secs);
    println!("flap_which:                     {}", args.flap_which);
    println!("flaps triggered:                {:>5}", flap_count);
    println!("---- ROS axis (ros_probe → ros_pub, in-region on s100) ----");
    println!("ros probes ok:                {:>7}", total_probes_ok);
    println!("ros probes timeout:           {:>7}", total_probes_to);
    println!(
        "ros probe timeout rate:         {:>5.2}%",
        100.0 * total_probes_to as f64 / (total_probes_ok + total_probes_to).max(1) as f64
    );
    println!(
        "longest ROS outage:              {:>5.2} s",
        max_ros_outage.as_secs_f64()
    );
    println!("---- Aorta axis (aorta_pub on cloud → aorta_sub on s100) ----");
    println!("aorta samples received:       {:>7}", total_recv);
    println!("aorta samples missed (gap):   {:>7}", total_miss);
    println!(
        "aorta data loss:                {:>5.2}%",
        100.0 * total_miss as f64 / (total_recv + total_miss).max(1) as f64
    );
    println!(
        "longest aorta outage:            {:>5.2} s",
        max_aorta_outage.as_secs_f64()
    );
    println!("---- Cross-axis spillover (the regions test) ----");
    println!("ticks with ros outage only:     {:>5}", ros_only_outage_ticks);
    println!("ticks with aorta outage only:   {:>5}", aorta_only_outage_ticks);
    println!("ticks with both outage:         {:>5}", both_outage_ticks);
    println!("===============================");

    drop(aorta_sub);
    drop(aorta_pub);
    drop(ros_probe);
    drop(ros_pub);
    let _ = router_x5;
    drop(router_s100);
    drop(router_cloud);

    Ok(())
}
