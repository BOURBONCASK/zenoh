// 4-quadrant router-router drift reproducer.
//
// The earlier z_router_drift_repro_regions.rs example had a methodological
// hole: its "ROS axis" had both probe and queryable attached as clients of
// the same s100 router, so the probe never read router_subs/router_qabls
// and the SPF storm on routers_net could not affect it by construction.
// Its "Aorta axis" measured a publisher whose physical e2e path crossed
// the link being torn down, so observed loss was indistinguishable from
// "the link is gone".
//
// This variant fills in the missing two quadrants:
//
//   Topology (one process; each router has its own Tables / hat / faces)
//   --------
//     router_cloud (mode=router, listen :17501)
//        ▲ TCP linkstate
//     router_x5    (mode=router, listen :17502, connects :17501 + :17503)
//        ▲ TCP linkstate
//     router_s100  (mode=router, listen :17503)
//
//   Clients (all share the routers above)
//   -------
//     intra_ros_pub      : client → s100       declares N intra/ros/q/{i}
//     intra_ros_probe    : client → s100       queries them every tick
//     cross_ros_pub      : client → x5         declares N cross/ros/q/{i}
//     cross_ros_probe    : client → s100       queries them every tick   *** key ***
//     intra_aorta_pub    : client → s100       publishes M intra/aorta/{j}
//     intra_aorta_sub    : client → s100       subscribes the above       *** key ***
//     cross_aorta_pub    : client → cloud      publishes M cross/aorta/{j}
//     cross_aorta_sub    : client → s100       subscribes the above
//
//   The four quadrants
//   ------------------
//                  | probe/sub on same router as pub | probe on s100, pub elsewhere
//     ROS-style    | intra_ros   (baseline control)  | cross_ros   (RPC over linkstate)
//     Aorta-style  | intra_aorta (data over face)    | cross_aorta (data over linkstate)
//
//   Cloud-flap interpretation
//   -------------------------
//   When the cloud router DROPs and re-OPENs, only the cross_aorta path
//   physically loses bytes. The other three quadrants stay routable
//   *physically*. Any non-zero loss / timeout on the other three is purely
//   the cost of x5 and s100 processing the resulting LinkStateList +
//   undeclare/redeclare burst on their routers_net and router_subs under
//   the Tables writer lock.
//
//   - intra_ros_to       == 0  → s100 face never blocked by remote events (good baseline)
//   - cross_ros_to        > 0  → x5 (and/or s100) Tables lock held long enough
//                                to stall a query traversing the link
//   - intra_aorta_miss    > 0  → s100 face actually stalled by remote events
//                                (== "everything dies even though it's local")
//   - cross_aorta_miss    > 0  → expected; physical link is gone
//
// Usage
// -----
//   cargo run --release --example z_router_drift_repro_4q -- \
//       --n 500 --m-pub 10 --pub-rate-hz 50 \
//       --probe-rate-hz 4 --probe-timeout-ms 800 \
//       --flap-interval-secs 25 --flap-down-ms 3000 \
//       --duration-secs 130

use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use zenoh::config::{Config, WhatAmI};
use zenoh::query::{ConsolidationMode, QueryTarget};

#[derive(Parser, Debug, Clone)]
#[command(about = "4-quadrant router-router drift reproducer")]
struct Args {
    #[arg(long, default_value_t = 500)]
    n: usize,
    #[arg(long, default_value_t = 10)]
    m_pub: usize,
    #[arg(long, default_value_t = 50)]
    pub_rate_hz: u32,
    #[arg(long, default_value_t = 4)]
    probe_rate_hz: u32,
    #[arg(long, default_value_t = 25)]
    flap_interval_secs: u64,
    #[arg(long, default_value_t = 3000)]
    flap_down_ms: u64,
    #[arg(long, default_value_t = 130)]
    duration_secs: u64,
    /// `cloud` flaps router_cloud (cloud-side link).
    /// `x5` flaps router_x5 (s100↔x5 link, the one whose entities populate
    /// s100.router_subs the most — biggest SPF storm on s100).
    #[arg(long, default_value = "cloud")]
    flap_which: String,
    #[arg(long, default_value_t = 17501)]
    port_cloud: u16,
    #[arg(long, default_value_t = 17502)]
    port_x5: u16,
    #[arg(long, default_value_t = 17503)]
    port_s100: u16,
    #[arg(long, default_value_t = 800)]
    probe_timeout_ms: u64,
}

fn ts_now() -> String {
    let dur = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = dur.as_secs();
    format!("{:02}:{:02}:{:02}.{:03}", (secs / 3600) % 24, (secs / 60) % 60, secs % 60, dur.subsec_millis())
}

fn router_cfg(listen: &str, connect: &[&str]) -> Config {
    let mut c = Config::default();
    c.set_mode(Some(WhatAmI::Router)).unwrap();
    c.scouting.multicast.set_enabled(Some(false)).unwrap();
    c.insert_json5("listen/endpoints", &serde_json::to_string(&[listen]).unwrap()).unwrap();
    if !connect.is_empty() {
        c.insert_json5("connect/endpoints", &serde_json::to_string(connect).unwrap()).unwrap();
    }
    c
}

fn client_cfg(connect: &str) -> Config {
    let mut c = Config::default();
    c.set_mode(Some(WhatAmI::Client)).unwrap();
    c.scouting.multicast.set_enabled(Some(false)).unwrap();
    c.insert_json5("connect/endpoints", &serde_json::to_string(&[connect]).unwrap()).unwrap();
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

async fn open_cloud_router(a: &Args) -> Result<zenoh::Session, Box<dyn Error + Send + Sync>> {
    let listen = format!("tcp/127.0.0.1:{}", a.port_cloud);
    Ok(zenoh::open(router_cfg(&listen, &[])).await?)
}

async fn open_s100_router(a: &Args) -> Result<zenoh::Session, Box<dyn Error + Send + Sync>> {
    let listen = format!("tcp/127.0.0.1:{}", a.port_s100);
    Ok(zenoh::open(router_cfg(&listen, &[])).await?)
}

async fn open_x5_router(a: &Args) -> Result<zenoh::Session, Box<dyn Error + Send + Sync>> {
    let listen = format!("tcp/127.0.0.1:{}", a.port_x5);
    let connect_cloud = format!("tcp/127.0.0.1:{}", a.port_cloud);
    let connect_s100 = format!("tcp/127.0.0.1:{}", a.port_s100);
    Ok(zenoh::open(router_cfg(&listen, &[&connect_cloud, &connect_s100])).await?)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();
    zenoh::init_log_from_env_or("zenoh=info");

    let cloud_listen = format!("tcp/127.0.0.1:{}", args.port_cloud);
    let s100_listen = format!("tcp/127.0.0.1:{}", args.port_s100);
    let x5_listen = format!("tcp/127.0.0.1:{}", args.port_x5);

    println!("[setup] N={}  M={}@{}Hz  probe={}Hz  timeout={}ms  duration={}s",
        args.n, args.m_pub, args.pub_rate_hz, args.probe_rate_hz, args.probe_timeout_ms, args.duration_secs);

    let mut router_cloud = Some(open_cloud_router(&args).await?);
    let router_s100_perm = open_s100_router(&args).await?;  // s100 is never torn down in this experiment
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut router_x5 = Some(open_x5_router(&args).await?);
    tokio::time::sleep(Duration::from_millis(700)).await;

    // --- client sessions (8 in total: 4 quadrants × 2 endpoints each) ---
    let intra_ros_pub   = zenoh::open(client_cfg(&s100_listen)).await?;
    let intra_ros_probe = zenoh::open(client_cfg(&s100_listen)).await?;
    let cross_ros_pub   = zenoh::open(client_cfg(&x5_listen)).await?;
    let cross_ros_probe = zenoh::open(client_cfg(&s100_listen)).await?;
    let intra_aorta_pub = zenoh::open(client_cfg(&s100_listen)).await?;
    let intra_aorta_sub = zenoh::open(client_cfg(&s100_listen)).await?;
    let cross_aorta_pub = zenoh::open(client_cfg(&cloud_listen)).await?;
    let cross_aorta_sub = zenoh::open(client_cfg(&s100_listen)).await?;
    println!("[setup] 8 client sessions opened");

    // --- declare N queryables on each ros pub ---
    let n = args.n;
    let mut intra_qbls = Vec::with_capacity(n);
    let mut cross_qbls = Vec::with_capacity(n);
    for i in 0..n {
        let k1 = format!("intra/ros/q/{i}");
        let k1c = k1.clone();
        intra_qbls.push(intra_ros_pub.declare_queryable(&k1)
            .callback(move |q| { let k = k1c.clone(); tokio::spawn(async move { let _ = q.reply(k, "ok").await; }); })
            .await?);
        let k2 = format!("cross/ros/q/{i}");
        let k2c = k2.clone();
        cross_qbls.push(cross_ros_pub.declare_queryable(&k2)
            .callback(move |q| { let k = k2c.clone(); tokio::spawn(async move { let _ = q.reply(k, "ok").await; }); })
            .await?);
    }
    println!("[setup] declared {n} intra + {n} cross queryables");

    // --- declare M subscribers on each aorta sub ---
    let m = args.m_pub;
    let mut intra_aorta_stats: Vec<Arc<SubStats>> = Vec::with_capacity(m);
    let mut cross_aorta_stats: Vec<Arc<SubStats>> = Vec::with_capacity(m);
    let mut _isubs = Vec::with_capacity(m);
    let mut _csubs = Vec::with_capacity(m);
    for j in 0..m {
        let is = Arc::new(SubStats::new());
        intra_aorta_stats.push(is.clone());
        let cs = Arc::new(SubStats::new());
        cross_aorta_stats.push(cs.clone());
        _isubs.push(intra_aorta_sub.declare_subscriber(format!("intra/aorta/{j}"))
            .callback(move |s| count(&is, s.payload().to_bytes().as_ref()))
            .await?);
        _csubs.push(cross_aorta_sub.declare_subscriber(format!("cross/aorta/{j}"))
            .callback(move |s| count(&cs, s.payload().to_bytes().as_ref()))
            .await?);
    }

    // --- spawn M publishers on each aorta pub ---
    let stop = Arc::new(AtomicBool::new(false));
    for j in 0..m {
        for (which, sess) in [("intra", intra_aorta_pub.clone()), ("cross", cross_aorta_pub.clone())] {
            let key = format!("{which}/aorta/{j}");
            let stop = stop.clone();
            let period = Duration::from_micros(1_000_000 / args.pub_rate_hz.max(1) as u64);
            tokio::spawn(async move {
                let p = match sess.declare_publisher(&key).await { Ok(p) => p, Err(_) => return };
                let mut seq: u64 = 0;
                let mut next = Instant::now() + period;
                while !stop.load(Ordering::Relaxed) {
                    let _ = p.put(seq.to_le_bytes().to_vec()).await;
                    seq = seq.wrapping_add(1);
                    let now = Instant::now();
                    if next > now { tokio::time::sleep(next - now).await; }
                    next += period;
                }
            });
        }
    }
    println!("[setup] {} aorta publishers per side started, settle 5s for linkstate", m);
    tokio::time::sleep(Duration::from_secs(5)).await;

    // --- probe loop ---
    let probe_period = Duration::from_millis(1000 / args.probe_rate_hz.max(1) as u64);
    let probe_timeout = Duration::from_millis(args.probe_timeout_ms);
    let total_duration = Duration::from_secs(args.duration_secs);
    let flap_interval = Duration::from_secs(args.flap_interval_secs.max(1));
    let flap_down = Duration::from_millis(args.flap_down_ms);

    let mut next_flap = Instant::now() + flap_interval;
    let mut next_recovery_end: Option<Instant> = None;
    let mut state = "STEADY";
    let mut tick = 0u64;
    let started = Instant::now();
    let mut flap_count = 0u64;

    let mut intra_ok = 0u64; let mut intra_to = 0u64;
    let mut cross_ok = 0u64; let mut cross_to = 0u64;
    let mut max_cross_lat = Duration::ZERO;

    let mut intra_aorta_outage_ticks = 0u64;
    let mut cross_aorta_outage_ticks = 0u64;
    let mut cross_ros_outage_ticks = 0u64;
    let mut intra_ros_outage_ticks = 0u64;

    let nominal_aorta = m as u64 * args.pub_rate_hz as u64;
    let mut last_recv_intra: u64 = 0;
    let mut last_recv_cross: u64 = 0;
    let mut last_tick_at = Instant::now();

    while started.elapsed() < total_duration {
        tick += 1;
        let tick_start = Instant::now();

        // flap scheduler
        if tick_start >= next_flap && state == "STEADY" {
            println!("\n[flap {}] tick={tick} dropping router_{} for {} ms (flap #{})",
                ts_now(), args.flap_which, args.flap_down_ms, flap_count + 1);
            flap_count += 1;
            match args.flap_which.as_str() {
                "cloud" => { router_cloud = None; }
                "x5"    => { router_x5    = None; }
                _ => unreachable!("flap_which must be cloud or x5"),
            }
            state = "FLAP";
            next_recovery_end = Some(tick_start + flap_down);
        }
        if state == "FLAP" {
            if let Some(end) = next_recovery_end {
                if tick_start >= end {
                    println!("[flap {}] tick={tick} re-opening router_{}", ts_now(), args.flap_which);
                    match args.flap_which.as_str() {
                        "cloud" => { router_cloud = Some(open_cloud_router(&args).await?); }
                        "x5"    => { router_x5    = Some(open_x5_router(&args).await?); }
                        _ => unreachable!(),
                    }
                    state = "RECOVER";
                    next_flap = tick_start + flap_interval;
                }
            }
        }

        // --- intra & cross ROS probes, run all in parallel ---
        let mut intra_futs = Vec::with_capacity(n);
        let mut cross_futs = Vec::with_capacity(n);
        for i in 0..n {
            let s = intra_ros_probe.clone();
            let key = format!("intra/ros/q/{i}");
            intra_futs.push(tokio::spawn(async move {
                let res = tokio::time::timeout(probe_timeout, async move {
                    let r = s.get(&key).target(QueryTarget::All).consolidation(ConsolidationMode::None).await?;
                    while r.recv_async().await.is_ok() { return Ok::<bool, Box<dyn Error + Send + Sync>>(true); }
                    Ok::<bool, Box<dyn Error + Send + Sync>>(false)
                }).await;
                matches!(res, Ok(Ok(true)))
            }));
            let s = cross_ros_probe.clone();
            let key = format!("cross/ros/q/{i}");
            let p_start = Instant::now();
            cross_futs.push(tokio::spawn(async move {
                let res = tokio::time::timeout(probe_timeout, async move {
                    let r = s.get(&key).target(QueryTarget::All).consolidation(ConsolidationMode::None).await?;
                    while r.recv_async().await.is_ok() { return Ok::<bool, Box<dyn Error + Send + Sync>>(true); }
                    Ok::<bool, Box<dyn Error + Send + Sync>>(false)
                }).await;
                (matches!(res, Ok(Ok(true))), p_start.elapsed())
            }));
        }
        let mut iok = 0u32; let mut ito = 0u32;
        for f in intra_futs { if matches!(f.await, Ok(true)) { iok += 1 } else { ito += 1 } }
        let mut cok = 0u32; let mut cto = 0u32; let mut cmax = Duration::ZERO;
        for f in cross_futs {
            match f.await {
                Ok((true, lat)) => { cok += 1; if lat > cmax { cmax = lat } }
                _ => cto += 1,
            }
        }
        if cmax > max_cross_lat { max_cross_lat = cmax }
        intra_ok += iok as u64; intra_to += ito as u64;
        cross_ok += cok as u64; cross_to += cto as u64;
        if ito > 0 { intra_ros_outage_ticks += 1 }
        if cto > 0 { cross_ros_outage_ticks += 1 }

        // --- aorta stats per axis ---
        let recv_intra: u64 = intra_aorta_stats.iter().map(|s| s.received.load(Ordering::Relaxed)).sum();
        let recv_cross: u64 = cross_aorta_stats.iter().map(|s| s.received.load(Ordering::Relaxed)).sum();
        let now = Instant::now();
        let dt = now.duration_since(last_tick_at).as_secs_f64().max(0.001);
        let irps = ((recv_intra - last_recv_intra) as f64 / dt) as u64;
        let crps = ((recv_cross - last_recv_cross) as f64 / dt) as u64;
        last_recv_intra = recv_intra;
        last_recv_cross = recv_cross;
        last_tick_at = now;
        if irps < nominal_aorta / 10 { intra_aorta_outage_ticks += 1 }
        if crps < nominal_aorta / 10 { cross_aorta_outage_ticks += 1 }

        if state == "RECOVER" && ito == 0 && cto == 0 && irps >= nominal_aorta / 2 && crps >= nominal_aorta / 2 {
            state = "STEADY";
        }

        println!("[{}] t={tick:4}  intra_ros: ok={iok:4} to={ito:3}  cross_ros: ok={cok:4} to={cto:3} max_lat={:>4}ms  intra_a: r/s={irps:>5}  cross_a: r/s={crps:>5}  {state}",
            ts_now(), cmax.as_millis());

        let elapsed = tick_start.elapsed();
        if elapsed < probe_period { tokio::time::sleep(probe_period - elapsed).await; }
    }

    stop.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let recv_intra: u64 = intra_aorta_stats.iter().map(|s| s.received.load(Ordering::Relaxed)).sum();
    let miss_intra: u64 = intra_aorta_stats.iter().map(|s| s.missed.load(Ordering::Relaxed)).sum();
    let recv_cross: u64 = cross_aorta_stats.iter().map(|s| s.received.load(Ordering::Relaxed)).sum();
    let miss_cross: u64 = cross_aorta_stats.iter().map(|s| s.missed.load(Ordering::Relaxed)).sum();

    println!("\n========== 4-QUADRANT SUMMARY ==========");
    println!("flaps:                   {flap_count}   (cloud router DROP/OPEN)");
    println!("duration:                {} s", args.duration_secs);
    println!("--- ROS (query/reply) ---");
    println!("intra_ros : ok={intra_ok} to={intra_to} timeout_rate={:.2}% outage_ticks={intra_ros_outage_ticks}",
        100.0 * intra_to as f64 / (intra_ok + intra_to).max(1) as f64);
    println!("cross_ros : ok={cross_ok} to={cross_to} timeout_rate={:.2}% outage_ticks={cross_ros_outage_ticks}  max_latency={}ms",
        100.0 * cross_to as f64 / (cross_ok + cross_to).max(1) as f64,
        max_cross_lat.as_millis());
    println!("--- Aorta (pub/sub) ---");
    println!("intra_aorta : recv={recv_intra} miss={miss_intra} loss={:.2}% outage_ticks={intra_aorta_outage_ticks}",
        100.0 * miss_intra as f64 / (recv_intra + miss_intra).max(1) as f64);
    println!("cross_aorta : recv={recv_cross} miss={miss_cross} loss={:.2}% outage_ticks={cross_aorta_outage_ticks}",
        100.0 * miss_cross as f64 / (recv_cross + miss_cross).max(1) as f64);
    println!("=========================================");
    println!();
    println!("Interpretation:");
    println!("  intra_ros / intra_aorta == 0  → s100 face is never blocked by");
    println!("                                  remote linkstate / declare storms.");
    println!("                                  (The previous 'ROS axis 0%' was THIS.)");
    println!("  intra_ros / intra_aorta  > 0  → cloud flap propagates into s100's");
    println!("                                  Tables writer lock and stalls clients");
    println!("                                  whose path doesn't touch the flapping");
    println!("                                  link → matches fleet symptom.");
    println!("  cross_ros  > 0               → x5/s100 forwarding stalls during the");
    println!("                                  declare/SPF burst (query needs both");
    println!("                                  routers' router_qabls + face lookups).");

    drop(intra_ros_pub); drop(intra_ros_probe);
    drop(cross_ros_pub); drop(cross_ros_probe);
    drop(intra_aorta_pub); drop(intra_aorta_sub);
    drop(cross_aorta_pub); drop(cross_aorta_sub);
    drop(router_cloud);
    drop(router_s100_perm);
    drop(router_x5);

    Ok(())
}

fn count(stats: &Arc<SubStats>, payload: &[u8]) {
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
}
