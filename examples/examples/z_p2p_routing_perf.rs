//
// Copyright (c) 2026 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//

use std::{
    env,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use clap::{Args as ClapArgs, Parser, Subcommand};
use serde_json::json;
use zenoh::{bytes::ZBytes, qos::CongestionControl, Config, Wait};

#[derive(Parser, Debug)]
struct Args {
    #[command(subcommand)]
    command: Role,
}

#[derive(Subcommand, Debug)]
enum Role {
    Supervisor(SupervisorArgs),
    Hub(NodeArgs),
    Idle(NodeArgs),
    Publisher(PublisherArgs),
    Subscriber(SubscriberArgs),
    Churn(ChurnArgs),
    /// Combined session that hosts N publishers + N subscribers + N
    /// queryables + N getter loops in a single zenoh session. Used by the
    /// realistic-50 preset where 50 peer processes carry 100+ topics and
    /// 100+ services distributed evenly across the mesh. The worker's
    /// declares use globally-unique key indexes derived from
    /// `worker_index * n_<role>`, so e.g. worker 0 owns publishers
    /// 0..n_publishers, worker 1 owns n_publishers..2*n_publishers, and so
    /// on. Getter targets are `{key}/svc/{(worker_index*n_getters + i)
    /// % (total_workers * n_queryables)}` — every queryable is hit by
    /// exactly one getter per cycle.
    Worker(WorkerArgs),
}

#[derive(ClapArgs, Clone, Debug)]
struct NodeArgs {
    #[arg(long, default_value = "peer")]
    mode: String,
    #[arg(long)]
    listen: Option<String>,
    /// Connect endpoints. May be specified multiple times for multi-hub
    /// backbone topologies (e.g. a router shard hub that connects to all
    /// previously-listed hubs to form a backbone ring or mesh).
    #[arg(long)]
    connect: Vec<String>,
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    wait_declares: bool,
    #[arg(long)]
    cfg: Vec<String>,
    #[arg(long, default_value_t = 0)]
    index: usize,
}

#[derive(ClapArgs, Clone, Debug)]
struct PublisherArgs {
    #[command(flatten)]
    node: NodeArgs,
    #[arg(long, default_value = "repro/routing/perf")]
    key: String,
    #[arg(long, default_value_t = 64)]
    payload_size: usize,
    #[arg(long, default_value_t = 10)]
    put_period_ms: u64,
}

#[derive(ClapArgs, Clone, Debug)]
struct SubscriberArgs {
    #[command(flatten)]
    node: NodeArgs,
    #[arg(long, default_value = "repro/routing/perf")]
    key: String,
}

#[derive(ClapArgs, Clone, Debug)]
struct WorkerArgs {
    #[command(flatten)]
    node: NodeArgs,
    /// Base key prefix. Used as `{key}/topic/{global_topic_idx}` for
    /// publishers and `{key}/svc/{global_svc_idx}` for queryables.
    /// Subscribers always declare on `{key}/topic/**`.
    #[arg(long, default_value = "repro/routing/perf")]
    key: String,
    /// This worker's index in 0..total_workers. Determines which key
    /// indexes this worker owns.
    #[arg(long, default_value_t = 0)]
    worker_index: usize,
    /// Total worker count in the system. Used for getter target dispersion
    /// so every queryable is contacted by the same number of getters.
    #[arg(long, default_value_t = 1)]
    total_workers: usize,
    /// Publisher declares per worker. Worker w hosts publishers on keys
    /// `{key}/topic/{w * n_publishers + i}` for i in 0..n_publishers.
    #[arg(long, default_value_t = 2)]
    n_publishers: usize,
    /// Subscriber declares per worker. All declare the wildcard
    /// `{key}/topic/**` so each receives traffic from all publishers.
    #[arg(long, default_value_t = 2)]
    n_subscribers: usize,
    /// Queryable declares per worker. Worker w hosts queryables on keys
    /// `{key}/svc/{w * n_queryables + i}` for i in 0..n_queryables.
    #[arg(long, default_value_t = 2)]
    n_queryables: usize,
    /// Getter loops per worker. Each getter targets a fixed service key
    /// derived from its global index (see file header).
    #[arg(long, default_value_t = 2)]
    n_getters: usize,
    #[arg(long, default_value_t = 64)]
    payload_size: usize,
    /// Period between consecutive puts on the same publisher (ms). 1000 ms
    /// → 1 msg/s/publisher (with 100 publishers that is 100 msg/s system
    /// publication, fanning out to all subscribers).
    #[arg(long, default_value_t = 1000)]
    put_period_ms: u64,
    /// Period between consecutive queries from the same getter (ms).
    #[arg(long, default_value_t = 1000)]
    get_period_ms: u64,
    /// Per-query timeout (ms).
    #[arg(long, default_value_t = 2000)]
    get_timeout_ms: u64,
    /// Publisher congestion control. `block` (default) uses
    /// `CongestionControl::Block` which can hang a publisher
    /// indefinitely if any of its destinations is in a bad state
    /// (Phase J ghost-worker issue). `drop` uses
    /// `CongestionControl::Drop` which silently drops to bad
    /// destinations after `wait_before_drop` (configured via
    /// `transport/link/tx/queue/congestion_control/drop/wait_before_drop`,
    /// default 1 ms). Recommended for mass-production deployments
    /// where a single bad peer must not stall the system.
    #[arg(long, default_value = "block")]
    pub_congestion: String,
}

#[derive(ClapArgs, Clone, Debug)]
struct ChurnArgs {
    #[command(flatten)]
    node: NodeArgs,
    #[arg(long, default_value = "repro/routing/perf")]
    key: String,
    #[arg(long, default_value_t = 20)]
    hold_ms: u64,
    #[arg(long, default_value_t = 20)]
    idle_ms: u64,
}

#[derive(ClapArgs, Clone, Debug)]
struct SupervisorArgs {
    #[arg(long, default_value = "tcp/127.0.0.1:17447")]
    endpoint: String,
    #[arg(long, default_value = "peer")]
    hub_mode: String,
    #[arg(long, default_value = "peer")]
    leaf_mode: String,
    #[arg(long, default_value = "repro/routing/perf")]
    key: String,
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,
    #[arg(long, default_value_t = 1000)]
    startup_delay_ms: u64,
    #[arg(long, default_value_t = 20)]
    grace_secs: u64,
    #[arg(long, default_value_t = 0)]
    idle_peers: usize,
    #[arg(long, default_value_t = 1)]
    publishers: usize,
    #[arg(long, default_value_t = 0)]
    subscribers: usize,
    #[arg(long, default_value_t = 0)]
    churners: usize,
    #[arg(long, default_value_t = 64)]
    payload_size: usize,
    #[arg(long, default_value_t = 10)]
    put_period_ms: u64,
    #[arg(long, default_value_t = 20)]
    churn_hold_ms: u64,
    #[arg(long, default_value_t = 20)]
    churn_idle_ms: u64,
    /// Number of distinct topics. When > 1, publisher_i uses key
    /// `{key}/{i % topics}` and subscribers use the wildcard key `{key}/**`.
    /// Default 1: all publishers and subscribers share the exact `--key`.
    #[arg(long, default_value_t = 1)]
    topics: usize,
    /// Restart subscribers at this elapsed time (seconds, after startup).
    /// 0 disables. The restart kills `restart_count` random subscribers and
    /// respawns them; this is a one-shot event distinct from continuous
    /// `--churners`. Useful for measuring the cost of mid-stream session
    /// resurrection vs steady state.
    #[arg(long, default_value_t = 0)]
    restart_at_secs: u64,
    /// Number of subscribers to restart at `restart_at_secs`.
    #[arg(long, default_value_t = 0)]
    restart_count: usize,
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    wait_declares: bool,
    #[arg(long)]
    cfg: Vec<String>,
    /// Number of hub shards. With `--shards 1` (default) the topology is a
    /// single hub plus all leaves connecting to it (the historical flat
    /// mesh). With `--shards N > 1`, the supervisor spawns N hubs that
    /// listen on consecutive ports starting at `--endpoint`'s port. Hub
    /// `i` connects to all previous hubs to form a backbone. Non-hub
    /// roles (idle / publisher / subscriber / churn) are distributed
    /// round-robin across shards: child `idx` connects to hub
    /// `idx % shards`. This is the cheap "router-hybrid" shard prototype
    /// for Phase 3 — it does not change zenoh internals, only the
    /// connect topology, which is sufficient when `--hub-mode router
    /// --leaf-mode client` because router-to-router connections form a
    /// backbone naturally.
    #[arg(long, default_value_t = 1)]
    shards: usize,
    /// When `topics > 1`, controls whether subscribers use a wildcard
    /// expression covering all topics or are partitioned into disjoint
    /// per-topic groups. With `--split-subscribers false` (the default),
    /// every subscriber declares `{key}/**` and receives all topics —
    /// useful for measuring K (declares per peer) as a fanout dimension.
    /// With `--split-subscribers true`, subscriber `i` declares
    /// `{key}/{i % topics}` only — disjoint groups. Used by Phase 5
    /// dual-topic isolate experiments: a restart targets subscriber 0
    /// (group 0), and topic 1's subscribers measure mechanism 2
    /// (wtable contention) in isolation from mechanism 1 (the dying
    /// peer's pipeline drain).
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    split_subscribers: bool,
    /// Number of combined-role workers to spawn. Each worker is a single
    /// peer process hosting `n_pubs_per_worker` publishers,
    /// `n_subs_per_worker` subscribers, `n_queryables_per_worker`
    /// queryables, and `n_getters_per_worker` getter loops. Used by the
    /// `realistic-50` and related presets for distributed-entity tests.
    /// Coexists with `publishers`/`subscribers` — set those to 0 if you
    /// only want workers.
    #[arg(long, default_value_t = 0)]
    workers: usize,
    #[arg(long, default_value_t = 2)]
    n_pubs_per_worker: usize,
    #[arg(long, default_value_t = 2)]
    n_subs_per_worker: usize,
    #[arg(long, default_value_t = 2)]
    n_queryables_per_worker: usize,
    #[arg(long, default_value_t = 2)]
    n_getters_per_worker: usize,
    #[arg(long, default_value_t = 1000)]
    get_period_ms: u64,
    #[arg(long, default_value_t = 2000)]
    get_timeout_ms: u64,
    /// Per-worker publisher congestion control: "block" or "drop".
    /// Forwarded to each Worker as `--pub-congestion`. Default
    /// `block` preserves the existing test semantics; `drop` is
    /// recommended for mass-production deployments and prevents
    /// indefinite publisher hangs when a destination peer enters
    /// the Phase J ghost state.
    #[arg(long, default_value = "block")]
    pub_congestion: String,
    /// Milliseconds between consecutive worker process spawns.
    /// Default 0 spawns all workers back-to-back, which causes the
    /// Phase J ghost-worker race under 50-peer simultaneous opens.
    /// A small stagger (e.g. 50-100 ms) maps to a realistic device-
    /// boot scenario and reliably eliminates the ghost-worker risk
    /// — recommended for mass-production validation.
    #[arg(long, default_value_t = 0)]
    worker_spawn_stagger_ms: u64,
}

fn topic_key(base: &str, topics: usize, idx: usize) -> String {
    if topics <= 1 {
        base.to_string()
    } else {
        format!("{}/{}", base, idx % topics)
    }
}

fn subscriber_key(base: &str, topics: usize) -> String {
    if topics <= 1 {
        base.to_string()
    } else {
        format!("{}/**", base)
    }
}

fn main() {
    zenoh::init_log_from_env_or("error");
    let args = Args::parse();
    match args.command {
        Role::Supervisor(args) => run_supervisor(args),
        Role::Hub(args) => run_idle("hub", args),
        Role::Idle(args) => run_idle("idle", args),
        Role::Publisher(args) => run_publisher(args),
        Role::Subscriber(args) => run_subscriber(args),
        Role::Churn(args) => run_churn(args),
        Role::Worker(args) => run_worker(args),
    }
}

fn config(args: &NodeArgs) -> Config {
    let mut config = Config::default();
    config
        .insert_json5("mode", &json!(args.mode).to_string())
        .unwrap();
    config
        .insert_json5("scouting/multicast/enabled", &json!(false).to_string())
        .unwrap();
    config
        .insert_json5(
            "open/return_conditions/declares",
            &json!(args.wait_declares).to_string(),
        )
        .unwrap();

    // Always bind listen sockets on loopback only. The zenoh default for
    // peer mode is `tcp/[::]:0` (bind on all interfaces), which causes
    // peers to advertise their host IP via linkstate. Other peers then
    // try to connect to those WAN addresses, generating real network
    // traffic during a benchmark that should be 100% local. Forcing
    // 127.0.0.1 keeps the mesh on loopback regardless of peer count.
    let listen_endpoint = args
        .listen
        .clone()
        .unwrap_or_else(|| "tcp/127.0.0.1:0".to_string());
    config
        .insert_json5("listen/endpoints", &json!([listen_endpoint]).to_string())
        .unwrap();
    if !args.connect.is_empty() {
        config
            .insert_json5("connect/endpoints", &json!(args.connect).to_string())
            .unwrap();
    }
    for cfg in &args.cfg {
        let Some((key, value)) = cfg.split_once(':') else {
            panic!("expected KEY:VALUE in --cfg, got {cfg}");
        };
        config.insert_json5(key, value).unwrap();
    }
    config
}

fn run_idle(role: &str, args: NodeArgs) {
    let start = Instant::now();
    let _session = zenoh::open(config(&args)).wait().unwrap();
    println!(
        "metric role={role} index={} mode={} open_ms={}",
        args.index,
        args.mode,
        start.elapsed().as_millis()
    );
    thread::sleep(Duration::from_secs(args.duration_secs));
    std::process::exit(0);
}

fn run_publisher(args: PublisherArgs) {
    let open_start = Instant::now();
    let session = zenoh::open(config(&args.node)).wait().unwrap();
    let open_ms = open_start.elapsed().as_millis();

    let declare_start = Instant::now();
    let publisher = session
        .declare_publisher(&args.key)
        .congestion_control(CongestionControl::Block)
        .wait()
        .unwrap();
    println!(
        "metric role=publisher index={} mode={} open_ms={} declare_ms={}",
        args.node.index,
        args.node.mode,
        open_ms,
        declare_start.elapsed().as_millis()
    );

    let period = Duration::from_millis(args.put_period_ms);
    let end = Instant::now() + Duration::from_secs(args.node.duration_secs);
    let mut stats = PeriodStats::default();
    let mut last_print = Instant::now();
    while Instant::now() < end {
        let put_start = Instant::now();
        publisher.put(payload(args.payload_size)).wait().unwrap();
        stats.push(put_start.elapsed());
        if args.put_period_ms > 0 {
            thread::sleep(period);
        }
        if last_print.elapsed() >= Duration::from_secs(1) {
            stats.print("publisher_put", args.node.index, open_start.elapsed());
            last_print = Instant::now();
        }
    }
    stats.print("publisher_put_final", args.node.index, open_start.elapsed());
    std::process::exit(0);
}

fn run_subscriber(args: SubscriberArgs) {
    let open_start = Instant::now();
    let session = zenoh::open(config(&args.node)).wait().unwrap();
    let open_ms = open_start.elapsed().as_millis();

    let receive_start = Instant::now();
    let stats = Arc::new(Mutex::new(SubscriberStats::default()));
    let callback_stats = stats.clone();
    let index = args.node.index;
    let declare_start = Instant::now();
    session
        .declare_subscriber(&args.key)
        .callback(move |sample| {
            let mut stats = callback_stats.lock().unwrap();
            if stats.count == 0 {
                println!(
                    "metric role=subscriber index={index} first_sample_ms={}",
                    receive_start.elapsed().as_millis()
                );
            }
            stats.count += 1;
            if let Some(data_latency) = sent_time(sample.payload().to_bytes().as_ref()) {
                stats.latency.push(data_latency);
            }
        })
        .background()
        .wait()
        .unwrap();
    let declare_ms = declare_start.elapsed().as_millis();
    println!(
        "metric role=subscriber index={} mode={} open_ms={} declare_ms={}",
        args.node.index, args.node.mode, open_ms, declare_ms
    );

    let end = Instant::now() + Duration::from_secs(args.node.duration_secs);
    while Instant::now() < end {
        thread::sleep(Duration::from_secs(1));
        let mut stats = stats.lock().unwrap();
        println!(
            "metric role=subscriber_rx index={} elapsed_ms={} count={} rate={:.1}",
            args.node.index,
            receive_start.elapsed().as_millis(),
            stats.count,
            stats.count as f64 / receive_start.elapsed().as_secs_f64()
        );
        stats.latency.print(
            "subscriber_latency",
            args.node.index,
            receive_start.elapsed(),
        );
    }

    let stats = stats.lock().unwrap();
    println!(
        "metric role=subscriber_rx_final index={} count={} rate={:.1}",
        args.node.index,
        stats.count,
        stats.count as f64 / receive_start.elapsed().as_secs_f64()
    );
    std::process::exit(0);
}

fn run_churn(args: ChurnArgs) {
    let end = Instant::now() + Duration::from_secs(args.node.duration_secs);
    let mut cycles = 0u64;
    let mut open_stats = PeriodStats::default();
    let mut declare_stats = PeriodStats::default();
    let mut close_stats = PeriodStats::default();
    let mut last_print = Instant::now();
    while Instant::now() < end {
        let open_start = Instant::now();
        let session = match zenoh::open(config(&args.node)).wait() {
            Ok(session) => session,
            Err(_) => {
                println!(
                    "metric role=churn_open_error index={} error=open_failed",
                    args.node.index
                );
                break;
            }
        };
        let open_elapsed = open_start.elapsed();
        let declare_start = Instant::now();
        // Use a draining callback subscriber instead of the default pull-mode
        // channel so received messages don't pile up in an unbounded queue
        // during the hold window. Without this, soak runs at 100 msg/sec ×
        // 5 s hold = ~500 buffered messages per cycle, which can swell the
        // churner process RSS by hundreds of MB over a few minutes.
        let subscriber = session
            .declare_subscriber(&args.key)
            .callback(|_sample| {})
            .wait()
            .unwrap();
        let declare_elapsed = declare_start.elapsed();
        thread::sleep(Duration::from_millis(args.hold_ms));
        subscriber.undeclare().wait().unwrap();
        let close_start = Instant::now();
        if session.close().wait().is_err() {
            println!(
                "metric role=churn_close_error index={} error=close_failed",
                args.node.index
            );
        }
        close_stats.push(close_start.elapsed());
        open_stats.push(open_elapsed);
        declare_stats.push(declare_elapsed);
        cycles += 1;
        if last_print.elapsed() >= Duration::from_secs(1) {
            println!(
                "metric role=churn index={} cycles={cycles}",
                args.node.index
            );
            open_stats.print("churn_open", args.node.index, last_print.elapsed());
            declare_stats.print("churn_declare", args.node.index, last_print.elapsed());
            close_stats.print("churn_close", args.node.index, last_print.elapsed());
            last_print = Instant::now();
        }
        thread::sleep(Duration::from_millis(args.idle_ms));
    }
    println!(
        "metric role=churn_final index={} cycles={cycles}",
        args.node.index
    );
    open_stats.print("churn_open_final", args.node.index, Duration::ZERO);
    declare_stats.print("churn_declare_final", args.node.index, Duration::ZERO);
    close_stats.print("churn_close_final", args.node.index, Duration::ZERO);
    std::process::exit(0);
}

fn run_worker(args: WorkerArgs) {
    let open_start = Instant::now();
    let session = zenoh::open(config(&args.node)).wait().unwrap();
    let open_ms = open_start.elapsed().as_millis();

    let total_queryables = (args.total_workers * args.n_queryables).max(1);

    // Publishers: one declare per replica, keys {key}/topic/{global_idx}.
    let cc = match args.pub_congestion.as_str() {
        "drop" => CongestionControl::Drop,
        "block" => CongestionControl::Block,
        other => panic!("--pub-congestion must be 'block' or 'drop', got '{other}'"),
    };
    let mut publishers = Vec::with_capacity(args.n_publishers);
    let declare_pubs_start = Instant::now();
    for i in 0..args.n_publishers {
        let g = args.worker_index * args.n_publishers + i;
        let topic_key = format!("{}/topic/{}", args.key, g);
        let p = session
            .declare_publisher(topic_key)
            .congestion_control(cc)
            .wait()
            .unwrap();
        publishers.push(p);
    }
    let declare_pubs_ms = declare_pubs_start.elapsed().as_millis();

    // Subscribers: every replica wildcard-subscribes to {key}/topic/** so
    // each subscriber sees traffic from all publishers in the system.
    let sub_stats = Arc::new(Mutex::new(SubscriberStats::default()));
    let declare_subs_start = Instant::now();
    let sub_wildcard = format!("{}/topic/**", args.key);
    let mut sub_handles = Vec::with_capacity(args.n_subscribers);
    for _i in 0..args.n_subscribers {
        let stats = sub_stats.clone();
        let receive_start = Instant::now();
        let recvd_first = Arc::new(Mutex::new(false));
        let recvd_first_cb = recvd_first.clone();
        let worker_index = args.worker_index;
        let sub = session
            .declare_subscriber(sub_wildcard.clone())
            .callback(move |sample| {
                let mut stats = stats.lock().unwrap();
                let mut first = recvd_first_cb.lock().unwrap();
                if !*first {
                    println!(
                        "metric role=worker_sub_first index={worker_index} first_sample_ms={}",
                        receive_start.elapsed().as_millis()
                    );
                    *first = true;
                }
                stats.count += 1;
                if let Some(latency) = sent_time(sample.payload().to_bytes().as_ref()) {
                    stats.latency.push(latency);
                }
            })
            .background()
            .wait()
            .unwrap();
        sub_handles.push(sub);
    }
    let declare_subs_ms = declare_subs_start.elapsed().as_millis();

    // Queryables: callback-style reply with a fixed payload of `payload_size`.
    let declare_qbls_start = Instant::now();
    let qbl_count = Arc::new(Mutex::new(0u64));
    let mut qbl_handles = Vec::with_capacity(args.n_queryables);
    for i in 0..args.n_queryables {
        let g = args.worker_index * args.n_queryables + i;
        let svc_key = format!("{}/svc/{}", args.key, g);
        let payload_size = args.payload_size;
        let count = qbl_count.clone();
        let reply_key = svc_key.clone();
        let q = session
            .declare_queryable(svc_key)
            .callback(move |query| {
                let _ = query
                    .reply(reply_key.clone(), payload(payload_size))
                    .wait();
                *count.lock().unwrap() += 1;
            })
            .background()
            .wait()
            .unwrap();
        qbl_handles.push(q);
    }
    let declare_qbls_ms = declare_qbls_start.elapsed().as_millis();

    println!(
        "metric role=worker index={} mode={} open_ms={open_ms} \
         declare_pubs_ms={declare_pubs_ms} declare_subs_ms={declare_subs_ms} \
         declare_qbls_ms={declare_qbls_ms} n_publishers={} n_subscribers={} \
         n_queryables={} n_getters={} total_queryables={total_queryables}",
        args.worker_index,
        args.node.mode,
        args.n_publishers,
        args.n_subscribers,
        args.n_queryables,
        args.n_getters,
    );

    let end = Instant::now() + Duration::from_secs(args.node.duration_secs);
    let put_period = Duration::from_millis(args.put_period_ms);
    let get_period = Duration::from_millis(args.get_period_ms);
    let get_timeout = Duration::from_millis(args.get_timeout_ms);
    let payload_size = args.payload_size;
    let worker_index = args.worker_index;

    // Spawn one thread per getter. Each one targets a fixed service key
    // (global getter index modulo total_queryables) and loops:
    // get -> drain replies until end of replies -> sleep get_period.
    let getter_stats = Arc::new(Mutex::new(GetterStats::default()));
    let mut getter_handles = Vec::with_capacity(args.n_getters);
    for i in 0..args.n_getters {
        let g = args.worker_index * args.n_getters + i;
        let target_svc_idx = g % total_queryables;
        let svc_key = format!("{}/svc/{}", args.key, target_svc_idx);
        let session = session.clone();
        let stats = getter_stats.clone();
        let handle = thread::spawn(move || {
            loop {
                let issued = Instant::now();
                if issued >= end {
                    break;
                }
                let r = session
                    .get(&svc_key)
                    .timeout(get_timeout)
                    .payload(payload(payload_size))
                    .wait();
                let mut s = stats.lock().unwrap();
                s.sent += 1;
                match r {
                    Ok(replies) => {
                        // First reply only — we just need to measure
                        // round-trip and confirm at least one reply.
                        let mut got_any = false;
                        // Use blocking recv with the same timeout budget;
                        // any further reply queueing isn't relevant to
                        // service-quality metric.
                        let deadline = issued + get_timeout;
                        loop {
                            let now = Instant::now();
                            if now >= deadline {
                                break;
                            }
                            match replies.recv_timeout(deadline - now) {
                                Ok(Some(reply)) => {
                                    got_any = true;
                                    if reply.result().is_ok() {
                                        s.success += 1;
                                        s.latency.push(issued.elapsed());
                                    } else {
                                        s.reply_errors += 1;
                                    }
                                    break;
                                }
                                Ok(None) => break,
                                Err(_) => break,
                            }
                        }
                        if !got_any {
                            s.timeouts += 1;
                        }
                    }
                    Err(_) => {
                        s.errors += 1;
                    }
                }
                drop(s);
                let sleep_until = issued + get_period;
                let now = Instant::now();
                if sleep_until > now {
                    thread::sleep(sleep_until - now);
                }
            }
            let _ = worker_index; // silence in case used in future logging
        });
        getter_handles.push(handle);
    }

    // Publisher tick: round-robin put across our n_publishers at put_period
    // cadence per publisher. We send one put on each publisher then sleep
    // put_period — that gives 1 msg/s/publisher with put_period_ms=1000.
    let pub_stats = Arc::new(Mutex::new(PeriodStats::default()));
    let pub_thread = {
        let publishers_ref = publishers;
        let pub_stats = pub_stats.clone();
        thread::spawn(move || {
            let mut next_tick = Instant::now();
            while Instant::now() < end {
                next_tick += put_period;
                for p in &publishers_ref {
                    let t = Instant::now();
                    let _ = p.put(payload(payload_size)).wait();
                    pub_stats.lock().unwrap().push(t.elapsed());
                }
                let now = Instant::now();
                if next_tick > now {
                    thread::sleep(next_tick - now);
                }
            }
        })
    };

    // Periodic status. We print 1/sec so the analyzer can compute per-second
    // throughput, latency, query success/timeout rates.
    let mut last_print = Instant::now();
    while Instant::now() < end {
        thread::sleep(Duration::from_millis(200));
        if last_print.elapsed() >= Duration::from_secs(1) {
            let mut sub = sub_stats.lock().unwrap();
            println!(
                "metric role=worker_sub_rx index={worker_index} count={} rate={:.1}",
                sub.count,
                sub.count as f64 / open_start.elapsed().as_secs_f64()
            );
            sub.latency
                .print("worker_sub_latency", worker_index, open_start.elapsed());

            let mut g = getter_stats.lock().unwrap();
            println!(
                "metric role=worker_get index={worker_index} sent={} success={} \
                 timeouts={} reply_errors={} errors={}",
                g.sent, g.success, g.timeouts, g.reply_errors, g.errors
            );
            g.latency
                .print("worker_get_latency", worker_index, open_start.elapsed());
            drop(g);

            let qcount = *qbl_count.lock().unwrap();
            println!("metric role=worker_qbl_handled index={worker_index} count={qcount}");

            pub_stats
                .lock()
                .unwrap()
                .print("worker_pub_put", worker_index, open_start.elapsed());
            last_print = Instant::now();
        }
    }

    // Wait for all getter threads to finish (they observe `end`).
    for h in getter_handles {
        let _ = h.join();
    }
    let _ = pub_thread.join();

    let sub_total = sub_stats.lock().unwrap();
    let getter_total = getter_stats.lock().unwrap();
    let qcount = *qbl_count.lock().unwrap();
    println!(
        "metric role=worker_final index={worker_index} \
         sub_count={} get_sent={} get_success={} get_timeouts={} \
         get_reply_errors={} get_errors={} qbl_handled={qcount}",
        sub_total.count,
        getter_total.sent,
        getter_total.success,
        getter_total.timeouts,
        getter_total.reply_errors,
        getter_total.errors
    );
    std::process::exit(0);
}

fn run_supervisor(args: SupervisorArgs) {
    let support_duration_secs = args.duration_secs + ((args.startup_delay_ms * 2 + 999) / 1000) + 1;
    let shards = args.shards.max(1);
    let hub_endpoints: Vec<String> = (0..shards).map(|i| bump_port(&args.endpoint, i)).collect();
    let mut children = Vec::new();

    // Spawn hubs. Hub 0 listens on the base endpoint and connects nowhere;
    // hub i listens on the i-th endpoint and connects back to all earlier
    // hubs, forming a complete backbone among hubs. For shards==1 this
    // collapses to the original single-hub topology.
    for s in 0..shards {
        let mut hub_argv: Vec<String> = vec![
            "hub".into(),
            "--mode".into(),
            args.hub_mode.clone(),
            "--listen".into(),
            hub_endpoints[s].clone(),
            "--duration-secs".into(),
            support_duration_secs.to_string(),
            "--index".into(),
            s.to_string(),
            "--wait-declares".into(),
            args.wait_declares.to_string(),
        ];
        for prev in &hub_endpoints[..s] {
            hub_argv.push("--connect".into());
            hub_argv.push(prev.clone());
        }
        let hub_argv_refs: Vec<&str> = hub_argv.iter().map(String::as_str).collect();
        children.push(spawn("hub", s, &hub_argv_refs, &args.cfg));
    }
    thread::sleep(Duration::from_millis(args.startup_delay_ms));

    let hub_for = |idx: usize| hub_endpoints[idx % shards].as_str();

    for idx in 0..args.idle_peers {
        children.push(spawn(
            "idle",
            idx,
            &[
                "idle",
                "--mode",
                &args.leaf_mode,
                "--connect",
                hub_for(idx),
                "--duration-secs",
                &support_duration_secs.to_string(),
                "--index",
                &idx.to_string(),
                "--wait-declares",
                &args.wait_declares.to_string(),
            ],
            &args.cfg,
        ));
    }

    for idx in 0..args.publishers {
        let pub_key = topic_key(&args.key, args.topics, idx);
        children.push(spawn(
            "publisher",
            idx,
            &[
                "publisher",
                "--mode",
                &args.leaf_mode,
                "--connect",
                hub_for(idx),
                "--duration-secs",
                &support_duration_secs.to_string(),
                "--index",
                &idx.to_string(),
                "--key",
                &pub_key,
                "--payload-size",
                &args.payload_size.to_string(),
                "--put-period-ms",
                &args.put_period_ms.to_string(),
                "--wait-declares",
                &args.wait_declares.to_string(),
            ],
            &args.cfg,
        ));
    }

    thread::sleep(Duration::from_millis(args.startup_delay_ms));

    // Resolve subscriber key per index. When `--split-subscribers` is
    // false this collapses to the historical behaviour
    // (`subscriber_key`, which is `{key}/**` for topics>1). When true,
    // subscriber `idx` is pinned to topic `idx % topics` — used by the
    // dual-topic isolate experiments.
    let sub_key_for = |idx: usize| -> String {
        if args.split_subscribers && args.topics > 1 {
            topic_key(&args.key, args.topics, idx)
        } else {
            subscriber_key(&args.key, args.topics)
        }
    };
    let mut subscriber_slots: Vec<Option<usize>> = (0..args.subscribers).map(Some).collect();
    for idx in 0..args.subscribers {
        let slot_idx = children.len();
        let sub_key = sub_key_for(idx);
        children.push(spawn(
            "subscriber",
            idx,
            &[
                "subscriber",
                "--mode",
                &args.leaf_mode,
                "--connect",
                hub_for(idx),
                "--duration-secs",
                &args.duration_secs.to_string(),
                "--index",
                &idx.to_string(),
                "--key",
                &sub_key,
                "--wait-declares",
                &args.wait_declares.to_string(),
            ],
            &args.cfg,
        ));
        // Track slot index in `children` for restart bookkeeping.
        if let Some(slot) = subscriber_slots.get_mut(idx) {
            *slot = Some(slot_idx);
        }
    }

    for idx in 0..args.workers {
        if idx > 0 && args.worker_spawn_stagger_ms > 0 {
            thread::sleep(Duration::from_millis(args.worker_spawn_stagger_ms));
        }
        children.push(spawn(
            "worker",
            idx,
            &[
                "worker",
                "--mode",
                &args.leaf_mode,
                "--connect",
                hub_for(idx),
                "--duration-secs",
                &args.duration_secs.to_string(),
                "--index",
                &idx.to_string(),
                "--worker-index",
                &idx.to_string(),
                "--total-workers",
                &args.workers.to_string(),
                "--key",
                &args.key,
                "--n-publishers",
                &args.n_pubs_per_worker.to_string(),
                "--n-subscribers",
                &args.n_subs_per_worker.to_string(),
                "--n-queryables",
                &args.n_queryables_per_worker.to_string(),
                "--n-getters",
                &args.n_getters_per_worker.to_string(),
                "--payload-size",
                &args.payload_size.to_string(),
                "--put-period-ms",
                &args.put_period_ms.to_string(),
                "--get-period-ms",
                &args.get_period_ms.to_string(),
                "--get-timeout-ms",
                &args.get_timeout_ms.to_string(),
                "--pub-congestion",
                &args.pub_congestion,
                "--wait-declares",
                &args.wait_declares.to_string(),
            ],
            &args.cfg,
        ));
    }

    for idx in 0..args.churners {
        children.push(spawn(
            "churn",
            idx,
            &[
                "churn",
                "--mode",
                &args.leaf_mode,
                "--connect",
                hub_for(idx),
                "--duration-secs",
                &args.duration_secs.to_string(),
                "--index",
                &idx.to_string(),
                "--key",
                &args.key,
                "--hold-ms",
                &args.churn_hold_ms.to_string(),
                "--idle-ms",
                &args.churn_idle_ms.to_string(),
                "--wait-declares",
                &args.wait_declares.to_string(),
            ],
            &args.cfg,
        ));
    }

    let test_start = Instant::now();
    let deadline = test_start + Duration::from_secs(args.duration_secs + args.grace_secs);
    let mut restart_done = args.restart_at_secs == 0 || args.restart_count == 0;
    loop {
        // Mid-stream subscriber restart: at restart_at_secs after test start,
        // kill the first restart_count subscribers and respawn them with the
        // same role/index. Distinct from continuous churn — the restart is
        // a single event and the new sessions live for the rest of the run.
        if !restart_done && test_start.elapsed().as_secs() >= args.restart_at_secs {
            let n = args.restart_count.min(args.subscribers);
            println!(
                "metric role=restart_event count={n} elapsed_ms={}",
                test_start.elapsed().as_millis()
            );
            let mut new_slots: Vec<(usize, usize)> = Vec::new();
            for sub_idx in 0..n {
                if let Some(Some(slot)) = subscriber_slots.get(sub_idx) {
                    let slot = *slot;
                    if let Some(child) = children.get_mut(slot) {
                        if !child.finished {
                            let _ = child.child.kill();
                            let _ = child.child.wait();
                            child.finished = true;
                            println!(
                                "metric role=restart_killed target_role=subscriber index={sub_idx}"
                            );
                        }
                    }
                    let remaining = args.duration_secs.saturating_sub(args.restart_at_secs).max(1);
                    let respawn_key = sub_key_for(sub_idx);
                    let new_slot = spawn(
                        "subscriber",
                        sub_idx,
                        &[
                            "subscriber",
                            "--mode",
                            &args.leaf_mode,
                            "--connect",
                            hub_for(sub_idx),
                            "--duration-secs",
                            &remaining.to_string(),
                            "--index",
                            &sub_idx.to_string(),
                            "--key",
                            &respawn_key,
                            "--wait-declares",
                            &args.wait_declares.to_string(),
                        ],
                        &args.cfg,
                    );
                    new_slots.push((sub_idx, children.len()));
                    children.push(new_slot);
                }
            }
            for (sub_idx, slot) in new_slots {
                if let Some(s) = subscriber_slots.get_mut(sub_idx) {
                    *s = Some(slot);
                }
            }
            restart_done = true;
        }

        let mut running = 0usize;
        for child in &mut children {
            if child.finished {
                continue;
            }
            match child.child.try_wait() {
                Ok(Some(status)) => {
                    child.finished = true;
                    println!(
                        "metric role=child_exit target_role={} index={} success={} code={}",
                        child.role,
                        child.index,
                        status.success(),
                        status.code().unwrap_or(-1)
                    );
                }
                Ok(None) => running += 1,
                Err(_error) => {
                    child.finished = true;
                    println!(
                        "metric role=child_wait_error target_role={} index={} error=wait_failed",
                        child.role, child.index
                    );
                }
            }
        }
        if running == 0 {
            break;
        }
        if Instant::now() >= deadline {
            for child in &mut children {
                if !child.finished {
                    println!(
                        "metric role=child_timeout target_role={} index={}",
                        child.role, child.index
                    );
                    let _ = child.child.kill();
                    let _ = child.child.wait();
                    child.finished = true;
                }
            }
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
}

struct ChildSlot {
    role: String,
    index: usize,
    child: Child,
    finished: bool,
}

/// Returns the endpoint with its port bumped by `delta`. Used to derive
/// per-shard hub endpoints from a single base endpoint flag. Accepts
/// `tcp/host:port` (with or without `tcp/` prefix). Panics on
/// unrecognised input — callers are configuration code, not user-facing.
fn bump_port(endpoint: &str, delta: usize) -> String {
    if delta == 0 {
        return endpoint.to_string();
    }
    let (scheme, rest) = match endpoint.split_once('/') {
        Some((s, r)) => (Some(s), r),
        None => (None, endpoint),
    };
    let (host, port) = rest.rsplit_once(':').unwrap_or_else(|| {
        panic!("endpoint missing `:port` for shard expansion: {endpoint}")
    });
    let port: u16 = port
        .parse()
        .unwrap_or_else(|_| panic!("endpoint port not a number: {endpoint}"));
    let new_port = port
        .checked_add(delta as u16)
        .expect("shard endpoint port overflow");
    match scheme {
        Some(s) => format!("{s}/{host}:{new_port}"),
        None => format!("{host}:{new_port}"),
    }
}

fn spawn(role: &str, index: usize, base_args: &[&str], cfg: &[String]) -> ChildSlot {
    let exe = env::current_exe().unwrap();
    let mut cmd = Command::new(exe);
    cmd.args(base_args);
    for item in cfg {
        cmd.arg("--cfg").arg(item);
    }
    cmd.stdout(Stdio::inherit());
    cmd.stderr(Stdio::inherit());
    let child = cmd.spawn().unwrap();
    println!("spawned role={role} pid={}", child.id());
    ChildSlot {
        role: role.to_string(),
        index,
        child,
        finished: false,
    }
}

fn payload(size: usize) -> ZBytes {
    let mut payload = vec![0u8; size.max(16)];
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    payload[..16].copy_from_slice(&now.to_le_bytes());
    payload.into()
}

fn sent_time(payload: &[u8]) -> Option<Duration> {
    if payload.len() < 16 {
        return None;
    }
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&payload[..16]);
    let sent = u128::from_le_bytes(bytes);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_micros();
    now.checked_sub(sent)
        .and_then(|micros| u64::try_from(micros).ok())
        .map(Duration::from_micros)
}

#[derive(Default)]
struct PeriodStats {
    samples: Vec<u128>,
}

#[derive(Default)]
struct SubscriberStats {
    count: u64,
    latency: PeriodStats,
}

#[derive(Default)]
struct GetterStats {
    sent: u64,
    success: u64,
    timeouts: u64,
    reply_errors: u64,
    errors: u64,
    latency: PeriodStats,
}

impl PeriodStats {
    fn push(&mut self, value: Duration) {
        self.samples.push(value.as_micros());
    }

    fn print(&mut self, name: &str, index: usize, elapsed: Duration) {
        if self.samples.is_empty() {
            return;
        }
        self.samples.sort_unstable();
        let count = self.samples.len();
        let sum: u128 = self.samples.iter().sum();
        println!(
            "metric name={name} index={index} elapsed_ms={} count={count} avg_us={} p50_us={} p95_us={} p99_us={} max_us={}",
            elapsed.as_millis(),
            sum / count as u128,
            percentile(&self.samples, 50),
            percentile(&self.samples, 95),
            percentile(&self.samples, 99),
            self.samples[count - 1],
        );
        self.samples.clear();
    }
}

fn percentile(samples: &[u128], percentile: usize) -> u128 {
    let idx = samples.len().saturating_sub(1) * percentile / 100;
    samples[idx]
}
