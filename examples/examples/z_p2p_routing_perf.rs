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
}

#[derive(ClapArgs, Clone, Debug)]
struct NodeArgs {
    #[arg(long, default_value = "peer")]
    mode: String,
    #[arg(long)]
    listen: Option<String>,
    #[arg(long)]
    connect: Option<String>,
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
    if let Some(endpoint) = &args.connect {
        config
            .insert_json5("connect/endpoints", &json!([endpoint]).to_string())
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
        let subscriber = session.declare_subscriber(&args.key).wait().unwrap();
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

fn run_supervisor(args: SupervisorArgs) {
    let support_duration_secs = args.duration_secs + ((args.startup_delay_ms * 2 + 999) / 1000) + 1;
    let mut children = Vec::new();
    children.push(spawn(
        "hub",
        0,
        &[
            "hub",
            "--mode",
            &args.hub_mode,
            "--listen",
            &args.endpoint,
            "--duration-secs",
            &support_duration_secs.to_string(),
            "--wait-declares",
            &args.wait_declares.to_string(),
        ],
        &args.cfg,
    ));
    thread::sleep(Duration::from_millis(args.startup_delay_ms));

    for idx in 0..args.idle_peers {
        children.push(spawn(
            "idle",
            idx,
            &[
                "idle",
                "--mode",
                &args.leaf_mode,
                "--connect",
                &args.endpoint,
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
                &args.endpoint,
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

    let sub_key = subscriber_key(&args.key, args.topics);
    let mut subscriber_slots: Vec<Option<usize>> = (0..args.subscribers).map(Some).collect();
    for idx in 0..args.subscribers {
        let slot_idx = children.len();
        children.push(spawn(
            "subscriber",
            idx,
            &[
                "subscriber",
                "--mode",
                &args.leaf_mode,
                "--connect",
                &args.endpoint,
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

    for idx in 0..args.churners {
        children.push(spawn(
            "churn",
            idx,
            &[
                "churn",
                "--mode",
                &args.leaf_mode,
                "--connect",
                &args.endpoint,
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
                    let new_slot = spawn(
                        "subscriber",
                        sub_idx,
                        &[
                            "subscriber",
                            "--mode",
                            &args.leaf_mode,
                            "--connect",
                            &args.endpoint,
                            "--duration-secs",
                            &remaining.to_string(),
                            "--index",
                            &sub_idx.to_string(),
                            "--key",
                            &sub_key,
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
