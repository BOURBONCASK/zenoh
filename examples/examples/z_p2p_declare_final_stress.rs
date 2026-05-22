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
    collections::HashMap,
    env,
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver, RecvTimeoutError, Sender},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use clap::{Parser, ValueEnum};
use zenoh::{bytes::ZBytes, Config, Wait};

const REPRO_KEY: &str = "repro/deadlock";

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum Role {
    Supervisor,
    Router,
    Publisher,
    Churn,
    InProcess,
}

impl Role {
    fn as_arg(self) -> &'static str {
        match self {
            Self::Supervisor => "supervisor",
            Self::Router => "router",
            Self::Publisher => "publisher",
            Self::Churn => "churn",
            Self::InProcess => "in-process",
        }
    }
}

#[derive(Parser, Clone, Debug)]
struct Args {
    /// Internal worker role. The default supervisor launches router, publisher, and churn workers.
    #[arg(long, value_enum, default_value = "supervisor", hide = true)]
    role: Role,
    /// Internal worker index assigned by the supervisor.
    #[arg(long, default_value_t = 0, hide = true)]
    worker_index: usize,
    /// Router endpoint used by all peer processes.
    #[arg(long, default_value = "tcp/127.0.0.1:17447")]
    router_endpoint: String,
    /// Time to wait for the router process to fail fast before starting peer processes.
    #[arg(long, default_value_t = 1000)]
    router_startup_ms: u64,
    /// Number of publisher worker processes.
    #[arg(long, default_value_t = 1)]
    publisher_processes: usize,
    /// Number of stable publisher sessions created in each publisher process.
    #[arg(long, default_value_t = 1)]
    publisher_sessions_per_process: usize,
    /// Number of churn worker processes.
    #[arg(long, default_value_t = 4)]
    churn_processes: usize,
    /// Number of churn sessions created in each churn process.
    #[arg(long, default_value_t = 2)]
    churn_sessions_per_process: usize,
    /// Payload size used by the publisher sessions.
    #[arg(long, default_value_t = 256)]
    payload_size: usize,
    /// Delay between synchronous puts from each publisher session.
    #[arg(long, default_value_t = 10)]
    put_period_ms: u64,
    /// How long a churn session keeps its subscriber before closing the session.
    #[arg(long, default_value_t = 20)]
    churn_hold_ms: u64,
    /// How long a churn session waits before opening the next session.
    #[arg(long, default_value_t = 20)]
    churn_idle_ms: u64,
    /// Backoff after a churn session fails to open or declare.
    #[arg(long, default_value_t = 250)]
    churn_error_backoff_ms: u64,
    /// Watchdog timeout. If a publisher session stops completing put(), the process parks for debugger attach.
    #[arg(long, default_value_t = 5)]
    stall_after_secs: u64,
    /// Print a slow put line when synchronous put().wait() takes at least this many milliseconds.
    #[arg(long, default_value_t = 500)]
    slow_put_threshold_ms: u64,
    /// Park the publisher process for debugger attach when put().wait() takes at least this many milliseconds.
    #[arg(long)]
    park_on_slow_put_ms: Option<u64>,
    /// Configure routing/interests/timeout in milliseconds.
    #[arg(long, default_value_t = 10000)]
    interests_timeout_ms: u64,
    /// Configure queries_default_timeout in milliseconds, matching the VITA ROS setting by default.
    #[arg(long, default_value_t = 600000)]
    queries_default_timeout_ms: u64,
    /// Send peer gossip only to routers. By default the peer target matches the current S100 config default.
    #[arg(long)]
    peer_gossip_target_router_only: bool,
    /// Use linkstate peer routing instead of peer_to_peer.
    #[arg(long)]
    linkstate: bool,
    /// Exit with code 2 on stall instead of parking for debugger attach.
    #[arg(long)]
    exit_on_stall: bool,
    /// Stop after this many seconds. By default the stress run continues until a stall is detected.
    #[arg(long)]
    max_runtime_secs: Option<u64>,
}

#[derive(Debug)]
enum Event {
    PublisherStarted {
        worker: usize,
    },
    PutStarted {
        worker: usize,
        seq: u64,
    },
    PutCompleted {
        worker: usize,
        seq: u64,
        elapsed: Duration,
    },
    ChurnCompleted {
        worker: usize,
        cycle: u64,
        elapsed: Duration,
    },
    Error {
        worker: usize,
        context: &'static str,
        message: String,
    },
}

struct ChildHandle {
    role: Role,
    index: usize,
    child: Child,
}

fn main() {
    zenoh::init_log_from_env_or("off");

    let args = Args::parse();

    match args.role {
        Role::Supervisor => run_supervisor(args),
        Role::Router => run_router(args),
        Role::Publisher => run_publisher_process(args),
        Role::Churn => run_churn_process(args),
        Role::InProcess => run_in_process(args),
    }
}

fn run_supervisor(args: Args) {
    println!(
        "[role=supervisor pid={} ts_ms={}] starting multi-process stress: router={} publisher_processes={} publisher_sessions_per_process={} churn_processes={} churn_sessions_per_process={} key={}",
        std::process::id(),
        now_ms(),
        args.router_endpoint,
        args.publisher_processes,
        args.publisher_sessions_per_process,
        args.churn_processes,
        args.churn_sessions_per_process,
        REPRO_KEY,
    );
    println!(
        "[role=supervisor pid={} ts_ms={}] publisher workers keep stable peer sessions and repeatedly call put().wait(); churn workers repeatedly open peer sessions, declare subscribers, then drop subscriber/session",
        std::process::id(),
        now_ms(),
    );

    let mut children = Vec::new();
    children.push(spawn_child(Role::Router, 0, &args));
    thread::sleep(Duration::from_millis(args.router_startup_ms));

    if let Some(status) = children[0]
        .child
        .try_wait()
        .expect("poll router process after startup")
    {
        eprintln!(
            "[role=supervisor pid={} ts_ms={}] router child pid={} exited during startup with {status}; aborting stress run",
            std::process::id(),
            now_ms(),
            children[0].child.id(),
        );
        std::process::exit(1);
    }

    for index in 0..args.publisher_processes {
        children.push(spawn_child(Role::Publisher, index, &args));
    }
    for index in 0..args.churn_processes {
        children.push(spawn_child(Role::Churn, index, &args));
    }

    if let Some(max_runtime_secs) = args.max_runtime_secs {
        thread::sleep(Duration::from_secs(max_runtime_secs));
        terminate_children(children);
        println!("completed requested runtime in supervisor: {max_runtime_secs}s");
        return;
    }

    loop {
        let mut index = 0;
        while index < children.len() {
            match children[index].child.try_wait() {
                Ok(Some(status)) => {
                    eprintln!(
                        "[role=supervisor pid={} ts_ms={}] child role={} index={} pid={} exited with {status}",
                        std::process::id(),
                        now_ms(),
                        children[index].role.as_arg(),
                        children[index].index,
                        children[index].child.id()
                    );
                    children.remove(index);
                }
                Ok(None) => index += 1,
                Err(err) => {
                    eprintln!(
                        "[role=supervisor pid={} ts_ms={}] failed to poll child role={} index={} pid={}: {err}",
                        std::process::id(),
                        now_ms(),
                        children[index].role.as_arg(),
                        children[index].index,
                        children[index].child.id()
                    );
                    index += 1;
                }
            }
        }

        if children.is_empty() {
            return;
        }
        thread::sleep(Duration::from_secs(1));
    }
}

fn spawn_child(role: Role, index: usize, args: &Args) -> ChildHandle {
    let exe = env::current_exe().expect("current executable path");
    let mut command = Command::new(exe);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .arg("--role")
        .arg(role.as_arg())
        .arg("--worker-index")
        .arg(index.to_string());
    append_common_args(&mut command, args);

    let child = command
        .spawn()
        .unwrap_or_else(|err| panic!("failed to spawn {:?} process {index}: {err}", role));
    println!(
        "[role=supervisor pid={} ts_ms={}] spawned child role={} index={index} pid={}",
        std::process::id(),
        now_ms(),
        role.as_arg(),
        child.id()
    );

    ChildHandle { role, index, child }
}

fn append_common_args(command: &mut Command, args: &Args) {
    command
        .arg("--router-endpoint")
        .arg(&args.router_endpoint)
        .arg("--router-startup-ms")
        .arg(args.router_startup_ms.to_string())
        .arg("--publisher-processes")
        .arg(args.publisher_processes.to_string())
        .arg("--publisher-sessions-per-process")
        .arg(args.publisher_sessions_per_process.to_string())
        .arg("--churn-processes")
        .arg(args.churn_processes.to_string())
        .arg("--churn-sessions-per-process")
        .arg(args.churn_sessions_per_process.to_string())
        .arg("--payload-size")
        .arg(args.payload_size.to_string())
        .arg("--put-period-ms")
        .arg(args.put_period_ms.to_string())
        .arg("--churn-hold-ms")
        .arg(args.churn_hold_ms.to_string())
        .arg("--churn-idle-ms")
        .arg(args.churn_idle_ms.to_string())
        .arg("--churn-error-backoff-ms")
        .arg(args.churn_error_backoff_ms.to_string())
        .arg("--stall-after-secs")
        .arg(args.stall_after_secs.to_string())
        .arg("--slow-put-threshold-ms")
        .arg(args.slow_put_threshold_ms.to_string())
        .arg("--interests-timeout-ms")
        .arg(args.interests_timeout_ms.to_string())
        .arg("--queries-default-timeout-ms")
        .arg(args.queries_default_timeout_ms.to_string());

    if args.peer_gossip_target_router_only {
        command.arg("--peer-gossip-target-router-only");
    }
    if args.linkstate {
        command.arg("--linkstate");
    }
    if args.exit_on_stall {
        command.arg("--exit-on-stall");
    }
    if let Some(park_on_slow_put_ms) = args.park_on_slow_put_ms {
        command
            .arg("--park-on-slow-put-ms")
            .arg(park_on_slow_put_ms.to_string());
    }
    if let Some(max_runtime_secs) = args.max_runtime_secs {
        command
            .arg("--max-runtime-secs")
            .arg(max_runtime_secs.to_string());
    }
}

fn terminate_children(mut children: Vec<ChildHandle>) {
    for child in &mut children {
        let _ = child.child.kill();
    }
    for mut child in children {
        let _ = child.child.wait();
    }
}

fn run_router(args: Args) {
    println!(
        "[role=router index=0 pid={} ts_ms={}] started endpoint={}",
        std::process::id(),
        now_ms(),
        args.router_endpoint
    );
    let _router = zenoh::open(router_config(&args)).wait().unwrap();
    wait_for_runtime(args.max_runtime_secs);
}

fn run_in_process(args: Args) {
    println!(
        "[role=in-process pid={} ts_ms={}] starting in-process stress router={} publisher_sessions={} churn_sessions={} key={}",
        std::process::id(),
        now_ms(),
        args.router_endpoint,
        args.publisher_sessions_per_process,
        args.churn_sessions_per_process,
        REPRO_KEY,
    );
    let _router = zenoh::open(router_config(&args)).wait().unwrap();
    thread::sleep(Duration::from_millis(500));

    let (tx, rx) = mpsc::channel();

    for session in 0..args.publisher_sessions_per_process {
        spawn_publisher(args.worker_index, session, args.clone(), tx.clone());
    }
    for session in 0..args.churn_sessions_per_process {
        spawn_churn_peer(args.worker_index, session, args.clone(), tx.clone());
    }

    monitor_publishers(rx, &args);
}

fn run_publisher_process(args: Args) {
    let (tx, rx) = mpsc::channel();

    println!(
        "[role=publisher index={} pid={} ts_ms={}] started sessions={} key={} action=repeated-put-wait",
        args.worker_index,
        std::process::id(),
        now_ms(),
        args.publisher_sessions_per_process,
        REPRO_KEY,
    );
    for session in 0..args.publisher_sessions_per_process {
        spawn_publisher(args.worker_index, session, args.clone(), tx.clone());
    }

    monitor_publishers(rx, &args);
}

fn run_churn_process(args: Args) {
    let (tx, rx) = mpsc::channel();

    println!(
        "[role=churn index={} pid={} ts_ms={}] started sessions={} key={} action=open-declare-subscriber-drop-close-loop",
        args.worker_index,
        std::process::id(),
        now_ms(),
        args.churn_sessions_per_process,
        REPRO_KEY,
    );
    for session in 0..args.churn_sessions_per_process {
        spawn_churn_peer(args.worker_index, session, args.clone(), tx.clone());
    }

    monitor_churn(rx, &args);
}

fn spawn_publisher(process_index: usize, session_index: usize, args: Args, tx: Sender<Event>) {
    thread::spawn(move || {
        let worker = process_index * args.publisher_sessions_per_process + session_index;
        let name = format!("publisher-{process_index}-{session_index}");
        let session = match zenoh::open(peer_config(&args, &name)).wait() {
            Ok(session) => session,
            Err(err) => {
                send_error(&tx, worker, "open publisher peer", err);
                return;
            }
        };
        let publisher = match session.declare_publisher(REPRO_KEY).wait() {
            Ok(publisher) => publisher,
            Err(err) => {
                send_error(&tx, worker, "declare publisher", err);
                return;
            }
        };

        let payload: ZBytes = (0..args.payload_size)
            .map(|idx| (idx % 251) as u8)
            .collect::<Vec<u8>>()
            .into();
        let _ = tx.send(Event::PublisherStarted { worker });

        let mut seq = 0;
        loop {
            let start = Instant::now();
            let _ = tx.send(Event::PutStarted { worker, seq });
            if let Err(err) = publisher.put(payload.clone()).wait() {
                send_error(&tx, worker, "put", err);
                return;
            }
            let elapsed = start.elapsed();
            let _ = tx.send(Event::PutCompleted {
                worker,
                seq,
                elapsed,
            });
            seq += 1;
            thread::sleep(Duration::from_millis(args.put_period_ms));
        }
    });
}

fn spawn_churn_peer(process_index: usize, session_index: usize, args: Args, tx: Sender<Event>) {
    thread::spawn(move || {
        let worker = process_index * args.churn_sessions_per_process + session_index;
        let mut cycle = 0;
        loop {
            let start = Instant::now();
            let name = format!("churn-{process_index}-{session_index}-{cycle}");
            match zenoh::open(peer_config(&args, &name)).wait() {
                Ok(session) => match session.declare_subscriber(REPRO_KEY).wait() {
                    Ok(subscriber) => {
                        thread::sleep(Duration::from_millis(args.churn_hold_ms));
                        drop(subscriber);
                        drop(session);
                        let _ = tx.send(Event::ChurnCompleted {
                            worker,
                            cycle,
                            elapsed: start.elapsed(),
                        });
                    }
                    Err(err) => {
                        send_error(&tx, worker, "declare subscriber", err);
                        thread::sleep(Duration::from_millis(args.churn_error_backoff_ms));
                    }
                },
                Err(err) => {
                    send_error(&tx, worker, "open churn peer", err);
                    thread::sleep(Duration::from_millis(args.churn_error_backoff_ms));
                }
            }

            cycle += 1;
            thread::sleep(Duration::from_millis(args.churn_idle_ms));
        }
    });
}

fn monitor_publishers(rx: Receiver<Event>, args: &Args) {
    let mut puts = 0_u64;
    let mut errors = 0_u64;
    let mut slowest_put = Duration::ZERO;
    let mut last_put_by_worker = HashMap::new();
    let mut in_flight_puts = HashMap::new();
    let mut last_report = Instant::now();
    let started = Instant::now();
    let max_runtime = args.max_runtime_secs.map(Duration::from_secs);
    let stall_after = Duration::from_secs(args.stall_after_secs);
    let slow_put_threshold = Duration::from_millis(args.slow_put_threshold_ms);
    let park_on_slow_put = args.park_on_slow_put_ms.map(Duration::from_millis);
    let process_index = args.worker_index;
    let pid = std::process::id();

    loop {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(Event::PublisherStarted { worker }) => {
                println!(
                    "[role=publisher index={process_index} pid={pid} ts_ms={}] session={worker} started",
                    now_ms()
                );
                last_put_by_worker.insert(worker, Instant::now());
            }
            Ok(Event::PutStarted { worker, seq }) => {
                in_flight_puts.insert(worker, (seq, Instant::now()));
            }
            Ok(Event::PutCompleted {
                worker,
                seq,
                elapsed,
            }) => {
                puts += 1;
                slowest_put = slowest_put.max(elapsed);
                last_put_by_worker.insert(worker, Instant::now());
                in_flight_puts.remove(&worker);
                if elapsed >= slow_put_threshold {
                    println!(
                        "[role=publisher index={process_index} pid={pid} ts_ms={}] slow-put session={worker} seq={seq} elapsed={elapsed:?}",
                        now_ms()
                    );
                }
            }
            Ok(Event::Error {
                worker,
                context,
                message,
            }) => {
                errors += 1;
                if errors <= 20 || errors % 100 == 0 {
                    eprintln!(
                        "[role=publisher index={process_index} pid={pid} ts_ms={}] error #{errors}: session={worker} context={context}: {message}",
                        now_ms()
                    );
                }
            }
            Ok(Event::ChurnCompleted { .. }) => {}
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }

        for (worker, last_put) in &last_put_by_worker {
            if last_put.elapsed() >= stall_after {
                report_publisher_stall(
                    process_index,
                    pid,
                    Some(*worker),
                    stall_after,
                    args.exit_on_stall,
                );
            }
        }
        if let Some(threshold) = park_on_slow_put {
            for (worker, (seq, started_at)) in &in_flight_puts {
                let elapsed = started_at.elapsed();
                if elapsed >= threshold {
                    report_in_flight_put_and_park(
                        process_index,
                        pid,
                        *worker,
                        *seq,
                        elapsed,
                        args.exit_on_stall,
                    );
                }
            }
        }

        if last_report.elapsed() >= Duration::from_secs(2) {
            let oldest_put_age = last_put_by_worker
                .values()
                .map(Instant::elapsed)
                .max()
                .unwrap_or_default();
            println!(
                "[role=publisher index={process_index} pid={pid} ts_ms={}] progress puts={puts} errors={errors} slowest_put={slowest_put:?} oldest_put_age={oldest_put_age:?} in_flight_puts={}",
                now_ms(),
                in_flight_puts.len()
            );
            last_report = Instant::now();
        }

        if max_runtime.is_some_and(|duration| started.elapsed() >= duration) {
            println!(
                "[role=publisher index={process_index} pid={pid} ts_ms={}] completed requested runtime puts={puts} errors={errors} slowest_put={slowest_put:?}",
                now_ms()
            );
            return;
        }
    }
}

fn monitor_churn(rx: Receiver<Event>, args: &Args) {
    let mut churns = 0_u64;
    let mut errors = 0_u64;
    let mut last_report = Instant::now();
    let started = Instant::now();
    let max_runtime = args.max_runtime_secs.map(Duration::from_secs);
    let process_index = args.worker_index;
    let pid = std::process::id();

    loop {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(Event::ChurnCompleted {
                worker,
                cycle,
                elapsed,
            }) => {
                churns += 1;
                if elapsed > Duration::from_secs(1) {
                    println!(
                        "[role=churn index={process_index} pid={pid} ts_ms={}] slow-churn session={worker} cycle={cycle} elapsed={elapsed:?}",
                        now_ms()
                    );
                }
            }
            Ok(Event::Error {
                worker,
                context,
                message,
            }) => {
                errors += 1;
                if errors <= 20 || errors % 100 == 0 {
                    eprintln!(
                        "[role=churn index={process_index} pid={pid} ts_ms={}] error #{errors}: session={worker} context={context}: {message}",
                        now_ms()
                    );
                }
            }
            Ok(
                Event::PublisherStarted { .. }
                | Event::PutStarted { .. }
                | Event::PutCompleted { .. },
            ) => {}
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }

        if last_report.elapsed() >= Duration::from_secs(2) {
            println!(
                "[role=churn index={process_index} pid={pid} ts_ms={}] progress churns={churns} errors={errors}",
                now_ms()
            );
            last_report = Instant::now();
        }

        if max_runtime.is_some_and(|duration| started.elapsed() >= duration) {
            println!(
                "[role=churn index={process_index} pid={pid} ts_ms={}] completed requested runtime churns={churns} errors={errors}",
                now_ms()
            );
            return;
        }
    }
}

fn report_in_flight_put_and_park(
    process_index: usize,
    pid: u32,
    worker: usize,
    seq: u64,
    elapsed: Duration,
    exit_on_stall: bool,
) -> ! {
    eprintln!(
        "[role=publisher index={process_index} pid={pid} ts_ms={}] STALL in-flight-put session={worker} seq={seq} elapsed={elapsed:?}; \
         process is parked for debugger attach. Run `lldb -p {pid}`, then `thread backtrace all`.",
        now_ms()
    );

    if exit_on_stall {
        std::process::exit(2);
    }

    loop {
        thread::sleep(Duration::from_secs(60));
    }
}

fn report_publisher_stall(
    process_index: usize,
    pid: u32,
    worker: Option<usize>,
    stall_after: Duration,
    exit_on_stall: bool,
) -> ! {
    match worker {
        Some(worker) => eprintln!(
            "[role=publisher index={process_index} pid={pid} ts_ms={}] STALL no-completed-put session={worker} stall_after={stall_after:?}; \
             process is parked for debugger attach. Run `lldb -p {pid}`, then `thread backtrace all`.",
            now_ms()
        ),
        None => eprintln!(
            "[role=publisher index={process_index} pid={pid} ts_ms={}] STALL no-completed-put stall_after={stall_after:?}; \
             process is parked for debugger attach. Run `lldb -p {pid}`, then `thread backtrace all`.",
            now_ms()
        ),
    }

    if exit_on_stall {
        std::process::exit(2);
    }

    loop {
        thread::sleep(Duration::from_secs(60));
    }
}

fn wait_for_runtime(max_runtime_secs: Option<u64>) {
    if let Some(max_runtime_secs) = max_runtime_secs {
        thread::sleep(Duration::from_secs(max_runtime_secs));
        return;
    }

    loop {
        thread::sleep(Duration::from_secs(60));
    }
}

fn send_error<E: std::fmt::Display>(
    tx: &Sender<Event>,
    worker: usize,
    context: &'static str,
    err: E,
) {
    let _ = tx.send(Event::Error {
        worker,
        context,
        message: err.to_string(),
    });
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn router_config(args: &Args) -> Config {
    Config::from_json5(&format!(
        r#"{{
          mode: "router",
          listen: {{
            endpoints: ["{}"],
            exit_on_failure: true,
          }},
          scouting: {{
            multicast: {{ enabled: false }},
            gossip: {{
              enabled: true,
              multihop: false,
              target: {{ router: ["router", "peer"], peer: ["router"] }},
              autoconnect: {{ router: [], peer: ["router", "peer"] }},
              autoconnect_strategy: {{ router: {{ to_router: "always", to_peer: "always" }} }},
            }},
          }},
          timestamping: {{
            enabled: {{ router: true, peer: false, client: false }},
            drop_future_timestamp: false,
          }},
          queries_default_timeout: {},
          routing: {{
            router: {{ peers_failover_brokering: true }},
            peer: {{ mode: "{}" }},
            interests: {{ timeout: {} }},
          }},
          transport: {{
            link: {{ tx: {{ lease: 3000, keep_alive: 4 }} }},
            shared_memory: {{ enabled: false }},
          }},
        }}"#,
        args.router_endpoint,
        args.queries_default_timeout_ms,
        peer_routing_mode(args),
        args.interests_timeout_ms,
    ))
    .unwrap()
}

fn peer_config(args: &Args, name: &str) -> Config {
    let peer_target = if args.peer_gossip_target_router_only {
        r#"["router"]"#
    } else {
        r#"["router", "peer"]"#
    };

    Config::from_json5(&format!(
        r#"{{
          namespace: "aorta/repro",
          mode: "peer",
          metadata: {{ name: "{}" }},
          connect: {{
            endpoints: ["{}"],
            timeout_ms: {{ router: -1, peer: -1, client: 0 }},
            exit_on_failure: {{ router: false, peer: false, client: true }},
            retry: {{
              period_init_ms: 300,
              period_max_ms: 3000,
              period_increase_factor: 2,
            }},
          }},
          listen: {{
            endpoints: ["tcp/127.0.0.1:0"],
            exit_on_failure: true,
          }},
          open: {{
            return_conditions: {{
              connect_scouted: true,
              declares: true,
            }},
          }},
          scouting: {{
            multicast: {{ enabled: false }},
            gossip: {{
              enabled: true,
              multihop: false,
              target: {{ router: ["router", "peer"], peer: {} }},
              autoconnect: {{ router: [], peer: ["peer"] }},
              autoconnect_strategy: {{
                peer: {{ to_router: "always", to_peer: "greater-zid" }},
              }},
            }},
          }},
          timestamping: {{
            enabled: {{ router: true, peer: false, client: false }},
            drop_future_timestamp: false,
          }},
          queries_default_timeout: {},
          routing: {{
            router: {{ peers_failover_brokering: true }},
            peer: {{ mode: "{}" }},
            interests: {{ timeout: {} }},
          }},
          transport: {{
            link: {{ tx: {{ lease: 3000, keep_alive: 4 }} }},
            shared_memory: {{ enabled: false }},
          }},
        }}"#,
        name,
        args.router_endpoint,
        peer_target,
        args.queries_default_timeout_ms,
        peer_routing_mode(args),
        args.interests_timeout_ms,
    ))
    .unwrap()
}

fn peer_routing_mode(args: &Args) -> &'static str {
    if args.linkstate {
        "linkstate"
    } else {
        "peer_to_peer"
    }
}
