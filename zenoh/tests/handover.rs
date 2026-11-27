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
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

//! Two routers whose link is selected by a hand-over rule on one side, through a proxy that
//! forwards only what the test allows: what applications observe when the link stops.
//!
//! The applications are the sessions of the routers themselves, so the link between the
//! routers is the only link, and the only one that the rule selects.
#![cfg(all(feature = "test", feature = "transport_tcp"))]

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use zenoh::{
    qos::{CongestionControl, Priority},
    query::{ConsolidationMode, QueryTarget},
    Config, Session, Wait,
};
use zenoh_test::{
    get_free_tcp_port,
    proxy::{Flow, GatedProxy},
};

const TIMEOUT: Duration = Duration::from_secs(60);
/// Nothing but the sender closes a transport within a test.
const LEASE_MS: u64 = 300_000;

fn router(listen: Option<u16>, connect: Option<String>, changes: &[(&str, String)]) -> Config {
    let mut config = Config::default();
    let mut set = |key: &str, value: &str| {
        config
            .insert_json5(key, value)
            .unwrap_or_else(|e| panic!("{key}: {e}"))
    };
    set("mode", r#""router""#);
    set("scouting/multicast/enabled", "false");
    set("scouting/gossip/enabled", "false");
    set("adminspace/enabled", "true");
    set("transport/link/tx/lease", &LEASE_MS.to_string());
    match listen {
        Some(port) => set("listen/endpoints", &format!(r#"["tcp/127.0.0.1:{port}"]"#)),
        None => set("listen/endpoints", "[]"),
    }
    if let Some(endpoint) = connect {
        set("connect/endpoints", &format!(r#"["{endpoint}"]"#));
    }
    for (key, value) in changes {
        set(key, value);
    }
    config
}

const HANDOVER: &str = "transport/link/tx/queue/handover";
const CLOSE: &str = "transport/link/tx/queue/congestion_control/block/wait_before_close";

struct Pair {
    /// The router that listens. No rule, unless the test gives it one.
    cloud: Session,
    /// The router that connects through the proxy. Its link is selected by a rule.
    robot: Session,
    proxy: GatedProxy,
    /// Where the cloud listens.
    cloud_port: u16,
    /// Where the robot listens, when the test asked for that.
    robot_port: Option<u16>,
}

async fn pair(rule: &str, cloud_changes: &[(&str, String)]) -> Pair {
    pair_with(rule, cloud_changes, &[]).await
}

async fn pair_with(
    rule: &str,
    cloud_changes: &[(&str, String)],
    robot_changes: &[(&str, String)],
) -> Pair {
    pair_opt(rule, cloud_changes, robot_changes, false).await
}

/// A pair whose robot listens too, for the sessions of the robot.
async fn pair_listening(rule: &str) -> Pair {
    pair_opt(rule, &[], &[], true).await
}

/// A client session with a namespace, as every session on a robot has: on the wire its keys
/// start with the namespace, and it drops what comes back with a key outside of it.
async fn namespaced_client(port: u16, namespace: &str) -> Session {
    let mut config = Config::default();
    let mut set = |key: &str, value: &str| {
        config
            .insert_json5(key, value)
            .unwrap_or_else(|e| panic!("{key}: {e}"))
    };
    set("mode", r#""client""#);
    set("scouting/multicast/enabled", "false");
    set("connect/endpoints", &format!(r#"["tcp/127.0.0.1:{port}"]"#));
    set("namespace", &format!(r#""{namespace}""#));
    zenoh::open(config).await.unwrap()
}

async fn pair_opt(
    rule: &str,
    cloud_changes: &[(&str, String)],
    robot_changes: &[(&str, String)],
    robot_listens: bool,
) -> Pair {
    zenoh_util::init_log_from_env_or("error");
    // Tests run side by side: a port that was free a moment ago may be taken.
    let mut listening = None;
    for _ in 0..50 {
        let port = get_free_tcp_port();
        if let Ok(cloud) = zenoh::open(router(Some(port), None, cloud_changes)).await {
            listening = Some((cloud, port));
            break;
        }
    }
    let (cloud, port) = listening.expect("no free port to listen on");
    let proxy = GatedProxy::spawn(format!("127.0.0.1:{port}")).await;
    let mut changes = vec![
        (HANDOVER, format!("{{ links: [{rule}] }}")),
        (CLOSE, "30000000".to_string()),
    ];
    changes.extend(robot_changes.iter().cloned());
    let mut robot_port = None;
    let robot = if robot_listens {
        let mut listening = None;
        for _ in 0..50 {
            let port = get_free_tcp_port();
            let config = router(Some(port), Some(proxy.endpoint()), &changes);
            if let Ok(robot) = zenoh::open(config).await {
                listening = Some(robot);
                robot_port = Some(port);
                break;
            }
        }
        listening.expect("no free port to listen on")
    } else {
        zenoh::open(router(None, Some(proxy.endpoint()), &changes))
            .await
            .unwrap()
    };
    Pair {
        cloud,
        robot,
        proxy,
        cloud_port: port,
        robot_port,
    }
}

async fn until(what: &str, mut condition: impl FnMut() -> bool) {
    let start = Instant::now();
    while !condition() {
        assert!(start.elapsed() < TIMEOUT, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// The router data of a session's own admin space.
async fn admin(session: &Session) -> serde_json::Value {
    let zid = session.zid();
    let replies = session.get(format!("@/{zid}/router")).await.unwrap();
    let reply = tokio::time::timeout(TIMEOUT, replies.recv_async())
        .await
        .unwrap()
        .unwrap();
    let sample = reply.result().expect("the admin space answers");
    serde_json::from_slice(&sample.payload().to_bytes()).unwrap()
}

/// The status of the sender of the only transport that a router has with another router.
async fn handover(session: &Session) -> serde_json::Value {
    let admin = admin(session).await;
    let routers = admin["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["whatami"] == "router")
        .collect::<Vec<_>>();
    assert_eq!(routers.len(), 1, "{admin}");
    routers[0]["handover"].clone()
}

fn payload(n: u64, size: usize) -> Vec<u8> {
    let mut payload = vec![0_u8; size.max(8)];
    payload[..8].copy_from_slice(&n.to_le_bytes());
    payload
}

fn number(payload: &zenoh::bytes::ZBytes) -> u64 {
    let bytes = payload.to_bytes();
    let mut n = [0_u8; 8];
    n.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(n)
}

/// A subscriber that records the numbers of the publications it receives.
async fn collect(session: &Session, key: &str) -> Arc<Mutex<Vec<u64>>> {
    let received = Arc::new(Mutex::new(Vec::new()));
    let sink = received.clone();
    session
        .declare_subscriber(key)
        .callback(move |sample| sink.lock().unwrap().push(number(sample.payload())))
        .background()
        .await
        .unwrap();
    received
}

/// Publishes on `key` from `from` until `to` receives: the subscription is known on the
/// other side of the link.
async fn linked(from: &Session, key: &str, to: &Arc<Mutex<Vec<u64>>>) {
    let start = Instant::now();
    while to.lock().unwrap().is_empty() {
        assert!(start.elapsed() < TIMEOUT, "no route for {key}");
        from.put(key, payload(u64::MAX, 8))
            .congestion_control(CongestionControl::Block)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Let what is on its way arrive, then start from nothing.
    tokio::time::sleep(Duration::from_millis(200)).await;
    to.lock().unwrap().clear();
}

/// The texts of the error replies and the number of the other replies of a query, in order
/// of arrival: `Err(text)` or `Ok(number of the reply)`.
async fn ask(session: &Session, selector: &str, priority: Priority) -> Vec<Result<u64, String>> {
    ask_with(
        session,
        selector,
        priority,
        ConsolidationMode::None,
        QueryTarget::BestMatching,
    )
    .await
}

async fn ask_with(
    session: &Session,
    selector: &str,
    priority: Priority,
    consolidation: ConsolidationMode,
    target: QueryTarget,
) -> Vec<Result<u64, String>> {
    let replies = session
        .get(selector)
        .priority(priority)
        .consolidation(consolidation)
        .target(target)
        .timeout(Duration::from_secs(40))
        .await
        .unwrap();
    let mut seen = Vec::new();
    while let Ok(reply) = tokio::time::timeout(TIMEOUT, replies.recv_async())
        .await
        .expect("the query does not end")
    {
        seen.push(match reply.result() {
            Ok(sample) => Ok(number(sample.payload())),
            Err(e) => Err(e.payload().try_to_string().unwrap().into_owned()),
        });
    }
    seen
}

/// Blocking publications from the robot until its link holds more than the sockets between
/// it and the stalled proxy absorb: the link is writing and cannot go on, and the budget of
/// the priority of data is used up.
async fn fill(pair: &Pair, key: &'static str) {
    // For a moment after the proxy stops forwarding, the sockets still absorb what the link
    // writes, and each batch that they absorb frees a part of the budget. The budget is used
    // up for good when a whole round of publications went by and the link wrote nothing.
    let sent = |status: &serde_json::Value| status["sent"].as_u64().unwrap();
    let mut before = sent(&handover(&pair.robot).await);
    let start = Instant::now();
    loop {
        let robot = pair.robot.clone();
        tokio::time::timeout(
            TIMEOUT,
            tokio::task::spawn_blocking(move || {
                for n in 0..400 {
                    robot
                        .put(key, payload(n, 60_000))
                        .congestion_control(CongestionControl::Block)
                        .wait()
                        .unwrap();
                }
                // What is left of the budget is less than any message that follows.
                for n in 0..2_000 {
                    robot
                        .put(key, payload(n, 8))
                        .congestion_control(CongestionControl::Block)
                        .wait()
                        .unwrap();
                }
            }),
        )
        .await
        .expect("a publication waited for the link")
        .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        let status = handover(&pair.robot).await;
        assert!(
            status["refused_link_budget"].as_u64().unwrap() > 0,
            "{status}"
        );
        let after = sent(&status);
        if after == before {
            break;
        }
        before = after;
        assert!(
            start.elapsed() < TIMEOUT,
            "the link keeps writing: {status}"
        );
    }
}

// The routing thread of a publisher does not wait for a stalled link: every publication
// returns, a subscriber of the same router receives all of them while the link is stalled,
// and when the link forwards again the remote subscriber gets, in order, those that the
// budget of the link took. The transport is not closed.
//
// Without the rule the first publication that finds the queue of the link full waits for
// `wait_before_close`, the local subscriber waits behind it, and the transport is closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_uplink_does_not_hold_local_traffic() {
    let pair = pair(r#"{ link_protocols: ["tcp"] }"#, &[]).await;
    let remote = collect(&pair.cloud, "t/**").await;
    let local = collect(&pair.robot, "t/**").await;
    linked(&pair.robot, "t/x", &remote).await;
    local.lock().unwrap().clear();

    pair.proxy.stall();
    const N: u64 = 2_000;
    let robot = pair.robot.clone();
    tokio::time::timeout(
        TIMEOUT,
        tokio::task::spawn_blocking(move || {
            for n in 0..N {
                robot
                    .put("t/x", payload(n, 8 * 1024))
                    .congestion_control(CongestionControl::Block)
                    .wait()
                    .unwrap();
            }
        }),
    )
    .await
    .expect("a publication waited for the link")
    .unwrap();
    assert_eq!(*local.lock().unwrap(), (0..N).collect::<Vec<_>>());
    let status = handover(&pair.robot).await;
    assert!(
        status["refused_link_budget"].as_u64().unwrap() > 0,
        "{status}"
    );

    pair.proxy.open();
    // The link works again: a publication made now arrives.
    let start = Instant::now();
    while remote.lock().unwrap().last() != Some(&N) {
        assert!(start.elapsed() < TIMEOUT, "the link did not recover");
        pair.robot
            .put("t/x", payload(N, 8))
            .congestion_control(CongestionControl::Block)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut remote = remote.lock().unwrap().clone();
    remote.retain(|n| *n != N);
    assert!(
        !remote.is_empty() && (remote.len() as u64) < N,
        "{}",
        remote.len()
    );
    assert!(remote.windows(2).all(|w| w[0] < w[1]), "out of order");
    let admin = admin(&pair.robot).await;
    for (reason, count) in admin["handover"]["closed_transports"].as_object().unwrap() {
        assert_eq!(count.as_u64(), Some(0), "closed for {reason}");
    }
}

/// A queryable of the cloud on `q/**` that counts its calls, and a route for publications on
/// `fill/x`. Returns the counter.
async fn asked_cloud(pair: &Pair) -> Arc<AtomicUsize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    pair.cloud
        .declare_queryable("q/**")
        .complete(true)
        .callback(move |query| {
            counter.fetch_add(1, Ordering::SeqCst);
            query.reply(query.key_expr(), payload(7, 8)).wait().unwrap();
        })
        .background()
        .await
        .unwrap();
    let sink = collect(&pair.cloud, "fill/**").await;
    linked(&pair.robot, "fill/x", &sink).await;
    let start = Instant::now();
    while ask(&pair.robot, "q/warm", Priority::Data).await != [Ok(7)] {
        assert!(start.elapsed() < TIMEOUT, "no route for q/**");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    calls
}

// A query that the stalled link does not take ends at once for the one who asked: one error
// reply that says so, and the end of the query, however the replies are to be consolidated
// and whichever queryables are targeted. A queryable that is not behind the link still
// answers. The refused query is not executed later, when the link forwards again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_query_refused_by_a_stalled_uplink_ends_with_an_error_reply() {
    // Nothing leaves the budget while the link is stalled: the age limit is the close period
    // of the test (30 s).
    let pair = pair_listening(r#"{ link_protocols: ["tcp"], data_bytes: 8388608 }"#).await;
    let calls = asked_cloud(&pair).await;
    // An application of the robot, with the namespace that every session on a robot has, and
    // the queryable of the cloud that it asks.
    let app = namespaced_client(pair.robot_port.unwrap(), "robot1").await;
    pair.cloud
        .declare_queryable("robot1/q/**")
        .callback(|query| query.reply(query.key_expr(), payload(7, 8)).wait().unwrap())
        .background()
        .await
        .unwrap();
    let start = Instant::now();
    while ask(&app, "q/warm", Priority::Data).await != [Ok(7)] {
        assert!(start.elapsed() < TIMEOUT, "no route for robot1/q/**");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let warm = calls.load(Ordering::SeqCst);

    pair.proxy.stall();
    fill(&pair, "fill/x").await;

    let refused = format!("q/refused?{}", "p".repeat(300));
    let asked = Instant::now();
    let mut refusals = 0;
    for consolidation in [
        ConsolidationMode::None,
        ConsolidationMode::Monotonic,
        ConsolidationMode::Latest,
    ] {
        for target in [
            QueryTarget::BestMatching,
            QueryTarget::All,
            QueryTarget::AllComplete,
        ] {
            assert_eq!(
                ask_with(&pair.robot, &refused, Priority::Data, consolidation, target).await,
                [Err("Congestion: query not delivered".to_string())],
                "{consolidation:?} {target:?}"
            );
            refusals += 1;
        }
    }
    // The application with a namespace sees the error too: the error reply carries the key
    // of the query, and so is inside the namespace.
    assert_eq!(
        ask(&app, &refused, Priority::Data).await,
        [Err("Congestion: query not delivered".to_string())]
    );
    refusals += 1;
    // A query for two queryables, one of them behind the stalled link: the other one answers,
    // the stalled branch ends with its error, and the query ends once.
    pair.robot
        .declare_queryable("q/fan/**")
        .callback(|query| {
            query.reply(query.key_expr(), payload(9, 8)).wait().unwrap();
        })
        .background()
        .await
        .unwrap();
    let fan = format!("q/fan/x?{}", "p".repeat(300));
    let mut both = ask_with(
        &pair.robot,
        &fan,
        Priority::Data,
        ConsolidationMode::None,
        QueryTarget::All,
    )
    .await;
    both.sort();
    assert_eq!(
        both,
        [Ok(9), Err("Congestion: query not delivered".to_string())]
    );
    refusals += 1;
    // None waited for the timeout of the query (40 s).
    assert!(asked.elapsed() < Duration::from_secs(20));
    let status = handover(&pair.robot).await;
    assert_eq!(
        status["queries_undelivered"].as_u64(),
        Some(refusals),
        "{status}"
    );
    assert_eq!(status["expired"].as_u64(), Some(0), "{status}");

    pair.proxy.open();
    let start = Instant::now();
    while ask(&pair.robot, "q/after", Priority::Data).await != [Ok(7)] {
        assert!(start.elapsed() < TIMEOUT, "the link did not recover");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Only queries made after the link recovered were executed: the answered one, and
    // those of the loop before it that the link did not take.
    let executed = calls.load(Ordering::SeqCst) - warm;
    let status = handover(&pair.robot).await;
    let unsent = status["queries_undelivered"].as_u64().unwrap() - refusals;
    assert!(executed >= 1, "{executed}");
    assert!(unsent < 1_000, "{status}");
}

// A query that the stalled link took and did not start within its age limit ends then for the
// one who asked: one error reply that says so, and the end of the query. It is not executed
// later, when the link forwards again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_query_that_expires_on_a_stalled_uplink_ends_with_an_error_reply() {
    let pair = pair(
        r#"{ link_protocols: ["tcp"], data_bytes: 8388608, max_age_ms: 400 }"#,
        &[],
    )
    .await;
    let calls = asked_cloud(&pair).await;
    let warm = calls.load(Ordering::SeqCst);

    pair.proxy.stall();
    // What waits leaves for its age. What the link is writing stays: once the sockets take
    // nothing more, the link is stuck in the middle of a message, and what is sent from then
    // on waits behind it.
    let mut status;
    let mut fills = 0;
    loop {
        fill(&pair, "fill/x").await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        status = handover(&pair.robot).await;
        fills += 1;
        if status["entries"].as_u64().unwrap() >= 1 {
            break;
        }
        assert!(fills < 8, "the sockets absorb everything: {status}");
    }
    assert!(status["expired"].as_u64().unwrap() > 0, "{status}");
    let sent = status["sent"].clone();

    let asked = Instant::now();
    assert_eq!(
        ask(&pair.robot, "q/expired", Priority::InteractiveHigh).await,
        [Err(
            "Congestion: query expired before transmission".to_string()
        )]
    );
    // It ended at the age limit of the link, not at the timeout of the query (40 s).
    assert!(asked.elapsed() >= Duration::from_millis(400));
    assert!(asked.elapsed() < Duration::from_secs(20));
    let status = handover(&pair.robot).await;
    assert_eq!(status["queries_undelivered"].as_u64(), Some(1), "{status}");
    assert_eq!(status["sent"], sent, "{status}");

    pair.proxy.open();
    let start = Instant::now();
    while ask(&pair.robot, "q/after", Priority::Data).await != [Ok(7)] {
        assert!(start.elapsed() < TIMEOUT, "the link did not recover");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let executed = calls.load(Ordering::SeqCst) - warm;
    let status = handover(&pair.robot).await;
    let unsent = (status["queries_undelivered"].as_u64().unwrap() - 1) as usize;
    assert!(executed >= 1, "{executed}");
    assert!(unsent < 1_000, "{status}");
}

// When the stalled link does not take all the replies of a queryable, the one who asked
// learns it: an error reply in front of the end of the query. The queryable is not held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replies_dropped_on_a_stalled_uplink_are_announced() {
    let pair = pair(r#"{ link_protocols: ["tcp"], data_bytes: 2097152 }"#, &[]).await;
    const REPLIES: u64 = 300;
    let answered = Arc::new(AtomicUsize::new(0));
    let done = answered.clone();
    pair.robot
        .declare_queryable("ns/r/**")
        .callback(move |query| {
            for n in 0..REPLIES {
                query
                    .reply(format!("ns/r/{n}"), payload(n, 60_000))
                    .wait()
                    .unwrap();
            }
            done.fetch_add(1, Ordering::SeqCst);
        })
        .background()
        .await
        .unwrap();
    // A second queryable, declared after the first: when the cloud knows it, it knows both.
    pair.robot
        .declare_queryable("ns/known/**")
        .callback(|query| query.reply(query.key_expr(), payload(7, 8)).wait().unwrap())
        .background()
        .await
        .unwrap();
    // The one who asks is a session with a namespace, behind the other router: it sees the
    // announcement only if that has a key inside the namespace, as the replies have.
    let asker = namespaced_client(pair.cloud_port, "ns").await;
    let start = Instant::now();
    while ask(&asker, "known/x", Priority::Data).await != [Ok(7)] {
        assert!(start.elapsed() < TIMEOUT, "no route for ns/known/**");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let before = answered.load(Ordering::SeqCst);

    // The robot receives, and cannot send.
    pair.proxy.stall_flow(Flow::Up);
    let asking = tokio::spawn(async move { ask(&asker, "r/**", Priority::Data).await });
    until("the queryable has answered", || {
        answered.load(Ordering::SeqCst) > before
    })
    .await;
    let status = handover(&pair.robot).await;
    assert!(status["replies_lost"].as_u64().unwrap() > 0, "{status}");

    pair.proxy.open();
    let seen = tokio::time::timeout(TIMEOUT, asking)
        .await
        .unwrap()
        .unwrap();
    let replies = seen
        .iter()
        .filter_map(|r| r.as_ref().ok().copied())
        .collect::<Vec<_>>();
    assert!(
        !replies.is_empty() && (replies.len() as u64) < REPLIES,
        "{}",
        replies.len()
    );
    assert!(replies.windows(2).all(|w| w[0] < w[1]), "out of order");
    // One announcement, in the place of the first reply that was dropped: replies that the
    // link took afterwards may follow it.
    assert_eq!(
        seen.iter().filter(|r| r.is_err()).collect::<Vec<_>>(),
        [&Err("Congestion: replies dropped".to_string())]
    );
}

// Declarations and liveliness tokens made while the link is stalled do not wait for it and
// are not lost: they are kept in the reserve of the link and arrive when it forwards again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declaration_burst_onto_a_stalled_uplink_arrives() {
    let pair = pair(
        r#"{ link_protocols: ["tcp"], reserve_max_bytes: 8388608 }"#,
        &[],
    )
    .await;
    let first = collect(&pair.robot, "d/first").await;
    linked(&pair.cloud, "d/first", &first).await;
    // The cloud watches the liveliness tokens of the robot.
    let alive = Arc::new(AtomicUsize::new(0));
    let seen_alive = alive.clone();
    pair.cloud
        .liveliness()
        .declare_subscriber("d/alive/**")
        .callback(move |sample| {
            if sample.kind() == zenoh::sample::SampleKind::Put {
                seen_alive.fetch_add(1, Ordering::SeqCst);
            }
        })
        .background()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    pair.proxy.stall();
    const N: usize = 2_000;
    const TOKENS: usize = 200;
    let received = Arc::new(AtomicUsize::new(0));
    let robot = pair.robot.clone();
    let counter = received.clone();
    let _tokens = tokio::time::timeout(
        TIMEOUT,
        tokio::task::spawn_blocking(move || {
            for n in 0..N {
                let counter = counter.clone();
                robot
                    .declare_subscriber(format!("d/burst/{n}"))
                    .callback(move |_| {
                        counter.fetch_add(1, Ordering::SeqCst);
                    })
                    .background()
                    .wait()
                    .unwrap();
            }
            (0..TOKENS)
                .map(|n| {
                    robot
                        .liveliness()
                        .declare_token(format!("d/alive/{n}"))
                        .wait()
                        .unwrap()
                })
                .collect::<Vec<_>>()
        }),
    )
    .await
    .expect("a declaration waited for the link");
    let _tokens = _tokens.unwrap();
    assert_eq!(alive.load(Ordering::SeqCst), 0);

    pair.proxy.open();
    until("every liveliness token is known to the cloud", || {
        alive.load(Ordering::SeqCst) == TOKENS
    })
    .await;
    // Every subscription is known to the cloud: a publication on each key arrives.
    let start = Instant::now();
    loop {
        received.store(0, Ordering::SeqCst);
        for n in 0..N {
            pair.cloud
                .put(format!("d/burst/{n}"), payload(0, 8))
                .congestion_control(CongestionControl::Block)
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        if received.load(Ordering::SeqCst) == N {
            break;
        }
        assert!(start.elapsed() < TIMEOUT, "declarations were lost");
    }
    let admin = admin(&pair.robot).await;
    for (reason, count) in admin["handover"]["closed_transports"].as_object().unwrap() {
        assert_eq!(count.as_u64(), Some(0), "closed for {reason}");
    }
}

// A link that its sender closed for lack of progress comes back as a new transport with a new
// sender. A query that waited on the old one ended with an error reply when it closed, and
// is not executed by the new one; nothing of the old sender is left in the process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_uplink_closed_without_progress_comes_back_as_a_new_transport() {
    let pair = pair_with(
        r#"{ link_protocols: ["tcp"], data_bytes: 8388608 }"#,
        &[],
        &[(CLOSE, "6000000".to_string())],
    )
    .await;
    let asked_keys = Arc::new(Mutex::new(Vec::new()));
    let keys = asked_keys.clone();
    pair.cloud
        .declare_queryable("q/**")
        .callback(move |query| {
            keys.lock().unwrap().push(query.key_expr().to_string());
            query.reply(query.key_expr(), payload(7, 8)).wait().unwrap();
        })
        .background()
        .await
        .unwrap();
    let sink = collect(&pair.cloud, "fill/**").await;
    linked(&pair.robot, "fill/x", &sink).await;
    let start = Instant::now();
    while ask(&pair.robot, "q/warm", Priority::Data).await != [Ok(7)] {
        assert!(start.elapsed() < TIMEOUT, "no route for q/**");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    pair.proxy.stall();
    fill(&pair, "fill/x").await;
    // Taken by the link, behind the write that cannot go on. Nothing is written any more:
    // the sender closes the transport, once, and the query ends then.
    let asked = Instant::now();
    assert_eq!(
        ask(&pair.robot, "q/lost", Priority::InteractiveHigh).await,
        [Err("Congestion: query not delivered".to_string())]
    );
    assert!(asked.elapsed() < Duration::from_secs(20));
    // The query ended while the transport was being deleted; the transport is gone next.
    let start = Instant::now();
    let mut status = admin(&pair.robot).await;
    while !status["sessions"].as_array().unwrap().is_empty() {
        assert!(start.elapsed() < TIMEOUT, "{status}");
        tokio::time::sleep(Duration::from_millis(20)).await;
        status = admin(&pair.robot).await;
    }
    assert_eq!(
        status["handover"]["closed_transports"]["no_progress"].as_u64(),
        Some(1),
        "{status}"
    );
    assert_eq!(
        status["handover"]["pool_bytes"].as_u64(),
        Some(0),
        "{status}"
    );

    pair.proxy.open();
    let start = Instant::now();
    while ask(&pair.robot, "q/after", Priority::Data).await != [Ok(7)] {
        assert!(start.elapsed() < TIMEOUT, "the link did not come back");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(pair.proxy.connections() >= 2);
    // A new sender: it has refused nothing and reported nothing, and the process holds what
    // one idle link holds.
    let start = Instant::now();
    loop {
        let status = admin(&pair.robot).await;
        let sender = handover(&pair.robot).await;
        // The minimum reserve of one link (the default, 64 KiB) and nothing else.
        if sender["entries"].as_u64() == Some(0)
            && status["handover"]["pool_bytes"].as_u64() == Some(64 * 1024)
        {
            assert_eq!(sender["refused_link_budget"].as_u64(), Some(0), "{sender}");
            assert_eq!(sender["queries_undelivered"].as_u64(), Some(0), "{sender}");
            assert_eq!(
                status["handover"]["closed_transports"]["no_progress"].as_u64(),
                Some(1),
                "{status}"
            );
            break;
        }
        assert!(start.elapsed() < TIMEOUT, "{sender}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!asked_keys.lock().unwrap().iter().any(|k| k == "q/lost"));
}

// Closing the session of a router writes what was published before the close.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_a_router_writes_what_was_published() {
    let pair = pair(
        r#"{ link_protocols: ["tcp"], data_bytes: 16777216, large_bytes: 67108864 }"#,
        &[],
    )
    .await;
    let remote = collect(&pair.cloud, "t/**").await;
    linked(&pair.robot, "t/x", &remote).await;
    const N: u64 = 300;
    let sizes = [64, 100_000, 8, 300_000, 1_000];
    for n in 0..N {
        pair.robot
            .put("t/x", payload(n, sizes[n as usize % sizes.len()]))
            .congestion_control(CongestionControl::Block)
            .await
            .unwrap();
    }
    tokio::time::timeout(TIMEOUT, pair.robot.close())
        .await
        .unwrap()
        .unwrap();
    until("everything published arrives", || {
        remote.lock().unwrap().len() as u64 >= N
    })
    .await;
    assert_eq!(*remote.lock().unwrap(), (0..N).collect::<Vec<_>>());
}

// With a rule on both sides of a healthy link everything goes through as without rules:
// publications of every priority, whole and fragmented, queries and their replies, in both
// directions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_healthy_link_selected_on_both_sides_carries_everything() {
    carries_everything(false).await
}

// The same with the batches of the link compressed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_healthy_compressed_link_selected_on_both_sides_carries_everything() {
    carries_everything(true).await
}

async fn carries_everything(compression: bool) {
    const COMPRESSION: &str = "transport/unicast/compression/enabled";
    let rule = r#"{ link_protocols: ["tcp"] }"#;
    let pair = pair_with(
        rule,
        &[
            (HANDOVER, format!("{{ links: [{rule}] }}")),
            (COMPRESSION, compression.to_string()),
        ],
        &[(COMPRESSION, compression.to_string())],
    )
    .await;
    assert!(handover(&pair.cloud).await.is_object());
    assert!(handover(&pair.robot).await.is_object());
    let priorities = [
        Priority::RealTime,
        Priority::InteractiveHigh,
        Priority::InteractiveLow,
        Priority::DataHigh,
        Priority::Data,
        Priority::DataLow,
        Priority::Background,
    ];
    for (way, (from, to)) in [(&pair.robot, &pair.cloud), (&pair.cloud, &pair.robot)]
        .into_iter()
        .enumerate()
    {
        // Each way has its own keys: what is declared for one does not answer the other.
        let topic = format!("both/{way}/t/x");
        let service = format!("both/{way}/q/x");
        let received = collect(to, &topic).await;
        linked(from, &topic, &received).await;
        let mut n = 0;
        for priority in priorities {
            for size in [8, 70_000, 2_000_000] {
                from.put(&topic, payload(n, size))
                    .priority(priority)
                    .congestion_control(CongestionControl::Block)
                    .await
                    .unwrap();
                n += 1;
            }
        }
        until("every publication arrives", || {
            received.lock().unwrap().len() as u64 == n
        })
        .await;
        // Each priority keeps its order.
        let received = received.lock().unwrap().clone();
        for p in 0..priorities.len() as u64 {
            let of_priority = received
                .iter()
                .copied()
                .filter(|n| n / 3 == p)
                .collect::<Vec<_>>();
            assert_eq!(of_priority, [3 * p, 3 * p + 1, 3 * p + 2]);
        }

        to.declare_queryable(&service)
            .callback(|query| {
                for n in 0..20 {
                    query
                        .reply(query.key_expr(), payload(n, 100_000))
                        .wait()
                        .unwrap();
                }
            })
            .background()
            .await
            .unwrap();
        let start = Instant::now();
        while ask(from, &service, Priority::Data).await != (0..20).map(Ok).collect::<Vec<_>>() {
            assert!(start.elapsed() < TIMEOUT, "no answer");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    for session in [&pair.robot, &pair.cloud] {
        let status = handover(session).await;
        for counter in [
            "refused_link_budget",
            "refused_process_limit",
            "refused_too_large",
            "expired",
            "replies_lost",
            "queries_undelivered",
        ] {
            assert_eq!(status[counter].as_u64(), Some(0), "{counter}: {status}");
        }
    }
}
