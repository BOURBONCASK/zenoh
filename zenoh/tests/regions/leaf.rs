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

// Leaf uplink (`gateway.north.mode: "client"`) over real transports
//
//     cloud (R) ── phone (C)
//       |            tcp: the uplink, on which the bridge announces itself as a client
//     bridge (R)
//       |            unix socket: the bridge announces itself as a router
//     south (R) ── app (C)

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use zenoh::{
    query::ConsolidationMode,
    sample::{Sample, SampleKind},
    Session, Wait,
};
use zenoh_config::WhatAmI::{self, Client, Router};
use zenoh_core::ztimeout;

use crate::{loc, unbounded_sink, Node};

const TIMEOUT: Duration = Duration::from_secs(10);
const SLEEP: Duration = Duration::from_millis(50);

const CLOUD_GATEWAY: &str = r#"{ south: [
    { filters: [{ modes: ["client", "peer"] }] },
    { filters: [{ modes: ["router"], region_names: ["bridge"] }] },
] }"#;

/// Everything of the robot `r1`, in one interest.
const ALL_OF_R1: &str =
    r#"{ key_expr: "r1/**", subscribers: true, queryables: true, tokens: true }"#;

fn bridge_gateway(south_zid: &str, uplink_protocol: &str) -> String {
    bridge_gateway_with(south_zid, uplink_protocol, ALL_OF_R1)
}

fn bridge_gateway_with(south_zid: &str, uplink_protocol: &str, interests: &str) -> String {
    format!(
        r#"{{
            north: {{
                mode: "client",
                uplinks: [{{ link_protocols: ["{uplink_protocol}"] }}],
                interests: [{interests}],
            }},
            south: [
                {{ filters: [{{ modes: ["router"], zids: ["{south_zid}"] }}] }},
                {{ filters: [{{ modes: ["client"] }}] }},
            ],
        }}"#
    )
}

/// The mode under which `session` sees `zid`, once it is connected to it.
async fn mode_of(session: &Session, zid: &str) -> Option<WhatAmI> {
    session
        .info()
        .transports()
        .await
        .into_iter()
        .find(|t| t.zid().to_string() == zid)
        .map(|t| t.whatami())
}

async fn eventually<F: Fn() -> bool>(what: &str, pred: F) {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while !pred() {
        assert!(tokio::time::Instant::now() < deadline, "timeout: {what}");
        tokio::time::sleep(SLEEP).await;
    }
}

/// The payloads of the replies to a query: all of them, without consolidation, so that a reply
/// that comes twice is seen twice.
async fn replies(session: &Session, key: &str) -> Vec<String> {
    replies_within(session, key, Duration::from_secs(2)).await
}

async fn replies_within(session: &Session, key: &str, timeout: Duration) -> Vec<String> {
    let replies = ztimeout!(session
        .get(key)
        .consolidation(ConsolidationMode::None)
        .timeout(timeout))
    .unwrap();
    let mut payloads = vec![];
    while let Ok(reply) = replies.recv_async().await {
        payloads.push(
            reply
                .result()
                .unwrap()
                .payload()
                .try_to_string()
                .unwrap()
                .to_string(),
        );
    }
    payloads
}

struct Topology {
    cloud: Session,
    south: Session,
    bridge: Session,
    phone: Session,
    app: Session,
}

async fn topology(uplink_protocol: &str, zid_prefix: &str) -> Topology {
    let cloud = Node::new(Router, &format!("{zid_prefix}01")).gateway(CLOUD_GATEWAY);
    let south_zid = format!("{zid_prefix}03");
    let gateway = bridge_gateway(&south_zid, uplink_protocol);
    topology_with(cloud, |bridge| bridge.gateway(gateway), zid_prefix).await
}

/// The same topology, with a cloud router and a bridge that the caller configures.
async fn topology_with(
    cloud: Node,
    bridge: impl FnOnce(Node) -> Node,
    zid_prefix: &str,
) -> Topology {
    let zid = |n: u8| format!("{zid_prefix}{n:02x}");
    let cloud = ztimeout!(cloud.listen("tcp/127.0.0.1:0").open());
    // the south link is a unix socket: its protocol tells it apart from the uplink
    let south_ep = format!(
        "unixsock-stream/{}",
        std::env::temp_dir()
            .join(format!(
                "zenoh-leaf-{zid_prefix}-{}.sock",
                std::process::id()
            ))
            .display()
    );
    let south = ztimeout!(Node::new(Router, &zid(3)).listen(&south_ep).open());
    let bridge = ztimeout!(bridge(
        Node::new(Router, &zid(2))
            .region("bridge")
            .endpoints("tcp/127.0.0.1:0", &[&south_ep, loc!(cloud)])
    )
    .open());
    let phone = ztimeout!(Node::new(Client, &zid(4)).connect(&[loc!(cloud)]).open());
    let app = ztimeout!(Node::new(Client, &zid(5)).connect(&[&south_ep]).open());
    Topology {
        cloud,
        south,
        bridge,
        phone,
        app,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_announces_client_to_the_north_only() {
    zenoh::init_log_from_env_or("error");
    let t = topology("tcp", "1eaf00").await;
    let bridge = t.bridge.zid().to_string();

    let mut modes = (None, None);
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while modes != (Some(Client), Some(Router)) {
        assert!(tokio::time::Instant::now() < deadline, "{modes:?}");
        tokio::time::sleep(SLEEP).await;
        modes = (
            mode_of(&t.cloud, &bridge).await,
            mode_of(&t.south, &bridge).await,
        );
    }
    assert_eq!(
        mode_of(&t.bridge, &t.cloud.zid().to_string()).await,
        Some(Router)
    );
    assert_eq!(ztimeout!(t.cloud.info().routers_zid()).count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_flows() {
    zenoh::init_log_from_env_or("error");
    let t = topology("tcp", "1eaf10").await;

    // subscribers, both ways
    let up = ztimeout!(t
        .phone
        .declare_subscriber("r1/tele/**")
        .with(unbounded_sink()))
    .unwrap();
    let fanout = ztimeout!(t
        .phone
        .declare_subscriber("*/tele/**")
        .with(unbounded_sink()))
    .unwrap();
    let down = ztimeout!(t
        .app
        .declare_subscriber("r1/control")
        .with(unbounded_sink()))
    .unwrap();
    // queryables, both ways
    let _robot_srv = ztimeout!(t
        .app
        .declare_queryable("r1/srv/list")
        .callback(|q| Wait::wait(q.reply("r1/srv/list", "robot")).unwrap()))
    .unwrap();
    let _cloud_srv = ztimeout!(t
        .phone
        .declare_queryable("**/srv/credential")
        .callback(|q| Wait::wait(q.reply(q.key_expr().clone(), "cloud")).unwrap()))
    .unwrap();
    // tokens, both ways
    let _robot_token = ztimeout!(t.app.liveliness().declare_token("r1/alive/app")).unwrap();
    let _cloud_token = ztimeout!(t.phone.liveliness().declare_token("r1/alive/phone")).unwrap();
    let live_up = ztimeout!(t
        .phone
        .liveliness()
        .declare_subscriber("r1/alive/**")
        .history(true)
        .with(unbounded_sink()))
    .unwrap();
    let live_down = ztimeout!(t
        .app
        .liveliness()
        .declare_subscriber("r1/alive/**")
        .history(true)
        .with(unbounded_sink()))
    .unwrap();

    // publications are retried: declarations take a moment to cross the two routers
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        ztimeout!(t.app.put("r1/tele/vital", "x")).unwrap();
        ztimeout!(t.phone.put("r1/control", "y")).unwrap();
        tokio::time::sleep(SLEEP).await;
        let seen = |s: &crate::UnboundedSink<zenoh::sample::Sample>| s.count_by(|_| true) > 0;
        if seen(&up) && seen(&fanout) && seen(&down) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "publications");
    }

    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        let (down, up) = (
            replies(&t.phone, "r1/srv/list").await,
            replies(&t.app, "r1/srv/credential").await,
        );
        if down == ["robot"] && up == ["cloud"] {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{down:?} {up:?}");
        tokio::time::sleep(SLEEP).await;
    }

    eventually("tokens", || {
        let has = |s: &crate::UnboundedSink<zenoh::sample::Sample>, key: &str| {
            s.count_by(|s| s.kind() == SampleKind::Put && s.key_expr().as_str() == key) == 1
        };
        has(&live_up, "r1/alive/app") && has(&live_down, "r1/alive/phone")
    })
    .await;
}

/// A leaf whose `uplinks` do not match the link to its north remote announced itself as a
/// router on it. The north remote would handle it as a router of its subregion, so the leaf
/// refuses the transport.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_refuses_a_north_remote_over_another_link() {
    zenoh::init_log_from_env_or("error");
    let t = topology("quic", "1eaf20").await;
    let bridge = t.bridge.zid().to_string();

    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while mode_of(&t.south, &bridge).await != Some(Router) {
        assert!(tokio::time::Instant::now() < deadline, "south link");
        tokio::time::sleep(SLEEP).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert_eq!(mode_of(&t.bridge, &t.cloud.zid().to_string()).await, None);
}

/// A session connects to a listener of the leaf whose protocol is the one of its uplinks. The
/// leaf accepts that link, it does not open it: it is not an uplink. The session sees a router,
/// as the locators the leaf advertises say, and is one of its clients.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_listeners_answer_as_a_router() {
    zenoh::init_log_from_env_or("error");
    let t = topology("tcp", "1eaf90").await;
    let bridge = t.bridge.zid().to_string();
    eventually_connected(&t.cloud, &bridge, Client).await;

    let local = ztimeout!(Node::new(Client, "1eaf9006")
        .connect(&[loc!(t.bridge)])
        .open());

    assert_eq!(mode_of(&local, &bridge).await, Some(Router));
    let sub = ztimeout!(t.app.declare_subscriber("r1/local").with(unbounded_sink())).unwrap();
    delivered(&local, "r1/local", &sub).await;
}

/// Waits until `session` sees `zid` under `mode`.
async fn eventually_connected(session: &Session, zid: &str, mode: WhatAmI) {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while mode_of(session, zid).await != Some(mode) {
        assert!(tokio::time::Instant::now() < deadline, "{zid} as {mode}");
        tokio::time::sleep(SLEEP).await;
    }
}

/// Only a router can be a leaf of its north region, and uplink interests need a leaf.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_configuration_is_checked() {
    zenoh::init_log_from_env_or("error");
    let peer = Node::new(WhatAmI::Peer, "1eaf3001").gateway(r#"{ north: { mode: "client" } }"#);
    assert!(ztimeout!(zenoh::open(peer.c)).is_err());

    let interests = r#"{ north: { interests: [{ key_expr: "r1/**", subscribers: true }] } }"#;
    let router = Node::new(Router, "1eaf3002")
        .listen("tcp/127.0.0.1:0")
        .gateway(interests);
    assert!(ztimeout!(zenoh::open(router.c)).is_err());

    // `uplinks` omitted does not mean that every link is an uplink: the node does not start
    let router = Node::new(Router, "1eaf3003")
        .listen("tcp/127.0.0.1:0")
        .gateway(r#"{ north: { mode: "client" } }"#);
    let error = ztimeout!(zenoh::open(router.c)).unwrap_err().to_string();
    assert!(
        error.contains("gateway.north.uplinks is required"),
        "{error}"
    );
}

/// A text that nothing else in this process publishes.
fn fresh(what: &str) -> String {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    format!("{what}-{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

fn text(sample: &Sample) -> String {
    sample
        .payload()
        .try_to_string()
        .map(|text| text.to_string())
        .unwrap_or_default()
}

/// Publishes on `key` from `publisher` until `subscriber` gets one of these publications. Their
/// payloads are those of this call only: what the subscriber got before, from this publisher
/// or from another one, does not count.
async fn delivered(publisher: &Session, key: &str, subscriber: &crate::UnboundedSink<Sample>) {
    let mine = format!("{}/", fresh("delivered"));
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let mut sent = 0;
    while subscriber.count_by(|s| text(s).starts_with(&mine)) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{key} not delivered"
        );
        sent += 1;
        ztimeout!(publisher.put(key, format!("{mine}{sent}"))).unwrap();
        tokio::time::sleep(SLEEP).await;
    }
}

/// Publishes on `key` from `publisher` for a while: `subscriber` gets none of it.
async fn not_delivered(publisher: &Session, key: &str, subscriber: &crate::UnboundedSink<Sample>) {
    let mine = format!("{}/", fresh("not-delivered"));
    for sent in 0..10 {
        ztimeout!(publisher.put(key, format!("{mine}{sent}"))).unwrap();
        tokio::time::sleep(SLEEP).await;
    }
    assert_eq!(
        subscriber.count_by(|s| text(s).starts_with(&mine)),
        0,
        "{key} delivered"
    );
}

/// Publishes once on `key` from `publisher`, when the routes are in place: each of
/// `subscribers` gets that publication, and gets it once.
async fn delivered_once(
    publisher: &Session,
    key: &str,
    subscribers: &[&crate::UnboundedSink<Sample>],
) {
    let mine = fresh("once");
    let copies =
        |subscriber: &crate::UnboundedSink<Sample>| subscriber.count_by(|s| text(s) == mine);
    ztimeout!(publisher.put(key, mine.as_str())).unwrap();
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while subscribers.iter().any(|subscriber| copies(subscriber) == 0) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{key} not delivered"
        );
        tokio::time::sleep(SLEEP).await;
    }
    // a second copy would come by another, possibly longer, path
    tokio::time::sleep(Duration::from_millis(500)).await;
    let copies = subscribers.iter().map(|s| copies(s)).collect::<Vec<_>>();
    assert!(copies.iter().all(|n| *n == 1), "{key}: copies {copies:?}");
}

/// A queryable of `session` on `key` that replies `payload`, and the number of queries it got.
async fn counting_queryable(
    session: &Session,
    key: &'static str,
    payload: &'static str,
) -> (zenoh::query::Queryable<()>, Arc<AtomicUsize>) {
    let asked = Arc::new(AtomicUsize::new(0));
    let queryable = ztimeout!(session.declare_queryable(key).callback({
        let asked = asked.clone();
        move |query| {
            asked.fetch_add(1, Ordering::SeqCst);
            query.reply(key, payload).wait().unwrap();
        }
    }))
    .unwrap();
    (queryable, asked)
}

/// Queries `key` from `session` until the replies are `want`, then once more: the replies are
/// `want` again, each once, and each of the queryables behind `asked` got that query once.
async fn answered_once(session: &Session, key: &str, want: &[&str], asked: &[&Arc<AtomicUsize>]) {
    let sorted = |mut replies: Vec<String>| {
        replies.sort();
        replies
    };
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        let got = sorted(replies(session, key).await);
        if got == want {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{key}: {got:?}");
    }
    let before = asked
        .iter()
        .map(|n| n.load(Ordering::SeqCst))
        .collect::<Vec<_>>();
    assert_eq!(sorted(replies(session, key).await), want, "{key}");
    let calls = asked
        .iter()
        .zip(before)
        .map(|(n, before)| n.load(Ordering::SeqCst) - before)
        .collect::<Vec<_>>();
    assert!(calls.iter().all(|n| *n == 1), "{key}: calls {calls:?}");
}

/// A leaf bridge connects to a cloud router that uses the `auto` gateway preset: that router
/// sends no bound. The bridge announced itself as a client, so the router put it south, and
/// the bridge infers that relationship from the mode it announced. The flows work both ways.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_to_a_router_with_the_auto_preset() {
    zenoh::init_log_from_env_or("error");
    let prefix = "1eaf40";
    let gateway = bridge_gateway(&format!("{prefix}03"), "tcp");
    let t = topology_with(
        Node::new(Router, &format!("{prefix}01")),
        |bridge| bridge.gateway(gateway),
        prefix,
    )
    .await;
    let (bridge, cloud) = (t.bridge.zid().to_string(), t.cloud.zid().to_string());

    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while (
        mode_of(&t.cloud, &bridge).await,
        mode_of(&t.bridge, &cloud).await,
    ) != (Some(Client), Some(Router))
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "uplink not established"
        );
        tokio::time::sleep(SLEEP).await;
    }

    let up = ztimeout!(t
        .phone
        .declare_subscriber("r1/tele/**")
        .with(unbounded_sink()))
    .unwrap();
    let down = ztimeout!(t
        .app
        .declare_subscriber("r1/control")
        .with(unbounded_sink()))
    .unwrap();
    delivered(&t.app, "r1/tele/vital", &up).await;
    delivered(&t.phone, "r1/control", &down).await;
    assert_eq!(mode_of(&t.cloud, &bridge).await, Some(Client));
}

/// The state of the uplink of `bridge`, from its admin space.
async fn uplink_state(bridge: &Session) -> serde_json::Value {
    let replies = ztimeout!(bridge
        .get(format!("@/{}/router/uplink", bridge.zid()))
        .timeout(Duration::from_secs(2)))
    .unwrap();
    let reply = ztimeout!(replies.recv_async()).expect("no reply from the admin space");
    serde_json::from_slice(&reply.result().unwrap().payload().to_bytes()).unwrap()
}

/// The states of the configured interests of `bridge`, from its admin space.
async fn interest_states(bridge: &Session) -> Vec<String> {
    uplink_state(bridge).await["interests"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["state"].as_str().unwrap().to_string())
        .collect()
}

/// An access control that denies liveliness subscribers on the keys of `r1`, on `flow`: an
/// interest that asks for tokens on these keys is dropped there.
fn deny_token_interests(flow: &str) -> String {
    format!(
        r#"{{
            enabled: true,
            default_permission: "allow",
            rules: [{{
                id: "no-liveliness-subscriber",
                permission: "deny",
                flows: ["{flow}"],
                messages: ["declare_liveliness_subscriber"],
                key_exprs: ["r1/**"],
            }}],
            subjects: [{{ id: "all" }}],
            policies: [{{ rules: ["no-liveliness-subscriber"], subjects: ["all"] }}],
        }}"#
    )
}

/// Which access control denies the interests that ask for tokens.
#[derive(Clone, Copy, Debug)]
enum DeniedBy {
    /// The bridge's own, when it sends the interest: the bridge knows it.
    Bridge,
    /// The cloud's, when it receives the interest: it answers that nothing matches.
    Cloud,
}

/// The topology with an access control that denies the interests that ask for tokens on the keys
/// of `r1`, and a bridge that asks for `interests`.
async fn topology_denying_token_interests(
    denied_by: DeniedBy,
    interests: &str,
    prefix: &str,
) -> Topology {
    let gateway = bridge_gateway_with(&format!("{prefix}03"), "tcp", interests);
    let cloud = Node::new(Router, &format!("{prefix}01")).gateway(CLOUD_GATEWAY);
    let cloud = match denied_by {
        DeniedBy::Cloud => cloud.insert("access_control", &deny_token_interests("ingress")),
        DeniedBy::Bridge => cloud,
    };
    topology_with(
        cloud,
        |bridge| {
            let bridge = bridge.gateway(gateway);
            match denied_by {
                DeniedBy::Bridge => {
                    bridge.insert("access_control", &deny_token_interests("egress"))
                }
                DeniedBy::Cloud => bridge,
            }
        },
        prefix,
    )
    .await
}

async fn eventually_interest_states(bridge: &Session, expected: &[&str]) {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while interest_states(bridge).await != expected {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{:?}",
            uplink_state(bridge).await
        );
        tokio::time::sleep(SLEEP).await;
    }
}

/// The interest that asks for tokens is denied; the one that asks for subscribers and
/// queryables, sent apart, is answered, and its flows work. The south region does not get the
/// tokens of the cloud. The admin space of the bridge reports the first interest `not_sent` when
/// its own access control denies it, and `finalized` when the cloud's does: the cloud answers a
/// denied interest as one that nothing matches.
async fn a_denied_token_interest_leaves_the_other_interests_alone(
    denied_by: DeniedBy,
    reported: &str,
    prefix: &str,
) {
    zenoh::init_log_from_env_or("error");
    let t = topology_denying_token_interests(
        denied_by,
        r#"{ key_expr: "r1/**", subscribers: true, queryables: true },
           { key_expr: "r1/alive/**", tokens: true }"#,
        prefix,
    )
    .await;

    eventually_interest_states(&t.bridge, &["finalized", reported]).await;
    let state = uplink_state(&t.bridge).await;
    assert_eq!(state["state"], "connected");
    assert_eq!(state["remote"], t.cloud.zid().to_string());
    assert_eq!(state["interests"][1]["key_expr"], "r1/alive/**");

    let up = ztimeout!(t
        .phone
        .declare_subscriber("r1/tele/**")
        .with(unbounded_sink()))
    .unwrap();
    let down = ztimeout!(t
        .app
        .declare_subscriber("r1/control")
        .with(unbounded_sink()))
    .unwrap();
    let _cloud_token = ztimeout!(t.phone.liveliness().declare_token("r1/alive/phone")).unwrap();
    let live_down = ztimeout!(t
        .app
        .liveliness()
        .declare_subscriber("r1/alive/**")
        .history(true)
        .with(unbounded_sink()))
    .unwrap();
    delivered(&t.app, "r1/tele/vital", &up).await;
    delivered(&t.phone, "r1/control", &down).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(live_down.count_by(|_| true), 0, "{denied_by:?}");
    assert_eq!(
        interest_states(&t.bridge).await,
        ["finalized", reported],
        "{denied_by:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_reports_a_token_interest_denied_by_its_access_control() {
    a_denied_token_interest_leaves_the_other_interests_alone(
        DeniedBy::Bridge,
        "not_sent",
        "1eaf50",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_token_interest_denied_by_the_north_remote_is_answered_empty() {
    a_denied_token_interest_leaves_the_other_interests_alone(
        DeniedBy::Cloud,
        "finalized",
        "1eaf51",
    )
    .await;
}

/// An interest that asks for subscribers, queryables and tokens at once is denied as a whole.
/// The bridge then knows no subscriber of the cloud: a publication of the robot on the keys of
/// the interest is not sent to the uplink, even from a session of the bridge. What the robot
/// declares still reaches the cloud, so the cloud still publishes to the robot.
async fn a_denied_mixed_interest_keeps_robot_publications_south(
    denied_by: DeniedBy,
    reported: &str,
    prefix: &str,
) {
    zenoh::init_log_from_env_or("error");
    let t = topology_denying_token_interests(denied_by, ALL_OF_R1, prefix).await;
    let up = ztimeout!(t
        .phone
        .declare_subscriber("r1/tele/**")
        .with(unbounded_sink()))
    .unwrap();
    let down = ztimeout!(t
        .app
        .declare_subscriber("r1/control")
        .with(unbounded_sink()))
    .unwrap();

    eventually_interest_states(&t.bridge, &[reported]).await;

    delivered(&t.phone, "r1/control", &down).await;
    not_delivered(&t.app, "r1/tele/vital", &up).await;
    not_delivered(&t.bridge, "r1/tele/vital", &up).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_reports_a_mixed_interest_denied_by_its_access_control() {
    a_denied_mixed_interest_keeps_robot_publications_south(DeniedBy::Bridge, "not_sent", "1eaf60")
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_mixed_interest_denied_by_the_north_remote_is_answered_empty() {
    a_denied_mixed_interest_keeps_robot_publications_south(DeniedBy::Cloud, "finalized", "1eaf61")
        .await;
}

/// Two free TCP ports of the loopback interface.
fn two_free_ports() -> (u16, u16) {
    let bind = || std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let (a, b) = (bind(), bind());
    (
        a.local_addr().unwrap().port(),
        b.local_addr().unwrap().port(),
    )
}

/// A leaf bridge with two endpoints that lead north, to cloud routers that are not started yet.
async fn bridge_of_two_clouds(zid: &str, ports: (u16, u16)) -> Session {
    let endpoints = [
        format!("tcp/127.0.0.1:{}", ports.0),
        format!("tcp/127.0.0.1:{}", ports.1),
    ];
    ztimeout!(Node::new(Router, zid)
        .endpoints("tcp/127.0.0.1:0", &[&endpoints[0], &endpoints[1]])
        .gateway(
            r#"{ north: {
                mode: "client",
                uplinks: [{ link_protocols: ["tcp"] }],
                interests: [
                    { key_expr: "r1/**", subscribers: true, queryables: true, tokens: true },
                ],
            } }"#
        )
        .insert(
            "connect/retry",
            r#"{ period_init_ms: 100, period_max_ms: 400, period_increase_factor: 2 }"#
        )
        .open())
}

async fn cloud_on(zid: &str, port: u16) -> Session {
    ztimeout!(Node::new(Router, zid)
        .listen(&format!("tcp/127.0.0.1:{port}"))
        .gateway(CLOUD_GATEWAY)
        .open())
}

/// The cloud routers `bridge` has a transport with.
async fn north_remotes(bridge: &Session, clouds: &[&Session]) -> Vec<String> {
    let transports = bridge.info().transports().await.collect::<Vec<_>>();
    clouds
        .iter()
        .map(|c| c.zid().to_string())
        .filter(|zid| transports.iter().any(|t| &t.zid().to_string() == zid))
        .collect()
}

/// Two north candidates complete at the same time, again and again: one of them is admitted,
/// they never refuse each other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_admits_one_of_two_simultaneous_north_remotes() {
    zenoh::init_log_from_env_or("error");
    for round in 0..6u8 {
        let ports = two_free_ports();
        let bridge = bridge_of_two_clouds(&format!("1eaf70{round:02x}02"), ports).await;
        let zids = (
            format!("1eaf70{round:02x}0a"),
            format!("1eaf70{round:02x}0b"),
        );
        let (a, b) = tokio::join!(cloud_on(&zids.0, ports.0), cloud_on(&zids.1, ports.1));

        let deadline = tokio::time::Instant::now() + TIMEOUT;
        while north_remotes(&bridge, &[&a, &b]).await.len() != 1 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "round {round}: no north remote admitted"
            );
            tokio::time::sleep(SLEEP).await;
        }
        // and it stays so over a few retry periods of the other endpoint
        let admitted = north_remotes(&bridge, &[&a, &b]).await;
        for _ in 0..20 {
            tokio::time::sleep(SLEEP).await;
            assert_eq!(
                north_remotes(&bridge, &[&a, &b]).await,
                admitted,
                "round {round}"
            );
        }
        assert_eq!(
            uplink_state(&bridge).await["remote"],
            admitted[0],
            "round {round}"
        );
        ztimeout!(bridge.close()).unwrap();
    }
}

/// A remote that the leaf refuses is not announced to the observers of its transports. The
/// first endpoint of the bridge leads to a router that keeps its clients in its north region:
/// the bridge refuses it at each attempt. The second one leads to a cloud router, started
/// later, which is admitted: the observers are told of that transport only.
#[cfg(feature = "unstable")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_shows_no_refused_remote_to_transport_observers() {
    use zenoh::session::TransportEvent;

    zenoh::init_log_from_env_or("error");
    let ports = two_free_ports();
    let stranger = ztimeout!(Node::new(Router, "1eafa001")
        .listen(&format!("tcp/127.0.0.1:{}", ports.0))
        .gateway(r#"{ south: [{ filters: [{ modes: ["router"] }] }] }"#)
        .open());
    let bridge = bridge_of_two_clouds("1eafa003", ports).await;
    let events = ztimeout!(bridge
        .info()
        .transport_events_listener()
        .with(unbounded_sink()))
    .unwrap();
    let events_of = |zid: String| -> Vec<SampleKind> {
        let kinds = std::cell::RefCell::new(vec![]);
        events.count_by(|e: &TransportEvent| {
            if e.transport().zid().to_string() == zid {
                kinds.borrow_mut().push(e.kind());
            }
            true
        });
        kinds.into_inner()
    };

    // several attempts: the retry period is at most 400 ms
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(uplink_state(&bridge).await["state"], "disconnected");
    assert!(north_remotes(&bridge, &[&stranger]).await.is_empty());

    let cloud = cloud_on("1eafa002", ports.1).await;
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while uplink_state(&bridge).await["remote"] != cloud.zid().to_string() {
        assert!(tokio::time::Instant::now() < deadline, "no north remote");
        tokio::time::sleep(SLEEP).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(events_of(cloud.zid().to_string()), [SampleKind::Put]);
    assert!(events_of(stranger.zid().to_string()).is_empty());
    assert_eq!(
        north_remotes(&bridge, &[&stranger, &cloud]).await,
        [cloud.zid().to_string()]
    );
}

/// A leaf bridge with two endpoints that lead north. While one cloud router owns the north
/// region, the other one is not announced to the observers of the transports of the bridge.
/// When the owner goes away, the bridge is admitted by the other cloud router, the declarations
/// of the old one are withdrawn, and publications and queries work with the new one, all of it
/// within [`HANDOVER_BOUND`] of the moment the owner starts to close.
///
/// Without `slow_close`, the owner closes its session: the bridge loses its link to it.
///
/// With `slow_close`, the owner stays up and the bridge gives up on it, as on a remote that no
/// longer reads: a message that cannot be dropped is not enqueued, and the bridge deletes the
/// transport at once, without losing a link first. That deletion is then held back, after it
/// marked the transport closed and before it closes the link and its tasks, until the
/// hand-over is complete: the clean-up of the old transport takes as long as it wants, and the
/// hand-over does not wait for it.
#[cfg(all(feature = "unstable", feature = "test"))]
async fn hands_over_to_the_other_north_remote(zid_prefix: &str, slow_close: bool) {
    use zenoh::{qos::CongestionControl, session::TransportEvent};
    use zenoh_transport::unicast::test_helpers::{fail_pushes, gate_deletions};

    /// The retry period of the endpoints is at most 400 ms: a bound of a few of them.
    const HANDOVER_BOUND: Duration = Duration::from_secs(3);
    const QUERY: Duration = Duration::from_millis(300);

    zenoh::init_log_from_env_or("error");
    let ports = two_free_ports();
    let bridge_zid = format!("{zid_prefix}02");
    let bridge = bridge_of_two_clouds(&bridge_zid, ports).await;
    let events = ztimeout!(bridge
        .info()
        .transport_events_listener()
        .with(unbounded_sink()))
    .unwrap();
    let alive = ztimeout!(bridge
        .liveliness()
        .declare_subscriber("r1/alive/**")
        .with(unbounded_sink()))
    .unwrap();
    let _robot_srv = counting_queryable(&bridge, "r1/robot/srv", "robot").await;
    let zids = (format!("{zid_prefix}0a"), format!("{zid_prefix}0b"));
    let (a, b) = tokio::join!(cloud_on(&zids.0, ports.0), cloud_on(&zids.1, ports.1));
    // each cloud router has a token, a subscriber and a queryable that names it
    let mut tokens = vec![];
    let mut queryables = vec![];
    let mut teles = vec![];
    for cloud in [&a, &b] {
        let zid = cloud.zid().to_string();
        tokens
            .push(ztimeout!(cloud.liveliness().declare_token(format!("r1/alive/{zid}"))).unwrap());
        teles.push(ztimeout!(cloud.declare_subscriber("r1/tele").with(unbounded_sink())).unwrap());
        queryables.push(
            ztimeout!(cloud
                .declare_queryable("r1/cloud/srv")
                .callback(move |query| {
                    query.reply("r1/cloud/srv", zid.as_str()).wait().unwrap();
                }))
            .unwrap(),
        );
    }

    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while north_remotes(&bridge, &[&a, &b]).await.len() != 1 {
        assert!(tokio::time::Instant::now() < deadline, "no north remote");
        tokio::time::sleep(SLEEP).await;
    }
    let a_owns = north_remotes(&bridge, &[&a, &b]).await[0] == a.zid().to_string();
    let (owner, other) = if a_owns { (a, b) } else { (b, a) };
    let (owner_tele, other_tele) = if a_owns {
        (&teles[0], &teles[1])
    } else {
        (&teles[1], &teles[0])
    };
    let (owner_zid, other_zid) = (owner.zid().to_string(), other.zid().to_string());
    let events_of = |zid: &str| -> Vec<SampleKind> {
        let kinds = std::cell::RefCell::new(vec![]);
        events.count_by(|e: &TransportEvent| {
            if e.transport().zid().to_string() == zid {
                kinds.borrow_mut().push(e.kind());
            }
            true
        });
        kinds.into_inner()
    };
    let tokens_of = |zid: &str| -> Vec<SampleKind> {
        let kinds = std::cell::RefCell::new(vec![]);
        alive.count_by(|s: &Sample| {
            if s.key_expr().as_str().ends_with(zid) {
                kinds.borrow_mut().push(s.kind());
            }
            true
        });
        kinds.into_inner()
    };

    // the flows with the owner
    delivered(&bridge, "r1/tele", owner_tele).await;
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while replies(&owner, "r1/robot/srv").await != ["robot"]
        || replies(&bridge, "r1/cloud/srv").await != [owner_zid.clone()]
        || tokens_of(&owner_zid) != [SampleKind::Put]
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no flow with the owner"
        );
    }
    // the other remote is refused, at least if it completed its handshake: no event for it
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(events_of(&owner_zid), [SampleKind::Put]);
    assert!(events_of(&other_zid).is_empty());
    assert!(tokens_of(&other_zid).is_empty());

    let gate = slow_close.then(|| gate_deletions(bridge_zid.parse().unwrap()));
    let start = tokio::time::Instant::now();
    let (closing, _failing, _owner) = if slow_close {
        let failing = fail_pushes(bridge_zid.parse().unwrap(), owner_zid.parse().unwrap());
        ztimeout!(bridge
            .put("r1/tele", "the one that is not enqueued")
            .congestion_control(CongestionControl::Block))
        .unwrap();
        (None, Some(failing), Some(owner))
    } else {
        let closing = tokio::spawn(async move { owner.close().await });
        (Some(closing), None, None)
    };
    let handed_over = async {
        loop {
            let uplink = uplink_state(&bridge).await;
            if uplink["remote"] == other_zid.as_str() && uplink["state"] == "connected" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let admitted = start.elapsed();
        // the declarations of the old owner are withdrawn, those of the new one are there
        while tokens_of(&owner_zid) != [SampleKind::Put, SampleKind::Delete]
            || tokens_of(&other_zid) != [SampleKind::Put]
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // and the flows work with the new one
        delivered(&bridge, "r1/tele", other_tele).await;
        while replies_within(&other, "r1/robot/srv", QUERY).await != ["robot"] {}
        while replies_within(&bridge, "r1/cloud/srv", QUERY).await != [other_zid.clone()] {}
        admitted
    };
    let admitted = tokio::time::timeout(HANDOVER_BOUND, handed_over)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "slow close {slow_close}: no complete hand-over within {HANDOVER_BOUND:?}: \
                 tokens of the old owner {:?}, of the new one {:?}",
                tokens_of(&owner_zid),
                tokens_of(&other_zid)
            )
        });
    let complete = start.elapsed();
    println!("slow close {slow_close}: admitted after {admitted:?}, flows back after {complete:?}");
    assert!(complete < HANDOVER_BOUND, "{complete:?}");

    if let Some(gate) = gate {
        // all of this happened while the transport of the old owner was not cleaned up
        assert!(gate.waiting() >= 1, "the old transport was not held back");
        assert_eq!(events_of(&owner_zid), [SampleKind::Put]);
        assert_eq!(
            north_remotes(&bridge, &[&other]).await,
            std::slice::from_ref(&other_zid)
        );
        gate.open();
    }
    if let Some(closing) = closing {
        ztimeout!(closing).unwrap().unwrap();
    }
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while events_of(&owner_zid) != [SampleKind::Put, SampleKind::Delete] {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{:?}",
            events_of(&owner_zid)
        );
        tokio::time::sleep(SLEEP).await;
    }
    assert_eq!(events_of(&other_zid), [SampleKind::Put]);
    let uplink = uplink_state(&bridge).await;
    assert_eq!(uplink["remote"], other_zid.as_str());
    assert_eq!(uplink["state"], "connected");
    // the late end of the old transport took nothing from the new one
    delivered_once(&bridge, "r1/tele", &[other_tele]).await;
    assert_eq!(tokens_of(&other_zid), [SampleKind::Put]);
    drop((tokens, queryables));
}

#[cfg(all(feature = "unstable", feature = "test"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_hands_over_to_the_other_north_remote() {
    hands_over_to_the_other_north_remote("1eaf80", false).await;
}

/// The same hand-over while the transport of the old owner cannot be cleaned up.
#[cfg(all(feature = "unstable", feature = "test"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_hands_over_before_the_old_transport_is_cleaned_up() {
    hands_over_to_the_other_north_remote("1eaf81", true).await;
}

// ---------------------------------------------------------------------------------------------
// What the old north remote still delivers after the next one took over
//
//     cloud a (R)   cloud b (R)      one of them owns the north region of the bridge
//            \       /
//           bridge (R)
//             |
//           south (R) ── app (C)
//
// The take-over does not wait for the clean-up of the transport of the old owner, and that
// clean-up waits for the read tasks of the transport. A read task that took the handler of the
// transport before the deletion started gives it its message afterwards, whenever it runs again.
//
// The expressions of these last messages start with a wildcard: a router declares no id for
// such an expression, the message carries it whole. A message that refers to an id is dropped
// anyway once the face is closed, since the ids of the face are forgotten with it.
// ---------------------------------------------------------------------------------------------

/// A robot whose leaf bridge has two endpoints that lead north, to cloud routers that are not
/// started yet: the bridge, a client session of the bridge, its south router and an application
/// session of that router.
#[cfg(all(feature = "unstable", feature = "test"))]
struct RobotOfTwoClouds {
    bridge: Session,
    x5: Session,
    _south: Session,
    app: Session,
}

#[cfg(all(feature = "unstable", feature = "test"))]
async fn robot_of_two_clouds(zid_prefix: &str, ports: (u16, u16)) -> RobotOfTwoClouds {
    let zid = |n: u8| format!("{zid_prefix}{n:02x}");
    let south_ep = format!(
        "unixsock-stream/{}",
        std::env::temp_dir()
            .join(format!(
                "zenoh-leaf-{zid_prefix}-{}.sock",
                std::process::id()
            ))
            .display()
    );
    let endpoints = [
        format!("tcp/127.0.0.1:{}", ports.0),
        format!("tcp/127.0.0.1:{}", ports.1),
    ];
    let south = ztimeout!(Node::new(Router, &zid(3)).listen(&south_ep).open());
    let bridge = ztimeout!(Node::new(Router, &zid(2))
        .region("bridge")
        .endpoints(
            "tcp/127.0.0.1:0",
            &[&south_ep, &endpoints[0], &endpoints[1]]
        )
        .gateway(bridge_gateway(&zid(3), "tcp"))
        .insert(
            "connect/retry",
            r#"{ period_init_ms: 100, period_max_ms: 400, period_increase_factor: 2 }"#
        )
        .open());
    let app = ztimeout!(Node::new(Client, &zid(5)).connect(&[&south_ep]).open());
    let x5 = ztimeout!(Node::new(Client, &zid(6)).connect(&[loc!(bridge)]).open());
    RobotOfTwoClouds {
        bridge,
        x5,
        _south: south,
        app,
    }
}

/// The north remote of `bridge` according to its admin space, if it says so in time: a query of
/// the bridge also goes to its north remote, which may not answer any more.
#[cfg(all(feature = "unstable", feature = "test"))]
async fn north_remote_of(bridge: &Session) -> Option<String> {
    let replies = bridge
        .get(format!("@/{}/router/uplink", bridge.zid()))
        .consolidation(ConsolidationMode::None)
        .timeout(Duration::from_millis(500))
        .await
        .ok()?;
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let uplink: serde_json::Value =
                serde_json::from_slice(&sample.payload().to_bytes()).ok()?;
            return uplink["remote"].as_str().map(str::to_string);
        }
    }
    None
}

/// The last message of the old north remote.
#[cfg(all(feature = "unstable", feature = "test"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Late {
    Token,
    Subscriber,
    Queryable,
    Publication,
    Query,
}

#[cfg(all(feature = "unstable", feature = "test"))]
impl Late {
    fn is(self, msg: zenoh_protocol::network::NetworkMessageRef) -> bool {
        use zenoh_protocol::network::{DeclareBody, NetworkBodyRef};

        match (self, msg.body) {
            (Late::Publication, NetworkBodyRef::Push(_)) => true,
            (Late::Query, NetworkBodyRef::Request(_)) => true,
            (late, NetworkBodyRef::Declare(declare)) => matches!(
                (late, &declare.body),
                (Late::Token, DeclareBody::DeclareToken(_))
                    | (Late::Subscriber, DeclareBody::DeclareSubscriber(_))
                    | (Late::Queryable, DeclareBody::DeclareQueryable(_))
            ),
            _ => false,
        }
    }
}

/// Where the last message of the old north remote is when the next one takes over.
#[cfg(all(feature = "unstable", feature = "test"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Caught {
    /// In the read task of the bridge, which holds the handler of the transport and has not
    /// given it the message yet.
    BeforeRouting,
    /// Inside routing: the tables of the bridge have it, and the thread that routes it has
    /// released them and is about to send its consequence to a client session of the bridge.
    /// (What goes to the south router is sent before the tables are released.)
    InsideRouting,
}

/// A leaf bridge with two cloud endpoints, a client session, a south router and an application
/// two hops south of the uplink. The owner of the north region sends one last message, `late`,
/// which is stopped on the bridge where `caught` says. The transport of the owner is then
/// deleted and the other cloud router takes over, while that message is still stopped. Then
/// the message goes on.
///
/// Whatever it is, nothing of it is left south once the clean-up of the old transport has
/// ended: no token, no subscriber, no queryable, no publication, no query. It is looked for two
/// hops south when the message was stopped before routing, and on the client session of the
/// bridge when it was stopped on its way there. And the new owner lost nothing to that
/// clean-up: its token is alive, its subscriber and its queryable are served, the queryable of
/// the robot answers it.
///
/// The bridge gives up on the owner as on a remote that no longer reads, and deletes the
/// transport without losing a link first. (The same over low-latency transports, which a test
/// from outside cannot delete while their only read task is stopped, is in
/// `zenoh/src/tests/leaf_uplink.rs`.)
#[cfg(all(feature = "unstable", feature = "test"))]
async fn the_old_north_remote_leaves_nothing_behind(zid_prefix: &str, late: Late, caught: Caught) {
    use zenoh::{qos::CongestionControl, session::TransportEvent};
    use zenoh_transport::unicast::test_helpers::{fail_pushes, hold_egress, hold_ingress};

    const QUERY: Duration = Duration::from_millis(300);

    zenoh::init_log_from_env_or("error");
    let ports = two_free_ports();
    let robot = robot_of_two_clouds(zid_prefix, ports).await;
    let (bridge, app) = (&robot.bridge, &robot.app);
    let (bridge_zid, x5_zid) = (bridge.zid().to_string(), robot.x5.zid().to_string());
    // where what the last message leaves behind is looked for
    let observer = match caught {
        Caught::BeforeRouting => app,
        Caught::InsideRouting => &robot.x5,
    };
    let events = ztimeout!(bridge
        .info()
        .transport_events_listener()
        .with(unbounded_sink()))
    .unwrap();
    let alive = ztimeout!(observer
        .liveliness()
        .declare_subscriber("r1/alive/**")
        .with(unbounded_sink()))
    .unwrap();
    let to_late_sub = ztimeout!(observer.declare_publisher("r1/late/sub")).unwrap();
    let to_late_srv = ztimeout!(observer.declare_querier("r1/late/srv")).unwrap();
    let to_tele = ztimeout!(observer.declare_publisher("r1/tele")).unwrap();
    // what the robot listens to and offers, two hops south of the uplink
    let control = ztimeout!(app.declare_subscriber("*/control").with(unbounded_sink())).unwrap();
    let (asked, late_queries) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let _robot_srv = ztimeout!(app.declare_queryable("*/robot/srv").callback({
        let (asked, late_queries) = (asked.clone(), late_queries.clone());
        move |query| {
            asked.fetch_add(1, Ordering::SeqCst);
            if query.key_expr().as_str().starts_with("fleet/") {
                late_queries.fetch_add(1, Ordering::SeqCst);
            }
            query
                .reply(query.key_expr().clone(), "robot")
                .wait()
                .unwrap();
        }
    }))
    .unwrap();

    let zids = (format!("{zid_prefix}0a"), format!("{zid_prefix}0b"));
    let (a, b) = tokio::join!(cloud_on(&zids.0, ports.0), cloud_on(&zids.1, ports.1));
    // each cloud router has a token, a subscriber and a queryable that names it
    let mut tokens = vec![];
    let mut queryables = vec![];
    let mut teles = vec![];
    for cloud in [&a, &b] {
        let zid = cloud.zid().to_string();
        tokens
            .push(ztimeout!(cloud.liveliness().declare_token(format!("r1/alive/{zid}"))).unwrap());
        teles.push(ztimeout!(cloud.declare_subscriber("r1/tele").with(unbounded_sink())).unwrap());
        queryables.push(
            ztimeout!(cloud
                .declare_queryable("r1/cloud/srv")
                .callback(move |query| {
                    query.reply("r1/cloud/srv", zid.as_str()).wait().unwrap();
                }))
            .unwrap(),
        );
    }

    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while north_remotes(bridge, &[&a, &b]).await.len() != 1 {
        assert!(tokio::time::Instant::now() < deadline, "no north remote");
        tokio::time::sleep(SLEEP).await;
    }
    let a_owns = north_remotes(bridge, &[&a, &b]).await[0] == a.zid().to_string();
    let (owner, other) = if a_owns { (a, b) } else { (b, a) };
    let (owner_tele, other_tele) = if a_owns {
        (&teles[0], &teles[1])
    } else {
        (&teles[1], &teles[0])
    };
    let (owner_zid, other_zid) = (owner.zid().to_string(), other.zid().to_string());
    let events_of = |zid: &str| -> Vec<SampleKind> {
        let kinds = std::cell::RefCell::new(vec![]);
        events.count_by(|e: &TransportEvent| {
            if e.transport().zid().to_string() == zid {
                kinds.borrow_mut().push(e.kind());
            }
            true
        });
        kinds.into_inner()
    };
    let tokens_of = |name: &str| -> Vec<SampleKind> {
        let kinds = std::cell::RefCell::new(vec![]);
        alive.count_by(|s: &Sample| {
            if s.key_expr().as_str().ends_with(name) {
                kinds.borrow_mut().push(s.kind());
            }
            true
        });
        kinds.into_inner()
    };
    let late_publications = || control.count_by(|s| text(s) == "late");

    // the flows with the owner, from two hops south
    delivered(app, "r1/tele", owner_tele).await;
    delivered(&owner, "r1/control", &control).await;
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while replies(&owner, "r1/robot/srv").await != ["robot"]
        || replies(app, "r1/cloud/srv").await != [owner_zid.clone()]
        || tokens_of(&owner_zid) != [SampleKind::Put]
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no flow with the owner"
        );
    }
    assert!(ztimeout!(to_tele.matching_status()).unwrap().matching());
    assert!(!ztimeout!(to_late_sub.matching_status()).unwrap().matching());
    assert!(!ztimeout!(to_late_srv.matching_status()).unwrap().matching());

    // the last message of the owner is stopped on the bridge
    let hold = match caught {
        Caught::BeforeRouting => hold_ingress(
            bridge_zid.parse().unwrap(),
            owner_zid.parse().unwrap(),
            move |msg| late.is(msg),
        ),
        Caught::InsideRouting => hold_egress(
            bridge_zid.parse().unwrap(),
            x5_zid.parse().unwrap(),
            move |msg| late.is(msg),
        ),
    };
    let mut late_declarations: Vec<Box<dyn std::any::Any>> = vec![];
    match late {
        Late::Token => late_declarations.push(Box::new(
            ztimeout!(owner.liveliness().declare_token("*/alive/late")).unwrap(),
        )),
        Late::Subscriber => late_declarations.push(Box::new(
            ztimeout!(owner.declare_subscriber("*/late/sub")).unwrap(),
        )),
        Late::Queryable => late_declarations.push(Box::new(
            ztimeout!(owner.declare_queryable("*/late/srv")).unwrap(),
        )),
        // keys outside of `r1`: the bridge declared an id for that prefix with its interest,
        // and the cloud router would use it
        Late::Publication => ztimeout!(owner.put("fleet/control", "late")).unwrap(),
        Late::Query => {
            let owner = owner.clone();
            tokio::spawn(async move {
                let _ = owner
                    .get("fleet/robot/srv")
                    .timeout(Duration::from_secs(1))
                    .await;
            });
        }
    }
    println!("{late:?} {caught:?}: waiting for the last message of the owner");
    eventually("the last message of the owner is stopped", || {
        hold.holding()
    })
    .await;

    // the transport of the owner is deleted, and its clean-up cannot end: it waits for the
    // read task that is stopped, or that waits behind the thread that is
    let _failing = fail_pushes(bridge_zid.parse().unwrap(), owner_zid.parse().unwrap());
    ztimeout!(bridge
        .put("r1/tele", "the one that is not enqueued")
        .congestion_control(CongestionControl::Block))
    .unwrap();

    println!("{late:?} {caught:?}: the transport of the owner is going away");

    // the other cloud router is given the north region
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while north_remote_of(bridge).await.as_ref() != Some(&other_zid) {
        assert!(tokio::time::Instant::now() < deadline, "no take-over");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    println!("{late:?} {caught:?}: the north region is given to the other remote");
    match caught {
        Caught::BeforeRouting => {
            // and takes over completely while the message is stopped: nothing of the old
            // owner is in routing
            let handed_over = async {
                while tokens_of(&owner_zid) != [SampleKind::Put, SampleKind::Delete]
                    || tokens_of(&other_zid) != [SampleKind::Put]
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                delivered(app, "r1/tele", other_tele).await;
                while replies_within(app, "r1/cloud/srv", QUERY).await != [other_zid.clone()] {}
                delivered(&other, "r1/control", &control).await;
                while replies_within(&other, "r1/robot/srv", QUERY).await != ["robot"] {}
            };
            tokio::time::timeout(TIMEOUT, handed_over)
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "{late:?}: no hand-over while the message is stopped: tokens of the old \
                         owner {:?}, of the new one {:?}",
                        tokens_of(&owner_zid),
                        tokens_of(&other_zid)
                    )
                });
        }
        Caught::InsideRouting => {
            // a message of the old owner is inside routing: what it sends south and what the
            // retirement of its face sends south must not be swapped. Time for a retirement
            // that does not wait for it.
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    assert!(hold.holding(), "{late:?}: the message is no longer stopped");
    assert_eq!(
        events_of(&owner_zid),
        [SampleKind::Put],
        "the old transport is cleaned up"
    );
    assert_eq!(late_publications(), 0);
    assert_eq!(late_queries.load(Ordering::SeqCst), 0);

    // the message goes on, and the clean-up of the old transport ends
    println!("{late:?} {caught:?}: the last message of the owner goes on");
    hold.release();
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while events_of(&owner_zid) != [SampleKind::Put, SampleKind::Delete] {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{:?}",
            events_of(&owner_zid)
        );
        tokio::time::sleep(SLEEP).await;
    }
    // the new owner is there, with everything it declared
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while tokens_of(&other_zid) != [SampleKind::Put]
        || replies_within(app, "r1/cloud/srv", QUERY).await != [other_zid.clone()]
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{late:?}: the new owner is not there: its tokens {:?}",
            tokens_of(&other_zid)
        );
        tokio::time::sleep(SLEEP).await;
    }
    delivered(app, "r1/tele", other_tele).await;
    delivered(&other, "r1/control", &control).await;
    // whatever is still on its way south has arrived
    tokio::time::sleep(Duration::from_millis(500)).await;

    // nothing of the old owner is left south
    let late_token = tokens_of("late");
    assert!(
        late_token.is_empty() || late_token == [SampleKind::Put, SampleKind::Delete],
        "{late:?}: a token of the old owner is alive south: {late_token:?}"
    );
    assert_eq!(tokens_of(&owner_zid), [SampleKind::Put, SampleKind::Delete]);
    let alive_now = ztimeout!(observer.liveliness().get("r1/alive/**")).unwrap();
    let mut alive_keys = vec![];
    while let Ok(reply) = alive_now.recv_async().await {
        alive_keys.push(reply.result().unwrap().key_expr().to_string());
    }
    assert_eq!(alive_keys, [format!("r1/alive/{other_zid}")], "{late:?}");
    assert!(
        !ztimeout!(to_late_sub.matching_status()).unwrap().matching(),
        "{late:?}: a subscriber of the old owner is left south"
    );
    assert!(
        !ztimeout!(to_late_srv.matching_status()).unwrap().matching(),
        "{late:?}: a queryable of the old owner is left south"
    );
    if caught == Caught::BeforeRouting {
        // the message never entered routing
        assert!(late_token.is_empty(), "{late_token:?}");
        assert_eq!(
            late_publications(),
            0,
            "a publication of the old owner was delivered"
        );
        assert_eq!(
            late_queries.load(Ordering::SeqCst),
            0,
            "a query of the old owner was delivered"
        );
    }
    // and the end of the old transport took nothing from the new owner
    assert_eq!(events_of(&other_zid), [SampleKind::Put]);
    let uplink = uplink_state(bridge).await;
    assert_eq!(uplink["remote"], other_zid.as_str());
    assert_eq!(uplink["state"], "connected");
    assert_eq!(tokens_of(&other_zid), [SampleKind::Put]);
    assert!(ztimeout!(to_tele.matching_status()).unwrap().matching());
    delivered_once(app, "r1/tele", &[other_tele]).await;
    delivered_once(&other, "r1/control", &[&control]).await;
    answered_once(&other, "r1/robot/srv", &["robot"], &[&asked]).await;
    assert_eq!(replies(app, "r1/cloud/srv").await, [other_zid.as_str()]);
    drop((tokens, queryables, late_declarations, owner));
}

#[cfg(all(feature = "unstable", feature = "test"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_drops_a_token_of_the_old_north_remote_that_comes_late() {
    the_old_north_remote_leaves_nothing_behind("1eaf90", Late::Token, Caught::BeforeRouting).await;
}

#[cfg(all(feature = "unstable", feature = "test"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_drops_a_subscriber_of_the_old_north_remote_that_comes_late() {
    the_old_north_remote_leaves_nothing_behind("1eaf91", Late::Subscriber, Caught::BeforeRouting)
        .await;
}

#[cfg(all(feature = "unstable", feature = "test"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_drops_a_queryable_of_the_old_north_remote_that_comes_late() {
    the_old_north_remote_leaves_nothing_behind("1eaf92", Late::Queryable, Caught::BeforeRouting)
        .await;
}

#[cfg(all(feature = "unstable", feature = "test"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_drops_a_publication_of_the_old_north_remote_that_comes_late() {
    the_old_north_remote_leaves_nothing_behind("1eaf93", Late::Publication, Caught::BeforeRouting)
        .await;
}

#[cfg(all(feature = "unstable", feature = "test"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_drops_a_query_of_the_old_north_remote_that_comes_late() {
    the_old_north_remote_leaves_nothing_behind("1eaf94", Late::Query, Caught::BeforeRouting).await;
}

/// A declaration of the old owner that is inside routing when the next remote takes over: it
/// is withdrawn south after it was declared there, not before.
#[cfg(all(feature = "unstable", feature = "test"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_withdraws_a_token_of_the_old_north_remote_caught_in_routing() {
    the_old_north_remote_leaves_nothing_behind("1eaf97", Late::Token, Caught::InsideRouting).await;
}

#[cfg(all(feature = "unstable", feature = "test"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_withdraws_a_subscriber_of_the_old_north_remote_caught_in_routing()
{
    the_old_north_remote_leaves_nothing_behind("1eaf98", Late::Subscriber, Caught::InsideRouting)
        .await;
}

// ---------------------------------------------------------------------------------------------
// Default route (`gateway.north.default_route`): two robots on one cloud router
//
//                    cloud (R) ── phone (C)
//                   /         \
//     r1:  bridge (R) ── x5 (C)  bridge (R)  :r2
//            |                     |
//          south (R) ── app (C)   south (R) ── app (C)
// ---------------------------------------------------------------------------------------------

/// The keys the robots address each other with.
const FLEET_KEYS: &str = "*/app/**";

struct Robot {
    south: Session,
    bridge: Session,
    /// A session of the south router, two hops south of the uplink.
    app: Session,
    /// A session of the bridge.
    x5: Session,
}

/// A robot named `name` (`r1`, `r2`), whose bridge is a leaf of `cloud` and asks for everything
/// of its name. With `default_route`, both of its routers have a default route for the keys of
/// the fleet that are not its own.
async fn robot(cloud: &Session, name: &str, zid_prefix: &str, default_route: bool) -> Robot {
    let zid = |n: u8| format!("{zid_prefix}{n:02x}");
    let default_route = if default_route {
        format!(r#"default_route: {{ key_exprs: ["{FLEET_KEYS}"], except: ["{name}/**"] }},"#)
    } else {
        String::new()
    };
    let south_ep = format!(
        "unixsock-stream/{}",
        std::env::temp_dir()
            .join(format!(
                "zenoh-leaf-{zid_prefix}-{}.sock",
                std::process::id()
            ))
            .display()
    );
    let south = ztimeout!(Node::new(Router, &zid(3))
        .listen(&south_ep)
        .gateway(format!(r#"{{ north: {{ {default_route} }} }}"#))
        .open());
    let bridge = ztimeout!(Node::new(Router, &zid(2))
        .endpoints("tcp/127.0.0.1:0", &[&south_ep, loc!(cloud)])
        .gateway(format!(
            r#"{{
                north: {{
                    mode: "client",
                    uplinks: [{{ link_protocols: ["tcp"] }}],
                    interests: [
                        {{ key_expr: "{name}/**", subscribers: true, queryables: true }},
                    ],
                    {default_route}
                }},
                south: [
                    {{ filters: [{{ modes: ["router"], zids: ["{}"] }}] }},
                    {{ filters: [{{ modes: ["client"] }}] }},
                ],
            }}"#,
            zid(3)
        ))
        .open());
    let app = ztimeout!(Node::new(Client, &zid(5)).connect(&[&south_ep]).open());
    let x5 = ztimeout!(Node::new(Client, &zid(6)).connect(&[loc!(bridge)]).open());
    eventually_interest_states(&bridge, &["finalized"]).await;
    Robot {
        south,
        bridge,
        app,
        x5,
    }
}

async fn fleet_of_two(zid_prefix: &str, default_route: bool) -> (Session, Robot, Robot) {
    let cloud = ztimeout!(Node::new(Router, &format!("{zid_prefix}01"))
        .listen("tcp/127.0.0.1:0")
        .gateway(CLOUD_GATEWAY)
        .open());
    let r1 = robot(&cloud, "r1", &format!("{zid_prefix}1"), default_route).await;
    let r2 = robot(&cloud, "r2", &format!("{zid_prefix}2"), default_route).await;
    (cloud, r1, r2)
}

/// The state of the default route of a router, from its admin space.
async fn default_route_state(router: &Session) -> serde_json::Value {
    let replies = ztimeout!(router
        .get(format!("@/{}/router/default_route", router.zid()))
        .timeout(Duration::from_secs(2)))
    .unwrap();
    let reply = ztimeout!(replies.recv_async()).expect("no reply from the admin space");
    serde_json::from_slice(&reply.result().unwrap().payload().to_bytes()).unwrap()
}

/// Sessions of a robot, on its south router and on its bridge, publish to and query a key of
/// another robot, both ways and from each board. Once the routes are in place a publication is
/// delivered once to its subscriber on the other robot and once to a wildcard subscriber of
/// the robot it comes from, and a query is answered once by a queryable that was asked once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_default_route_reaches_another_robot() {
    zenoh::init_log_from_env_or("error");
    let (cloud, r1, r2) = fleet_of_two("1eafb", true).await;

    let control1 = ztimeout!(r1
        .app
        .declare_subscriber("r1/app/control")
        .with(unbounded_sink()))
    .unwrap();
    let control2 = ztimeout!(r2
        .app
        .declare_subscriber("r2/app/control")
        .with(unbounded_sink()))
    .unwrap();
    // on the bridge of the first robot, for the keys of every robot
    let recorder1 = ztimeout!(r1
        .x5
        .declare_subscriber("*/app/control")
        .with(unbounded_sink()))
    .unwrap();
    let (_srv1, asked1) = counting_queryable(&r1.x5, "r1/app/srv", "from r1").await;
    let (_srv2, asked2) = counting_queryable(&r2.app, "r2/app/srv", "from r2").await;

    // what the operator reads: the gateway of each router of the first robot
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while default_route_state(&r1.south).await["gateway"] != r1.bridge.zid().to_string()
        || default_route_state(&r1.bridge).await["gateway"] != cloud.zid().to_string()
    {
        assert!(tokio::time::Instant::now() < deadline, "no default gateway");
        tokio::time::sleep(SLEEP).await;
    }
    let state = default_route_state(&r1.south).await;
    assert_eq!(state["key_exprs"], serde_json::json!([FLEET_KEYS]));
    assert_eq!(state["except"], serde_json::json!(["r1/**"]));

    // each board of a robot publishes to the other robot, with publications of its own
    for source in [&r1.app, &r1.x5] {
        delivered(source, "r2/app/control", &control2).await;
        delivered(source, "r2/app/control", &recorder1).await;
    }
    for source in [&r2.app, &r2.x5] {
        delivered(source, "r1/app/control", &control1).await;
        delivered(source, "r1/app/control", &recorder1).await;
    }
    // the routes are in place: one publication, one copy for each subscriber
    for source in [&r1.app, &r1.x5] {
        delivered_once(source, "r2/app/control", &[&control2, &recorder1]).await;
    }
    for source in [&r2.app, &r2.x5] {
        delivered_once(source, "r1/app/control", &[&control1, &recorder1]).await;
    }

    for session in [&r1.app, &r1.x5] {
        answered_once(session, "r2/app/srv", &["from r2"], &[&asked2]).await;
    }
    for session in [&r2.app, &r2.x5] {
        answered_once(session, "r1/app/srv", &["from r1"], &[&asked1]).await;
    }
    // a query that nobody answers ends at once, without waiting for its timeout
    let start = tokio::time::Instant::now();
    assert!(replies(&r1.app, "r2/app/nobody").await.is_empty());
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "{:?}",
        start.elapsed()
    );
}

/// The same sessions without the default route: the session of the bridge reaches the other
/// robot, the session of the south router does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_without_default_route_only_the_bridge_reaches_another_robot() {
    zenoh::init_log_from_env_or("error");
    let (_cloud, r1, r2) = fleet_of_two("1eafc", false).await;
    let control2 = ztimeout!(r2
        .app
        .declare_subscriber("r2/app/control")
        .with(unbounded_sink()))
    .unwrap();

    delivered(&r1.x5, "r2/app/control", &control2).await;
    not_delivered(&r1.app, "r2/app/control", &control2).await;
}

/// Wildcards inside the keys of the fleet, which cover the robot and the other ones: a query
/// gets the replies of the robot and of the other one, once each, and a publication is delivered
/// once on the robot and once on the other one. A key outside the keys of the fleet is not sent
/// to the cloud by a robot with a default route, even from a session of its bridge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_default_route_keeps_other_keys_on_the_robot() {
    zenoh::init_log_from_env_or("error");
    let (cloud, r1, r2) = fleet_of_two("1eafd", true).await;
    let phone = ztimeout!(Node::new(Client, "1eafd004").connect(&[loc!(cloud)]).open());
    let (_status1, asked1) = counting_queryable(&r1.x5, "r1/app/status", "r1").await;
    let (_status2, asked2) = counting_queryable(&r2.app, "r2/app/status", "r2").await;
    let event1 = ztimeout!(r1
        .x5
        .declare_subscriber("r1/app/event")
        .with(unbounded_sink()))
    .unwrap();
    let event2 = ztimeout!(r2
        .app
        .declare_subscriber("r2/app/event")
        .with(unbounded_sink()))
    .unwrap();
    // outside of the interests of the bridges: the cloud does not declare them to the robots
    let outside = ztimeout!(phone.declare_subscriber("rt/**").with(unbounded_sink())).unwrap();
    let foreign = ztimeout!(phone.declare_subscriber("r9/app/**").with(unbounded_sink())).unwrap();

    answered_once(&r1.app, "*/app/status", &["r1", "r2"], &[&asked1, &asked2]).await;
    answered_once(&r1.x5, "*/app/status", &["r1", "r2"], &[&asked1, &asked2]).await;

    for source in [&r1.app, &r1.x5] {
        delivered(source, "*/app/event", &event1).await;
        delivered(source, "*/app/event", &event2).await;
        delivered_once(source, "*/app/event", &[&event1, &event2]).await;
    }

    delivered(&r1.x5, "r9/app/log", &foreign).await;
    delivered(&r1.app, "r9/app/log", &foreign).await;
    not_delivered(&r1.x5, "rt/camera", &outside).await;
    not_delivered(&r1.app, "rt/camera", &outside).await;
    assert_eq!(interest_states(&r1.bridge).await, ["finalized"]);
}
