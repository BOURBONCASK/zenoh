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

use std::time::Duration;

use zenoh::{
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

/// The payloads of the replies to a query.
async fn replies(session: &Session, key: &str) -> Vec<String> {
    let replies = ztimeout!(session.get(key).timeout(Duration::from_secs(2))).unwrap();
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

/// Publishes on `key` from `publisher` until `subscriber`, a subscriber of that key, gets it.
async fn delivered(publisher: &Session, key: &str, subscriber: &crate::UnboundedSink<Sample>) {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while subscriber.count_by(|s| s.key_expr().as_str() == key) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{key} not delivered"
        );
        ztimeout!(publisher.put(key, "x")).unwrap();
        tokio::time::sleep(SLEEP).await;
    }
}

/// Publishes on `key` from `publisher` for a while: `subscriber` gets none of it.
async fn not_delivered(publisher: &Session, key: &str, subscriber: &crate::UnboundedSink<Sample>) {
    for _ in 0..10 {
        ztimeout!(publisher.put(key, "x")).unwrap();
        tokio::time::sleep(SLEEP).await;
    }
    assert_eq!(
        subscriber.count_by(|s| s.key_expr().as_str() == key),
        0,
        "{key} delivered"
    );
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
/// tokens of the cloud. The admin space of the bridge reports the first interest `denied` when
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
    a_denied_token_interest_leaves_the_other_interests_alone(DeniedBy::Bridge, "denied", "1eaf50")
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
    a_denied_mixed_interest_keeps_robot_publications_south(DeniedBy::Bridge, "denied", "1eaf60")
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
                interests: [{ key_expr: "r1/**", subscribers: true }],
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
/// When the owner goes away, the bridge reaches the other cloud router within
/// [`HANDOVER_BOUND`], and the observers are told of one closed and one opened transport.
#[cfg(feature = "unstable")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_hands_over_to_the_other_north_remote() {
    use zenoh::session::TransportEvent;

    /// The retry period of the endpoints is at most 400 ms: a bound of a few of them.
    const HANDOVER_BOUND: Duration = Duration::from_secs(3);

    zenoh::init_log_from_env_or("error");
    let ports = two_free_ports();
    let bridge = bridge_of_two_clouds("1eaf8002", ports).await;
    let events = ztimeout!(bridge
        .info()
        .transport_events_listener()
        .with(unbounded_sink()))
    .unwrap();
    let (a, b) = tokio::join!(cloud_on("1eaf800a", ports.0), cloud_on("1eaf800b", ports.1));

    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while north_remotes(&bridge, &[&a, &b]).await.len() != 1 {
        assert!(tokio::time::Instant::now() < deadline, "no north remote");
        tokio::time::sleep(SLEEP).await;
    }
    let (owner, other) = if north_remotes(&bridge, &[&a, &b]).await[0] == a.zid().to_string() {
        (a, b)
    } else {
        (b, a)
    };
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
    // the other remote is refused, at least if it completed its handshake: no event for it
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(events_of(owner.zid().to_string()), [SampleKind::Put]);
    assert!(events_of(other.zid().to_string()).is_empty());

    let (owner_zid, start) = (owner.zid().to_string(), tokio::time::Instant::now());
    ztimeout!(owner.close()).unwrap();
    while north_remotes(&bridge, &[&other]).await.len() != 1 {
        assert!(
            start.elapsed() < HANDOVER_BOUND,
            "no uplink to the other cloud router after {:?}",
            start.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    println!("time without uplink: {:?}", start.elapsed());
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(events_of(owner_zid), [SampleKind::Put, SampleKind::Delete]);
    assert_eq!(events_of(other.zid().to_string()), [SampleKind::Put]);
    assert_eq!(
        uplink_state(&bridge).await["remote"],
        other.zid().to_string()
    );
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
    _south: Session,
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
        _south: south,
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

/// Sessions of a robot, on its south router and on its bridge, publish to and query a key of
/// another robot, both ways.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_default_route_reaches_another_robot() {
    zenoh::init_log_from_env_or("error");
    let (_cloud, r1, r2) = fleet_of_two("1eafb", true).await;

    let control2 = ztimeout!(r2
        .app
        .declare_subscriber("r2/app/control")
        .with(unbounded_sink()))
    .unwrap();
    let control1 = ztimeout!(r1
        .x5
        .declare_subscriber("r1/app/control")
        .with(unbounded_sink()))
    .unwrap();
    let _srv2 = ztimeout!(r2.app.declare_queryable("r2/app/srv").callback(|query| {
        query.reply("r2/app/srv", "from r2").wait().unwrap();
    }))
    .unwrap();

    delivered(&r1.app, "r2/app/control", &control2).await;
    let seen = control2.count_by(|_| true);
    delivered(&r2.app, "r1/app/control", &control1).await;
    delivered(&r2.x5, "r1/app/control", &control1).await;
    assert_eq!(control2.count_by(|_| true), seen);

    for session in [&r1.app, &r1.x5] {
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        while replies(session, "r2/app/srv").await != ["from r2"] {
            assert!(tokio::time::Instant::now() < deadline, "no reply");
        }
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
    let seen = control2.count_by(|_| true);
    for _ in 0..10 {
        ztimeout!(r1.app.put("r2/app/control", "x")).unwrap();
        tokio::time::sleep(SLEEP).await;
    }
    assert_eq!(control2.count_by(|_| true), seen);
}

/// A wildcard query inside the keys of the fleet gets the replies of the robot and of the other
/// one, once each. A key outside the keys of the fleet is not sent to the cloud by a robot with
/// a default route, even from a session of its bridge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_regions_leaf_uplink_default_route_keeps_other_keys_on_the_robot() {
    zenoh::init_log_from_env_or("error");
    let (cloud, r1, r2) = fleet_of_two("1eafd", true).await;
    let phone = ztimeout!(Node::new(Client, "1eafd004").connect(&[loc!(cloud)]).open());
    let _status1 = ztimeout!(r1.x5.declare_queryable("r1/app/status").callback(|query| {
        query.reply("r1/app/status", "r1").wait().unwrap();
    }))
    .unwrap();
    let _status2 = ztimeout!(r2.app.declare_queryable("r2/app/status").callback(|query| {
        query.reply("r2/app/status", "r2").wait().unwrap();
    }))
    .unwrap();
    // outside of the interests of the bridges: the cloud does not declare them to the robots
    let outside = ztimeout!(phone.declare_subscriber("rt/**").with(unbounded_sink())).unwrap();
    let foreign = ztimeout!(phone.declare_subscriber("r9/app/**").with(unbounded_sink())).unwrap();

    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        let mut got = replies(&r1.app, "*/app/status").await;
        got.sort();
        if got == ["r1", "r2"] {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{got:?}");
    }

    delivered(&r1.x5, "r9/app/log", &foreign).await;
    delivered(&r1.app, "r9/app/log", &foreign).await;
    not_delivered(&r1.x5, "rt/camera", &outside).await;
    not_delivered(&r1.app, "rt/camera", &outside).await;
    assert_eq!(interest_states(&r1.bridge).await, ["finalized"]);
}
