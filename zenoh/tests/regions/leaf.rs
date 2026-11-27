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

use zenoh::{sample::SampleKind, Session, Wait};
use zenoh_config::WhatAmI::{self, Client, Router};
use zenoh_core::ztimeout;

use crate::{loc, unbounded_sink, Node};

const TIMEOUT: Duration = Duration::from_secs(10);
const SLEEP: Duration = Duration::from_millis(50);

const CLOUD_GATEWAY: &str = r#"{ south: [
    { filters: [{ modes: ["client", "peer"] }] },
    { filters: [{ modes: ["router"], region_names: ["bridge"] }] },
] }"#;

fn bridge_gateway(south_zid: &str, uplink_protocol: &str) -> String {
    format!(
        r#"{{
            north: {{
                mode: "client",
                uplinks: [{{ link_protocols: ["{uplink_protocol}"] }}],
                interests: [
                    {{ key_expr: "r1/**", subscribers: true, queryables: true, tokens: true }},
                ],
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
    let zid = |n: u8| format!("{zid_prefix}{n:02x}");
    let cloud = ztimeout!(Node::new(Router, &zid(1))
        .listen("tcp/127.0.0.1:0")
        .gateway(CLOUD_GATEWAY)
        .open());
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
    let bridge = ztimeout!(Node::new(Router, &zid(2))
        .region("bridge")
        .endpoints("tcp/127.0.0.1:0", &[&south_ep, loc!(cloud)])
        .gateway(bridge_gateway(&zid(3), uplink_protocol))
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
}
