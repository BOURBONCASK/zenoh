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
#![cfg(all(
    feature = "unstable",
    feature = "internal",
    feature = "test",
    feature = "transport_tcp"
))]

//! Leaf uplink (`gateway.north.mode: "client"`): a transport that the leaf deletes itself while
//! one of its read tasks holds a message.
//!
//! ```text
//!     cloud a (R)   cloud b (R)      one of them owns the north region of the bridge
//!            \       /
//!           bridge (R)
//! ```
//!
//! The tests of `zenoh/tests/regions/leaf.rs` make the bridge give up on a remote that no longer
//! reads, which only a universal transport does. Here the bridge closes the transport of its
//! north remote through its transport manager, which a low-latency transport does too, and
//! which only a test of this crate can ask for.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use zenoh_core::ztimeout;
use zenoh_protocol::network::{DeclareBody, NetworkBodyRef};
use zenoh_transport::unicast::test_helpers::hold_ingress;

use crate::{sample::SampleKind, Config, Session};

const TIMEOUT: Duration = Duration::from_secs(10);
const SLEEP: Duration = Duration::from_millis(20);

fn node(zid: &str, low_latency: bool) -> Config {
    let mut c = Config::default();
    c.insert_json5("mode", r#""router""#).unwrap();
    c.insert_json5("id", &format!("\"{zid}\"")).unwrap();
    c.insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    if low_latency {
        c.insert_json5("transport/unicast/lowlatency", "true")
            .unwrap();
        c.insert_json5("transport/unicast/qos/enabled", "false")
            .unwrap();
    }
    c
}

async fn cloud(zid: &str, port: u16, low_latency: bool) -> Session {
    let mut c = node(zid, low_latency);
    c.insert_json5("listen/endpoints", &format!(r#"["tcp/127.0.0.1:{port}"]"#))
        .unwrap();
    c.insert_json5(
        "gateway",
        r#"{ south: [{ filters: [{ modes: ["client", "peer"] }] }] }"#,
    )
    .unwrap();
    ztimeout!(crate::open(c)).unwrap()
}

async fn eventually(what: &str, pred: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while !pred() {
        assert!(tokio::time::Instant::now() < deadline, "timeout: {what}");
        tokio::time::sleep(SLEEP).await;
    }
}

/// A leaf bridge with two cloud endpoints. The owner of its north region declares one more
/// token, whose declaration is stopped in the read task of the bridge, which has taken the
/// handler of the transport and has not given it the message yet. The bridge then closes that
/// transport; its clean-up waits for the read task. The other cloud router takes over
/// meanwhile. Then the read task goes on.
///
/// The late token is never declared on the bridge, and the token and the subscriber of the new
/// owner are untouched once the clean-up of the old transport has ended.
async fn a_token_of_the_old_north_remote_comes_late(zid_prefix: &str, low_latency: bool) {
    crate::init_log_from_env_or("error");
    let ports = (
        zenoh_test::get_free_tcp_port(),
        zenoh_test::get_free_tcp_port(),
    );
    let mut c = node(&format!("{zid_prefix}02"), low_latency);
    c.insert_json5("listen/endpoints", r#"["tcp/127.0.0.1:0"]"#)
        .unwrap();
    c.insert_json5(
        "connect/endpoints",
        &format!(
            r#"["tcp/127.0.0.1:{}", "tcp/127.0.0.1:{}"]"#,
            ports.0, ports.1
        ),
    )
    .unwrap();
    c.insert_json5(
        "connect/retry",
        r#"{ period_init_ms: 100, period_max_ms: 400, period_increase_factor: 2 }"#,
    )
    .unwrap();
    c.insert_json5(
        "gateway",
        r#"{ north: {
            mode: "client",
            uplinks: [{ link_protocols: ["tcp"] }],
            interests: [
                { key_expr: "r1/**", subscribers: true, queryables: true, tokens: true },
            ],
        } }"#,
    )
    .unwrap();
    let bridge = ztimeout!(crate::open(c)).unwrap();
    let alive = Arc::new(Mutex::new(Vec::<(String, SampleKind)>::new()));
    let _alive = ztimeout!(bridge
        .liveliness()
        .declare_subscriber("r1/alive/**")
        .callback({
            let alive = alive.clone();
            move |sample| {
                alive
                    .lock()
                    .unwrap()
                    .push((sample.key_expr().to_string(), sample.kind()))
            }
        }))
    .unwrap();
    let tokens_of = |name: &str| -> Vec<SampleKind> {
        alive
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.ends_with(name))
            .map(|(_, kind)| *kind)
            .collect()
    };

    let zids = (format!("{zid_prefix}0a"), format!("{zid_prefix}0b"));
    let (a, b) = tokio::join!(
        cloud(&zids.0, ports.0, low_latency),
        cloud(&zids.1, ports.1, low_latency)
    );
    let received = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let mut declared: Vec<Box<dyn std::any::Any>> = vec![];
    for cloud in [&a, &b] {
        let zid = cloud.zid().to_string();
        declared.push(Box::new(
            ztimeout!(cloud.liveliness().declare_token(format!("r1/alive/{zid}"))).unwrap(),
        ));
        declared.push(Box::new(
            ztimeout!(cloud.declare_subscriber("r1/tele").callback({
                let received = received.clone();
                move |sample| {
                    let text = sample.payload().try_to_string().unwrap().to_string();
                    received.lock().unwrap().push((zid.clone(), text))
                }
            }))
            .unwrap(),
        ));
    }
    let received_by = |zid: &str, text: &str| -> usize {
        received
            .lock()
            .unwrap()
            .iter()
            .filter(|(by, got)| by == zid && got == text)
            .count()
    };

    // one of the two owns the north region
    eventually("a north remote", || {
        tokens_of(&zids.0) == [SampleKind::Put] || tokens_of(&zids.1) == [SampleKind::Put]
    })
    .await;
    let a_owns = tokens_of(&zids.0) == [SampleKind::Put];
    let (owner, other) = if a_owns { (a, b) } else { (b, a) };
    let (owner_zid, other_zid) = (owner.zid().to_string(), other.zid().to_string());
    let manager = bridge.static_runtime().unwrap().manager().clone();
    let transport = ztimeout!(manager.get_transport_unicast(&owner.zid().into())).unwrap();
    assert!(ztimeout!(manager.get_transport_unicast(&other.zid().into())).is_none());

    // the last message of the owner is stopped in the read task of the bridge. Its
    // expression starts with a wildcard: the cloud router declares no id for it, the message
    // carries it whole and does not depend on an id that the closed face has forgotten.
    let hold = hold_ingress(
        bridge.zid().into(),
        owner.zid().into(),
        |msg| matches!(msg.body, NetworkBodyRef::Declare(d) if matches!(d.body, DeclareBody::DeclareToken(_))),
    );
    let _late = ztimeout!(owner.liveliness().declare_token("*/alive/late")).unwrap();
    eventually("the last message of the owner is stopped", || {
        hold.holding()
    })
    .await;

    // the bridge closes the transport: its clean-up waits for the read task
    let closing = tokio::spawn(async move { transport.close().await });

    // the other cloud router takes over meanwhile
    eventually("the take-over", || {
        tokens_of(&owner_zid) == [SampleKind::Put, SampleKind::Delete]
            && tokens_of(&other_zid) == [SampleKind::Put]
    })
    .await;
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while received_by(&other_zid, "after the take-over") == 0 {
        assert!(tokio::time::Instant::now() < deadline, "no flow");
        ztimeout!(bridge.put("r1/tele", "after the take-over")).unwrap();
        tokio::time::sleep(SLEEP).await;
    }
    assert!(hold.holding(), "the read task is no longer stopped");
    assert!(
        ztimeout!(manager.get_transport_unicast(&owner.zid().into())).is_some(),
        "the old transport is cleaned up"
    );
    assert!(tokens_of("late").is_empty());

    // the read task goes on, and the clean-up of the old transport ends
    hold.release();
    ztimeout!(closing).unwrap().unwrap();
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while ztimeout!(manager.get_transport_unicast(&owner.zid().into())).is_some() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the old transport stays"
        );
        tokio::time::sleep(SLEEP).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // nothing of the old owner is left
    assert_eq!(
        tokens_of("late"),
        [],
        "low latency {low_latency}: a token of the old owner was declared after its face \
         was closed"
    );
    let alive_now = ztimeout!(bridge.liveliness().get("r1/alive/**")).unwrap();
    let mut alive_keys = vec![];
    while let Ok(reply) = alive_now.recv_async().await {
        alive_keys.push(reply.result().unwrap().key_expr().to_string());
    }
    assert_eq!(alive_keys, [format!("r1/alive/{other_zid}")]);
    // and the new owner lost nothing
    assert_eq!(tokens_of(&other_zid), [SampleKind::Put]);
    ztimeout!(bridge.put("r1/tele", "after the clean-up")).unwrap();
    eventually("the flow with the new owner", || {
        received_by(&other_zid, "after the clean-up") == 1
    })
    .await;
    drop(declared);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leaf_uplink_drops_a_late_token_of_the_old_north_remote_low_latency() {
    a_token_of_the_old_north_remote_comes_late("1eafb0", true).await;
}

/// The same when the bridge closes a universal transport itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leaf_uplink_drops_a_late_token_of_the_old_north_remote_closed_by_the_leaf() {
    a_token_of_the_old_north_remote_comes_late("1eafb1", false).await;
}
