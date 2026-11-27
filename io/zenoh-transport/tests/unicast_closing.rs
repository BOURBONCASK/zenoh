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
#![cfg(all(feature = "transport_tcp", feature = "test"))]

use std::{
    any::Any,
    convert::TryFrom,
    sync::{Arc, Mutex},
    time::Duration,
};

use zenoh_core::ztimeout;
use zenoh_link::{EndPoint, Link};
use zenoh_protocol::{
    core::{WhatAmI, ZenohIdProto},
    network::NetworkMessageMut,
};
use zenoh_result::ZResult;
use zenoh_test::get_free_tcp_port;
use zenoh_transport::{
    multicast::TransportMulticast,
    unicast::{
        test_helpers::{gate_deletions, make_basic_transport_manager_builder},
        TransportUnicast,
    },
    TransportEventHandler, TransportManager, TransportMulticastEventHandler, TransportPeer,
    TransportPeerEventHandler,
};

const TIMEOUT: Duration = Duration::from_secs(60);
const SLEEP: Duration = Duration::from_millis(20);

/// Records the notifications of the end of the transports of a manager, in order.
#[derive(Default)]
struct Recorder {
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl TransportEventHandler for Recorder {
    fn new_unicast(
        &self,
        _peer: TransportPeer,
        _transport: TransportUnicast,
    ) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        Ok(Arc::new(Recorder {
            events: self.events.clone(),
        }))
    }

    fn new_multicast(
        &self,
        _transport: TransportMulticast,
    ) -> ZResult<Arc<dyn TransportMulticastEventHandler>> {
        panic!();
    }
}

impl TransportPeerEventHandler for Recorder {
    fn handle_message(&self, _message: NetworkMessageMut) -> ZResult<()> {
        Ok(())
    }

    fn new_link(&self, _link: Link) {}
    fn del_link(&self, _link: Link) {
        self.events.lock().unwrap().push("del_link");
    }
    fn closing(&self) {
        self.events.lock().unwrap().push("closing");
    }
    fn closed(&self) {
        self.events.lock().unwrap().push("closed");
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Copy, Debug)]
enum Closes {
    /// The remote closes the transport: the node deletes its own on the `Close` it receives.
    Remote,
    /// The node closes the transport itself.
    Local,
}

fn manager(id: u8, lowlatency: bool, handler: Arc<dyn TransportEventHandler>) -> TransportManager {
    TransportManager::builder()
        .whatami(WhatAmI::Router)
        .zid(ZenohIdProto::try_from([id]).unwrap())
        .unicast(make_basic_transport_manager_builder(lowlatency))
        .build_test(handler)
        .unwrap()
}

/// The handler of a transport is told that the transport is closing before the deletion awaits
/// the clean-up of its links and tasks, and that it is closed after. The clean-up is held back
/// for as long as the test wants: `closing` must not wait for it.
async fn closing_is_notified_before_the_clean_up(ids: [u8; 2], lowlatency: bool, closes: Closes) {
    zenoh_util::init_log_from_env_or("error");

    let port = get_free_tcp_port();
    let endpoint: EndPoint = format!("tcp/127.0.0.1:{port}").parse().unwrap();
    let listener = manager(ids[0], lowlatency, Arc::new(Recorder::default()));
    ztimeout!(listener.add_listener(endpoint.clone())).unwrap();

    let events = Arc::new(Mutex::new(vec![]));
    let node_zid = ZenohIdProto::try_from([ids[1]]).unwrap();
    let node = manager(
        ids[1],
        lowlatency,
        Arc::new(Recorder {
            events: events.clone(),
        }),
    );
    let transport = ztimeout!(node.open_transport_unicast(endpoint)).unwrap();
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let remote = loop {
        if let Some(remote) = listener.get_transport_unicast(&node_zid).await {
            break remote;
        }
        assert!(tokio::time::Instant::now() < deadline, "no transport");
        tokio::time::sleep(SLEEP).await;
    };

    let gate = gate_deletions(node_zid);
    let closer = tokio::spawn(async move {
        let _ = match closes {
            Closes::Remote => remote.close().await,
            Closes::Local => transport.close().await,
        };
    });

    // the deletion stops before its clean-up
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while gate.waiting() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{closes:?}: the transport is not being deleted"
        );
        tokio::time::sleep(SLEEP).await;
    }
    tokio::time::sleep(10 * SLEEP).await;
    let seen = events.lock().unwrap().clone();
    assert!(
        seen.contains(&"closing") && !seen.contains(&"closed"),
        "{closes:?}, low latency {lowlatency}: while the clean-up is held back: {seen:?}"
    );

    gate.open();
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while !events.lock().unwrap().contains(&"closed") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{closes:?}: the transport is not closed"
        );
        tokio::time::sleep(SLEEP).await;
    }
    let seen = events.lock().unwrap().clone();
    let position = |event| seen.iter().position(|e| *e == event).unwrap();
    assert!(position("closing") < position("closed"), "{seen:?}");

    ztimeout!(closer).unwrap();
    ztimeout!(listener.close());
    ztimeout!(node.close());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_precedes_the_clean_up_when_the_remote_closes() {
    closing_is_notified_before_the_clean_up([0xc1, 0xc2], false, Closes::Remote).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_precedes_the_clean_up_when_the_node_closes() {
    closing_is_notified_before_the_clean_up([0xc3, 0xc4], false, Closes::Local).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_precedes_the_clean_up_of_a_low_latency_transport_when_the_remote_closes() {
    closing_is_notified_before_the_clean_up([0xc5, 0xc6], true, Closes::Remote).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_precedes_the_clean_up_of_a_low_latency_transport_when_the_node_closes() {
    closing_is_notified_before_the_clean_up([0xc7, 0xc8], true, Closes::Local).await;
}
