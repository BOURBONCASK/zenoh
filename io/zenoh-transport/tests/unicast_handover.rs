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

//! A transport whose link is selected by a hand-over rule, against a peer without rules,
//! through a proxy that forwards only what the test allows.
#![cfg(all(feature = "transport_tcp", feature = "test"))]

use std::{
    any::Any,
    convert::TryFrom,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use zenoh_buffers::buffer::SplitBuffer;
use zenoh_config::HandoverLinkConf;
use zenoh_link::{EndPoint, Link};
use zenoh_protocol::{
    core::{CongestionControl, Priority, WhatAmI, ZenohIdProto},
    network::{
        declare, ext, request, Declare, DeclareBody, DeclareKeyExpr, NetworkBody, NetworkBodyMut,
        NetworkMessage, NetworkMessageMut, Push, Request, RequestId,
    },
    zenoh::{PushBody, Query, RequestBody},
};
use zenoh_result::ZResult;
use zenoh_test::{
    get_free_tcp_port,
    proxy::{Flow, GatedProxy},
};
use zenoh_transport::{
    multicast::TransportMulticast,
    unicast::{
        HandoverRules, RequestLifetime, TransportManagerBuilderUnicast, TransportUnicast,
        Undelivered, UndeliveredReason,
    },
    TransportEventHandler, TransportManager, TransportMulticastEventHandler, TransportPeer,
    TransportPeerEventHandler,
};

const TIMEOUT: Duration = Duration::from_secs(60);
// Lease expiry stays outside the tests: nothing but the sender closes a transport.
const LEASE: Duration = Duration::from_secs(300);
const KEEP_ALIVE: usize = 4;
const LONG: Duration = Duration::from_secs(600);

/// What a handler saw, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    /// A publication: its priority and the number its payload starts with.
    Put(Priority, u64),
    Request(RequestId),
    Declare,
    Undelivered(RequestId, UndeliveredReason),
    Closed,
}

#[derive(Default)]
struct Events(Mutex<Vec<Event>>);

impl Events {
    fn push(&self, event: Event) {
        self.0.lock().unwrap().push(event);
    }

    fn all(&self) -> Vec<Event> {
        self.0.lock().unwrap().clone()
    }

    fn puts(&self, priority: Priority) -> Vec<u64> {
        self.all()
            .into_iter()
            .filter_map(|e| match e {
                Event::Put(p, n) if p == priority => Some(n),
                _ => None,
            })
            .collect()
    }

    async fn wait(&self, what: &str, condition: impl Fn(&[Event]) -> bool) {
        let start = Instant::now();
        while !condition(&self.0.lock().unwrap()) {
            assert!(
                start.elapsed() < TIMEOUT,
                "timed out waiting until {what}: {:?}",
                self.all().iter().rev().take(8).collect::<Vec<_>>()
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}

struct Handler(Arc<Events>);

impl TransportEventHandler for Handler {
    fn new_unicast(
        &self,
        _peer: TransportPeer,
        _transport: TransportUnicast,
    ) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        Ok(Arc::new(Handler(self.0.clone())))
    }

    fn new_multicast(
        &self,
        _transport: TransportMulticast,
    ) -> ZResult<Arc<dyn TransportMulticastEventHandler>> {
        panic!();
    }
}

impl TransportPeerEventHandler for Handler {
    fn handle_message(&self, msg: NetworkMessageMut) -> ZResult<()> {
        let priority = zenoh_protocol::network::NetworkMessageExt::priority(&msg);
        match msg.body {
            NetworkBodyMut::Push(push) => {
                if let PushBody::Put(put) = &push.payload {
                    let bytes = put.payload.contiguous();
                    let mut n = [0_u8; 8];
                    n.copy_from_slice(&bytes[..8]);
                    self.0.push(Event::Put(priority, u64::from_le_bytes(n)));
                }
            }
            NetworkBodyMut::Request(request) => self.0.push(Event::Request(request.id)),
            NetworkBodyMut::Declare(_) => self.0.push(Event::Declare),
            _ => {}
        }
        Ok(())
    }

    fn new_link(&self, _link: Link) {}
    fn del_link(&self, _link: Link) {}

    fn closed(&self) {
        self.0.push(Event::Closed);
    }

    fn handle_undelivered(&self, undelivered: Vec<Undelivered>) {
        for u in undelivered {
            self.0.push(Event::Undelivered(u.rid, u.reason));
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn put(priority: Priority, cc: CongestionControl, n: u64, size: usize) -> NetworkMessage {
    let mut payload = vec![0_u8; size.max(8)];
    payload[..8].copy_from_slice(&n.to_le_bytes());
    NetworkMessage::from(Push {
        wire_expr: "test".into(),
        ext_qos: ext::QoSType::new(priority, cc, false),
        ..Push::from(payload)
    })
}

fn query(rid: RequestId, priority: Priority) -> NetworkMessage {
    NetworkMessage::from(NetworkBody::Request(Request {
        id: rid,
        wire_expr: "q".into(),
        ext_qos: ext::QoSType::new(priority, CongestionControl::Block, false),
        ext_tstamp: None,
        ext_nodeid: request::ext::NodeIdType::DEFAULT,
        ext_target: Default::default(),
        ext_budget: None,
        ext_timeout: None,
        ext_ts_stack: None,
        payload: RequestBody::Query(Query::default()),
    }))
}

fn declare_key() -> NetworkMessage {
    NetworkMessage::from(NetworkBody::Declare(Declare {
        interest_id: None,
        ext_qos: declare::ext::QoSType::DECLARE,
        ext_tstamp: None,
        ext_nodeid: declare::ext::NodeIdType::DEFAULT,
        body: DeclareBody::DeclareKeyExpr(DeclareKeyExpr {
            id: 1,
            wire_expr: "some/key".into(),
        }),
    }))
}

fn rule() -> HandoverLinkConf {
    HandoverLinkConf {
        id: Some("test".into()),
        link_protocols: vec!["tcp".into()],
        interfaces: vec![],
        data_bytes: 256 * 1024,
        large_bytes: 1024 * 1024,
        max_age_ms: None,
        reserve_min_bytes: 64 * 1024,
        reserve_max_bytes: 1024 * 1024,
        control_max_age_ms: None,
    }
}

/// Makes the manager listen on a free loopback port. Tests run side by side: a port that was
/// free a moment ago may be taken, so this tries again.
async fn listen(manager: &TransportManager) -> u16 {
    for _ in 0..50 {
        let port = get_free_tcp_port();
        let endpoint: EndPoint = format!("tcp/127.0.0.1:{port}").parse().unwrap();
        if let Ok(Ok(_)) = tokio::time::timeout(TIMEOUT, manager.add_listener(endpoint)).await {
            return port;
        }
    }
    panic!("no free port to listen on");
}

struct Pair {
    listener: TransportManager,
    connector: TransportManager,
    proxy: GatedProxy,
    transport: TransportUnicast,
    rules: Arc<HandoverRules>,
    /// What the peer without rules received.
    received: Arc<Events>,
    /// What the handler of the selected transport was told.
    told: Arc<Events>,
}

/// A listener without rules, and a connector whose link to it, through the proxy, is selected
/// by `rule`.
async fn pair(rule: HandoverLinkConf, wait_before_close: Duration) -> Pair {
    zenoh_util::init_log_from_env_or("error");
    let received = Arc::new(Events::default());
    let told = Arc::new(Events::default());
    let unicast = || {
        TransportManagerBuilderUnicast::default()
            .qos(true)
            .lease(LEASE)
            .keep_alive(KEEP_ALIVE)
    };
    let listener = TransportManager::builder()
        .whatami(WhatAmI::Router)
        .zid(ZenohIdProto::try_from([1]).unwrap())
        .unicast(unicast())
        .build_test(Arc::new(Handler(received.clone())))
        .unwrap();
    let port = listen(&listener).await;
    let proxy = GatedProxy::spawn(format!("127.0.0.1:{port}")).await;
    let rules = HandoverRules::new(
        256 * 1024 * 1024,
        &[rule],
        wait_before_close,
        Duration::from_secs(10),
    )
    .unwrap();
    let connector = TransportManager::builder()
        .whatami(WhatAmI::Router)
        .zid(ZenohIdProto::try_from([2]).unwrap())
        .unicast(unicast())
        .wait_before_close(wait_before_close)
        .handover(Some(rules.clone()))
        .build_test(Arc::new(Handler(told.clone())))
        .unwrap();
    let endpoint: EndPoint = proxy.endpoint().parse().unwrap();
    let transport = tokio::time::timeout(TIMEOUT, connector.open_transport_unicast(endpoint))
        .await
        .unwrap()
        .unwrap();
    Pair {
        listener,
        connector,
        proxy,
        transport,
        rules,
        received,
        told,
    }
}

impl Pair {
    fn schedule(&self, mut msg: NetworkMessage) -> bool {
        self.transport.schedule(msg.as_mut()).unwrap_or(false)
    }

    /// Blocking publications of `size` bytes at `priority`, numbered from `first`, until one
    /// is refused. Returns how many were taken. Runs on the calling thread: a call that
    /// waited for the link would hold the test.
    fn fill(&self, priority: Priority, first: u64, size: usize) -> u64 {
        for n in first.. {
            if !self.schedule(put(priority, CongestionControl::Block, n, size)) {
                return n - first;
            }
            assert!(n - first < 1_000_000, "nothing is ever refused");
        }
        unreachable!()
    }

    /// Blocking publications until one is refused, and then until the link takes nothing
    /// more: for a moment after the proxy stops forwarding, the sockets still absorb what the
    /// sender writes. Afterwards a write is pending, and what is scheduled waits behind it.
    async fn fill_until_stuck(&self, priority: Priority, first: u64, size: usize) {
        self.fill(priority, first, size);
        let sent = || self.transport.get_handover_status().unwrap().unwrap().sent;
        let (mut last, mut since) = (sent(), Instant::now());
        while since.elapsed() < Duration::from_millis(300) {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if sent() != last {
                (last, since) = (sent(), Instant::now());
            }
        }
    }

    async fn close(self) {
        let _ = tokio::time::timeout(TIMEOUT, self.connector.close()).await;
        let _ = tokio::time::timeout(TIMEOUT, self.listener.close()).await;
    }
}

// A link that stopped forwarding does not hold the threads that schedule on it, whatever the
// congestion control of their messages; what its budgets cannot take is refused; the
// transport stays; and when the link forwards again everything taken arrives, in order.
//
// Without a rule the first blocking publication that finds the queue full waits for
// `wait_before_close` (ten minutes here) and then the transport is closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_selected_link_does_not_hold_its_callers() {
    let pair = pair(rule(), LONG).await;
    for n in 0..10 {
        assert!(pair.schedule(put(Priority::Data, CongestionControl::Block, n, 64)));
    }
    pair.received
        .wait("the link works", |e| e.len() == 10)
        .await;

    pair.proxy.stall();
    // Far more than the sockets hold: the link is writing and cannot go on.
    let taken = pair.fill(Priority::Data, 10, 1_000);
    assert!(taken >= 200, "{taken}");
    // The budget of a priority is its own: another priority is still taken.
    assert!(pair.schedule(put(Priority::RealTime, CongestionControl::Block, 0, 64)));
    // Declarations have the reserve.
    assert!(pair.schedule(declare_key()));
    // A droppable publication is dropped as today: the queue of its priority is full.
    assert!(!pair.schedule(put(Priority::Data, CongestionControl::Drop, 0, 64)));
    // A refused blocking publication does not close the transport.
    assert_eq!(pair.connector.get_transports_unicast().await.len(), 1);
    assert_eq!(pair.told.all(), []);

    pair.proxy.open();
    pair.received
        .wait("everything taken arrives", |e| {
            e.len() as u64 == 10 + taken + 2
        })
        .await;
    assert_eq!(
        pair.received.puts(Priority::Data),
        (0..10 + taken).collect::<Vec<_>>()
    );
    assert_eq!(pair.received.puts(Priority::RealTime), [0]);
    assert!(pair.received.all().contains(&Event::Declare));
    pair.close().await;
}

// A selected link on which no write completes for `wait_before_close` closes its transport,
// once. A query that was taken and not started is reported to the handler before the handler
// learns that the transport is closed; everything the link held is given back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_selected_link_without_progress_closes_its_transport_once() {
    let pair = pair(
        HandoverLinkConf {
            data_bytes: 16 * 1024 * 1024,
            ..rule()
        },
        Duration::from_millis(3_000),
    )
    .await;
    assert!(pair.schedule(put(Priority::Data, CongestionControl::Block, 0, 64)));
    pair.received.wait("the link works", |e| e.len() == 1).await;
    pair.proxy.stall();
    // More than the sockets between the sender and the proxy can absorb: a write is pending.
    pair.fill_until_stuck(Priority::Data, 1, 60_000).await;
    let lifetime = RequestLifetime {
        expiry: Instant::now() + LONG,
        branch: None,
    };
    let mut request = query(7, Priority::RealTime);
    assert!(pair
        .transport
        .schedule_request(request.as_mut(), &lifetime)
        .unwrap());

    pair.told
        .wait("the transport is closed", |e| e.contains(&Event::Closed))
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        pair.told.all(),
        [
            Event::Undelivered(7, UndeliveredReason::NotDelivered),
            Event::Closed
        ]
    );
    assert!(pair.connector.get_transports_unicast().await.is_empty());
    assert_eq!(
        pair.rules.closes().collect::<Vec<_>>(),
        [
            ("no_progress", 1),
            ("control_age", 0),
            ("reserve_overflow", 0),
            ("key_id_reuse", 0)
        ]
    );
    assert_eq!(pair.rules.pool_usage().0, 0);
    // After the close nothing is taken, and nothing waits.
    assert!(!pair.schedule(put(Priority::Data, CongestionControl::Block, 0, 64)));
    assert!(!pair.received.all().contains(&Event::Request(7)));
    pair.close().await;
}

// A transport that ends for a reason of its own (here its peer closes it) while its sender
// holds a query that was not started: the handler is told about the query, and then that the
// transport is closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transport_closed_by_its_peer_reports_unsent_queries_before_closed() {
    let pair = pair(
        HandoverLinkConf {
            data_bytes: 16 * 1024 * 1024,
            ..rule()
        },
        LONG,
    )
    .await;
    assert!(pair.schedule(put(Priority::Data, CongestionControl::Block, 0, 64)));
    pair.received.wait("the link works", |e| e.len() == 1).await;
    // The selected side cannot send; it still receives.
    pair.proxy.stall_flow(Flow::Up);
    pair.fill_until_stuck(Priority::Data, 1, 60_000).await;
    let lifetime = RequestLifetime {
        expiry: Instant::now() + LONG,
        branch: None,
    };
    let mut request = query(7, Priority::RealTime);
    assert!(pair
        .transport
        .schedule_request(request.as_mut(), &lifetime)
        .unwrap());
    assert_eq!(pair.told.all(), []);

    let _ = tokio::time::timeout(TIMEOUT, pair.listener.close()).await;
    pair.told
        .wait("the transport is closed", |e| e.contains(&Event::Closed))
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        pair.told.all(),
        [
            Event::Undelivered(7, UndeliveredReason::NotDelivered),
            Event::Closed
        ]
    );
    assert_eq!(pair.rules.pool_usage().0, 0);
    // The sender did not decide this close.
    assert!(pair.rules.closes().all(|(_, count)| count == 0));
    let _ = tokio::time::timeout(TIMEOUT, pair.connector.close()).await;
}

// A query that is not started before the end of its routing branch, or within the age limit
// of the link, is never sent: its handler is told, the transport stays, and the peer does not
// see it when the link forwards again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_query_that_expires_on_a_stalled_link_is_reported_and_not_sent() {
    let pair = pair(
        HandoverLinkConf {
            data_bytes: 16 * 1024 * 1024,
            max_age_ms: Some(400),
            ..rule()
        },
        LONG,
    )
    .await;
    assert!(pair.schedule(put(Priority::Data, CongestionControl::Block, 0, 64)));
    pair.received.wait("the link works", |e| e.len() == 1).await;
    pair.proxy.stall();
    pair.fill_until_stuck(Priority::Data, 1, 60_000).await;

    // Its branch ends before the age limit of the link.
    let mut early = query(1, Priority::RealTime);
    let lifetime = RequestLifetime {
        expiry: Instant::now() + Duration::from_millis(50),
        branch: None,
    };
    assert!(pair
        .transport
        .schedule_request(early.as_mut(), &lifetime)
        .unwrap());
    // Its branch ends late: the age limit of the link applies.
    let mut late = query(2, Priority::RealTime);
    let lifetime = RequestLifetime {
        expiry: Instant::now() + LONG,
        branch: None,
    };
    assert!(pair
        .transport
        .schedule_request(late.as_mut(), &lifetime)
        .unwrap());

    pair.told
        .wait("both queries are reported", |e| e.len() >= 2)
        .await;
    assert_eq!(
        pair.told.all(),
        [
            Event::Undelivered(1, UndeliveredReason::Expired),
            Event::Undelivered(2, UndeliveredReason::Expired)
        ]
    );
    assert_eq!(pair.connector.get_transports_unicast().await.len(), 1);

    // The link forwards again: a publication scheduled now arrives, the queries do not.
    pair.proxy.open();
    assert!(pair.schedule(put(Priority::RealTime, CongestionControl::Block, 99, 64)));
    pair.received
        .wait("the link works again", |e| {
            e.contains(&Event::Put(Priority::RealTime, 99))
        })
        .await;
    assert!(!pair
        .received
        .all()
        .iter()
        .any(|e| matches!(e, Event::Request(_))));
    pair.close().await;
}

// Closing a selected transport writes what was scheduled before the close, whole messages and
// fragmented ones, and then the Close message.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_orderly_close_writes_what_was_scheduled() {
    let pair = pair(
        HandoverLinkConf {
            data_bytes: 4 * 1024 * 1024,
            large_bytes: 32 * 1024 * 1024,
            ..rule()
        },
        Duration::from_secs(30),
    )
    .await;
    let sizes = [64, 100_000, 8, 300_000, 1_000];
    for n in 0..200_u64 {
        let size = sizes[n as usize % sizes.len()];
        assert!(pair.schedule(put(Priority::Data, CongestionControl::Block, n, size)));
    }
    tokio::time::timeout(TIMEOUT, pair.transport.close())
        .await
        .unwrap()
        .unwrap();
    pair.received
        .wait("the peer sees the close", |e| e.contains(&Event::Closed))
        .await;
    assert_eq!(
        pair.received.puts(Priority::Data),
        (0..200).collect::<Vec<_>>()
    );
    assert_eq!(pair.received.all().last(), Some(&Event::Closed));
    assert_eq!(pair.rules.pool_usage().0, 0);
    // An orderly close reports nothing as lost.
    assert_eq!(pair.told.all(), [Event::Closed]);
    pair.close().await;
}

// A link that a rule selects and that the sender does not support is refused when it is
// added, and counted: it never runs on the queues that the rule was meant to replace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_selected_link_without_qos_is_refused() {
    zenoh_util::init_log_from_env_or("error");
    let events = Arc::new(Events::default());
    let unicast = || {
        TransportManagerBuilderUnicast::default()
            .qos(false)
            .lease(LEASE)
            .keep_alive(KEEP_ALIVE)
    };
    let listener = TransportManager::builder()
        .whatami(WhatAmI::Router)
        .zid(ZenohIdProto::try_from([1]).unwrap())
        .unicast(unicast())
        .build_test(Arc::new(Handler(events.clone())))
        .unwrap();
    let port = listen(&listener).await;
    let endpoint: EndPoint = format!("tcp/127.0.0.1:{port}").parse().unwrap();
    let rules = HandoverRules::new(
        64 * 1024 * 1024,
        &[rule()],
        Duration::from_secs(5),
        Duration::from_secs(10),
    )
    .unwrap();
    let connector = TransportManager::builder()
        .whatami(WhatAmI::Router)
        .zid(ZenohIdProto::try_from([2]).unwrap())
        .unicast(unicast())
        .handover(Some(rules.clone()))
        .build_test(Arc::new(Handler(events.clone())))
        .unwrap();
    let res = tokio::time::timeout(TIMEOUT, connector.open_transport_unicast(endpoint))
        .await
        .unwrap();
    assert!(res.is_err());
    assert!(connector.get_transports_unicast().await.is_empty());
    assert_eq!(
        rules.refusals().find(|(reason, _)| *reason == "qos"),
        Some(("qos", 1))
    );
    assert_eq!(rules.pool_usage().0, 0);
    let _ = tokio::time::timeout(TIMEOUT, connector.close()).await;
    let _ = tokio::time::timeout(TIMEOUT, listener.close()).await;
}
