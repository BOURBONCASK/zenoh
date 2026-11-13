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
#![cfg(feature = "transport_tcp")]

use std::{
    any::Any,
    convert::TryFrom,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use zenoh_core::ztimeout;
use zenoh_link::{EndPoint, Link};
use zenoh_protocol::{
    core::{CongestionControl, Priority, WhatAmI, ZenohIdProto},
    network::{push::ext::QoSType, NetworkMessage, NetworkMessageMut, Push},
};
use zenoh_result::ZResult;
use zenoh_test::get_free_tcp_port;
use zenoh_transport::{
    multicast::TransportMulticast,
    unicast::{TransportManagerBuilderUnicast, TransportUnicast},
    DummyTransportPeerEventHandler, TransportEventHandler, TransportManager,
    TransportMulticastEventHandler, TransportPeer, TransportPeerEventHandler,
};

const TIMEOUT: Duration = Duration::from_secs(60);
const WAIT_BEFORE_CLOSE: Duration = Duration::from_secs(1);
// Allow for the final TX flush and the default 10-second TCP linger on Linux.
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);
// Keep lease expiry outside the test deadline so it cannot close the transport by itself.
// A short keep-alive bounds the final TX flush during teardown.
const LEASE: Duration = Duration::from_secs(120);
const KEEP_ALIVE: usize = 120;
// A second close task finishes right behind the first one: both wait for the same links.
const SETTLE: Duration = Duration::from_secs(2);

#[derive(Default)]
struct SHDummy;

impl TransportEventHandler for SHDummy {
    fn new_unicast(
        &self,
        _peer: TransportPeer,
        _transport: TransportUnicast,
    ) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        Ok(Arc::new(DummyTransportPeerEventHandler))
    }

    fn new_multicast(
        &self,
        _transport: TransportMulticast,
    ) -> ZResult<Arc<dyn TransportMulticastEventHandler>> {
        panic!();
    }
}

/// Counts how many times the transports of a manager report that they are closed.
#[derive(Default)]
struct SHCountClosed {
    closed: Arc<AtomicUsize>,
}

impl TransportEventHandler for SHCountClosed {
    fn new_unicast(
        &self,
        _peer: TransportPeer,
        _transport: TransportUnicast,
    ) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        Ok(Arc::new(CountClosed {
            closed: self.closed.clone(),
        }))
    }

    fn new_multicast(
        &self,
        _transport: TransportMulticast,
    ) -> ZResult<Arc<dyn TransportMulticastEventHandler>> {
        panic!();
    }
}

struct CountClosed {
    closed: Arc<AtomicUsize>,
}

impl TransportPeerEventHandler for CountClosed {
    fn handle_message(&self, _message: NetworkMessageMut) -> ZResult<()> {
        Ok(())
    }

    fn new_link(&self, _link: Link) {}
    fn del_link(&self, _link: Link) {}

    fn closed(&self) {
        self.closed.fetch_add(1, Ordering::SeqCst);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A TCP proxy from `proxy_addr` to `target_addr` that stops reading from both sides
/// once `stalled` is set: the connection stays established and nothing is consumed.
async fn spawn_stallable_proxy(
    proxy_addr: String,
    target_addr: String,
    stalled: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    let listener = TcpListener::bind(proxy_addr).await.unwrap();
    tokio::spawn(async move {
        let (downstream, _) = listener.accept().await.unwrap();
        let upstream = TcpStream::connect(target_addr).await.unwrap();
        let (down_rx, down_tx) = downstream.into_split();
        let (up_rx, up_tx) = upstream.into_split();

        async fn pump(
            mut rx: tokio::net::tcp::OwnedReadHalf,
            mut tx: tokio::net::tcp::OwnedWriteHalf,
            stalled: Arc<AtomicBool>,
        ) {
            let mut buf = [0u8; 16384];
            loop {
                if stalled.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                match rx.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if tx.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }

        tokio::join!(
            pump(down_rx, up_tx, stalled.clone()),
            pump(up_rx, down_tx, stalled),
        );
    })
}

fn make_message(payload_size: usize, priority: Priority) -> NetworkMessage {
    NetworkMessage::from(Push {
        wire_expr: "test".into(),
        ext_qos: QoSType::new(priority, CongestionControl::Block, false),
        ..Push::from(vec![0u8; payload_size])
    })
}

/// Pushes non droppable messages of `priority` until one of them is not accepted, and
/// returns how long that push took and what it returned.
fn push_until_refused(
    transport: TransportUnicast,
    priority: Priority,
) -> (Duration, ZResult<bool>) {
    for _ in 0..10_000 {
        let mut msg = make_message(60_000, priority);
        let start = Instant::now();
        let res = transport.schedule(msg.as_mut());
        if !matches!(res, Ok(true)) {
            return (start.elapsed(), res);
        }
    }
    panic!("no push was refused: the peer did not stall?");
}

/// With QoS every priority has its own queue, so producers of different priorities wait
/// for a batch at the same time and each of them can use up `wait_before_close` on a
/// peer that stopped reading. The transport must still be torn down once: one close
/// task, one `closed()` notification to the handler.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unresponsive_transport_is_closed_once() {
    zenoh_util::init_log_from_env_or("error");

    // Listener side (the peer that will become unresponsive)
    let listener_port = get_free_tcp_port();
    let listener_manager = TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(ZenohIdProto::try_from([1]).unwrap())
        .unicast(
            TransportManagerBuilderUnicast::default()
                .qos(true)
                .lease(LEASE)
                .keep_alive(KEEP_ALIVE),
        )
        .build_test(Arc::new(SHDummy))
        .unwrap();
    let listen_endpoint: EndPoint = format!("tcp/127.0.0.1:{listener_port}").parse().unwrap();
    ztimeout!(listener_manager.add_listener(listen_endpoint)).unwrap();

    // Stallable TCP proxy in front of the listener
    let proxy_port = get_free_tcp_port();
    let stalled = Arc::new(AtomicBool::new(false));
    let proxy = spawn_stallable_proxy(
        format!("127.0.0.1:{proxy_port}"),
        format!("127.0.0.1:{listener_port}"),
        stalled.clone(),
    )
    .await;

    // Connecting side
    let closed = Arc::new(AtomicUsize::new(0));
    let connect_manager = TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(ZenohIdProto::try_from([2]).unwrap())
        .unicast(
            TransportManagerBuilderUnicast::default()
                .qos(true)
                .lease(LEASE)
                .keep_alive(KEEP_ALIVE),
        )
        .wait_before_close(WAIT_BEFORE_CLOSE)
        .build_test(Arc::new(SHCountClosed {
            closed: closed.clone(),
        }))
        .unwrap();
    let connect_endpoint: EndPoint = format!("tcp/127.0.0.1:{proxy_port}").parse().unwrap();
    let transport = ztimeout!(connect_manager.open_transport_unicast(connect_endpoint)).unwrap();

    // Two priorities, hence two queues of the same link
    let priorities = [Priority::Data, Priority::DataLow];
    for priority in priorities {
        assert!(transport
            .schedule(make_message(64, priority).as_mut())
            .unwrap());
    }

    // Make the peer unresponsive, then fill both queues at the same time
    stalled.store(true, Ordering::Relaxed);
    let pushers = priorities.map(|priority| {
        let transport = transport.clone();
        tokio::task::spawn_blocking(move || push_until_refused(transport, priority))
    });
    for (pusher, priority) in pushers.into_iter().zip(priorities) {
        let (elapsed, res) = ztimeout!(pusher).unwrap();
        // What is under test needs both producers to give up after a whole budget of
        // their own. A producer that is refused early did not get to wait for a batch
        // before the other one gave up, and the run shows nothing.
        assert!(
            matches!(res, Ok(false)) && elapsed >= WAIT_BEFORE_CLOSE,
            "the {priority:?} producer did not use up wait_before_close: {res:?} after {elapsed:?}"
        );
    }

    // Wait for the teardown, and then for a second close task, if there is one, to
    // go through it as well.
    tokio::time::timeout(CLEANUP_TIMEOUT, async {
        while !connect_manager.get_transports_unicast().await.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the unresponsive transport was not removed from the manager");
    let settle = Instant::now();
    while closed.load(Ordering::SeqCst) < 2 && settle.elapsed() < SETTLE {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        closed.load(Ordering::SeqCst),
        1,
        "the transport was closed more than once"
    );

    proxy.abort();
    let _ = proxy.await;
    ztimeout!(connect_manager.close());
    ztimeout!(listener_manager.close());
}
