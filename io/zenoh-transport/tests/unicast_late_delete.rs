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
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
    time::{Duration, Instant},
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
    multicast::TransportMulticast, unicast::TransportUnicast, DummyTransportPeerEventHandler,
    TransportEventHandler, TransportManager, TransportMulticastEventHandler, TransportPeer,
    TransportPeerEventHandler,
};

const TIMEOUT: Duration = Duration::from_secs(60);

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

/// Gives the first transport of a manager a handler that holds every `closed()`
/// notification until the test lets it go. Later transports get a dummy handler.
struct SHGateClosed {
    first: Mutex<Option<Arc<GateClosed>>>,
}

impl TransportEventHandler for SHGateClosed {
    fn new_unicast(
        &self,
        _peer: TransportPeer,
        _transport: TransportUnicast,
    ) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        match self.first.lock().unwrap().take() {
            Some(gate) => Ok(gate),
            None => Ok(Arc::new(DummyTransportPeerEventHandler)),
        }
    }

    fn new_multicast(
        &self,
        _transport: TransportMulticast,
    ) -> ZResult<Arc<dyn TransportMulticastEventHandler>> {
        panic!();
    }
}

struct GateClosed {
    calls: AtomicUsize,
    entered: Mutex<mpsc::Sender<usize>>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl TransportPeerEventHandler for GateClosed {
    fn handle_message(&self, _message: NetworkMessageMut) -> ZResult<()> {
        Ok(())
    }

    fn new_link(&self, _link: Link) {}
    fn del_link(&self, _link: Link) {}

    fn closed(&self) {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let _ = self.entered.lock().unwrap().send(call);
        let _ = self.release.lock().unwrap().recv_timeout(TIMEOUT);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Waits for the next `closed()` notification of the gated transport and returns its index.
async fn next_closed(entered: &Arc<Mutex<mpsc::Receiver<usize>>>) -> usize {
    let entered = entered.clone();
    ztimeout!(tokio::task::spawn_blocking(move || entered
        .lock()
        .unwrap()
        .recv_timeout(TIMEOUT)))
    .unwrap()
    .expect("no teardown reached its closed() notification")
}

/// Two teardowns of the same transport run one behind the other (here: two `close()`
/// calls; in the field also a link failure next to a close, or two producers giving up
/// on an unresponsive peer). The first one unregisters the transport, the peer connects
/// again, and only then the second one gets to its last step. That step must not
/// unregister the transport of the new connection, which has the same peer id.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_delete_keeps_reconnected_transport() {
    zenoh_util::init_log_from_env_or("error");

    let listener_zid = ZenohIdProto::try_from([1]).unwrap();
    let listener_port = get_free_tcp_port();
    let listener_manager = TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(listener_zid)
        .build_test(Arc::new(SHDummy))
        .unwrap();
    let endpoint: EndPoint = format!("tcp/127.0.0.1:{listener_port}").parse().unwrap();
    ztimeout!(listener_manager.add_listener(endpoint.clone())).unwrap();

    let (entered_tx, entered_rx) = mpsc::channel();
    let entered = Arc::new(Mutex::new(entered_rx));
    let (release_tx, release_rx) = mpsc::channel();
    let connect_manager = TransportManager::builder()
        .whatami(WhatAmI::Peer)
        .zid(ZenohIdProto::try_from([2]).unwrap())
        .build_test(Arc::new(SHGateClosed {
            first: Mutex::new(Some(Arc::new(GateClosed {
                calls: AtomicUsize::new(0),
                entered: Mutex::new(entered_tx),
                release: Mutex::new(release_rx),
            }))),
        }))
        .unwrap();
    let old = ztimeout!(connect_manager.open_transport_unicast(endpoint.clone())).unwrap();

    // First teardown: stopped in its `closed()` notification. The old transport is
    // still registered and its status is locked.
    let first = tokio::spawn({
        let old = old.clone();
        async move {
            let _ = old.close().await;
        }
    });
    assert_eq!(next_closed(&entered).await, 0);

    // Second teardown: it has taken hold of the old transport and waits for its status.
    let mut second = Box::pin(async move {
        let _ = old.close().await;
    });
    assert!(futures::poll!(second.as_mut()).is_pending());

    // The first teardown finishes and unregisters the old transport. The second one
    // stops in its own `closed()` notification, right before its last step.
    release_tx.send(()).unwrap();
    ztimeout!(first).unwrap();
    let second = tokio::spawn(second);
    assert_eq!(next_closed(&entered).await, 1);

    // The peer connects again. Retry: the listener may still be closing its side.
    assert!(connect_manager
        .get_transport_unicast(&listener_zid)
        .await
        .is_none());
    let deadline = Instant::now() + TIMEOUT;
    let new = loop {
        assert!(Instant::now() < deadline, "could not connect again");
        if let Ok(Ok(transport)) = tokio::time::timeout(
            Duration::from_secs(5),
            connect_manager.open_transport_unicast(endpoint.clone()),
        )
        .await
        {
            break transport;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(connect_manager
        .get_transport_unicast(&listener_zid)
        .await
        .is_some());

    // Let the second teardown of the old transport finish.
    release_tx.send(()).unwrap();
    ztimeout!(second).unwrap();

    assert!(
        connect_manager
            .get_transport_unicast(&listener_zid)
            .await
            .is_some(),
        "the late teardown of the old transport unregistered the new one"
    );
    assert!(
        new.get_zid().is_ok(),
        "the handle of the new transport is dead"
    );

    ztimeout!(connect_manager.close());
    ztimeout!(listener_manager.close());
}
