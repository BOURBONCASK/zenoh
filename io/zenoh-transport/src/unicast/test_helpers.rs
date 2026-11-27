//
// Copyright (c) 2023 ZettaScale Technology
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
use std::{
    fmt::{self, DebugStruct},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use tokio::sync::MutexGuard as AsyncMutexGuard;
use zenoh_core::zcondfeat;
use zenoh_link::Link;
use zenoh_protocol::{
    core::{Bound, RegionName, WhatAmI, ZenohIdProto},
    network::{NetworkBody, NetworkBodyMut, NetworkMessage, NetworkMessageMut},
    transport::TransportSn,
};
use zenoh_result::ZResult;

use crate::{
    unicast::{
        authentication::TransportAuthId,
        link::LinkUnicastWithOpenAck,
        transport_unicast_inner::{AddLinkResult, TransportStatus, TransportUnicastTrait},
        TransportConfigUnicast, TransportManagerBuilderUnicast, TransportUnicast,
    },
    TransportManager, TransportPeerEventHandler,
};

/// A minimal stub implementation of `TransportUnicastTrait` for use in routing tests.
///
/// All methods panic with `unimplemented!()` except `get_zid`, `get_whatami`, and `schedule`.
/// `schedule` converts each incoming [`NetworkMessageMut`] to an owned [`NetworkMessage`] and
/// calls the `on_schedule` callback provided at construction time, allowing tests to observe
/// every message that the routing layer sends to this face without a real network connection.
pub struct MockTransportUnicastInner {
    zid: ZenohIdProto,
    whatami: WhatAmI,
    on_schedule: Arc<dyn Fn(NetworkMessage) + Send + Sync>,
}

impl fmt::Debug for MockTransportUnicastInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockTransportUnicastInner")
            .field("zid", &self.zid)
            .field("whatami", &self.whatami)
            .field("on_schedule", &"..")
            .finish()
    }
}

#[async_trait]
impl TransportUnicastTrait for MockTransportUnicastInner {
    fn set_callback(&self, _callback: Arc<dyn TransportPeerEventHandler>) {}

    async fn get_status(&self) -> AsyncMutexGuard<'_, TransportStatus> {
        unimplemented!("MockTransportUnicastInner::get_status")
    }

    fn get_zid(&self) -> ZenohIdProto {
        self.zid
    }

    fn get_whatami(&self) -> WhatAmI {
        self.whatami
    }

    fn get_local_whatami(&self) -> WhatAmI {
        unimplemented!("MockTransportUnicastInner::get_local_whatami")
    }

    fn get_callback(&self) -> Option<Arc<dyn TransportPeerEventHandler>> {
        unimplemented!("MockTransportUnicastInner::get_callback")
    }

    fn get_links(&self) -> Vec<Link> {
        vec![]
    }

    fn get_auth_ids(&self) -> TransportAuthId {
        unimplemented!("MockTransportUnicastInner::get_auth_ids")
    }

    #[cfg(feature = "shared-memory")]
    fn is_shm(&self) -> bool {
        false
    }

    fn is_qos(&self) -> bool {
        false
    }

    fn region_name(&self) -> Option<RegionName> {
        None
    }

    fn get_bound(&self) -> Option<Bound> {
        None
    }

    fn get_config(&self) -> &TransportConfigUnicast {
        unimplemented!("MockTransportUnicastInner::get_config")
    }

    #[cfg(feature = "stats")]
    fn stats(&self) -> zenoh_stats::TransportStats {
        unimplemented!("MockTransportUnicastInner::stats")
    }

    async fn add_link(
        &self,
        _link: LinkUnicastWithOpenAck,
        _other_initial_sn: TransportSn,
        _other_lease: Duration,
    ) -> AddLinkResult {
        unimplemented!("MockTransportUnicastInner::add_link")
    }

    fn schedule(&self, msg: NetworkMessageMut) -> ZResult<bool> {
        let body = match msg.body {
            NetworkBodyMut::Push(p) => NetworkBody::Push(p.clone()),
            NetworkBodyMut::Request(r) => NetworkBody::Request(r.clone()),
            NetworkBodyMut::Response(r) => NetworkBody::Response(r.clone()),
            NetworkBodyMut::ResponseFinal(r) => NetworkBody::ResponseFinal(r.clone()),
            NetworkBodyMut::Interest(i) => NetworkBody::Interest(i.clone()),
            NetworkBodyMut::Declare(d) => NetworkBody::Declare(d.clone()),
            NetworkBodyMut::OAM(o) => NetworkBody::OAM(o.clone()),
        };
        (self.on_schedule)(NetworkMessage {
            body,
            reliability: msg.reliability,
        });
        Ok(true)
    }

    async fn close(&self, _reason: u8) -> ZResult<()> {
        Ok(())
    }

    async fn close_link(&self, _link: Link) -> ZResult<()> {
        Ok(())
    }

    fn add_debug_fields<'a, 'b: 'a, 'c>(
        &self,
        s: &'c mut DebugStruct<'a, 'b>,
    ) -> &'c mut DebugStruct<'a, 'b> {
        s
    }
}

/// Creates a [`TransportUnicast`] backed by a [`MockTransportUnicastInner`].
///
/// Every message that the routing layer sends to this face is converted to an owned
/// [`NetworkMessage`] and forwarded to `on_schedule`.  The caller can use this callback to
/// record or assert on outgoing messages without a real network connection.
///
/// The caller must keep the returned `Arc<MockTransportUnicastInner>` alive for the duration of
/// any operation that uses the `TransportUnicast`, because `TransportUnicast` only holds a `Weak`
/// reference.
pub fn mock_transport_unicast(
    zid: ZenohIdProto,
    whatami: WhatAmI,
    on_schedule: Arc<dyn Fn(NetworkMessage) + Send + Sync>,
) -> (TransportUnicast, Arc<MockTransportUnicastInner>) {
    let inner = Arc::new(MockTransportUnicastInner {
        zid,
        whatami,
        on_schedule,
    });
    let erased: Arc<dyn TransportUnicastTrait> = inner.clone();
    let transport = TransportUnicast::from(&erased);
    (transport, inner)
}

pub fn make_transport_manager_builder(
    #[cfg(feature = "transport_multilink")] max_links: usize,
    lowlatency_transport: bool,
) -> TransportManagerBuilderUnicast {
    let transport = make_basic_transport_manager_builder(lowlatency_transport);

    zcondfeat!(
        "transport_multilink",
        {
            println!("...with max links: {max_links}...");
            transport.max_links(max_links)
        },
        transport
    )
}

pub fn make_basic_transport_manager_builder(
    lowlatency_transport: bool,
) -> TransportManagerBuilderUnicast {
    println!("Create transport manager builder...");
    let config = TransportManager::config_unicast();
    if lowlatency_transport {
        println!("...with LowLatency transport...");
    }
    match lowlatency_transport {
        true => config.lowlatency(true).qos(false),
        false => config,
    }
}

/// The deletions that tests hold back, by the Zenoh ID of the node that deletes.
static DELETION_GATES: std::sync::Mutex<Vec<(ZenohIdProto, Arc<tokio::sync::Semaphore>)>> =
    std::sync::Mutex::new(Vec::new());

/// Holds back the deletion of the unicast transports of a node, see [`gate_deletions`].
pub struct DeletionGate {
    zid: ZenohIdProto,
    gate: Arc<tokio::sync::Semaphore>,
}

impl DeletionGate {
    /// The number of deletions that are waiting at the gate.
    pub fn waiting(&self) -> usize {
        // each deletion that waits holds a clone, the registry holds one and `self` another
        Arc::strong_count(&self.gate) - 2
    }

    /// Lets the deletions that wait, and the later ones, go on.
    pub fn open(&self) {
        DELETION_GATES
            .lock()
            .unwrap()
            .retain(|(_, gate)| !Arc::ptr_eq(gate, &self.gate));
        self.gate.close();
    }
}

impl Drop for DeletionGate {
    fn drop(&mut self) {
        tracing::trace!("Deletions of the transports of {} go on", self.zid);
        self.open();
    }
}

/// From now on and until the returned gate is opened or dropped, the deletion of a unicast
/// transport of the node `zid` stops after it has marked the transport closed and told its
/// handler that it is closing, and before it closes the links and the tasks of the transport:
/// a clean-up that takes as long as the test wants.
pub fn gate_deletions(zid: ZenohIdProto) -> DeletionGate {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    DELETION_GATES.lock().unwrap().push((zid, gate.clone()));
    DeletionGate { zid, gate }
}

/// Waits at the gate of the node `zid`, if tests have closed one.
pub(crate) async fn deletion_gate(zid: &ZenohIdProto) {
    let gate = DELETION_GATES
        .lock()
        .unwrap()
        .iter()
        .find(|(gated, _)| gated == zid)
        .map(|(_, gate)| gate.clone());
    if let Some(gate) = gate {
        // no permit is ever added: this returns when the gate is closed, which opens it
        let _ = gate.acquire().await;
    }
}

/// The pushes that tests make fail: (node, remote).
static FAILING_PUSHES: std::sync::Mutex<Vec<(ZenohIdProto, ZenohIdProto)>> =
    std::sync::Mutex::new(Vec::new());

/// Makes the pushes of a node to a remote fail, see [`fail_pushes`].
pub struct FailingPushes(ZenohIdProto, ZenohIdProto);

impl Drop for FailingPushes {
    fn drop(&mut self) {
        FAILING_PUSHES
            .lock()
            .unwrap()
            .retain(|(zid, remote)| (zid, remote) != (&self.0, &self.1));
    }
}

/// From now on and until the returned value is dropped, the node `zid` takes each network message
/// it sends to `remote` over a universal transport as one it could not enqueue, as when the
/// queue towards a remote that no longer reads stays full. The first such message that cannot
/// be dropped makes the node delete that transport, which is the deletion that does not go
/// through the removal of its links.
pub fn fail_pushes(zid: ZenohIdProto, remote: ZenohIdProto) -> FailingPushes {
    FAILING_PUSHES.lock().unwrap().push((zid, remote));
    FailingPushes(zid, remote)
}

pub(crate) fn push_fails(zid: &ZenohIdProto, remote: &ZenohIdProto) -> bool {
    FAILING_PUSHES
        .lock()
        .unwrap()
        .iter()
        .any(|(z, r)| z == zid && r == remote)
}
