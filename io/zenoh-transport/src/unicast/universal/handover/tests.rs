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

//! The contract of the hand-over sender: order, age, capacity, life cycle.
//!
//! The tests drive the sender by hand: they decide when a batch is taken, when its write
//! completes and what time it is. What the peer would deliver is read back with
//! [`TestReceiver`]. No test sleeps.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use zenoh_protocol::{
    core::{Bits, CongestionControl, ExprId, Priority, Reliability, WireExpr},
    network::{
        declare, ext, request, Declare, DeclareBody, DeclareKeyExpr, DeclareSubscriber, Mapping,
        NetworkBody, NetworkMessage, NetworkMessageExt, Push, Request, RequestId, Response,
        ResponseFinal, UndeclareKeyExpr,
    },
    transport::TransportSn,
    zenoh::{PushBody, Put, Query, Reply, RequestBody, ResponseBody},
};

use super::*;
use crate::common::{
    batch::WBatch,
    test_receiver::{Seen, TestReceiver},
};

const MTU: u16 = 1024;
const DATA: Priority = Priority::DEFAULT;
const MAX_AGE_MS: u64 = 250;
const CONTROL_MAX_AGE_MS: u64 = 1_000;
const CLOSE_MS: u64 = 2_000;

fn conf() -> HandoverConf {
    HandoverConf {
        batch: BatchConfig {
            mtu: MTU,
            is_streamed: false,
            #[cfg(feature = "transport_compression")]
            is_compression: false,
        },
        is_qos: true,
        queue_size: [2; Priority::NUM],
        data_bytes: 16 * 1024,
        large_bytes: 96 * 1024,
        reserve_min_bytes: 4 * 1024,
        reserve_max_bytes: 8 * 1024,
        max_age: Duration::from_millis(MAX_AGE_MS),
        control_max_age: Duration::from_millis(CONTROL_MAX_AGE_MS),
        wait_before_close: Duration::from_millis(CLOSE_MS),
        default_query_timeout: Duration::from_secs(10),
    }
}

fn push(priority: Priority, cc: CongestionControl, size: usize, scope: ExprId) -> NetworkMessage {
    NetworkMessage::from(Push {
        wire_expr: WireExpr {
            scope,
            suffix: "k".into(),
            mapping: Mapping::Sender,
        },
        ext_qos: ext::QoSType::new(priority, cc, false),
        ..Push::from(vec![0_u8; size])
    })
}

/// A blocking publication.
fn put(priority: Priority, size: usize) -> NetworkMessage {
    push(priority, CongestionControl::Block, size, 0)
}

/// A blocking publication whose key expression uses an id declared by the sender.
fn put_key(priority: Priority, size: usize, id: ExprId) -> NetworkMessage {
    push(priority, CongestionControl::Block, size, id)
}

fn put_droppable(priority: Priority, size: usize) -> NetworkMessage {
    push(priority, CongestionControl::Drop, size, 0)
}

fn declare_body(body: DeclareBody) -> NetworkMessage {
    NetworkMessage::from(NetworkBody::Declare(Declare {
        interest_id: None,
        ext_qos: declare::ext::QoSType::DECLARE,
        ext_tstamp: None,
        ext_nodeid: declare::ext::NodeIdType::DEFAULT,
        body,
    }))
}

fn declare_key(id: ExprId) -> NetworkMessage {
    declare_body(DeclareBody::DeclareKeyExpr(DeclareKeyExpr {
        id,
        wire_expr: "some/key".into(),
    }))
}

fn undeclare_key(id: ExprId) -> NetworkMessage {
    declare_body(DeclareBody::UndeclareKeyExpr(UndeclareKeyExpr { id }))
}

fn query(rid: RequestId, timeout: Option<Duration>) -> NetworkMessage {
    NetworkMessage::from(NetworkBody::Request(Request {
        id: rid,
        wire_expr: "q".into(),
        ext_qos: ext::QoSType::new(DATA, CongestionControl::Block, false),
        ext_tstamp: None,
        ext_nodeid: request::ext::NodeIdType::DEFAULT,
        ext_target: Default::default(),
        ext_budget: None,
        ext_timeout: timeout,
        ext_ts_stack: None,
        payload: RequestBody::Query(Query {
            parameters: "p".repeat(32),
            ..Query::default()
        }),
    }))
}

fn reply(rid: RequestId, size: usize) -> NetworkMessage {
    reply_on(rid, size, "q".into())
}

/// A reply with this key expression.
fn reply_on(rid: RequestId, size: usize, wire_expr: WireExpr<'static>) -> NetworkMessage {
    NetworkMessage::from(NetworkBody::Response(Response {
        rid,
        wire_expr,
        payload: ResponseBody::Reply(Reply {
            consolidation: Default::default(),
            ext_unknown: vec![],
            payload: PushBody::Put(Put {
                payload: vec![0_u8; size].into(),
                ..Put::default()
            }),
        }),
        ext_qos: ext::QoSType::new(DATA, CongestionControl::Block, false),
        ext_tstamp: None,
        ext_respid: None,
        ext_ts_stack: None,
    }))
}

fn fin(rid: RequestId) -> NetworkMessage {
    NetworkMessage::from(NetworkBody::ResponseFinal(ResponseFinal {
        rid,
        ext_qos: ext::QoSType::new(DATA, CongestionControl::Block, false),
        ext_tstamp: None,
    }))
}

/// A sender, its task driven by hand, and the receiving end.
struct Rig {
    t0: Instant,
    pool: Arc<ProcessPool>,
    sender: Arc<Sender>,
    task: Option<SenderTask>,
    /// The batch whose write is pending.
    writing: Option<WBatch>,
    rx: TestReceiver,
    closed: Option<CloseReason>,
    flushed: bool,
}

impl Rig {
    fn new(conf: HandoverConf) -> Self {
        Self::in_pool(conf, ProcessPool::new(1 << 30)).unwrap()
    }

    fn in_pool(conf: HandoverConf, pool: Arc<ProcessPool>) -> Result<Self, ActivationError> {
        Self::make(conf, pool, true)
    }

    fn make(
        conf: HandoverConf,
        pool: Arc<ProcessPool>,
        fragmentation_markers: bool,
    ) -> Result<Self, ActivationError> {
        let channels = if conf.is_qos { Priority::NUM } else { 1 };
        let priorities = (0..channels)
            .map(|_| TransportPriorityTx::make(Bits::from(TransportSn::MAX)).unwrap())
            .collect::<Vec<_>>();
        let rx = TestReceiver::new(Bits::from(TransportSn::MAX), fragmentation_markers);
        let (sender, task) = Sender::activate(conf, pool.clone(), &priorities)?;
        Ok(Self {
            t0: Instant::now(),
            pool,
            sender,
            task: Some(task),
            writing: None,
            rx,
            closed: None,
            flushed: false,
        })
    }

    fn at(&self, ms: u64) -> Instant {
        self.t0 + Duration::from_millis(ms)
    }

    fn admit(&self, msg: &NetworkMessage, ms: u64) -> Result<(), Refusal> {
        self.sender.admit(msg.as_ref(), None, self.at(ms))
    }

    /// Admits a query whose routing branch ends at `expiry_ms`.
    fn admit_until(&self, msg: &NetworkMessage, ms: u64, expiry_ms: u64) -> Result<(), Refusal> {
        let lifetime = RequestLifetime {
            expiry: self.at(expiry_ms),
            branch: None,
        };
        self.sender
            .admit(msg.as_ref(), Some(&lifetime), self.at(ms))
    }

    /// The queries reported as not sent since the last call.
    fn undelivered(&self) -> Vec<(RequestId, UndeliveredReason)> {
        self.sender
            .take_undelivered()
            .into_iter()
            .map(|u| (u.rid, u.reason))
            .collect()
    }

    /// One step at `ms`. True if the sender gave a batch: the receiver has read it, and its
    /// write is left pending until [`Rig::complete_write`].
    fn start_write(&mut self, ms: u64) -> bool {
        let now = self.at(ms);
        loop {
            match self.task.as_mut().unwrap().step(now) {
                Step::Write(batch) => {
                    assert!(self.writing.is_none(), "a batch was given during a write");
                    self.rx.read(batch.as_slice());
                    self.writing = Some(batch);
                    return true;
                }
                Step::Again => continue,
                Step::Idle(_) => return false,
                Step::Close(reason) => {
                    self.closed = Some(reason);
                    return false;
                }
                Step::Flushed => {
                    self.flushed = true;
                    return false;
                }
            }
        }
    }

    fn complete_write(&mut self, ms: u64) {
        let now = self.at(ms);
        let batch = self.writing.take().expect("no write is pending");
        self.task.as_mut().unwrap().written(batch, now);
    }

    /// Steps at `ms`, every write completing at once, until the sender gives no more batch.
    /// Returns the number of batches.
    fn pump(&mut self, ms: u64) -> usize {
        let mut batches = 0;
        while self.start_write(ms) {
            self.complete_write(ms);
            batches += 1;
        }
        batches
    }

    fn seen(&self) -> Vec<Seen> {
        assert!(self.rx.errors.is_empty(), "{:?}", self.rx.errors);
        self.rx.seen()
    }

    fn position(&self, what: &Seen) -> usize {
        let seen = self.seen();
        seen.iter()
            .position(|s| s == what)
            .unwrap_or_else(|| panic!("{what:?} was not delivered: {seen:?}"))
    }

    fn used(&self) -> usize {
        let u = self.sender.usage();
        u.data.iter().sum::<usize>() + u.large + u.reserve
    }
}

// ---------------------------------------------------------------------------------------------
// Order
// ---------------------------------------------------------------------------------------------

// Messages of one priority are delivered in the order of admission, whole and fragmented.
#[test]
fn one_priority_keeps_the_order_of_admission() {
    let mut rig = Rig::new(conf());
    let sizes = [8, 900, 5 * MTU as usize, 64, MTU as usize + 1, 17];
    for size in sizes {
        rig.admit(&put(DATA, size), 0).unwrap();
    }
    rig.pump(0);
    assert_eq!(rig.rx.put_sizes(), sizes);
    assert!(rig.rx.errors.is_empty(), "{:?}", rig.rx.errors);
    assert_eq!(rig.used(), 0);
}

// The same without fragmentation markers (a peer that did not negotiate them).
#[test]
fn a_peer_without_fragmentation_markers_reassembles() {
    let mut rig = Rig::make(conf(), ProcessPool::new(1 << 30), false).unwrap();
    let sizes = [5 * MTU as usize, 8, 3 * MTU as usize];
    for size in sizes {
        rig.admit(&put(DATA, size), 0).unwrap();
    }
    rig.pump(0);
    assert_eq!(rig.rx.put_sizes(), sizes);
    assert!(rig.rx.errors.is_empty(), "{:?}", rig.rx.errors);
}

// Without QoS there is one queue: everything is delivered in the order of admission.
#[test]
fn without_qos_everything_keeps_the_order_of_admission() {
    let mut rig = Rig::new(HandoverConf {
        is_qos: false,
        ..conf()
    });
    rig.admit(&put(DATA, 10), 0).unwrap();
    rig.admit(&declare_key(7), 0).unwrap();
    rig.admit(&put_key(DATA, 3 * MTU as usize, 7), 0).unwrap();
    rig.admit(&undeclare_key(7), 0).unwrap();
    rig.pump(0);
    assert_eq!(
        rig.seen(),
        [
            Seen::Put(DATA, 10, 0),
            Seen::DeclareKey(7),
            Seen::Put(DATA, 3 * MTU as usize, 7),
            Seen::UndeclareKey(7),
        ]
    );
}

// A control message and a message of a higher priority go out between the fragments of a large
// message of a lower priority; the large message is still delivered whole.
#[test]
fn higher_priorities_go_out_between_the_fragments_of_a_large_message() {
    let mut rig = Rig::new(conf());
    let large = 20 * MTU as usize;
    rig.admit(&put(Priority::Background, large), 0).unwrap();
    // The first fragment is written.
    assert!(rig.start_write(0));
    rig.complete_write(0);
    rig.admit(&declare_key(3), 1).unwrap();
    rig.admit(&put(Priority::RealTime, 100), 1).unwrap();
    // The next batch carries them, not the second fragment alone.
    assert!(rig.start_write(1));
    rig.complete_write(1);
    assert_eq!(
        rig.seen(),
        [Seen::DeclareKey(3), Seen::Put(Priority::RealTime, 100, 0)]
    );
    rig.pump(2);
    assert_eq!(
        rig.seen().last(),
        Some(&Seen::Put(Priority::Background, large, 0))
    );
}

// A key expression declaration is delivered before every message admitted after it, whatever
// the priority of that message.
#[test]
fn a_declaration_is_delivered_before_its_first_use() {
    for priority in [Priority::RealTime, DATA, Priority::Background] {
        let mut rig = Rig::new(conf());
        // Something of that priority is already queued in front.
        rig.admit(&put(priority, 500), 0).unwrap();
        rig.admit(&declare_key(7), 0).unwrap();
        rig.admit(&put_key(priority, 100, 7), 0).unwrap();
        rig.pump(0);
        assert!(
            rig.position(&Seen::DeclareKey(7)) < rig.position(&Seen::Put(priority, 100, 7)),
            "{:?}",
            rig.seen()
        );
    }
}

// An undeclaration is delivered after the queued messages that use its id, and does not hold
// back the control messages admitted after it.
#[test]
fn an_undeclaration_waits_for_the_queued_uses_of_its_id() {
    let mut rig = Rig::new(conf());
    // A write is pending, so that what follows stays queued.
    rig.admit(&put(DATA, 900), 0).unwrap();
    assert!(rig.start_write(0));
    rig.admit(&put_key(Priority::Background, 100, 7), 1)
        .unwrap();
    rig.admit(&undeclare_key(7), 1).unwrap();
    rig.admit(&declare_key(8), 1).unwrap();
    rig.complete_write(2);
    rig.pump(2);
    let used = rig.position(&Seen::Put(Priority::Background, 100, 7));
    assert!(
        used < rig.position(&Seen::UndeclareKey(7)),
        "{:?}",
        rig.seen()
    );
    assert!(
        rig.position(&Seen::DeclareKey(8)) < used,
        "{:?}",
        rig.seen()
    );
}

// The same when the use is a large message whose fragments are being sent.
#[test]
fn an_undeclaration_waits_for_a_use_in_emission_until_it_is_written() {
    let mut rig = Rig::new(conf());
    let large = 10 * MTU as usize;
    rig.admit(&put_key(DATA, large, 7), 0).unwrap();
    assert!(rig.start_write(0));
    rig.complete_write(0);
    rig.admit(&undeclare_key(7), 1).unwrap();
    rig.pump(1);
    assert_eq!(
        rig.seen(),
        [Seen::Put(DATA, large, 7), Seen::UndeclareKey(7)]
    );
}

// An id declared again while its undeclaration waits: no order of emission is right, the
// transport is closed.
#[test]
fn an_id_declared_again_while_its_undeclaration_waits_closes_the_transport() {
    let mut rig = Rig::new(conf());
    rig.admit(&put(DATA, 900), 0).unwrap();
    assert!(rig.start_write(0));
    rig.admit(&put_key(DATA, 100, 7), 1).unwrap();
    rig.admit(&undeclare_key(7), 1).unwrap();
    rig.admit(&declare_key(7), 1).unwrap();
    rig.complete_write(2);
    rig.pump(2);
    assert_eq!(rig.closed, Some(CloseReason::KeyIdReuse));
}

// Replies are delivered before the final of their query. When a reply was refused, an error
// reply that says so is delivered in its place, in front of the final.
#[test]
fn dropped_replies_are_announced_before_the_final() {
    let mut rig = Rig::new(conf());
    rig.admit(&reply(5, 100), 0).unwrap();
    // Fill the budget of the priority, until a reply of the query is refused.
    while rig.admit(&put(DATA, 900), 0).is_ok() {}
    assert_eq!(rig.admit(&reply(5, 950), 0), Err(Refusal::LinkBudget));
    rig.admit(&fin(5), 0).unwrap();
    rig.pump(0);
    let seen = rig.seen();
    let last = seen.len() - 1;
    assert_eq!(seen[last], Seen::Final(5));
    assert_eq!(
        seen[last - 1],
        Seen::ReplyErr(5, REPLIES_DROPPED.to_string())
    );
    assert!(rig.position(&Seen::Reply(5)) < last - 1);
    // A query that lost nothing gets no marker.
    rig.admit(&reply(6, 100), 1).unwrap();
    rig.admit(&fin(6), 1).unwrap();
    rig.pump(1);
    let seen = rig.seen();
    assert_eq!(seen[seen.len() - 2..], [Seen::Reply(6), Seen::Final(6)]);
}

/// The key expression of the error reply that a query received: its scope and its suffix.
fn error_key(rig: &Rig, rid: RequestId) -> (ExprId, String) {
    rig.rx
        .delivered
        .iter()
        .find_map(|(_, msg)| match &msg.body {
            NetworkBody::Response(r)
                if r.rid == rid && matches!(r.payload, ResponseBody::Err(_)) =>
            {
                Some((r.wire_expr.scope, r.wire_expr.suffix.to_string()))
            }
            _ => None,
        })
        .expect("an error reply")
}

// The error reply that stands for dropped replies carries the key expression of the first of
// them: a session with a namespace and the access control let it pass where they let the
// replies pass, and drop a reply without a key. This holds for a reply that was refused and
// for one that was taken and expired. A key written relative to a declared id is not kept.
#[test]
fn dropped_replies_are_announced_with_their_key() {
    let mut rig = Rig::new(conf());
    while rig.admit(&put(DATA, 900), 0).is_ok() {}
    let refused = reply_on(5, 950, "robot/srv/first".into());
    assert_eq!(rig.admit(&refused, 0), Err(Refusal::LinkBudget));
    let refused = reply_on(5, 950, "robot/srv/second".into());
    assert_eq!(rig.admit(&refused, 0), Err(Refusal::LinkBudget));
    rig.admit(&fin(5), 0).unwrap();
    let relative = WireExpr {
        scope: 9,
        suffix: "/srv".into(),
        mapping: Mapping::Sender,
    };
    assert_eq!(
        rig.admit(&reply_on(6, 950, relative), 0),
        Err(Refusal::LinkBudget)
    );
    rig.admit(&fin(6), 0).unwrap();
    rig.pump(0);
    assert_eq!(error_key(&rig, 5), (0, "robot/srv/first".to_string()));
    assert_eq!(error_key(&rig, 6), (0, String::new()));
    assert_eq!(rig.used(), 0);

    // Taken, and not started within its age.
    let mut rig = Rig::new(conf());
    rig.admit(&put(DATA, 900), 0).unwrap();
    assert!(rig.start_write(0));
    rig.admit(&reply_on(7, 100, "robot/srv/late".into()), 0)
        .unwrap();
    rig.admit(&fin(7), 0).unwrap();
    rig.complete_write(MAX_AGE_MS + 1);
    rig.pump(MAX_AGE_MS + 1);
    let seen = rig.seen();
    assert_eq!(
        seen[seen.len() - 2..],
        [
            Seen::ReplyErr(7, REPLIES_DROPPED.to_string()),
            Seen::Final(7)
        ]
    );
    assert_eq!(error_key(&rig, 7), (0, "robot/srv/late".to_string()));
    assert_eq!(rig.used(), 0);
}

// ---------------------------------------------------------------------------------------------
// Age
// ---------------------------------------------------------------------------------------------

// A publication that has not started to be sent `max_age` after its admission is discarded;
// one that is younger is sent.
#[test]
fn an_unstarted_publication_expires_at_max_age() {
    let mut rig = Rig::new(conf());
    rig.admit(&put(DATA, 900), 0).unwrap();
    assert!(rig.start_write(0));
    rig.admit(&put(DATA, 11), 0).unwrap();
    rig.admit(&put(DATA, 22), 100).unwrap();
    // The link gives the write back after the first publication's age, before the second's.
    rig.complete_write(MAX_AGE_MS + 1);
    rig.pump(MAX_AGE_MS + 1);
    assert_eq!(rig.rx.put_sizes(), [900, 22]);
    assert_eq!(rig.used(), 0);
}

// A query that expires unsent is never sent afterwards, and is reported once.
#[test]
fn an_expired_query_is_not_sent_and_is_reported_once() {
    let mut rig = Rig::new(conf());
    rig.admit(&put(DATA, 900), 0).unwrap();
    assert!(rig.start_write(0));
    rig.admit(&query(9, None), 0).unwrap();
    assert!(!rig.start_write(MAX_AGE_MS + 1));
    assert_eq!(rig.undelivered(), [(9, UndeliveredReason::Expired)]);
    rig.complete_write(MAX_AGE_MS + 2);
    rig.pump(MAX_AGE_MS + 2);
    assert_eq!(rig.seen(), [Seen::Put(DATA, 900, 0)]);
    assert!(rig.undelivered().is_empty());
}

// A query whose own timeout is shorter than `max_age` expires at its timeout.
#[test]
fn a_query_expires_at_its_own_timeout_when_that_is_shorter() {
    let mut rig = Rig::new(conf());
    rig.admit(&put(DATA, 900), 0).unwrap();
    assert!(rig.start_write(0));
    rig.admit(&query(9, Some(Duration::from_millis(50))), 0)
        .unwrap();
    rig.admit(&query(10, None), 0).unwrap();
    rig.complete_write(60);
    rig.pump(60);
    assert_eq!(rig.seen(), [Seen::Put(DATA, 900, 0), Seen::Request(10)]);
    assert_eq!(rig.undelivered(), [(9, UndeliveredReason::Expired)]);
}

// A message that has started is completed, however long it takes.
#[test]
fn a_started_message_is_completed_after_its_age() {
    let mut rig = Rig::new(conf());
    let large = 6 * MTU as usize;
    rig.admit(&put(DATA, large), 0).unwrap();
    let mut ms = 0;
    while rig.start_write(ms) {
        // Every write takes longer than `max_age`, and less than the close period.
        ms += MAX_AGE_MS + 50;
        rig.complete_write(ms);
    }
    assert_eq!(rig.closed, None);
    assert_eq!(rig.rx.put_sizes(), [large]);
}

// A final is never discarded for its age.
#[test]
fn a_final_does_not_expire() {
    let mut rig = Rig::new(conf());
    rig.admit(&put(DATA, 900), 0).unwrap();
    assert!(rig.start_write(0));
    rig.admit(&fin(4), 0).unwrap();
    rig.complete_write(CLOSE_MS - 1);
    rig.pump(CLOSE_MS - 1);
    assert_eq!(rig.seen(), [Seen::Put(DATA, 900, 0), Seen::Final(4)]);
}

// A control message that has not started after `control_max_age` closes the transport.
#[test]
fn a_control_message_older_than_its_limit_closes_the_transport() {
    let mut rig = Rig::new(conf());
    rig.admit(&put(DATA, 900), 0).unwrap();
    assert!(rig.start_write(0));
    rig.admit(&declare_key(1), 0).unwrap();
    assert!(!rig.start_write(CONTROL_MAX_AGE_MS - 1));
    assert_eq!(rig.closed, None);
    assert!(!rig.start_write(CONTROL_MAX_AGE_MS + 1));
    assert_eq!(rig.closed, Some(CloseReason::ControlAge));
}

// ---------------------------------------------------------------------------------------------
// Capacity
// ---------------------------------------------------------------------------------------------

// The budget of a priority is never exceeded; what does not fit is refused; other priorities
// and control messages are still admitted; everything is given back when it has been sent.
#[test]
fn the_budget_of_a_priority_is_a_hard_limit() {
    let conf = conf();
    let mut rig = Rig::new(conf.clone());
    let mut admitted = 0;
    loop {
        match rig.admit(&put(DATA, 900), 0) {
            Ok(()) => admitted += 1,
            Err(refusal) => {
                assert_eq!(refusal, Refusal::LinkBudget);
                break;
            }
        }
        let usage = rig.sender.usage();
        assert!(usage.data[DATA as usize] <= conf.data_bytes, "{usage:?}");
    }
    assert!(
        admitted >= 7 && admitted <= conf.data_bytes / 900,
        "{admitted}"
    );
    rig.admit(&put(Priority::RealTime, 900), 0).unwrap();
    rig.admit(&declare_key(1), 0).unwrap();
    rig.admit(&fin(1), 0).unwrap();
    let usage = rig.sender.usage();
    assert!(usage.reserve > 0 && usage.reserve <= conf.reserve_min_bytes);
    // The reserve in use is inside the minimum taken at activation.
    assert_eq!(
        rig.pool.in_use(),
        conf.reserve_min_bytes + usage.data.iter().sum::<usize>() + usage.large
    );
    rig.pump(0);
    assert_eq!(rig.rx.put_sizes().len(), admitted + 1);
    assert_eq!(rig.used(), 0);
    assert_eq!(rig.pool.in_use(), conf.reserve_min_bytes);
}

// A message longer than the budget of its priority uses the budget for large messages; one
// that fits neither is refused; one larger than any budget is refused as too large.
#[test]
fn large_messages_have_their_own_budget() {
    let conf = conf();
    let rig = Rig::new(conf.clone());
    rig.admit(&put(DATA, 40 * 1024), 0).unwrap();
    let usage = rig.sender.usage();
    assert!(usage.large >= 40 * 1024 && usage.large <= conf.large_bytes);
    assert_eq!(usage.data[DATA as usize], 0);
    assert_eq!(
        rig.admit(&put(DATA, 60 * 1024), 0),
        Err(Refusal::LinkBudget)
    );
    assert_eq!(
        rig.admit(&put(DATA, conf.large_bytes + 1), 0),
        Err(Refusal::TooLarge)
    );
    assert_eq!(rig.sender.usage(), usage);
}

// A droppable message is admitted only while its priority holds less than today's queue of
// that priority; blocking messages are still admitted then.
#[test]
fn a_droppable_message_is_refused_where_todays_queue_is_full() {
    let conf = conf();
    let rig = Rig::new(conf.clone());
    let todays_queue = conf.queue_size[DATA as usize] * MTU as usize;
    let mut admitted = 0;
    while rig.admit(&put_droppable(DATA, 500), 0).is_ok() {
        admitted += 1;
    }
    assert!(
        admitted >= 1 && admitted * 500 <= todays_queue + 500,
        "{admitted}"
    );
    assert_eq!(
        rig.admit(&put_droppable(DATA, 500), 0),
        Err(Refusal::Droppable)
    );
    let mut best_effort = put(DATA, 500);
    best_effort.reliability = Reliability::BestEffort;
    assert_eq!(rig.admit(&best_effort, 0), Err(Refusal::Droppable));
    rig.admit(&put(DATA, 500), 0).unwrap();
}

// Control messages, finals and markers have a reserve of their own; when it cannot hold one
// more, the transport is closed instead of dropping it.
#[test]
fn the_reserve_overflowing_closes_the_transport() {
    let conf = conf();
    let mut rig = Rig::new(conf.clone());
    rig.admit(&put(DATA, 900), 0).unwrap();
    assert!(rig.start_write(0));
    let mut admitted = 0;
    while rig.admit(&declare_key(admitted as ExprId + 1), 0).is_ok() {
        admitted += 1;
        assert!(rig.sender.usage().reserve <= conf.reserve_max_bytes);
        assert!(admitted < 10_000);
    }
    assert!(admitted >= 8, "{admitted}");
    assert!(!rig.start_write(1));
    assert_eq!(rig.closed, Some(CloseReason::ReserveOverflow));
    assert_eq!(rig.admit(&put(DATA, 10), 1), Err(Refusal::Closed));
}

// Each link takes its minimum reserve when it is activated; a link that cannot get it is not
// activated; the reserve comes back when the link's sender ends.
#[test]
fn the_minimum_reserve_is_taken_at_activation() {
    let conf = conf();
    let pool = ProcessPool::new(conf.reserve_min_bytes * 3 / 2);
    let mut first = Rig::in_pool(conf.clone(), pool.clone()).unwrap();
    assert_eq!(pool.in_use(), conf.reserve_min_bytes);
    assert_eq!(
        Rig::in_pool(conf.clone(), pool.clone()).err(),
        Some(ActivationError::ReserveUnavailable)
    );
    // A control message of the first link still fits: its minimum is its own.
    first.admit(&declare_key(1), 0).unwrap();
    first.task.take().unwrap().abort();
    assert_eq!(pool.in_use(), 0);
    assert!(Rig::in_pool(conf, pool).is_ok());
}

// Links that admit at the same time never hold more than the process pool together, and the
// refusals say that the process limit was the reason.
#[test]
fn concurrent_admissions_never_exceed_the_process_pool() {
    const LINKS: usize = 4;
    const THREADS_PER_LINK: usize = 2;
    let conf = conf();
    let total = LINKS * conf.reserve_min_bytes + 24 * 1024;
    let pool = ProcessPool::new(total);
    let rigs: Vec<Rig> = (0..LINKS)
        .map(|_| Rig::in_pool(conf.clone(), pool.clone()).unwrap())
        .collect();
    let senders: Vec<Arc<Sender>> = rigs.iter().map(|rig| rig.sender.clone()).collect();
    let now = Instant::now();
    let process_limited = AtomicUsize::new(0);
    let done = AtomicBool::new(false);
    std::thread::scope(|s| {
        s.spawn(|| {
            while !done.load(Ordering::Relaxed) {
                let in_use = pool.in_use();
                assert!(in_use <= total, "{in_use} > {total}");
            }
        });
        let producers: Vec<_> = senders
            .iter()
            .flat_map(|sender| (0..THREADS_PER_LINK).map(move |_| sender))
            .map(|sender| {
                let process_limited = &process_limited;
                s.spawn(move || {
                    for i in 0..2_000 {
                        let priority = if i % 2 == 0 { DATA } else { Priority::RealTime };
                        let msg = put(priority, 700);
                        if sender.admit(msg.as_ref(), None, now) == Err(Refusal::ProcessLimit) {
                            process_limited.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();
        for p in producers {
            p.join().unwrap();
        }
        done.store(true, Ordering::Relaxed);
    });
    assert!(process_limited.load(Ordering::Relaxed) > 0);
    let held: usize = rigs.iter().map(|rig| rig.used()).sum();
    assert!(held <= total - LINKS * conf.reserve_min_bytes, "{held}");
    assert_eq!(pool.in_use(), held + LINKS * conf.reserve_min_bytes);
}

// A forced close gives every reservation back and reports the queries that were not started.
#[test]
fn a_forced_close_gives_everything_back() {
    let mut rig = Rig::new(conf());
    rig.admit(&put(DATA, 5 * MTU as usize), 0).unwrap();
    assert!(rig.start_write(0));
    rig.admit(&query(3, None), 0).unwrap();
    rig.admit(&declare_key(1), 0).unwrap();
    rig.admit(&put(Priority::Background, 40 * 1024), 0).unwrap();
    rig.task.take().unwrap().abort();
    assert_eq!(rig.pool.in_use(), 0);
    assert_eq!(rig.undelivered(), [(3, UndeliveredReason::NotDelivered)]);
    assert_eq!(rig.admit(&put(DATA, 10), 1), Err(Refusal::Closed));
}

// A query refused at admission is reported, so that its branch ends at once.
#[test]
fn a_refused_query_is_reported() {
    let rig = Rig::new(conf());
    while rig.admit(&put(DATA, 900), 0).is_ok() {}
    // The priority is full to the last 900 bytes: fill the rest with small publications.
    while rig.admit(&put(DATA, 1), 0).is_ok() {}
    assert_eq!(rig.admit(&query(3, None), 0), Err(Refusal::LinkBudget));
    assert_eq!(rig.undelivered(), [(3, UndeliveredReason::NotDelivered)]);
}

// ---------------------------------------------------------------------------------------------
// Life cycle
// ---------------------------------------------------------------------------------------------

// A link whose write does not complete for `wait_before_close` is closed, although everything
// that waited behind the write has expired meanwhile. The decision is final.
#[test]
fn a_link_without_progress_is_closed_once() {
    let mut rig = Rig::new(conf());
    rig.admit(&put(DATA, 900), 0).unwrap();
    assert!(rig.start_write(0));
    rig.admit(&put(DATA, 10), 0).unwrap();
    assert!(!rig.start_write(CLOSE_MS - 1));
    assert_eq!(rig.closed, None);
    assert!(!rig.start_write(CLOSE_MS + 1));
    assert_eq!(rig.closed, Some(CloseReason::NoProgress));
    // The write completing afterwards changes nothing.
    rig.closed = None;
    rig.complete_write(CLOSE_MS + 2);
    assert!(!rig.start_write(CLOSE_MS + 3));
    assert_eq!(rig.closed, Some(CloseReason::NoProgress));
    assert_eq!(
        rig.admit(&put(DATA, 10), CLOSE_MS + 3),
        Err(Refusal::Closed)
    );
}

// A slow link whose writes complete inside the period is not closed.
#[test]
fn progress_inside_the_period_keeps_the_link() {
    let mut rig = Rig::new(conf());
    rig.admit(&put(Priority::Background, 8 * MTU as usize), 0)
        .unwrap();
    let mut ms = 0;
    while rig.start_write(ms) {
        ms += CLOSE_MS - 1;
        assert!(!rig.start_write(ms));
        rig.complete_write(ms);
    }
    assert_eq!(rig.closed, None);
    assert_eq!(rig.rx.put_sizes(), [8 * MTU as usize]);
}

// An idle sender asks for no wake-up, and time passing does not close it.
#[test]
fn an_idle_link_has_no_deadline() {
    let mut rig = Rig::new(conf());
    let now = rig.at(0);
    assert!(matches!(
        rig.task.as_mut().unwrap().step(now),
        Step::Idle(None)
    ));
    rig.admit(&put(DATA, 10), 0).unwrap();
    rig.pump(0);
    let later = rig.at(10 * CLOSE_MS);
    assert!(matches!(
        rig.task.as_mut().unwrap().step(later),
        Step::Idle(None)
    ));
}

// While something waits behind a pending write, the sender asks to be woken at the earliest
// of its deadlines.
#[test]
fn a_waiting_sender_asks_for_its_earliest_deadline() {
    let mut rig = Rig::new(conf());
    rig.admit(&put(DATA, 900), 0).unwrap();
    assert!(rig.start_write(0));
    rig.admit(&put(DATA, 10), 10).unwrap();
    let now = rig.at(20);
    let expiry = rig.at(10 + MAX_AGE_MS);
    match rig.task.as_mut().unwrap().step(now) {
        Step::Idle(Some(deadline)) => assert_eq!(deadline, expiry),
        _ => panic!("the sender should wait for the age of the queued publication"),
    }
    let now = rig.at(10 + MAX_AGE_MS + 1);
    let close = rig.at(CLOSE_MS);
    match rig.task.as_mut().unwrap().step(now) {
        Step::Idle(Some(deadline)) => assert_eq!(deadline, close),
        _ => panic!("the sender should wait for the close period of the pending write"),
    }
}

// An orderly close sends what was admitted before it and admits nothing more.
#[test]
fn an_orderly_close_sends_what_was_admitted() {
    let mut rig = Rig::new(conf());
    rig.admit(&put(DATA, 3 * MTU as usize), 0).unwrap();
    rig.admit(&put(Priority::Background, 10), 0).unwrap();
    rig.admit(&fin(2), 0).unwrap();
    rig.sender.seal();
    assert_eq!(rig.admit(&put(DATA, 10), 0), Err(Refusal::Closed));
    rig.pump(0);
    assert!(rig.flushed);
    assert_eq!(rig.rx.put_sizes(), [3 * MTU as usize, 10]);
    assert!(rig.seen().contains(&Seen::Final(2)));
    assert_eq!(rig.used(), 0);
}

// ---------------------------------------------------------------------------------------------
// Conditions of the second design review
// ---------------------------------------------------------------------------------------------

// C2. A query is not started at or after the end of its routing branch, also when that end
// falls between its admission and the batch that would carry it.
#[test]
fn a_query_is_not_started_at_its_branch_expiry() {
    let mut rig = Rig::new(conf());
    rig.admit(&put(DATA, 900), 0).unwrap();
    assert!(rig.start_write(0));
    rig.admit_until(&query(9, None), 0, 100).unwrap();
    rig.admit_until(&query(10, None), 0, 101).unwrap();
    rig.complete_write(100);
    rig.pump(100);
    assert_eq!(rig.seen(), [Seen::Put(DATA, 900, 0), Seen::Request(10)]);
    assert_eq!(rig.undelivered(), [(9, UndeliveredReason::Expired)]);
}

// C2. A query forwards the lifetime it has left when it is started.
#[test]
fn a_query_forwards_its_remaining_lifetime() {
    let timeout_of = |rig: &Rig, rid: RequestId| {
        rig.rx
            .delivered
            .iter()
            .find_map(|(_, m)| match &m.body {
                NetworkBody::Request(r) if r.id == rid => Some(r.ext_timeout),
                _ => None,
            })
            .unwrap()
    };
    let mut rig = Rig::new(conf());
    // Sent at once: unchanged.
    rig.admit_until(&query(1, Some(Duration::from_millis(1_000))), 0, 1_000)
        .unwrap();
    rig.pump(0);
    assert_eq!(timeout_of(&rig, 1), Some(Duration::from_millis(1_000)));
    // Sent after 200 ms behind a pending write: 200 ms less.
    rig.admit(&put(DATA, 900), 0).unwrap();
    assert!(rig.start_write(0));
    rig.admit_until(&query(2, Some(Duration::from_millis(1_000))), 0, 1_000)
        .unwrap();
    rig.admit_until(&query(3, None), 0, 1_000).unwrap();
    rig.complete_write(200);
    rig.pump(200);
    assert_eq!(timeout_of(&rig, 2), Some(Duration::from_millis(800)));
    // A query without a timeout stays without one.
    assert_eq!(timeout_of(&rig, 3), None);
    assert!(rig.rx.errors.is_empty(), "{:?}", rig.rx.errors);
}

// C2. Whatever the query, what is sent after a wait is the query itself with its timeout
// reduced: encoding the head again in front of the unchanged body gives what the codec gives.
#[test]
fn a_delayed_query_is_the_same_query_with_less_time() {
    for i in 0..300_u32 {
        let mut request = Request::rand();
        request.id = i;
        request.ext_qos = ext::QoSType::new(DATA, CongestionControl::Block, false);
        request.ext_timeout = (i % 3 != 0).then(|| Duration::from_millis(10_000 + i as u64));
        let mut expected = request.clone();
        expected.ext_timeout = request.ext_timeout.map(|t| t - Duration::from_millis(30));
        let mut rig = Rig::new(HandoverConf {
            data_bytes: 1 << 20,
            ..conf()
        });
        rig.admit(&put(DATA, 900), 0).unwrap();
        assert!(rig.start_write(0));
        let msg = NetworkMessage::from(NetworkBody::Request(request));
        rig.admit_until(&msg, 0, 5_000).unwrap();
        rig.complete_write(30);
        rig.pump(30);
        assert!(rig.rx.errors.is_empty(), "{:?}", rig.rx.errors);
        let (_, delivered) = rig.rx.delivered.last().unwrap();
        assert_eq!(delivered.body, NetworkBody::Request(expected), "query {i}");
    }
}

// C3. When more queries lose replies than can be remembered, the transport is closed: a final
// is never sent as if nothing had been dropped.
#[test]
fn too_many_lost_replies_close_the_transport() {
    let mut rig = Rig::new(HandoverConf {
        reserve_max_bytes: 1 << 20,
        ..conf()
    });
    while rig.admit(&put(DATA, 900), 0).is_ok() {}
    let mut closed_at = None;
    for rid in 0..(LOSS_CAPACITY as RequestId + 10) {
        if rig.admit(&reply(rid, 950), 0) == Err(Refusal::Closed) {
            closed_at = Some(rid as usize);
            break;
        }
    }
    assert_eq!(closed_at, Some(LOSS_CAPACITY + 1));
    assert!(!rig.start_write(0));
    assert_eq!(rig.closed, Some(CloseReason::ReserveOverflow));
}

// C3. An undeclaration that waits for a use keeps the age of its admission: it closes the
// transport like a control message that waits in its queue.
#[test]
fn a_parked_undeclaration_keeps_its_age() {
    let mut rig = Rig::new(conf());
    rig.admit(&put_key(DATA, 5 * MTU as usize, 7), 0).unwrap();
    assert!(rig.start_write(0));
    rig.admit(&undeclare_key(7), 1).unwrap();
    assert!(!rig.start_write(CONTROL_MAX_AGE_MS));
    assert_eq!(rig.closed, None);
    assert!(!rig.start_write(CONTROL_MAX_AGE_MS + 2));
    assert_eq!(rig.closed, Some(CloseReason::ControlAge));
}

// C3. A declaration that names a key expression id uses it like a publication does, until the
// batch that carries it has been written.
#[test]
fn a_declaration_that_names_an_id_counts_as_a_use() {
    let mut rig = Rig::new(conf());
    let subscriber = declare_body(DeclareBody::DeclareSubscriber(DeclareSubscriber {
        id: 1,
        wire_expr: WireExpr {
            scope: 7,
            suffix: "".into(),
            mapping: Mapping::Sender,
        },
    }));
    rig.admit(&subscriber, 0).unwrap();
    assert!(rig.start_write(0));
    // Its write is pending: the undeclaration of the id waits, and the id cannot be declared
    // again.
    rig.admit(&undeclare_key(7), 1).unwrap();
    rig.admit(&declare_key(7), 1).unwrap();
    assert!(!rig.start_write(1));
    assert_eq!(rig.closed, Some(CloseReason::KeyIdReuse));
}

// A link that takes a batch slowly advances: the close period counts from the last part of
// the batch that the link took, not from the start of the batch.
#[test]
fn a_write_that_advances_keeps_the_link() {
    let mut rig = Rig::new(conf());
    rig.admit(&put(DATA, 900), 0).unwrap();
    assert!(rig.start_write(0));
    rig.admit(&put(DATA, 10), 0).unwrap();
    let at = rig.at(CLOSE_MS - 500);
    rig.task.as_mut().unwrap().progressed(at);
    assert!(!rig.start_write(CLOSE_MS + 1));
    assert_eq!(rig.closed, None);
    assert!(!rig.start_write(2 * CLOSE_MS - 501));
    assert_eq!(rig.closed, None);
    assert!(!rig.start_write(2 * CLOSE_MS - 500));
    assert_eq!(rig.closed, Some(CloseReason::NoProgress));
}

// C4. One step sorts a bounded number of handed-over entries, whatever was handed over.
#[test]
fn a_step_does_bounded_work() {
    let mut rig = Rig::new(HandoverConf {
        data_bytes: 64 * 1024,
        ..conf()
    });
    for _ in 0..200 {
        rig.admit(&put(DATA, 1), 0).unwrap();
    }
    assert!(rig.start_write(0));
    assert_eq!(rig.rx.put_sizes().len(), task::QUANTUM);
    rig.complete_write(0);
    rig.pump(0);
    assert_eq!(rig.rx.put_sizes().len(), 200);
}

// C4. While the link writes a batch the sender gives no other one, whatever is handed over
// and whatever time passes; it has given the batch away and cannot change it.
#[test]
fn while_a_write_is_pending_no_other_batch_is_given() {
    let mut rig = Rig::new(conf());
    rig.admit(&put(DATA, 100), 0).unwrap();
    assert!(rig.start_write(0));
    let written = rig.writing.as_ref().unwrap().as_slice().to_vec();
    for ms in 1..50 {
        rig.admit(&put(Priority::RealTime, 10), ms).unwrap();
        rig.admit(&declare_key(ms as ExprId), ms).unwrap();
        assert!(!rig.start_write(ms));
    }
    assert_eq!(rig.writing.as_ref().unwrap().as_slice(), written);
    rig.complete_write(50);
    assert!(rig.start_write(50));
}

// C5. A link whose neighbours have used up the process pool still admits control messages and
// finals into its own minimum reserve.
#[test]
fn a_quiet_link_keeps_its_reserve_when_the_process_pool_is_empty() {
    let conf = conf();
    let pool = ProcessPool::new(2 * conf.reserve_min_bytes + 6 * 1024);
    let quiet = Rig::in_pool(conf.clone(), pool.clone()).unwrap();
    let busy = Rig::in_pool(conf.clone(), pool.clone()).unwrap();
    let mut refusal = None;
    for size in [900, 300, 100, 30, 1] {
        loop {
            match busy.admit(&put(DATA, size), 0) {
                Ok(()) => {}
                Err(r) => {
                    refusal = Some(r);
                    break;
                }
            }
        }
    }
    assert_eq!(refusal, Some(Refusal::ProcessLimit));
    let in_use = pool.in_use();
    assert!(pool.total() - in_use < 1 + ENTRY_CHARGE + 64, "{in_use}");
    quiet.admit(&fin(1), 0).unwrap();
    quiet.admit(&declare_key(1), 0).unwrap();
    assert_eq!(pool.in_use(), in_use);
    assert!(quiet.sender.usage().reserve <= conf.reserve_min_bytes);
    // Its publications are refused for the process limit, not for its own budget.
    assert_eq!(quiet.admit(&put(DATA, 900), 0), Err(Refusal::ProcessLimit));
}

// C5. A refusal leaves nothing behind: neither in the budgets of the link nor in the pool.
#[test]
fn a_refusal_gives_everything_back() {
    let rig = Rig::new(conf());
    while rig.admit(&put(DATA, 900), 0).is_ok() {}
    let usage = rig.sender.usage();
    let in_use = rig.pool.in_use();
    assert_eq!(rig.admit(&put(DATA, 950), 0), Err(Refusal::LinkBudget));
    assert_eq!(rig.admit(&put(DATA, 1 << 20), 0), Err(Refusal::TooLarge));
    assert_eq!(
        rig.admit(&put_droppable(DATA, 10), 0),
        Err(Refusal::Droppable)
    );
    assert_eq!(rig.sender.usage(), usage);
    assert_eq!(rig.pool.in_use(), in_use);
}

// C6. A producer that finished encoding after the transport was sealed or closed is refused
// and gives its reservation back itself: nothing is left to a sender that has gone.
#[test]
fn a_commit_after_the_fence_is_refused_and_refunded() {
    let conf = conf();
    // Sealed for an orderly close.
    let mut rig = Rig::new(conf.clone());
    let at = rig.at(0);
    let prepared = rig
        .sender
        .prepare(put(DATA, 500).as_ref(), None, at)
        .unwrap();
    assert!(rig.used() > 0);
    rig.sender.seal();
    assert_eq!(rig.sender.commit(prepared).err(), Some(Refusal::Closed));
    assert_eq!(rig.used(), 0);
    assert_eq!(rig.pool.in_use(), conf.reserve_min_bytes);
    rig.pump(0);
    assert!(rig.flushed);
    assert!(rig.seen().is_empty());
    // Closed by force.
    let mut rig = Rig::new(conf);
    let prepared = rig
        .sender
        .prepare(query(4, None).as_ref(), None, at)
        .unwrap();
    rig.task.take().unwrap().abort();
    assert_eq!(rig.sender.commit(prepared).err(), Some(Refusal::Closed));
    assert_eq!(rig.pool.in_use(), 0);
    assert_eq!(rig.sender.usage(), Usage::default());
}
