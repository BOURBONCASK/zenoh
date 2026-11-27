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

//! The owned sender of a selected transport.
//!
//! A thread that routes a message to a selected transport does not wait for the link: it
//! encodes the message into a buffer reserved against the budgets of the link and of the
//! process, hands it over ([`Sender::admit`]) and returns. One task per transport owns
//! everything else ([`SenderTask`]): the order of emission, batches, fragments, sequence
//! numbers, the age of what is queued and the decision to close a link that makes no progress.
//!
//! The task part takes the current instant as an argument and performs no input or output:
//! [`SenderTask::step`] says what to do next, the caller writes the batch and gives it back
//! with [`SenderTask::written`]. The link task drives it with the real clock and the real
//! link; the tests drive it by hand.
// Nothing selects a link yet: a rule is refused when the process starts.
#![allow(dead_code, unused_imports)]

use std::{
    any::Any,
    collections::{LinkedList, VecDeque},
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
    time::{Duration, Instant},
};

use zenoh_buffers::writer::{DidntWrite, HasWriter, Writer};
use zenoh_codec::{WCodec, Zenoh080};
use zenoh_protocol::{
    core::{ExprId, Priority, Reliability},
    network::{
        DeclareBody, Mapping, NetworkBodyRef, NetworkMessageExt, NetworkMessageRef, Request,
        RequestId,
    },
    zenoh::{Query, RequestBody},
};

use crate::common::{batch::BatchConfig, priority::TransportPriorityTx};

mod driver;
mod rules;
mod task;
#[cfg(test)]
mod tests;

pub(crate) use driver::{run, End, Progress, Sink};
pub(crate) use rules::ActivationRefusal;
pub use rules::{Rules, RulesStatus};
pub(crate) use task::{SenderTask, Step};

/// The text of the error reply that ends a query which was not sent.
pub const QUERY_NOT_DELIVERED: &str = "Congestion: query not delivered";
/// The text of the error reply that ends a query which waited too long to be sent.
pub const QUERY_EXPIRED: &str = "Congestion: query expired before transmission";
/// The text of the error reply sent before the final of a query some replies of which were
/// dropped.
pub const REPLIES_DROPPED: &str = "Congestion: replies dropped";

/// What an entry is charged on top of its encoded bytes: the entry itself, its list node, its
/// part of the key expression and parking bookkeeping.
pub(crate) const ENTRY_CHARGE: usize = 128;
/// What a reply-loss record is charged, from its admission until the final of its query.
pub(crate) const LOSS_CHARGE: usize = 64;
/// Reply-loss records waiting in the intake, and query ids with lost replies, at most.
pub(crate) const LOSS_CAPACITY: usize = 1024;
/// Queries reported as not sent and not yet taken by the completion task, at most.
pub(crate) const COMPLETION_CAPACITY: usize = 1024;

#[derive(Debug, Clone)]
pub(crate) struct HandoverConf {
    pub(crate) batch: BatchConfig,
    pub(crate) is_qos: bool,
    /// The size, in batches, of the queue each priority has on an unselected link: a droppable
    /// message is admitted while its priority holds less than that.
    pub(crate) queue_size: [usize; Priority::NUM],
    /// Budget of each priority for publications, queries and replies.
    pub(crate) data_bytes: usize,
    /// Budget of the link for messages longer than one batch that do not fit their priority.
    pub(crate) large_bytes: usize,
    /// Taken from the process pool at activation: room that no other link can use for control
    /// messages, finals and markers.
    pub(crate) reserve_min_bytes: usize,
    /// What control messages, finals and markers may hold at most.
    pub(crate) reserve_max_bytes: usize,
    /// A publication, a query or a reply that has not started to be sent after this time is
    /// discarded.
    pub(crate) max_age: Duration,
    /// A control message that has not started to be sent after this time closes the transport.
    pub(crate) control_max_age: Duration,
    /// Without a completed write for this time while something is pending, the transport is
    /// closed.
    pub(crate) wait_before_close: Duration,
    /// The lifetime of a query that comes without one.
    pub(crate) default_query_timeout: Duration,
}

/// The budget shared by all selected links of the process.
#[derive(Debug)]
pub(crate) struct ProcessPool {
    total: usize,
    used: AtomicUsize,
}

impl ProcessPool {
    pub(crate) fn new(total_max_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            total: total_max_bytes,
            used: AtomicUsize::new(0),
        })
    }

    /// Bytes reserved at this moment, the minimum of every active link included.
    pub(crate) fn in_use(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    pub(crate) fn total(&self) -> usize {
        self.total
    }

    fn try_take(&self, bytes: usize) -> bool {
        try_add(&self.used, bytes, self.total)
    }

    fn give(&self, bytes: usize) {
        self.used.fetch_sub(bytes, Ordering::AcqRel);
    }
}

/// Adds `bytes` to `counter` unless the sum would exceed `limit`.
fn try_add(counter: &AtomicUsize, bytes: usize, limit: usize) -> bool {
    let mut used = counter.load(Ordering::Acquire);
    loop {
        let Some(sum) = used.checked_add(bytes).filter(|sum| *sum <= limit) else {
            return false;
        };
        match counter.compare_exchange_weak(used, sum, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(now) => used = now,
        }
    }
}

/// Why a message was not admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The budget of the link for this message is full.
    LinkBudget,
    /// The budget of the process is full.
    ProcessLimit,
    /// The message is larger than any budget of the link.
    TooLarge,
    /// A droppable message found its priority as full as the queue of an unselected link.
    Droppable,
    /// The transport is sealed for an orderly close, or closed.
    Closed,
}

/// Why the sender closes its transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum CloseReason {
    /// No write completed for `wait_before_close` while something was pending.
    NoProgress = 1,
    /// A control message waited longer than `control_max_age`.
    ControlAge = 2,
    /// A control message, a final, a marker or a reply-loss record did not fit the reserve.
    ReserveOverflow = 3,
    /// A key expression id was declared again while its undeclaration waited for queued uses.
    KeyIdReuse = 4,
}

impl CloseReason {
    pub(crate) const ALL: [Self; 4] = [
        Self::NoProgress,
        Self::ControlAge,
        Self::ReserveOverflow,
        Self::KeyIdReuse,
    ];

    fn from_u8(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::NoProgress),
            2 => Some(Self::ControlAge),
            3 => Some(Self::ReserveOverflow),
            4 => Some(Self::KeyIdReuse),
            _ => None,
        }
    }

    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::NoProgress => "no_progress",
            Self::ControlAge => "control_age",
            Self::ReserveOverflow => "reserve_overflow",
            Self::KeyIdReuse => "key_id_reuse",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActivationError {
    /// The process pool cannot give the link its minimum reserve.
    ReserveUnavailable,
}

/// The routing branch a query belongs to, as far as the transport needs to know it.
#[derive(Clone)]
pub struct RequestLifetime {
    /// The instant at which routing gives the branch up. The query is not started at or after
    /// it.
    pub expiry: Instant,
    /// Identifies the branch to routing when the query is reported as not sent. It is opaque
    /// here, and weak: it keeps nothing alive but its own identity.
    pub branch: Option<Weak<dyn Any + Send + Sync>>,
}

/// Why a query was not sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UndeliveredReason {
    /// Refused at admission, or discarded unsent when the transport closed.
    NotDelivered,
    /// Not started before `max_age`, or before the end of its routing branch.
    Expired,
}

impl UndeliveredReason {
    /// The text of the error reply that ends the branch.
    pub fn text(&self) -> &'static str {
        match self {
            Self::NotDelivered => QUERY_NOT_DELIVERED,
            Self::Expired => QUERY_EXPIRED,
        }
    }
}

/// A query that was not sent: routing ends its branch with an error reply and a final.
#[derive(Clone)]
pub struct Undelivered {
    pub rid: RequestId,
    pub reason: UndeliveredReason,
    /// The handle given with the query (see [`RequestLifetime::branch`]).
    pub branch: Option<Weak<dyn Any + Send + Sync>>,
}

/// What the sender of a transport holds and has done since its link was added, for the admin
/// space.
#[derive(Debug, Clone, serde::Serialize)]
pub struct HandoverStatus {
    /// Bytes reserved by publications, queries and replies that fit a batch, for each priority.
    pub data_bytes: [usize; Priority::NUM],
    /// Bytes reserved by messages longer than a batch.
    pub large_bytes: usize,
    /// Bytes reserved by declarations, interests, finals and the records of dropped replies.
    pub reserve_bytes: usize,
    /// Messages taken and not yet written.
    pub entries: usize,
    /// Messages written.
    pub sent: usize,
    pub refused_link_budget: usize,
    pub refused_process_limit: usize,
    pub refused_too_large: usize,
    pub refused_droppable: usize,
    pub refused_closed: usize,
    /// Publications, queries and replies dropped because they did not start in time.
    pub expired: usize,
    pub discarded_at_close: usize,
    /// Replies refused or expired; their queries got the error reply that says so.
    pub replies_lost: usize,
    /// Queries refused, expired or discarded, reported to routing.
    pub queries_undelivered: usize,
    /// Reports of unsent queries that did not fit: their branches end by their timeout.
    pub completion_overflow: usize,
}

/// What the budgets of a link hold at this moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Usage {
    /// Per priority.
    pub(crate) data: [usize; Priority::NUM],
    pub(crate) large: usize,
    pub(crate) reserve: usize,
    pub(crate) entries: usize,
}

/// What happened to the messages of a link, by outcome.
#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub(crate) sent: AtomicUsize,
    pub(crate) refused_link_budget: AtomicUsize,
    pub(crate) refused_process_limit: AtomicUsize,
    pub(crate) refused_too_large: AtomicUsize,
    pub(crate) refused_droppable: AtomicUsize,
    pub(crate) refused_closed: AtomicUsize,
    pub(crate) expired: AtomicUsize,
    pub(crate) discarded_at_close: AtomicUsize,
    pub(crate) replies_lost: AtomicUsize,
    pub(crate) queries_undelivered: AtomicUsize,
    pub(crate) completion_overflow: AtomicUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Class {
    /// A publication.
    Data,
    Request,
    Reply,
    /// A control message, a final, a marker: never dropped while the transport is up.
    Reserve,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PoolKind {
    Data(usize),
    Large,
    Reserve,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Charge {
    pub(super) kind: PoolKind,
    pub(super) bytes: usize,
}

/// What a query needs to be encoded again with the lifetime it has left when it is started.
pub(super) struct RequestMeta {
    /// The query without its body.
    pub(super) head: Request,
    /// Where the body starts in the encoded bytes of the entry.
    pub(super) body_offset: usize,
    pub(super) branch: Option<Weak<dyn Any + Send + Sync>>,
}

pub(super) struct Entry {
    /// The network message, encoded.
    pub(super) bytes: Vec<u8>,
    /// The length of the encoding at admission: what the entry counts for in the queued bytes
    /// of its priority.
    pub(super) queued_len: usize,
    pub(super) lane: usize,
    pub(super) reliability: Reliability,
    pub(super) class: Class,
    pub(super) block_first: bool,
    /// The id of the query, for a query, a reply and a final.
    pub(super) rid: Option<RequestId>,
    pub(super) is_final: bool,
    /// The key expression id of the sender that the message refers to.
    pub(super) key_use: Option<ExprId>,
    pub(super) declares_key: Option<ExprId>,
    pub(super) undeclares_key: Option<ExprId>,
    pub(super) admitted: Instant,
    /// The entry is discarded if it has not started at this instant.
    pub(super) expiry: Option<Instant>,
    pub(super) request: Option<Box<RequestMeta>>,
    pub(super) charge: Charge,
    /// Discarded in place: nothing left but its position in the queue.
    pub(super) dead: bool,
}

pub(super) enum Item {
    Entry(Box<Entry>),
    /// A reply of this query was refused at admission: its key expression, when it was
    /// written in full, and the queue of its priority.
    ReplyLost(RequestId, Option<Box<str>>, usize),
}

/// What the record of a refused reply holds of the reserve until the task has seen it. It
/// keeps the key expression of the reply for the error reply that will stand for the dropped
/// replies of the query: an error reply without a key is dropped by a session that has a
/// namespace and denied by the access control, where the replies themselves pass.
pub(super) fn loss_charge(key: &Option<Box<str>>) -> usize {
    LOSS_CHARGE + key.as_ref().map_or(0, |key| key.len())
}

/// The key expression of a reply, when it is written in full and not relative to a declared
/// id: a router forwards every reply that way.
pub(super) fn reply_key(wire_expr: &zenoh_protocol::core::WireExpr) -> Option<Box<str>> {
    (wire_expr.scope == 0 && !wire_expr.suffix.is_empty()).then(|| wire_expr.suffix.as_ref().into())
}

const OPEN: u8 = 0;
const SEALED: u8 = 1;
const CLOSED: u8 = 2;

struct Intake {
    items: LinkedList<Item>,
    state: u8,
}

/// A message that has its reservation and its encoding, not yet handed over.
pub(crate) struct Prepared {
    item: LinkedList<Item>,
}

/// The part of the sender that routing threads use.
pub(crate) struct Sender {
    pub(super) conf: HandoverConf,
    pub(super) pool: Arc<ProcessPool>,
    intake: Mutex<Intake>,
    /// Mirror of the state in `intake`, for the check before any work.
    state: AtomicU8,
    /// A close asked for by a producer (the reserve overflowed).
    close_request: AtomicU8,
    data: [AtomicUsize; Priority::NUM],
    /// Encoded bytes queued per priority: what a droppable message is compared with.
    queued: [AtomicUsize; Priority::NUM],
    large: AtomicUsize,
    /// Held for a few instructions: the part of the reserve above the minimum of the link is
    /// taken from and given back to the process pool together with the change of this value.
    reserve: Mutex<usize>,
    entries: AtomicUsize,
    block_first: [AtomicBool; Priority::NUM],
    loss_records: AtomicUsize,
    reserve_released: AtomicBool,
    undelivered: Mutex<VecDeque<Undelivered>>,
    pub(crate) counters: Counters,
    /// Something was handed over, sealed or closed: the task of the sender has work.
    pub(crate) wake: tokio::sync::Notify,
    /// A query was reported as not sent: the completion task has work.
    pub(crate) completion_wake: tokio::sync::Notify,
    /// The reason that the Close message of an orderly close carries.
    close_message_reason: AtomicU8,
    /// Cancelled when the task of the sender has ended.
    done: tokio_util::sync::CancellationToken,
    created: Instant,
    /// Milliseconds after `created`, plus one, of the last refusal that was logged.
    last_report: std::sync::atomic::AtomicU64,
}

impl Sender {
    /// Creates the sender of a transport and takes the minimum reserve of its link from the
    /// process pool.
    pub(crate) fn activate(
        conf: HandoverConf,
        pool: Arc<ProcessPool>,
        priorities: &[TransportPriorityTx],
    ) -> Result<(Arc<Sender>, SenderTask), ActivationError> {
        if !pool.try_take(conf.reserve_min_bytes) {
            return Err(ActivationError::ReserveUnavailable);
        }
        let sender = Arc::new(Sender {
            conf,
            pool,
            intake: Mutex::new(Intake {
                items: LinkedList::new(),
                state: OPEN,
            }),
            state: AtomicU8::new(OPEN),
            close_request: AtomicU8::new(0),
            data: Default::default(),
            queued: Default::default(),
            large: AtomicUsize::new(0),
            reserve: Mutex::new(0),
            entries: AtomicUsize::new(0),
            block_first: Default::default(),
            loss_records: AtomicUsize::new(0),
            reserve_released: AtomicBool::new(false),
            undelivered: Mutex::new(VecDeque::new()),
            counters: Counters::default(),
            wake: tokio::sync::Notify::new(),
            completion_wake: tokio::sync::Notify::new(),
            close_message_reason: AtomicU8::new(0),
            done: tokio_util::sync::CancellationToken::new(),
            created: Instant::now(),
            last_report: std::sync::atomic::AtomicU64::new(0),
        });
        let task = SenderTask::new(sender.clone(), priorities);
        Ok((sender, task))
    }

    /// Hands a message over. Never waits for the link.
    ///
    /// A refused query is also reported through [`Sender::take_undelivered`]; a refused reply
    /// is recorded and announced before the final of its query.
    pub(crate) fn admit(
        &self,
        msg: NetworkMessageRef,
        lifetime: Option<&RequestLifetime>,
        now: Instant,
    ) -> Result<(), Refusal> {
        let prepared = self.prepare(msg, lifetime, now)?;
        self.commit(prepared)
    }

    /// The first half of [`Sender::admit`]: reserves and encodes.
    pub(crate) fn prepare(
        &self,
        msg: NetworkMessageRef,
        lifetime: Option<&RequestLifetime>,
        now: Instant,
    ) -> Result<Prepared, Refusal> {
        if self.state.load(Ordering::Acquire) != OPEN {
            return Err(self.refused(Refusal::Closed));
        }
        let lane = if self.conf.is_qos {
            msg.priority() as usize
        } else {
            0
        };
        let (class, rid, is_final) = match msg.body {
            NetworkBodyRef::Push(_) => (Class::Data, None, false),
            NetworkBodyRef::Request(r) => (Class::Request, Some(r.id), false),
            NetworkBodyRef::Response(r) => (Class::Reply, Some(r.rid), false),
            NetworkBodyRef::ResponseFinal(r) => (Class::Reserve, Some(r.rid), true),
            NetworkBodyRef::Interest(_) | NetworkBodyRef::Declare(_) | NetworkBodyRef::OAM(_) => {
                (Class::Reserve, None, false)
            }
        };
        let branch = lifetime.and_then(|l| l.branch.clone());

        // A droppable message is admitted while its priority holds less than the queue of an
        // unselected link can. A message that asks to be the only blocking one of its priority
        // is droppable when another one is already waiting.
        let mut droppable = class != Class::Reserve && msg.is_droppable();
        let mut block_first = false;
        #[cfg(feature = "unstable")]
        if class != Class::Reserve
            && msg.congestion_control() == zenoh_protocol::core::CongestionControl::BlockFirst
        {
            if self.block_first[lane].swap(true, Ordering::AcqRel) {
                droppable = true;
            } else {
                block_first = true;
            }
        }
        let give_slot_back = |sender: &Self| {
            if block_first {
                sender.block_first[lane].store(false, Ordering::Release);
            }
        };
        if droppable {
            let threshold = self.conf.queue_size[lane] * self.conf.batch.mtu as usize;
            if self.queued[lane].load(Ordering::Acquire) >= threshold {
                give_slot_back(self);
                return Err(self.refuse(Refusal::Droppable, class, rid, branch, msg, lane));
            }
        }

        // The length first, without allocating or copying; then the reservation; then one
        // buffer of exactly that length.
        let len = encoded_len(msg);
        let mut bytes_charged = len + ENTRY_CHARGE;
        let mut request = None;
        if let NetworkBodyRef::Request(r) = msg.body {
            let mut counter = Counter(0);
            let _ = Zenoh080::new().write(&mut counter, &r.payload);
            let head = Request {
                id: r.id,
                wire_expr: r.wire_expr.to_owned(),
                ext_qos: r.ext_qos,
                ext_tstamp: r.ext_tstamp,
                ext_nodeid: r.ext_nodeid,
                ext_target: r.ext_target,
                ext_budget: r.ext_budget,
                ext_timeout: r.ext_timeout,
                ext_ts_stack: r.ext_ts_stack.clone(),
                payload: RequestBody::Query(Query::default()),
            };
            bytes_charged += len.saturating_sub(counter.0);
            request = Some(Box::new(RequestMeta {
                head,
                body_offset: len.saturating_sub(counter.0),
                branch: branch.clone(),
            }));
        }
        let charge = match self.reserve_for(class, lane, bytes_charged, len) {
            Ok(charge) => charge,
            Err(refusal) => {
                give_slot_back(self);
                if class == Class::Reserve {
                    // Never dropped while the transport is up: the transport is closed.
                    self.request_close(CloseReason::ReserveOverflow);
                    return Err(self.refused(Refusal::Closed));
                }
                return Err(self.refuse(refusal, class, rid, branch, msg, lane));
            }
        };
        let mut bytes = Vec::with_capacity(len);
        let mut writer = (&mut bytes).writer();
        if Zenoh080::new().write(&mut writer, msg).is_err() || bytes.len() != len {
            // The codec writes into a vector: this cannot fail.
            self.release(&charge);
            give_slot_back(self);
            return Err(self.refuse(Refusal::TooLarge, class, rid, branch, msg, lane));
        }

        let wire_expr = msg.wire_expr();
        let key_use = wire_expr
            .filter(|w| w.mapping == Mapping::Sender && w.scope != 0)
            .map(|w| w.scope);
        let (declares_key, undeclares_key) = match msg.body {
            NetworkBodyRef::Declare(d) => match &d.body {
                DeclareBody::DeclareKeyExpr(k) => (Some(k.id), None),
                DeclareBody::UndeclareKeyExpr(k) => (None, Some(k.id)),
                _ => (None, None),
            },
            _ => (None, None),
        };
        let expiry = match class {
            Class::Reserve => None,
            Class::Data | Class::Reply => Some(now + self.conf.max_age),
            Class::Request => {
                let of_branch = match (lifetime, &msg.body) {
                    (Some(lifetime), _) => lifetime.expiry,
                    (None, NetworkBodyRef::Request(r)) => {
                        now + r.ext_timeout.unwrap_or(self.conf.default_query_timeout)
                    }
                    (None, _) => now + self.conf.default_query_timeout,
                };
                Some(of_branch.min(now + self.conf.max_age))
            }
        };
        self.queued[lane].fetch_add(len, Ordering::AcqRel);
        self.entries.fetch_add(1, Ordering::AcqRel);
        let entry = Box::new(Entry {
            bytes,
            queued_len: len,
            lane,
            reliability: msg.reliability,
            class,
            block_first,
            rid,
            is_final,
            key_use,
            declares_key,
            undeclares_key,
            admitted: now,
            expiry,
            request,
            charge,
            dead: false,
        });
        let mut item = LinkedList::new();
        item.push_back(Item::Entry(entry));
        Ok(Prepared { item })
    }

    /// The second half of [`Sender::admit`]: hands over, unless the transport was sealed or
    /// closed meanwhile; then the reservation is given back here.
    pub(crate) fn commit(&self, mut prepared: Prepared) -> Result<(), Refusal> {
        {
            let mut intake = self.intake.lock().unwrap();
            if intake.state == OPEN {
                intake.items.append(&mut prepared.item);
            }
        }
        if prepared.item.is_empty() {
            self.wake.notify_one();
            return Ok(());
        }
        for item in prepared.item {
            if let Item::Entry(entry) = item {
                self.forget(&entry);
            }
        }
        Err(self.refused(Refusal::Closed))
    }

    /// Starts an orderly close: nothing more is admitted; the task sends what was admitted and
    /// then reports [`Step::Flushed`].
    pub(crate) fn seal(&self) {
        {
            let mut intake = self.intake.lock().unwrap();
            if intake.state == OPEN {
                intake.state = SEALED;
                self.state.store(SEALED, Ordering::Release);
            }
        }
        self.wake.notify_one();
    }

    /// [`Sender::seal`], with the reason that the Close message carries.
    pub(crate) fn seal_for_close(&self, reason: u8) {
        self.close_message_reason.store(reason, Ordering::Release);
        self.seal();
    }

    pub(super) fn close_message_reason(&self) -> u8 {
        self.close_message_reason.load(Ordering::Acquire)
    }

    /// The task of the sender has ended: nothing more is written.
    pub(crate) fn mark_done(&self) {
        self.done.cancel();
    }

    /// Waits until the task of the sender has ended.
    pub(crate) async fn done(&self) {
        self.done.cancelled().await
    }

    pub(crate) fn is_sealed(&self) -> bool {
        self.state.load(Ordering::Acquire) == SEALED
    }

    pub(crate) fn usage(&self) -> Usage {
        let mut usage = Usage {
            large: self.large.load(Ordering::Acquire),
            reserve: *self.reserve.lock().unwrap(),
            entries: self.entries.load(Ordering::Acquire),
            ..Usage::default()
        };
        for (lane, data) in self.data.iter().enumerate() {
            usage.data[lane] = data.load(Ordering::Acquire);
        }
        usage
    }

    pub(crate) fn status(&self) -> HandoverStatus {
        let usage = self.usage();
        let counters = &self.counters;
        let get = |counter: &AtomicUsize| counter.load(Ordering::Relaxed);
        HandoverStatus {
            data_bytes: usage.data,
            large_bytes: usage.large,
            reserve_bytes: usage.reserve,
            entries: usage.entries,
            sent: get(&counters.sent),
            refused_link_budget: get(&counters.refused_link_budget),
            refused_process_limit: get(&counters.refused_process_limit),
            refused_too_large: get(&counters.refused_too_large),
            refused_droppable: get(&counters.refused_droppable),
            refused_closed: get(&counters.refused_closed),
            expired: get(&counters.expired),
            discarded_at_close: get(&counters.discarded_at_close),
            replies_lost: get(&counters.replies_lost),
            queries_undelivered: get(&counters.queries_undelivered),
            completion_overflow: get(&counters.completion_overflow),
        }
    }

    /// True for the first refusal of a blocking message, and then at most once every ten
    /// seconds: the refusals worth a line in the log.
    pub(crate) fn should_report(&self, refusal: Refusal) -> bool {
        if matches!(refusal, Refusal::Droppable | Refusal::Closed) {
            return false;
        }
        let now = self.created.elapsed().as_millis() as u64 + 1;
        let last = self.last_report.load(Ordering::Relaxed);
        (last == 0 || now.saturating_sub(last) >= 10_000)
            && self
                .last_report
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
    }

    /// The queries that were not sent since the last call (bounded).
    pub(crate) fn take_undelivered(&self) -> Vec<Undelivered> {
        self.undelivered.lock().unwrap().drain(..).collect()
    }

    // -- used by the task ----------------------------------------------------------------

    /// Takes what was handed over since the last call.
    pub(super) fn take_intake(&self) -> LinkedList<Item> {
        std::mem::take(&mut self.intake.lock().unwrap().items)
    }

    /// Nothing is admitted any more. Returns what was handed over and not yet taken.
    pub(super) fn close_intake(&self) -> LinkedList<Item> {
        let mut intake = self.intake.lock().unwrap();
        intake.state = CLOSED;
        self.state.store(CLOSED, Ordering::Release);
        std::mem::take(&mut intake.items)
    }

    pub(super) fn close_requested(&self) -> Option<CloseReason> {
        CloseReason::from_u8(self.close_request.load(Ordering::Acquire))
    }

    fn request_close(&self, reason: CloseReason) {
        let _ = self.close_request.compare_exchange(
            0,
            reason as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        {
            let mut intake = self.intake.lock().unwrap();
            intake.state = CLOSED;
            self.state.store(CLOSED, Ordering::Release);
        }
        self.wake.notify_one();
    }

    /// Reports a query as not sent.
    pub(super) fn report_undelivered(
        &self,
        rid: RequestId,
        reason: UndeliveredReason,
        branch: Option<Weak<dyn Any + Send + Sync>>,
    ) {
        self.counters
            .queries_undelivered
            .fetch_add(1, Ordering::Relaxed);
        {
            let mut ring = self.undelivered.lock().unwrap();
            if ring.len() >= COMPLETION_CAPACITY {
                // The branch ends by its routing timeout.
                self.counters
                    .completion_overflow
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            ring.push_back(Undelivered {
                rid,
                reason,
                branch,
            });
        }
        self.completion_wake.notify_one();
    }

    /// Gives back what an entry holds, when it has been sent, discarded or refused.
    pub(super) fn forget(&self, entry: &Entry) {
        self.release(&entry.charge);
        self.queued[entry.lane].fetch_sub(entry.queued_len, Ordering::AcqRel);
        self.entries.fetch_sub(1, Ordering::AcqRel);
        if entry.block_first {
            self.block_first[entry.lane].store(false, Ordering::Release);
        }
    }

    /// Takes room in the reserve for something the task itself creates (a marker, the record
    /// of a reply that expired).
    pub(super) fn reserve_take(&self, bytes: usize) -> Option<Charge> {
        self.reserve_for(Class::Reserve, 0, bytes, 0).ok()
    }

    /// Counts an entry that the task itself creates.
    pub(super) fn count_entry(&self, lane: usize, len: usize) {
        self.queued[lane].fetch_add(len, Ordering::AcqRel);
        self.entries.fetch_add(1, Ordering::AcqRel);
    }

    pub(super) fn loss_record_consumed(&self) {
        self.loss_records.fetch_sub(1, Ordering::AcqRel);
    }

    pub(super) fn release(&self, charge: &Charge) {
        match charge.kind {
            PoolKind::Data(lane) => {
                self.data[lane].fetch_sub(charge.bytes, Ordering::AcqRel);
                self.pool.give(charge.bytes);
            }
            PoolKind::Large => {
                self.large.fetch_sub(charge.bytes, Ordering::AcqRel);
                self.pool.give(charge.bytes);
            }
            PoolKind::Reserve => {
                // Only the part above the minimum of the link came from the process pool.
                let min = self.conf.reserve_min_bytes;
                let mut used = self.reserve.lock().unwrap();
                let after = *used - charge.bytes;
                self.pool
                    .give(used.saturating_sub(min) - after.saturating_sub(min));
                *used = after;
            }
        }
    }

    /// Gives the minimum reserve of the link back, once.
    pub(super) fn release_minimum(&self) {
        if !self.reserve_released.swap(true, Ordering::AcqRel) {
            self.pool.give(self.conf.reserve_min_bytes);
        }
    }

    fn reserve_for(
        &self,
        class: Class,
        lane: usize,
        bytes: usize,
        encoded_len: usize,
    ) -> Result<Charge, Refusal> {
        let conf = &self.conf;
        if class == Class::Reserve {
            // The minimum of the link was taken at activation: the reserve is used up to it
            // without asking the process pool.
            let min = conf.reserve_min_bytes;
            let mut used = self.reserve.lock().unwrap();
            let after = used
                .checked_add(bytes)
                .filter(|sum| *sum <= conf.reserve_max_bytes.max(min))
                .ok_or(Refusal::LinkBudget)?;
            let above = after.saturating_sub(min) - used.saturating_sub(min);
            if above > 0 && !self.pool.try_take(above) {
                return Err(Refusal::ProcessLimit);
            }
            *used = after;
            return Ok(Charge {
                kind: PoolKind::Reserve,
                bytes,
            });
        }
        if bytes > conf.data_bytes.max(conf.large_bytes) {
            return Err(Refusal::TooLarge);
        }
        let longer_than_a_batch = encoded_len + 16 > conf.batch.mtu as usize;
        let kind = if try_add(&self.data[lane], bytes, conf.data_bytes) {
            PoolKind::Data(lane)
        } else if longer_than_a_batch && try_add(&self.large, bytes, conf.large_bytes) {
            PoolKind::Large
        } else {
            return Err(Refusal::LinkBudget);
        };
        if !self.pool.try_take(bytes) {
            match kind {
                PoolKind::Data(lane) => self.data[lane].fetch_sub(bytes, Ordering::AcqRel),
                _ => self.large.fetch_sub(bytes, Ordering::AcqRel),
            };
            return Err(Refusal::ProcessLimit);
        }
        Ok(Charge { kind, bytes })
    }

    /// Counts a refusal and gives it its outcome.
    fn refuse(
        &self,
        refusal: Refusal,
        class: Class,
        rid: Option<RequestId>,
        branch: Option<Weak<dyn Any + Send + Sync>>,
        msg: NetworkMessageRef,
        lane: usize,
    ) -> Refusal {
        match (class, rid) {
            (Class::Request, Some(rid)) => {
                self.report_undelivered(rid, UndeliveredReason::NotDelivered, branch)
            }
            (Class::Reply, Some(rid)) => {
                self.record_lost_reply(rid, msg.wire_expr().and_then(reply_key), lane)
            }
            _ => {}
        }
        self.refused(refusal)
    }

    fn refused(&self, refusal: Refusal) -> Refusal {
        let counter = match refusal {
            Refusal::LinkBudget => &self.counters.refused_link_budget,
            Refusal::ProcessLimit => &self.counters.refused_process_limit,
            Refusal::TooLarge => &self.counters.refused_too_large,
            Refusal::Droppable => &self.counters.refused_droppable,
            Refusal::Closed => &self.counters.refused_closed,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        refusal
    }

    /// A reply was refused: the task must learn it before the final of the query, so the
    /// record goes through the intake like a message, and the error reply that stands for the
    /// dropped replies takes its place there. If it cannot be recorded, the transport is
    /// closed.
    fn record_lost_reply(&self, rid: RequestId, key: Option<Box<str>>, lane: usize) {
        self.counters.replies_lost.fetch_add(1, Ordering::Relaxed);
        if !try_add(&self.loss_records, 1, LOSS_CAPACITY) {
            self.request_close(CloseReason::ReserveOverflow);
            return;
        }
        let charge = loss_charge(&key);
        if self.reserve_for(Class::Reserve, 0, charge, 0).is_err() {
            self.loss_records.fetch_sub(1, Ordering::AcqRel);
            self.request_close(CloseReason::ReserveOverflow);
            return;
        }
        let mut item = LinkedList::new();
        item.push_back(Item::ReplyLost(rid, key, lane));
        {
            let mut intake = self.intake.lock().unwrap();
            if intake.state == OPEN {
                intake.items.append(&mut item);
            }
        }
        if item.is_empty() {
            self.wake.notify_one();
        } else {
            self.loss_records.fetch_sub(1, Ordering::AcqRel);
            self.release(&Charge {
                kind: PoolKind::Reserve,
                bytes: charge,
            });
        }
    }
}

/// Counts what the codec would write.
struct Counter(usize);

impl Writer for Counter {
    fn write(&mut self, bytes: &[u8]) -> Result<NonZeroUsize, DidntWrite> {
        self.0 += bytes.len();
        NonZeroUsize::new(bytes.len()).ok_or(DidntWrite)
    }

    fn write_exact(&mut self, bytes: &[u8]) -> Result<(), DidntWrite> {
        self.0 += bytes.len();
        Ok(())
    }

    fn remaining(&self) -> usize {
        usize::MAX
    }

    unsafe fn with_slot<F>(&mut self, len: usize, write: F) -> Result<NonZeroUsize, DidntWrite>
    where
        F: FnOnce(&mut [u8]) -> usize,
    {
        let mut slot = [0_u8; 32];
        let written = if len <= slot.len() {
            write(&mut slot[..len])
        } else {
            write(&mut vec![0_u8; len])
        };
        self.0 += written;
        NonZeroUsize::new(written).ok_or(DidntWrite)
    }
}

/// The length of the encoding of a message, without encoding it into memory.
pub(super) fn encoded_len(msg: NetworkMessageRef) -> usize {
    let mut counter = Counter(0);
    let _ = Zenoh080::new().write(&mut counter, msg);
    counter.0
}
