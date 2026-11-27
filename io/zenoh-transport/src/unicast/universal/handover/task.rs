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

//! The part of the sender that its task owns: the queues, the order of emission, batches,
//! fragments, ages and the decision to close.

use std::{
    collections::{HashMap, HashSet, LinkedList, VecDeque},
    sync::{atomic::Ordering, Arc},
    time::Instant,
};

use zenoh_buffers::{
    reader::{AdvanceableReader, HasReader, Reader},
    writer::{BacktrackableWriter, HasWriter, Writer},
    ZBuf,
};
use zenoh_codec::{RCodec, WCodec, Zenoh080, Zenoh080Condition};
use zenoh_protocol::{
    core::{CongestionControl, Encoding, ExprId, Priority, Reliability, WireExpr},
    network::{ext, NetworkBody, NetworkMessage, NetworkMessageExt, RequestId, Response},
    transport::{
        fragment::{self, FragmentHeader},
        frame::{self, FrameHeader},
    },
    zenoh::{self, Query, RequestBody, ResponseBody},
};

use super::{
    encoded_len, loss_charge, reply_key, Charge, Class, CloseReason, Entry, Item, PoolKind, Sender,
    UndeliveredReason, ENTRY_CHARGE, LOSS_CAPACITY, LOSS_CHARGE, REPLIES_DROPPED,
};
use crate::common::{
    batch::{Encode, WBatch},
    priority::TransportPriorityTx,
};

/// Entries classified, and entries examined for their age, in one step at most.
pub(crate) const QUANTUM: usize = 64;

/// What the task of the sender does next.
pub(crate) enum Step {
    /// Write this batch, then give it back with [`SenderTask::written`]. Until then `step`
    /// gives no other batch.
    Write(WBatch),
    /// More work is ready: call `step` again without waiting.
    Again,
    /// Nothing to write now. Call `step` again at the given instant, or when something is
    /// handed over, or when the pending write completes.
    Idle(Option<Instant>),
    /// The transport must be closed. The decision is final: every later `step` returns it.
    Close(CloseReason),
    /// After [`Sender::seal`]: everything handed over before the seal has been written.
    Flushed,
}

/// A message longer than a batch, some fragments of which have been given to the link.
struct Emission {
    entry: Box<Entry>,
    bytes: ZBuf,
    len: usize,
    offset: usize,
}

#[derive(Default)]
struct Lane {
    queue: VecDeque<Box<Entry>>,
    emitting: Option<Emission>,
    /// The entries at the front that have been examined for their age.
    swept: usize,
    /// The entries of the queue that are not discarded.
    live: usize,
}

pub(crate) struct SenderTask {
    sender: Arc<Sender>,
    priorities: Vec<TransportPriorityTx>,
    /// Handed over, not yet sorted into the lanes.
    unclassified: LinkedList<Item>,
    /// One queue per priority (one in all without QoS), in the order of admission.
    lanes: Vec<Lane>,
    /// Messages not yet written that refer to a key expression id of the sender.
    key_uses: HashMap<ExprId, u32>,
    /// Undeclarations that wait for the last use of their id.
    parked: HashMap<ExprId, Box<Entry>>,
    /// Queries some replies of which were dropped and that have their error reply in front of
    /// their final: they get no second one. A query leaves when its final has been written.
    /// Each holds `LOSS_CHARGE` of the reserve.
    lost: HashSet<RequestId>,
    /// `None` while the link writes it.
    batch: Option<WBatch>,
    /// The entries whose last byte is in the batch being written.
    // An entry is boxed once, when it is admitted, and moves between the queues as a pointer.
    #[allow(clippy::vec_box)]
    flight: Vec<Box<Entry>>,
    /// The last completed write, or the admission of the first pending message: the close
    /// period counts from here. `None` when nothing is pending.
    clock: Option<Instant>,
    closed: Option<CloseReason>,
    torn_down: bool,
}

impl SenderTask {
    pub(super) fn new(sender: Arc<Sender>, priorities: &[TransportPriorityTx]) -> Self {
        let lanes = if sender.conf.is_qos { Priority::NUM } else { 1 };
        let batch = WBatch::new(sender.conf.batch);
        Self {
            sender,
            priorities: priorities.to_vec(),
            unclassified: LinkedList::new(),
            lanes: (0..lanes).map(|_| Lane::default()).collect(),
            key_uses: HashMap::new(),
            parked: HashMap::new(),
            lost: HashSet::new(),
            batch: Some(batch),
            flight: Vec::new(),
            clock: None,
            closed: None,
            torn_down: false,
        }
    }

    /// One bounded amount of work: sorts at most [`QUANTUM`] handed-over entries, examines at
    /// most [`QUANTUM`] entries for their age, builds at most one batch.
    pub(crate) fn step(&mut self, now: Instant) -> Step {
        if let Some(reason) = self.closed {
            return Step::Close(reason);
        }
        if let Some(reason) = self.sender.close_requested() {
            return self.close(reason);
        }
        if self.unclassified.is_empty() {
            self.unclassified = self.sender.take_intake();
        }
        for _ in 0..QUANTUM {
            let Some(item) = self.unclassified.pop_front() else {
                break;
            };
            if let Err(reason) = self.classify(item, now) {
                return self.close(reason);
            }
        }
        if let Err(reason) = self.sweep(now) {
            return self.close(reason);
        }
        if self
            .oldest_control()
            .is_some_and(|admitted| admitted + self.sender.conf.control_max_age <= now)
        {
            return self.close(CloseReason::ControlAge);
        }
        if !self.pending() {
            self.clock = None;
        }
        if self
            .clock
            .is_some_and(|clock| clock + self.sender.conf.wait_before_close <= now)
        {
            return self.close(CloseReason::NoProgress);
        }
        let Some(mut batch) = self.batch.take() else {
            // The link is writing.
            return Step::Idle(self.deadline());
        };
        batch.clear();
        if let Err(reason) = self.build(&mut batch, now) {
            self.batch = Some(batch);
            return self.close(reason);
        }
        if !batch.is_empty() {
            return Step::Write(batch);
        }
        self.batch = Some(batch);
        if !self.unclassified.is_empty() {
            return Step::Again;
        }
        if !self.pending() {
            self.clock = None;
            if self.sender.is_sealed() {
                // Nothing can have been handed over after the seal: look once more.
                self.unclassified = self.sender.take_intake();
                if self.unclassified.is_empty() {
                    return Step::Flushed;
                }
                return Step::Again;
            }
        }
        Step::Idle(self.deadline())
    }

    /// The batch given by the last [`Step::Write`] has been written.
    pub(crate) fn written(&mut self, batch: WBatch, now: Instant) {
        self.batch = Some(batch);
        for entry in std::mem::take(&mut self.flight) {
            self.sender.counters.sent.fetch_add(1, Ordering::Relaxed);
            self.finish(entry);
        }
        if self.closed.is_none() {
            self.clock = self.pending().then_some(now);
        }
    }

    /// The link has taken a part of the batch that it is writing: it advances, however slowly,
    /// and the close period counts from that moment.
    pub(crate) fn progressed(&mut self, at: Instant) {
        if self.batch.is_none() && self.clock.is_some_and(|clock| clock < at) {
            self.clock = Some(at);
        }
    }

    /// Forced close: discards everything, reports the queries that were not started, and gives
    /// every reservation back, the minimum reserve of the link included.
    pub(crate) fn abort(mut self) {
        self.teardown();
    }

    /// True while something handed over has not been written.
    pub(crate) fn pending(&self) -> bool {
        self.batch.is_none()
            || !self.unclassified.is_empty()
            || !self.parked.is_empty()
            || self
                .lanes
                .iter()
                .any(|lane| lane.emitting.is_some() || lane.live > 0)
    }

    fn close(&mut self, reason: CloseReason) -> Step {
        self.closed = Some(reason);
        self.teardown();
        Step::Close(reason)
    }

    fn priority_of(&self, lane: usize) -> Priority {
        if self.sender.conf.is_qos {
            Priority::try_from(lane as u8).unwrap_or(Priority::DEFAULT)
        } else {
            Priority::DEFAULT
        }
    }

    // -- intake --------------------------------------------------------------------------

    fn classify(&mut self, item: Item, now: Instant) -> Result<(), CloseReason> {
        let entry = match item {
            Item::ReplyLost(rid, key, lane) => {
                self.sender.loss_record_consumed();
                // The record came with room in the reserve for the query in `lost` and for its
                // key: the key goes into the error reply, which takes its own room.
                self.sender.release(&Charge {
                    kind: PoolKind::Reserve,
                    bytes: loss_charge(&key) - LOSS_CHARGE,
                });
                // The error reply takes the place that the refused reply would have had: in
                // front of the final of the query, which comes behind.
                if let Some(marker) = Self::announce(
                    &self.sender,
                    &mut self.lost,
                    (rid, key.as_deref()),
                    lane,
                    now,
                    true,
                )? {
                    self.push_back(marker);
                }
                return Ok(());
            }
            Item::Entry(entry) => entry,
        };
        if self.clock.is_none() {
            self.clock = Some(entry.admitted);
        }
        if let Some(id) = entry.key_use {
            *self.key_uses.entry(id).or_insert(0) += 1;
        }
        if entry
            .declares_key
            .is_some_and(|id| self.parked.contains_key(&id))
        {
            // The retirement of this id still waits for its uses: neither order of emission
            // would be right.
            self.push_back(entry);
            return Err(CloseReason::KeyIdReuse);
        }
        if let Some(id) = entry.undeclares_key {
            if self.key_uses.get(&id).is_some_and(|uses| *uses > 0)
                && !self.parked.contains_key(&id)
            {
                self.parked.insert(id, entry);
                return Ok(());
            }
        }
        self.push_back(entry);
        Ok(())
    }

    fn push_back(&mut self, entry: Box<Entry>) {
        let lane = &mut self.lanes[entry.lane];
        lane.live += 1;
        lane.queue.push_back(entry);
    }

    /// A reply of this query, with this key expression, is dropped. Gives the error reply that
    /// says so, for the place of the dropped reply, unless the query already has one in front
    /// of its final. `charged`: the room of the query in `lost` is taken already.
    fn announce(
        sender: &Sender,
        lost: &mut HashSet<RequestId>,
        (rid, key): (RequestId, Option<&str>),
        lane: usize,
        now: Instant,
        charged: bool,
    ) -> Result<Option<Box<Entry>>, CloseReason> {
        let known = lost.contains(&rid);
        let room = !known
            && lost.len() < LOSS_CAPACITY
            && (charged || sender.reserve_take(LOSS_CHARGE).is_some());
        if !room {
            if charged {
                sender.release(&Charge {
                    kind: PoolKind::Reserve,
                    bytes: LOSS_CHARGE,
                });
            }
            return if known {
                Ok(None)
            } else {
                Err(CloseReason::ReserveOverflow)
            };
        }
        lost.insert(rid);
        Self::marker(sender, rid, key, lane, now).map(Some)
    }

    /// The error reply that says, in front of the final of a query, that replies were dropped.
    /// It carries the key expression of the first of them, as a router writes the key of a
    /// reply that it forwards: whoever lets the replies of this query pass (a namespace, the
    /// access control) lets this one pass.
    fn marker(
        sender: &Sender,
        rid: RequestId,
        key: Option<&str>,
        lane: usize,
        now: Instant,
    ) -> Result<Box<Entry>, CloseReason> {
        let priority = if sender.conf.is_qos {
            Priority::try_from(lane as u8).unwrap_or(Priority::DEFAULT)
        } else {
            Priority::DEFAULT
        };
        let msg = NetworkMessage {
            body: NetworkBody::Response(Response {
                rid,
                wire_expr: match key {
                    Some(key) => WireExpr::empty().with_suffix(key).to_owned(),
                    None => WireExpr::empty(),
                },
                payload: ResponseBody::Err(zenoh::Err {
                    encoding: Encoding::default(),
                    ext_sinfo: None,
                    #[cfg(feature = "shared-memory")]
                    ext_shm: None,
                    ext_unknown: vec![],
                    payload: ZBuf::from(REPLIES_DROPPED.as_bytes().to_vec()),
                }),
                ext_qos: ext::QoSType::new(priority, CongestionControl::Block, false),
                ext_tstamp: None,
                ext_respid: None,
                ext_ts_stack: None,
            }),
            reliability: Reliability::Reliable,
        };
        let len = encoded_len(msg.as_ref());
        let charge = sender
            .reserve_take(len + ENTRY_CHARGE)
            .ok_or(CloseReason::ReserveOverflow)?;
        let mut bytes = Vec::with_capacity(len);
        let _ = Zenoh080::new().write(&mut (&mut bytes).writer(), msg.as_ref());
        sender.count_entry(lane, len);
        Ok(Box::new(Entry {
            bytes,
            queued_len: len,
            lane,
            reliability: Reliability::Reliable,
            class: Class::Reserve,
            block_first: false,
            rid: Some(rid),
            is_final: false,
            key_use: None,
            declares_key: None,
            undeclares_key: None,
            admitted: now,
            expiry: None,
            request: None,
            charge,
            dead: false,
        }))
    }

    // -- ages ----------------------------------------------------------------------------

    /// Discards, in place, the publications, queries and replies that have not started
    /// `max_age` after their admission.
    fn sweep(&mut self, now: Instant) -> Result<(), CloseReason> {
        let max_age = self.sender.conf.max_age;
        let mut budget = QUANTUM;
        let mut released_keys = Vec::new();
        let mut result = Ok(());
        let Self {
            lanes,
            lost,
            sender,
            ..
        } = self;
        'lanes: for (l, lane) in lanes.iter_mut().enumerate() {
            loop {
                while lane.queue.front().is_some_and(|entry| entry.dead) {
                    lane.queue.pop_front();
                    lane.swept = lane.swept.saturating_sub(1);
                }
                if budget == 0 {
                    break;
                }
                let Some(entry) = lane.queue.get_mut(lane.swept) else {
                    break;
                };
                if entry.admitted + max_age > now {
                    break;
                }
                budget -= 1;
                lane.swept += 1;
                if entry.class == Class::Reserve || entry.dead {
                    continue;
                }
                sender.forget(entry);
                sender.counters.expired.fetch_add(1, Ordering::Relaxed);
                if let Some(id) = entry.key_use.take() {
                    released_keys.push(id);
                }
                let mut marker = None;
                match (entry.class, entry.rid) {
                    (Class::Request, Some(rid)) => sender.report_undelivered(
                        rid,
                        UndeliveredReason::Expired,
                        entry.request.take().and_then(|meta| meta.branch),
                    ),
                    (Class::Reply, Some(rid)) => {
                        sender.counters.replies_lost.fetch_add(1, Ordering::Relaxed);
                        let key = encoded_reply_key(&entry.bytes);
                        match Self::announce(sender, lost, (rid, key.as_deref()), l, now, false) {
                            Ok(announced) => marker = announced,
                            Err(reason) => result = Err(reason),
                        }
                    }
                    _ => {}
                }
                match marker {
                    // The error reply takes the place of the first reply that is dropped:
                    // in front of the final of the query, which may be queued already.
                    Some(marker) => *entry = marker,
                    None => {
                        entry.dead = true;
                        entry.bytes = Vec::new();
                        lane.live -= 1;
                    }
                }
                if result.is_err() {
                    break 'lanes;
                }
            }
        }
        for id in released_keys {
            self.release_key(id);
        }
        result
    }

    /// The admission of the oldest control message that has not started: the head of the
    /// Control lane, or a parked undeclaration.
    fn oldest_control(&self) -> Option<Instant> {
        let parked = self.parked.values().map(|entry| entry.admitted).min();
        let head = self
            .sender
            .conf
            .is_qos
            .then(|| {
                self.lanes[Priority::Control as usize]
                    .queue
                    .iter()
                    .find(|entry| !entry.dead)
                    .filter(|entry| entry.class == Class::Reserve)
                    .map(|entry| entry.admitted)
            })
            .flatten();
        match (parked, head) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// The earliest instant at which time alone gives the task something to do.
    fn deadline(&self) -> Option<Instant> {
        let conf = &self.sender.conf;
        let ages = self
            .lanes
            .iter()
            .filter_map(|lane| lane.queue.get(lane.swept))
            .map(|entry| entry.admitted + conf.max_age);
        let control = self
            .oldest_control()
            .map(|admitted| admitted + conf.control_max_age);
        let close = self.clock.map(|clock| clock + conf.wait_before_close);
        ages.chain(control).chain(close).min()
    }

    // -- emission ------------------------------------------------------------------------

    /// Fills the batch: from the highest priority downwards, whole messages as frames while
    /// they fit; a message longer than a batch as fragments, one batch each.
    fn build(&mut self, batch: &mut WBatch, now: Instant) -> Result<(), CloseReason> {
        let mut frame: Option<(usize, Reliability)> = None;
        'lanes: for l in 0..self.lanes.len() {
            loop {
                if self.lanes[l].emitting.is_some() {
                    if batch.is_empty() {
                        self.write_fragment(l, batch);
                    }
                    break 'lanes;
                }
                let Some(mut entry) = self.pop(l) else {
                    break;
                };
                if entry.expiry.is_some_and(|expiry| expiry <= now) {
                    // Not started in time. For a query this is the last moment at which it
                    // can still be held back: afterwards it may execute.
                    self.sender.counters.expired.fetch_add(1, Ordering::Relaxed);
                    self.discard(entry, UndeliveredReason::Expired, now)?;
                    continue;
                }
                forward_remaining_lifetime(&mut entry, now);
                let new_frame = frame != Some((l, entry.reliability));
                if self.write_frame(batch, &entry, new_frame) {
                    frame = Some((l, entry.reliability));
                    self.flight.push(entry);
                    continue;
                }
                if batch.is_empty() {
                    // Longer than a batch.
                    let len = entry.bytes.len();
                    let bytes = ZBuf::from(std::mem::take(&mut entry.bytes));
                    self.lanes[l].emitting = Some(Emission {
                        entry,
                        bytes,
                        len,
                        offset: 0,
                    });
                    continue;
                }
                // It goes into the next batch. Nothing of a lower priority overtakes it.
                let lane = &mut self.lanes[l];
                lane.live += 1;
                lane.queue.push_front(entry);
                break 'lanes;
            }
        }
        Ok(())
    }

    /// The next entry of a lane that was not discarded.
    fn pop(&mut self, l: usize) -> Option<Box<Entry>> {
        let lane = &mut self.lanes[l];
        while let Some(entry) = lane.queue.pop_front() {
            lane.swept = lane.swept.saturating_sub(1);
            if !entry.dead {
                lane.live -= 1;
                return Some(entry);
            }
        }
        None
    }

    fn write_frame(&self, batch: &mut WBatch, entry: &Entry, new_frame: bool) -> bool {
        let channels = &self.priorities[entry.lane];
        let channel = match entry.reliability {
            Reliability::Reliable => &channels.reliable,
            Reliability::BestEffort => &channels.best_effort,
        };
        // This task is the only user of the sequence numbers of its transport.
        let mut channel = channel.lock().unwrap();
        let mut writer = batch.buffer.writer();
        let mark = writer.mark();
        let mut taken = None;
        if new_frame {
            let sn = channel.sn.get();
            taken = Some(sn);
            let header = FrameHeader {
                reliability: entry.reliability,
                sn,
                ext_qos: frame::ext::QoSType::new(self.priority_of(entry.lane)),
            };
            if Zenoh080::new().write(&mut writer, &header).is_err() {
                writer.rewind(mark);
                let _ = channel.sn.set(sn);
                return false;
            }
        }
        if writer.write_exact(&entry.bytes).is_err() {
            writer.rewind(mark);
            if let Some(sn) = taken {
                let _ = channel.sn.set(sn);
            }
            return false;
        }
        #[cfg(feature = "stats")]
        if new_frame {
            batch.stats.t_msgs += 1;
        }
        true
    }

    /// Writes the next fragment of the message in emission of a lane into an empty batch.
    fn write_fragment(&mut self, l: usize, batch: &mut WBatch) {
        let priority = self.priority_of(l);
        let Some(emission) = self.lanes[l].emitting.as_mut() else {
            return;
        };
        let channels = &self.priorities[l];
        let channel = match emission.entry.reliability {
            Reliability::Reliable => &channels.reliable,
            Reliability::BestEffort => &channels.best_effort,
        };
        let mut channel = channel.lock().unwrap();
        let sn = channel.sn.get();
        let mut header = FragmentHeader {
            reliability: emission.entry.reliability,
            more: true,
            sn,
            ext_qos: frame::ext::QoSType::new(priority),
            ext_first: (emission.offset == 0).then(fragment::ext::First::new),
            ext_drop: None,
        };
        let mut reader = emission.bytes.reader();
        let _ = reader.skip(emission.offset);
        let before = reader.remaining();
        if batch.encode((&mut reader, &mut header)).is_ok() {
            emission.offset += before - reader.remaining();
        } else {
            // An empty batch always takes a fragment: this cannot happen. The sequence number
            // was not used; the message is given up rather than sent in a wrong shape.
            let _ = channel.sn.set(sn);
            tracing::error!("hand-over sender: a fragment did not fit an empty batch");
            emission.offset = emission.len;
        }
        drop(channel);
        if emission.offset >= emission.len {
            if let Some(emission) = self.lanes[l].emitting.take() {
                self.flight.push(emission.entry);
            }
        }
    }

    // -- endings -------------------------------------------------------------------------

    /// An entry has been written, or discarded: gives back what it holds.
    fn finish(&mut self, entry: Box<Entry>) {
        self.sender.forget(&entry);
        if let Some(id) = entry.key_use {
            self.release_key(id);
        }
        if entry.is_final && entry.rid.is_some_and(|rid| self.lost.remove(&rid)) {
            // The query is over: a later one with its id starts from nothing.
            self.sender.release(&Charge {
                kind: PoolKind::Reserve,
                bytes: LOSS_CHARGE,
            });
        }
    }

    /// One use less of a key expression id. After the last one its undeclaration, if it
    /// waited, goes to the head of the Control lane.
    fn release_key(&mut self, id: ExprId) {
        let Some(uses) = self.key_uses.get_mut(&id) else {
            return;
        };
        *uses -= 1;
        if *uses > 0 {
            return;
        }
        self.key_uses.remove(&id);
        if let Some(entry) = self.parked.remove(&id) {
            let lane = &mut self.lanes[entry.lane];
            lane.live += 1;
            lane.swept += 1;
            lane.queue.push_front(entry);
        }
    }

    /// Discards an entry that has not started, with its outcome.
    fn discard(
        &mut self,
        mut entry: Box<Entry>,
        reason: UndeliveredReason,
        now: Instant,
    ) -> Result<(), CloseReason> {
        let mut result = Ok(());
        match (entry.class, entry.rid) {
            (Class::Request, Some(rid)) => self.sender.report_undelivered(
                rid,
                reason,
                entry.request.take().and_then(|meta| meta.branch),
            ),
            (Class::Reply, Some(rid)) if reason == UndeliveredReason::Expired => {
                self.sender
                    .counters
                    .replies_lost
                    .fetch_add(1, Ordering::Relaxed);
                let key = encoded_reply_key(&entry.bytes);
                let l = entry.lane;
                let query = (rid, key.as_deref());
                match Self::announce(&self.sender, &mut self.lost, query, l, now, false) {
                    // In the place of the reply, at the head of its queue: in front of the
                    // final of the query.
                    Ok(Some(marker)) => {
                        let lane = &mut self.lanes[l];
                        lane.live += 1;
                        lane.swept += 1;
                        lane.queue.push_front(marker);
                    }
                    Ok(None) => {}
                    Err(reason) => result = Err(reason),
                }
            }
            _ => {}
        }
        self.finish(entry);
        result
    }

    /// Ends the sender: nothing is admitted any more; what was not started is discarded (the
    /// queries are reported); every reservation is given back. Runs once.
    fn teardown(&mut self) {
        if self.torn_down {
            return;
        }
        self.torn_down = true;
        let mut rest = self.sender.close_intake();
        self.unclassified.append(&mut rest);
        let counters = &self.sender.counters;
        let mut unsent = Vec::new();
        for item in std::mem::take(&mut self.unclassified) {
            match item {
                Item::Entry(entry) => unsent.push(entry),
                Item::ReplyLost(_, key, _) => {
                    self.sender.loss_record_consumed();
                    self.sender.release(&Charge {
                        kind: PoolKind::Reserve,
                        bytes: loss_charge(&key),
                    });
                }
            }
        }
        for lane in self.lanes.iter_mut() {
            unsent.extend(lane.queue.drain(..).filter(|entry| !entry.dead));
            lane.live = 0;
            lane.swept = 0;
            if let Some(emission) = lane.emitting.take() {
                // Started: it may have been received in part; it is not reported as unsent.
                self.sender.forget(&emission.entry);
            }
        }
        unsent.extend(self.parked.drain().map(|(_, entry)| entry));
        for mut entry in unsent {
            counters.discarded_at_close.fetch_add(1, Ordering::Relaxed);
            if let (Class::Request, Some(rid)) = (entry.class, entry.rid) {
                self.sender.report_undelivered(
                    rid,
                    UndeliveredReason::NotDelivered,
                    entry.request.take().and_then(|meta| meta.branch),
                );
            }
            self.sender.forget(&entry);
        }
        for entry in std::mem::take(&mut self.flight) {
            self.sender.forget(&entry);
        }
        for _ in self.lost.drain() {
            self.sender.release(&Charge {
                kind: PoolKind::Reserve,
                bytes: LOSS_CHARGE,
            });
        }
        self.key_uses.clear();
        self.sender.release_minimum();
        self.sender.wake.notify_one();
        self.sender.completion_wake.notify_one();
    }
}

impl Drop for SenderTask {
    fn drop(&mut self) {
        self.teardown();
    }
}

/// The key expression of an encoded reply, when it is written in full: what [`reply_key`]
/// gives for the reply before it is encoded.
fn encoded_reply_key(bytes: &[u8]) -> Option<Box<str>> {
    use zenoh_protocol::{
        common::imsg,
        network::{id, response::flag},
    };

    let mut reader = bytes.reader();
    let codec = Zenoh080::new();
    let header: u8 = codec.read(&mut reader).ok()?;
    if imsg::mid(header) != id::RESPONSE {
        return None;
    }
    let _rid: RequestId = codec.read(&mut reader).ok()?;
    let wire_expr: WireExpr<'static> = Zenoh080Condition::new(imsg::has_flag(header, flag::N))
        .read(&mut reader)
        .ok()?;
    reply_key(&wire_expr)
}

/// A query forwards the lifetime it has left when it is started: its head is encoded again
/// with its timeout reduced by the time it waited here, in front of its unchanged body.
fn forward_remaining_lifetime(entry: &mut Entry, now: Instant) {
    let Some(meta) = entry.request.as_mut() else {
        return;
    };
    let Some(timeout) = meta.head.ext_timeout else {
        return;
    };
    let waited = now.saturating_duration_since(entry.admitted);
    if waited.as_millis() == 0 {
        return;
    }
    let codec = Zenoh080::new();
    let mut head = Vec::with_capacity(meta.body_offset + 16);
    meta.head.ext_timeout = Some(timeout.saturating_sub(waited));
    let encoded = codec.write(&mut (&mut head).writer(), &meta.head).is_ok();
    meta.head.ext_timeout = Some(timeout);
    let mut body = Vec::with_capacity(16);
    let empty_body = RequestBody::Query(Query::default());
    if !encoded
        || codec.write(&mut (&mut body).writer(), &empty_body).is_err()
        || head.len() < body.len()
    {
        return;
    }
    head.truncate(head.len() - body.len());
    let body_offset = meta.body_offset;
    meta.body_offset = head.len();
    entry.bytes.splice(..body_offset, head);
}
