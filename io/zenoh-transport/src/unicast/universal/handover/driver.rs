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

//! The task of the sender: it drives [`SenderTask`] with the clock and writes its batches to
//! the link.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use tokio_util::sync::CancellationToken;
use zenoh_protocol::transport::{Close, KeepAlive, TransportMessage};
use zenoh_result::{zerror, ZResult};

use super::{CloseReason, Sender, SenderTask, Step};
use crate::common::batch::WBatch;

/// Steps without waiting after which the task gives the runtime back to the other links.
const STEPS_BEFORE_YIELD: usize = 16;

/// When the link last took a part of the batch that it is writing.
pub(crate) struct Progress {
    since: Instant,
    /// Microseconds after `since`, plus one. Zero: nothing new.
    at: AtomicU64,
}

impl Progress {
    fn new() -> Self {
        Self {
            since: Instant::now(),
            at: AtomicU64::new(0),
        }
    }

    /// The link has just taken some bytes.
    pub(crate) fn mark(&self) {
        let at = self.since.elapsed().as_micros() as u64 + 1;
        self.at.store(at, Ordering::Release);
    }

    fn take(&self) -> Option<Instant> {
        match self.at.swap(0, Ordering::AcqRel) {
            0 => None,
            at => Some(self.since + Duration::from_micros(at - 1)),
        }
    }
}

/// Where the sender writes.
pub(crate) trait Sink: Send + Sized + 'static {
    /// Writes a batch, and marks `progress` each time the link has taken a part of it. The
    /// future is polled to completion or dropped with the connection.
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a mut WBatch,
        progress: &'a Progress,
    ) -> impl Future<Output = ZResult<()>> + Send + 'a;

    /// A second way to the peer on the same link, independent of the first: a KeepAlive goes
    /// there while a batch takes long. `None` when the link has one way only; the peer then
    /// hears nothing until the batch is through, as with the transmission pipeline.
    fn beside(&self) -> Option<Self> {
        None
    }

    /// Writes a transport message (KeepAlive, Close) on its own.
    fn send_message<'a>(
        &'a mut self,
        msg: &'a TransportMessage,
    ) -> impl Future<Output = ZResult<()>> + Send + 'a;

    /// A batch of this many bytes, carrying this many frames and fragments, was written.
    fn written(&mut self, _bytes: usize, _transport_messages: usize) {}
}

/// How the task of the sender ended.
pub(crate) enum End {
    /// Orderly close: everything handed over before the seal and the Close message were
    /// written.
    Flushed,
    /// The sender decided to close the transport.
    Close(CloseReason),
    /// The task was cancelled: the transport is being deleted.
    Cancelled,
    /// A write failed.
    Failed(zenoh_result::Error),
}

type Write<S> = Pin<Box<dyn Future<Output = (ZResult<()>, S, WBatch)> + Send>>;
type KeepAliveBeside<S> = Pin<Box<dyn Future<Output = (ZResult<()>, S)> + Send>>;

/// Runs the sender until its transport ends. Returns the state of the sender with the reason,
/// so that the caller decides when what is left is discarded.
pub(crate) async fn run<S: Sink>(
    mut task: SenderTask,
    sender: Arc<Sender>,
    sink: S,
    keep_alive: Duration,
    token: CancellationToken,
) -> (End, SenderTask) {
    let progress = Arc::new(Progress::new());
    // Where a KeepAlive goes while a batch is being written, and the KeepAlive on its way.
    let mut beside = sink.beside();
    let mut keep_alive_beside: Option<KeepAliveBeside<S>> = None;
    let mut sink = Some(sink);
    // The one write in flight. It owns the link and the batch until it completes: nothing
    // else can write, and the batch cannot change under it. It is never created twice for a
    // batch; dropping it means that the connection is given up.
    let mut write: Option<Write<S>> = None;
    let mut last_write = tokio::time::Instant::now();
    let mut steps = 0;
    loop {
        if let Some(at) = progress.take() {
            task.progressed(at);
        }
        let deadline = match task.step(Instant::now()) {
            Step::Write(batch) => {
                let Some(mut link) = sink.take() else {
                    // The sender gives no batch while one is being written.
                    return (
                        End::Failed(zerror!("hand-over sender: two writes at once").into()),
                        task,
                    );
                };
                let mut batch = batch;
                let progress = progress.clone();
                write = Some(Box::pin(async move {
                    let res = link.send_batch(&mut batch, &progress).await;
                    (res, link, batch)
                }));
                continue;
            }
            Step::Again => {
                steps += 1;
                if steps >= STEPS_BEFORE_YIELD {
                    steps = 0;
                    tokio::task::yield_now().await;
                    if token.is_cancelled() {
                        return (End::Cancelled, task);
                    }
                }
                continue;
            }
            Step::Idle(deadline) => deadline,
            Step::Close(reason) => return (End::Close(reason), task),
            Step::Flushed => {
                // Nothing is in flight: the link is ours for the Close message.
                let end = match sink.as_mut() {
                    Some(link) => {
                        let msg: TransportMessage = Close {
                            reason: sender.close_message_reason(),
                            session: false,
                        }
                        .into();
                        tokio::select! {
                            biased;
                            _ = token.cancelled() => End::Cancelled,
                            res = link.send_message(&msg) => match res {
                                Ok(()) => End::Flushed,
                                Err(e) => End::Failed(e),
                            },
                        }
                    }
                    None => End::Flushed,
                };
                return (end, task);
            }
        };
        steps = 0;
        let wake_at = deadline.map(tokio::time::Instant::from_std);
        let keep_alive_at = last_write + keep_alive;
        tokio::select! {
            biased;
            _ = token.cancelled() => return (End::Cancelled, task),
            (res, link, batch) = async { write.as_mut().unwrap().await }, if write.is_some() => {
                write = None;
                let mut link = link;
                if let Err(e) = res {
                    return (End::Failed(e), task);
                }
                last_write = tokio::time::Instant::now();
                link.written(batch.len() as usize, batch_transport_messages(&batch));
                sink = Some(link);
                task.written(batch, Instant::now());
            }
            (res, link) = async { keep_alive_beside.as_mut().unwrap().await }, if keep_alive_beside.is_some() => {
                keep_alive_beside = None;
                beside = Some(link);
                if let Err(e) = res {
                    return (End::Failed(e), task);
                }
            }
            _ = sender.wake.notified() => {}
            _ = tokio::time::sleep_until(wake_at.unwrap_or(keep_alive_at)), if wake_at.is_some() => {}
            _ = tokio::time::sleep_until(keep_alive_at), if write.is_some() && beside.is_some() => {
                // The batch takes long. The peer counts whole batches: it gets a KeepAlive
                // beside the batch, as it does from the transmission pipeline of a link with
                // several ways. This says nothing about progress either.
                if let Some(mut link) = beside.take() {
                    last_write = tokio::time::Instant::now();
                    keep_alive_beside = Some(Box::pin(async move {
                        let msg: TransportMessage = KeepAlive.into();
                        let res = link.send_message(&msg).await;
                        (res, link)
                    }));
                }
            }
            _ = tokio::time::sleep_until(keep_alive_at), if write.is_none() => {
                // Nothing was written for the keep-alive interval, and nothing is being
                // written. This says nothing about progress: the clock of the sender is not
                // touched.
                if let Some(link) = sink.as_mut() {
                    let msg: TransportMessage = KeepAlive.into();
                    let res = tokio::select! {
                        biased;
                        _ = token.cancelled() => return (End::Cancelled, task),
                        res = tokio::time::timeout(keep_alive, link.send_message(&msg)) => res,
                    };
                    match res {
                        Ok(Ok(())) => last_write = tokio::time::Instant::now(),
                        Ok(Err(e)) => return (End::Failed(e), task),
                        Err(_) => {
                            return (
                                End::Failed(
                                    zerror!("KeepAlive not written in {keep_alive:?}").into(),
                                ),
                                task,
                            )
                        }
                    }
                }
            }
        }
    }
}

fn batch_transport_messages(_batch: &WBatch) -> usize {
    #[cfg(feature = "stats")]
    {
        _batch.stats.t_msgs
    }
    #[cfg(not(feature = "stats"))]
    {
        0
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    use tokio::sync::Semaphore;
    use zenoh_protocol::{
        core::{Bits, CongestionControl, Priority},
        network::{ext, NetworkMessage, NetworkMessageExt, Push},
        transport::{BatchSize, TransportSn},
    };

    use super::*;
    use crate::{
        common::{batch::BatchConfig, priority::TransportPriorityTx, test_receiver::TestReceiver},
        unicast::universal::handover::{HandoverConf, ProcessPool},
    };

    const TIMEOUT: Duration = Duration::from_secs(30);

    /// A link that takes a batch in pieces, each when the test allows it.
    struct PiecewiseLink {
        received: Arc<Mutex<Vec<u8>>>,
        pieces: Arc<Semaphore>,
        batches: Arc<AtomicUsize>,
        messages: Arc<Mutex<Vec<TransportMessage>>>,
        /// What a second way to the peer receives, if the link has one.
        beside: Option<Arc<Mutex<Vec<TransportMessage>>>>,
    }

    impl Sink for PiecewiseLink {
        async fn send_batch(&mut self, batch: &mut WBatch, progress: &Progress) -> ZResult<()> {
            self.batches.fetch_add(1, Ordering::SeqCst);
            for piece in batch.as_slice().chunks(16) {
                self.pieces.acquire().await.unwrap().forget();
                self.received.lock().unwrap().extend_from_slice(piece);
                progress.mark();
            }
            Ok(())
        }

        fn beside(&self) -> Option<Self> {
            self.beside.as_ref().map(|messages| PiecewiseLink {
                received: Arc::new(Mutex::new(Vec::new())),
                pieces: Arc::new(Semaphore::new(0)),
                batches: Arc::new(AtomicUsize::new(0)),
                messages: messages.clone(),
                beside: None,
            })
        }

        async fn send_message(&mut self, msg: &TransportMessage) -> ZResult<()> {
            self.messages.lock().unwrap().push(msg.clone());
            Ok(())
        }
    }

    fn conf() -> HandoverConf {
        HandoverConf {
            batch: BatchConfig {
                mtu: BatchSize::MAX,
                is_streamed: false,
                #[cfg(feature = "transport_compression")]
                is_compression: false,
            },
            is_qos: true,
            queue_size: [2; Priority::NUM],
            data_bytes: 1 << 20,
            large_bytes: 1 << 20,
            reserve_min_bytes: 1 << 16,
            reserve_max_bytes: 1 << 20,
            max_age: Duration::from_secs(3600),
            control_max_age: Duration::from_secs(3600),
            wait_before_close: Duration::from_secs(3600),
            default_query_timeout: Duration::from_secs(10),
        }
    }

    fn put(size: usize) -> NetworkMessage {
        NetworkMessage::from(Push {
            wire_expr: "k".into(),
            ext_qos: ext::QoSType::new(Priority::DEFAULT, CongestionControl::Block, false),
            ..Push::from(vec![0_u8; size])
        })
    }

    async fn until(what: &str, mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(TIMEOUT, async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting until {what}"));
    }

    // C4. A write that the link takes in pieces is never started again, whatever is handed
    // over and whichever timer fires meanwhile: every byte is received once, in order, and
    // the orderly close writes what was handed over and then the Close message.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_write_is_never_restarted() {
        let priorities = (0..Priority::NUM)
            .map(|_| TransportPriorityTx::make(Bits::from(TransportSn::MAX)).unwrap())
            .collect::<Vec<_>>();
        let (sender, task) =
            Sender::activate(conf(), ProcessPool::new(1 << 30), &priorities).unwrap();
        let received = Arc::new(Mutex::new(Vec::new()));
        let pieces = Arc::new(Semaphore::new(0));
        let batches = Arc::new(AtomicUsize::new(0));
        let messages = Arc::new(Mutex::new(Vec::new()));
        let link = PiecewiseLink {
            received: received.clone(),
            pieces: pieces.clone(),
            batches: batches.clone(),
            messages: messages.clone(),
            beside: None,
        };
        let token = CancellationToken::new();
        let run = tokio::spawn(run(
            task,
            sender.clone(),
            link,
            // The keep-alive timer fires all the time.
            Duration::from_millis(1),
            token.clone(),
        ));

        // The first batch: one message of 400 bytes, taken 16 bytes at a time.
        let now = Instant::now();
        sender.admit(put(400).as_ref(), None, now).unwrap();
        until("the first write starts", || {
            batches.load(Ordering::SeqCst) == 1
        })
        .await;
        let mut sizes = vec![400];
        for i in 0..10 {
            // More is handed over while the write is in progress: the task is woken each time.
            sender.admit(put(i + 1).as_ref(), None, now).unwrap();
            sizes.push(i + 1);
            pieces.add_permits(1);
            let expected = 16 * (i + 1);
            until("one more piece is taken", || {
                received.lock().unwrap().len() == expected
            })
            .await;
            tokio::time::sleep(Duration::from_millis(3)).await;
            assert_eq!(
                batches.load(Ordering::SeqCst),
                1,
                "the write was started again"
            );
        }
        pieces.add_permits(1 << 20);
        sender.seal();
        let (end, _task) = tokio::time::timeout(TIMEOUT, run).await.unwrap().unwrap();
        assert!(matches!(end, End::Flushed));

        // What the link received decodes to every message once, in order.
        let mut rx = TestReceiver::new(Bits::from(TransportSn::MAX), true);
        rx.read(&received.lock().unwrap());
        assert!(rx.errors.is_empty(), "{:?}", rx.errors);
        assert_eq!(rx.put_sizes(), sizes);
        // No KeepAlive was written into the stream while a batch was in flight, and the Close
        // came last.
        let messages = messages.lock().unwrap();
        assert!(matches!(
            messages.last().map(|m| &m.body),
            Some(zenoh_protocol::transport::TransportBody::Close(_))
        ));
        assert_eq!(batches.load(Ordering::SeqCst), 2);
        // The link has one way: no KeepAlive went into the stream in the middle of a batch.
        let keep_alives_during_the_first_write = messages
            .iter()
            .take_while(|m| {
                matches!(
                    m.body,
                    zenoh_protocol::transport::TransportBody::KeepAlive(_)
                )
            })
            .count();
        assert_eq!(keep_alives_during_the_first_write, 0);
    }

    struct Slow {
        sender: Arc<Sender>,
        pieces: Arc<Semaphore>,
        received: Arc<Mutex<Vec<u8>>>,
        beside: Arc<Mutex<Vec<TransportMessage>>>,
        run: tokio::task::JoinHandle<(End, SenderTask)>,
    }

    /// A link with a second way to the peer, a short close period and a batch of `size`
    /// bytes that it has started to write.
    async fn slow(size: usize, wait_before_close: Duration, keep_alive: Duration) -> Slow {
        let priorities = (0..Priority::NUM)
            .map(|_| TransportPriorityTx::make(Bits::from(TransportSn::MAX)).unwrap())
            .collect::<Vec<_>>();
        let conf = HandoverConf {
            wait_before_close,
            ..conf()
        };
        let (sender, task) =
            Sender::activate(conf, ProcessPool::new(1 << 30), &priorities).unwrap();
        let received = Arc::new(Mutex::new(Vec::new()));
        let pieces = Arc::new(Semaphore::new(0));
        let batches = Arc::new(AtomicUsize::new(0));
        let beside = Arc::new(Mutex::new(Vec::new()));
        let link = PiecewiseLink {
            received: received.clone(),
            pieces: pieces.clone(),
            batches: batches.clone(),
            messages: Arc::new(Mutex::new(Vec::new())),
            beside: Some(beside.clone()),
        };
        let run = tokio::spawn(run(
            task,
            sender.clone(),
            link,
            keep_alive,
            CancellationToken::new(),
        ));
        sender
            .admit(put(size).as_ref(), None, Instant::now())
            .unwrap();
        until("the write starts", || batches.load(Ordering::SeqCst) == 1).await;
        Slow {
            sender,
            pieces,
            received,
            beside,
            run,
        }
    }

    // A link that takes a batch slowly, a part at a time, advances: it is not closed while
    // parts keep going, however long the whole batch takes; it is closed when they stop.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_slow_write_that_advances_is_not_closed() {
        let close = Duration::from_millis(400);
        let slow = slow(1_600, close, Duration::from_secs(3600)).await;
        // 100 parts, one every 20 ms: the batch takes five close periods.
        let started = Instant::now();
        for i in 1..=60 {
            slow.pieces.add_permits(1);
            until("the part is taken", || {
                slow.received.lock().unwrap().len() == 16 * i
            })
            .await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(!slow.run.is_finished(), "closed while it advanced");
        }
        assert!(started.elapsed() > 2 * close);
        // No more parts: the link is closed a close period after the last one.
        let stopped = Instant::now();
        let (end, _task) = tokio::time::timeout(TIMEOUT, slow.run)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(end, End::Close(CloseReason::NoProgress)));
        assert!(stopped.elapsed() >= close - Duration::from_millis(40));
        assert_eq!(
            slow.sender
                .admit(put(1).as_ref(), None, Instant::now())
                .ok(),
            None
        );
    }

    // While a batch takes long on a link that has a second way to the peer, KeepAlive
    // messages go that way: the peer, which counts whole batches, does not give the link up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn keep_alives_go_beside_a_slow_write() {
        let slow = slow(400, Duration::from_secs(3600), Duration::from_millis(5)).await;
        until("three KeepAlive went beside the batch", || {
            slow.beside.lock().unwrap().len() >= 3
        })
        .await;
        assert!(slow.beside.lock().unwrap().iter().all(|m| matches!(
            m.body,
            zenoh_protocol::transport::TransportBody::KeepAlive(_)
        )));
        // Nothing of the batch was needed for that.
        assert!(slow.received.lock().unwrap().is_empty());
        slow.pieces.add_permits(1 << 20);
        slow.sender.seal();
        let (end, _task) = tokio::time::timeout(TIMEOUT, slow.run)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(end, End::Flushed));
    }
}
