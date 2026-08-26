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
use std::{fmt, sync::Arc};

use zenoh_buffers::{BBuf, ZSlice, ZSliceBuffer};
use zenoh_core::zcondfeat;
use zenoh_link::{Link, LinkUnicast};
use zenoh_protocol::{
    core::{Priority, PriorityRange, Reliability},
    transport::{BatchSize, Close, OpenAck, TransportMessage},
};
use zenoh_result::{bail, zerror, ZResult};

use crate::common::batch::{BatchConfig, Decode, Encode, Finalize, RBatch, WBatch};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TransportLinkUnicastDirection {
    Inbound,
    Outbound,
}

#[derive(Clone, Debug)]
pub(crate) struct TransportLinkUnicastConfig {
    // Inbound / outbound
    pub(crate) direction: TransportLinkUnicastDirection,
    pub(crate) batch: BatchConfig,
    pub(crate) priorities: Option<PriorityRange>,
    pub(crate) reliability: Option<Reliability>,
}

#[derive(Clone)]
pub(crate) struct TransportLinkUnicast {
    pub(crate) link: LinkUnicast,
    pub(crate) config: TransportLinkUnicastConfig,
}

impl TransportLinkUnicast {
    pub(crate) fn new(link: LinkUnicast, config: TransportLinkUnicastConfig) -> Self {
        Self::init(link, config)
    }

    pub(crate) fn reconfigure(self, new_config: TransportLinkUnicastConfig) -> Self {
        Self::init(self.link, new_config)
    }

    fn init(link: LinkUnicast, mut config: TransportLinkUnicastConfig) -> Self {
        config.batch.mtu = link.get_mtu().min(config.batch.mtu);
        Self { link, config }
    }

    pub(crate) fn link(&self) -> Link {
        Link::new_unicast(
            &self.link,
            self.config.priorities.clone(),
            self.config.reliability,
        )
    }

    pub(crate) fn tx(&self) -> TransportLinkUnicastTx {
        TransportLinkUnicastTx {
            inner: self.clone(),
            buffer: zcondfeat!(
                "transport_compression",
                self.config
                    .batch
                    .is_compression
                    .then_some(BBuf::with_capacity(
                        lz4_flex::block::get_maximum_output_size(self.config.batch.mtu as usize),
                    )),
                None
            ),
        }
    }

    /// A **throwaway** reader: strict one-read-per-length-prefix framing, so it
    /// can never leave bytes stranded when it is dropped after a single
    /// message (the establishment handshake does exactly that — see
    /// [`Self::recv`]).
    pub(crate) fn rx(&self) -> TransportLinkUnicastRx {
        TransportLinkUnicastRx {
            link: self.link.clone(),
            config: self.config.clone(),
            carry: None,
        }
    }

    /// A **long-lived** reader that owns the link's byte stream for its whole
    /// lifetime and may therefore over-read: surplus bytes are carried over to
    /// the next batch instead of costing a second syscall. Only the per-link RX
    /// task may use this — a reader created here must be the sole consumer of
    /// the socket until it is dropped, or the carried bytes are lost.
    pub(crate) fn rx_buffered(&self) -> TransportLinkUnicastRx {
        TransportLinkUnicastRx {
            link: self.link.clone(),
            config: self.config.clone(),
            carry: Some(RxCarry::default()),
        }
    }

    pub(crate) async fn send(&self, msg: &TransportMessage) -> ZResult<usize> {
        let mut link = self.tx();
        link.send(msg, None).await
    }

    pub(crate) async fn recv(&self) -> ZResult<TransportMessage> {
        let mut link = self.rx();
        link.recv().await
    }

    pub(crate) async fn close(&self, reason: Option<u8>) -> ZResult<()> {
        if let Some(reason) = reason {
            // Build the close message
            let message: TransportMessage = Close {
                reason,
                session: false,
            }
            .into();
            // Send the close message on the link
            let _ = self.send(&message).await;
        }
        self.link.close().await
    }
}

impl fmt::Display for TransportLinkUnicast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.link)
    }
}

impl fmt::Debug for TransportLinkUnicast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransportLinkUnicast")
            .field("link", &self.link)
            .field("config", &self.config)
            .finish()
    }
}

impl PartialEq<Link> for TransportLinkUnicast {
    fn eq(&self, other: &Link) -> bool {
        &other.src == self.link.get_src() && &other.dst == self.link.get_dst()
    }
}

#[derive(Clone)]
pub(crate) struct TransportLinkUnicastTx {
    pub(crate) inner: TransportLinkUnicast,
    pub(crate) buffer: Option<BBuf>,
}

impl TransportLinkUnicastTx {
    pub(crate) async fn send_batch(
        &mut self,
        batch: &mut WBatch,
        priority: Option<Priority>,
    ) -> ZResult<()> {
        const ERR: &str = "Write error on link: ";

        // tracing::trace!("WBatch: {:?}", batch);

        let res = batch
            .finalize(self.buffer.as_mut())
            .map_err(|_| zerror!("{ERR}{self}"))?;

        let bytes = match res {
            Finalize::Batch => batch.as_slice(),
            Finalize::Buffer => self
                .buffer
                .as_ref()
                .ok_or_else(|| zerror!("Invalid buffer finalization"))?
                .as_slice(),
        };

        // tracing::trace!("WBytes: {:02x?}", bytes);

        // Send the message on the link
        self.inner.link.write_all(bytes, priority).await?;

        Ok(())
    }

    pub(crate) async fn send(
        &mut self,
        msg: &TransportMessage,
        priority: Option<Priority>,
    ) -> ZResult<usize> {
        const ERR: &str = "Write error on link: ";

        // Create the batch for serializing the message
        let mut batch = WBatch::new(self.inner.config.batch);
        batch.encode(msg).map_err(|_| zerror!("{ERR}{self}"))?;
        let len = batch.len() as usize;
        self.send_batch(&mut batch, priority).await?;
        Ok(len)
    }
}

impl fmt::Display for TransportLinkUnicastTx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl fmt::Debug for TransportLinkUnicastTx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransportLinkUnicastRx")
            .field("link", &self.inner.link)
            .field("config", &self.inner.config)
            .field("buffer", &self.buffer.as_ref().map(|b| b.capacity()))
            .finish()
    }
}

/// Length of the streamed framing prefix that precedes every batch.
const L_LEN: usize = core::mem::size_of::<BatchSize>();

/// Over-read carry for the streamed single-read fold.
///
/// A streamed batch is `[u16 length][body]`, and reading it used to cost TWO
/// socket reads — one for the prefix, one for the body — because the reader
/// could not afford to over-read: the surplus belongs to the NEXT batch and had
/// nowhere to live. This is that home, so one `read()` can serve a whole batch
/// (and, under batching, several).
///
/// Bytes are handed out front-to-back through `start` and the buffer is only
/// refilled once fully drained, so a carried byte is copied exactly twice (in
/// and out) no matter how many batches a single read delivered. Draining with a
/// cursor rather than re-copying the remainder is what keeps a big read that
/// contains N batches at O(bytes) instead of O(N * bytes).
#[derive(Debug, Default)]
struct RxCarry {
    buf: Vec<u8>,
    start: usize,
}

impl RxCarry {
    #[inline]
    fn pending(&self) -> usize {
        self.buf.len() - self.start
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.start == self.buf.len()
    }

    /// Move at most `want` pending bytes into `dst[at..]`, returning how many
    /// were moved.
    fn take_into(&mut self, dst: &mut [u8], at: usize, want: usize) -> usize {
        let n = want.min(self.pending()).min(dst.len() - at);
        if n == 0 {
            return 0;
        }
        dst[at..at + n].copy_from_slice(&self.buf[self.start..self.start + n]);
        self.start += n;
        if self.is_empty() {
            self.buf.clear();
            self.start = 0;
        }
        n
    }

    /// Take ownership of an over-read tail. Only ever reached with the carry
    /// already drained: the reader issues a `read()` only after the carry is
    /// exhausted, and only a `read()` can over-read.
    fn stash(&mut self, src: &[u8]) {
        debug_assert!(
            self.is_empty(),
            "the carry must be drained before it is refilled"
        );
        self.buf.clear();
        self.start = 0;
        self.buf.extend_from_slice(src);
    }
}

pub(crate) struct TransportLinkUnicastRx {
    pub(crate) link: LinkUnicast,
    pub(crate) config: TransportLinkUnicastConfig,
    /// `Some` only for a reader that owns the byte stream for its whole
    /// lifetime ([`TransportLinkUnicast::rx_buffered`]); `None` keeps the
    /// strict two-read framing for throwaway readers
    /// ([`TransportLinkUnicast::rx`]).
    carry: Option<RxCarry>,
}

impl Clone for TransportLinkUnicastRx {
    fn clone(&self) -> Self {
        // The only clone site is the per-priority RX fan-out in
        // `universal::link::rx_task`, which happens before the first read.
        // Cloning a reader that already holds pending bytes would either
        // duplicate or drop them, so it is a bug rather than a supported mode.
        debug_assert!(
            self.carry.as_ref().map_or(true, RxCarry::is_empty),
            "cloning a buffered reader that holds pending bytes"
        );
        Self {
            link: self.link.clone(),
            config: self.config.clone(),
            carry: self.carry.as_ref().map(|_| RxCarry::default()),
        }
    }
}

impl TransportLinkUnicastRx {
    #[inline]
    fn carry_take(&mut self, dst: &mut [u8], at: usize, want: usize) -> usize {
        match self.carry.as_mut() {
            Some(carry) => carry.take_into(dst, at, want),
            None => 0,
        }
    }

    #[inline]
    fn carry_stash(&mut self, src: &[u8]) {
        if let Some(carry) = self.carry.as_mut() {
            carry.stash(src);
        }
    }

    /// One `read()` into `dst[at..]`, rejecting the zero-length read that
    /// signals a closed stream (`read_exact` reports that as an error; the
    /// loops below would otherwise spin on it forever).
    async fn read_some(
        &self,
        dst: &mut [u8],
        at: usize,
        priority: Option<Priority>,
    ) -> ZResult<usize> {
        let n = self.link.read(&mut dst[at..], priority).await?;
        if n == 0 {
            bail!("Read error from link: {}. Connection closed.", self.link);
        }
        Ok(n)
    }

    /// Streamed framing, strict form: exactly the pre-fold behaviour, one read
    /// for the prefix and one for the body, never a byte more.
    async fn recv_streamed_strict(
        &mut self,
        dst: &mut [u8],
        priority: Option<Priority>,
    ) -> ZResult<usize> {
        const ERR: &str = "Read error from link: ";

        let link = self.link.clone();
        let mut len = BatchSize::MIN.to_le_bytes();
        self.link.read_exact(&mut len, priority).await?;
        let l = BatchSize::from_le_bytes(len) as usize;

        let slice = dst
            .get_mut(L_LEN..L_LEN + l)
            .ok_or_else(|| zerror!("{ERR}{link}. Invalid batch length or buffer size."))?;
        self.link.read_exact(slice, priority).await?;
        Ok(L_LEN + l)
    }

    /// Streamed framing, folded form: assemble the batch with as few reads as
    /// the stream allows, keeping any surplus for the next call.
    ///
    /// Cancellation: the only await points are the reads, and at each of them
    /// everything gathered so far lives in `dst[..have]`. Dropping the future
    /// there loses those bytes — exactly as dropping a `read_exact` does today,
    /// so this is not a new failure mode. The carry is written as the last step
    /// before returning, with no await in between, so a dropped future can
    /// never leave the carry holding the *next* batch while the current one is
    /// discarded.
    async fn recv_streamed_folded(
        &mut self,
        dst: &mut [u8],
        priority: Option<Priority>,
    ) -> ZResult<usize> {
        const ERR: &str = "Read error from link: ";

        // The length prefix.
        let mut have = self.carry_take(dst, 0, L_LEN);
        while have < L_LEN {
            have += self.read_some(dst, have, priority).await?;
        }

        let l = BatchSize::from_le_bytes([dst[0], dst[1]]) as usize;
        let need = L_LEN + l;
        if need > dst.len() {
            bail!("{ERR}{}. Invalid batch length or buffer size.", self.link);
        }

        // The body. `carry_take` is capped at what is still missing, so the
        // carry is never over-drained and the surplus stays put for next time.
        if have < need {
            have += self.carry_take(dst, have, need - have);
        }
        while have < need {
            have += self.read_some(dst, have, priority).await?;
        }

        // Hand the over-read tail over BEFORE the caller freezes `dst` into an
        // `Arc` — after that the bytes are unreachable.
        if have > need {
            self.carry_stash(&dst[need..have]);
        }

        Ok(need)
    }

    pub async fn recv_batch<C, T>(&mut self, buff: C, priority: Option<Priority>) -> ZResult<RBatch>
    where
        C: Fn() -> T + Copy,
        T: AsMut<[u8]> + ZSliceBuffer + 'static,
    {
        const ERR: &str = "Read error from link: ";

        let mut into = (buff)();
        let end = if self.link.is_streamed() {
            if self.carry.is_some() {
                self.recv_streamed_folded(into.as_mut(), priority).await?
            } else {
                self.recv_streamed_strict(into.as_mut(), priority).await?
            }
        } else {
            // Datagram links are one message per read: nothing to fold, and the
            // carry must stay unused.
            debug_assert!(self.carry.as_ref().map_or(true, RxCarry::is_empty));
            self.link.read(into.as_mut(), priority).await?
        };

        // tracing::trace!("RBytes: {:02x?}", &into.as_slice()[0..end]);

        let buffer = ZSlice::new(Arc::new(into), 0, end)
            .map_err(|_| zerror!("{ERR}{self}. ZSlice index(es) out of bounds"))?;
        let mut batch = RBatch::new(self.config.batch, buffer);
        batch
            .initialize(buff)
            .map_err(|e| zerror!("{ERR}{self}. {e}."))?;

        // tracing::trace!("RBatch: {:?}", batch);

        Ok(batch)
    }

    pub async fn recv(&mut self) -> ZResult<TransportMessage> {
        let mtu = self.config.batch.mtu as usize;
        let mut batch = self
            .recv_batch(|| zenoh_buffers::vec::uninit(mtu).into_boxed_slice(), None)
            .await?;
        let msg = batch
            .decode()
            .map_err(|_| zerror!("Decode error on link: {}", self))?;
        Ok(msg)
    }
}

impl fmt::Display for TransportLinkUnicastRx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{:?}", self.link, self.config)
    }
}

impl fmt::Debug for TransportLinkUnicastRx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransportLinkUnicastRx")
            .field("link", &self.link)
            .field("config", &self.config)
            .finish()
    }
}

pub(crate) struct MaybeOpenAck {
    link: TransportLinkUnicastTx,
    open_ack: Option<OpenAck>,
}

impl MaybeOpenAck {
    pub(crate) fn new(link: &TransportLinkUnicast, open_ack: Option<OpenAck>) -> Self {
        Self {
            link: link.tx(),
            open_ack,
        }
    }

    pub(crate) async fn send_open_ack(mut self) -> ZResult<()> {
        if let Some(msg) = self.open_ack {
            zcondfeat!(
                "transport_compression",
                {
                    // !!! Workaround !!! as the state of the link is set with compression once the OpenSyn is received.
                    // Here we are disabling the compression just to send the OpenAck (that is not supposed to be compressed).
                    // Then then we re-enable it, in case it was enabled, after the OpenAck has been sent.
                    let compression = self.link.inner.config.batch.is_compression;
                    self.link.inner.config.batch.is_compression = false;
                    self.link.send(&msg.into(), None).await?;
                    self.link.inner.config.batch.is_compression = compression;
                },
                {
                    self.link.send(&msg.into(), None).await?;
                }
            )
        }
        Ok(())
    }

    pub(crate) fn link(&self) -> Link {
        self.link.inner.link()
    }
}

pub(crate) struct LinkUnicastWithOpenAck {
    pub(crate) link: TransportLinkUnicast,
    ack: Option<OpenAck>,
    pub(crate) associated_link: Option<TransportLinkUnicast>,
}

impl LinkUnicastWithOpenAck {
    pub(crate) fn new(
        link: TransportLinkUnicast,
        ack: Option<OpenAck>,
        associated_link: Option<TransportLinkUnicast>,
    ) -> Self {
        Self {
            link,
            ack,
            associated_link,
        }
    }

    pub(crate) fn inner_config(&self) -> &TransportLinkUnicastConfig {
        &self.link.config
    }

    pub(crate) fn unpack(
        self,
    ) -> (
        TransportLinkUnicast,
        MaybeOpenAck,
        Option<TransportLinkUnicast>,
    ) {
        let ack = MaybeOpenAck::new(&self.link, self.ack);
        (self.link, ack, self.associated_link)
    }

    pub(crate) fn fail(self) -> (TransportLinkUnicast, Option<TransportLinkUnicast>) {
        (self.link, self.associated_link)
    }
}

impl fmt::Display for LinkUnicastWithOpenAck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.ack.as_ref() {
            Some(ack) => write!(f, "{}({:?})", self.link, ack),
            None => write!(f, "{}", self.link),
        }
    }
}

#[cfg(test)]
mod streamed_read_fold_tests {
    //! Carry-over correctness for the streamed single-read fold.
    //!
    //! Every case drives `recv_batch` over a scripted byte stream whose read
    //! chunking is chosen adversarially — a batch split across reads, several
    //! batches in one read, a length prefix straddling a read boundary — and
    //! checks that the batches come back byte-for-byte, in order, and that the
    //! folded reader issues strictly fewer reads than the strict one.

    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };

    use async_trait::async_trait;
    use zenoh_link::{LinkAuthId, LinkUnicast, LinkUnicastTrait, Locator};
    use zenoh_protocol::{core::Priority, transport::BatchSize};
    use zenoh_result::{bail, ZResult};

    use super::{
        BatchConfig, TransportLinkUnicast, TransportLinkUnicastConfig,
        TransportLinkUnicastDirection, L_LEN,
    };

    const MTU: BatchSize = 1024;

    /// A link whose `read` hands out pre-scripted chunks, so the batch/read
    /// boundary alignment is exactly what each test wants it to be.
    struct ScriptedLink {
        /// Remaining chunks, front first. Each is one `read()` worth of bytes
        /// (truncated if the caller's buffer is smaller).
        chunks: Mutex<std::collections::VecDeque<Vec<u8>>>,
        reads: AtomicUsize,
        src: Locator,
        dst: Locator,
        auth: LinkAuthId,
    }

    impl ScriptedLink {
        fn new(chunks: Vec<Vec<u8>>) -> Arc<Self> {
            Arc::new(Self {
                chunks: Mutex::new(chunks.into()),
                reads: AtomicUsize::new(0),
                src: "tcp/127.0.0.1:1".parse().unwrap(),
                dst: "tcp/127.0.0.1:2".parse().unwrap(),
                auth: LinkAuthId::Tcp,
            })
        }
    }

    #[async_trait]
    impl LinkUnicastTrait for ScriptedLink {
        fn get_mtu(&self) -> BatchSize {
            MTU
        }
        fn get_src(&self) -> &Locator {
            &self.src
        }
        fn get_dst(&self) -> &Locator {
            &self.dst
        }
        fn is_reliable(&self) -> bool {
            true
        }
        fn is_streamed(&self) -> bool {
            true
        }
        fn get_interface_names(&self) -> Vec<String> {
            vec![]
        }
        fn get_auth_id(&self) -> &LinkAuthId {
            &self.auth
        }
        async fn write(&self, buffer: &[u8], _p: Option<Priority>) -> ZResult<usize> {
            Ok(buffer.len())
        }
        async fn write_all(&self, _buffer: &[u8], _p: Option<Priority>) -> ZResult<()> {
            Ok(())
        }
        async fn read(&self, buffer: &mut [u8], _p: Option<Priority>) -> ZResult<usize> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            let mut chunks = self.chunks.lock().unwrap();
            let Some(front) = chunks.front_mut() else {
                // Scripted stream exhausted: report EOF, like a closed socket.
                return Ok(0);
            };
            let n = front.len().min(buffer.len());
            buffer[..n].copy_from_slice(&front[..n]);
            front.drain(..n);
            if front.is_empty() {
                chunks.pop_front();
            }
            Ok(n)
        }
        async fn read_exact(&self, buffer: &mut [u8], p: Option<Priority>) -> ZResult<()> {
            let mut done = 0;
            while done < buffer.len() {
                let n = self.read(&mut buffer[done..], p).await?;
                if n == 0 {
                    bail!("scripted stream exhausted");
                }
                done += n;
            }
            Ok(())
        }
        async fn close(&self) -> ZResult<()> {
            Ok(())
        }
    }

    fn transport_link(link: Arc<ScriptedLink>) -> (TransportLinkUnicast, Arc<ScriptedLink>) {
        let probe = Arc::clone(&link);
        let unicast: LinkUnicast = (link as Arc<dyn LinkUnicastTrait>).into();
        let config = TransportLinkUnicastConfig {
            direction: TransportLinkUnicastDirection::Inbound,
            batch: BatchConfig {
                mtu: MTU,
                is_streamed: true,
                #[cfg(feature = "transport_compression")]
                is_compression: false,
            },
            priorities: None,
            reliability: None,
        };
        (TransportLinkUnicast::new(unicast, config), probe)
    }

    /// `[u16 len][body]` on the wire; the body is `len` bytes of `fill`.
    fn framed(body: &[u8]) -> Vec<u8> {
        let mut out = (body.len() as BatchSize).to_le_bytes().to_vec();
        out.extend_from_slice(body);
        out
    }

    fn body(seed: u8, len: usize) -> Vec<u8> {
        (0..len).map(|i| seed.wrapping_add(i as u8)).collect()
    }

    async fn drain(rx: &mut super::TransportLinkUnicastRx, count: usize) -> ZResult<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        for _ in 0..count {
            let batch = rx
                .recv_batch(
                    || zenoh_buffers::vec::uninit(MTU as usize).into_boxed_slice(),
                    None,
                )
                .await?;
            // `initialize` has already stripped the framing prefix, so what is
            // left is exactly the body the peer wrote.
            out.push(batch.as_slice().to_vec());
        }
        Ok(out)
    }

    /// Several whole batches delivered by ONE read: the folded reader must
    /// return them all, in order, and never go back to the socket.
    #[tokio::test]
    async fn one_read_carrying_three_batches() {
        let bodies = [body(1, 40), body(2, 7), body(3, 100)];
        let mut wire = Vec::new();
        for b in &bodies {
            wire.extend_from_slice(&framed(b));
        }
        let (link, probe) = transport_link(ScriptedLink::new(vec![wire]));
        let mut rx = link.rx_buffered();

        let got = drain(&mut rx, 3).await.expect("three batches");
        assert_eq!(
            got,
            bodies.to_vec(),
            "batches must survive the carry intact"
        );
        assert_eq!(
            probe.reads.load(Ordering::Relaxed),
            1,
            "three batches arrived in one read; the fold must not read again"
        );
    }

    /// The same stream through the STRICT reader costs two reads per batch —
    /// this is the baseline the fold is measured against.
    #[tokio::test]
    async fn strict_reader_still_costs_two_reads_per_batch() {
        let bodies = [body(1, 40), body(2, 7), body(3, 100)];
        let mut wire = Vec::new();
        for b in &bodies {
            wire.extend_from_slice(&framed(b));
        }
        let (link, probe) = transport_link(ScriptedLink::new(vec![wire]));
        let mut rx = link.rx();

        let got = drain(&mut rx, 3).await.expect("three batches");
        assert_eq!(got, bodies.to_vec());
        assert_eq!(
            probe.reads.load(Ordering::Relaxed),
            6,
            "strict framing: one read for the prefix and one for the body, per batch"
        );
    }

    /// A batch split across read boundaries: the fold must keep pulling until
    /// the body is complete.
    #[tokio::test]
    async fn batch_split_across_reads() {
        let b = body(9, 300);
        let wire = framed(&b);
        let (head, tail) = wire.split_at(120);
        let (link, _probe) = transport_link(ScriptedLink::new(vec![
            head.to_vec(),
            tail[..50].to_vec(),
            tail[50..].to_vec(),
        ]));
        let mut rx = link.rx_buffered();

        let got = drain(&mut rx, 1).await.expect("one batch");
        assert_eq!(got, vec![b]);
    }

    /// The length prefix itself straddling a read boundary — one byte in the
    /// first read, one in the next.
    #[tokio::test]
    async fn length_prefix_split_across_reads() {
        let b = body(5, 64);
        let wire = framed(&b);
        assert_eq!(L_LEN, 2);
        let (link, _probe) = transport_link(ScriptedLink::new(vec![
            wire[..1].to_vec(),
            wire[1..2].to_vec(),
            wire[2..].to_vec(),
        ]));
        let mut rx = link.rx_buffered();

        let got = drain(&mut rx, 1).await.expect("one batch");
        assert_eq!(got, vec![b]);
    }

    /// One read ending in the MIDDLE of the next batch's length prefix: the
    /// carry holds a single orphan byte that the next call must consume before
    /// it reads again.
    #[tokio::test]
    async fn carry_holds_a_lone_prefix_byte() {
        let first = body(11, 30);
        let second = body(22, 45);
        let mut wire = framed(&first);
        let second_framed = framed(&second);
        wire.push(second_framed[0]); // half of the next prefix
        let (link, probe) =
            transport_link(ScriptedLink::new(vec![wire, second_framed[1..].to_vec()]));
        let mut rx = link.rx_buffered();

        let got = drain(&mut rx, 2).await.expect("two batches");
        assert_eq!(got, vec![first, second]);
        assert_eq!(
            probe.reads.load(Ordering::Relaxed),
            2,
            "one read per scripted chunk, and no more"
        );
    }

    /// One read ending with a COMPLETE prefix but no body: the carry holds two
    /// bytes and the body arrives next time.
    #[tokio::test]
    async fn carry_holds_a_whole_prefix_without_its_body() {
        let first = body(31, 16);
        let second = body(41, 80);
        let mut wire = framed(&first);
        let second_framed = framed(&second);
        wire.extend_from_slice(&second_framed[..L_LEN]);
        let (link, _probe) = transport_link(ScriptedLink::new(vec![
            wire,
            second_framed[L_LEN..].to_vec(),
        ]));
        let mut rx = link.rx_buffered();

        let got = drain(&mut rx, 2).await.expect("two batches");
        assert_eq!(got, vec![first, second]);
    }

    /// A long run of batches with pathological chunking (7 bytes per read):
    /// order and content must hold across dozens of carry refills.
    #[tokio::test]
    async fn many_batches_through_tiny_reads() {
        let bodies: Vec<Vec<u8>> = (0..25u8).map(|i| body(i, 3 + (i as usize % 17))).collect();
        let mut wire = Vec::new();
        for b in &bodies {
            wire.extend_from_slice(&framed(b));
        }
        let chunks: Vec<Vec<u8>> = wire.chunks(7).map(<[u8]>::to_vec).collect();
        let (link, _probe) = transport_link(ScriptedLink::new(chunks));
        let mut rx = link.rx_buffered();

        let got = drain(&mut rx, bodies.len()).await.expect("all batches");
        assert_eq!(got, bodies);
    }

    /// An empty-bodied batch (`len == 0`) must still be framed correctly and
    /// must not desynchronise the batch that follows it.
    #[tokio::test]
    async fn zero_length_batch_does_not_desync_the_stream() {
        let next = body(77, 20);
        let mut wire = framed(&[]);
        wire.extend_from_slice(&framed(&next));
        let (link, _probe) = transport_link(ScriptedLink::new(vec![wire]));
        let mut rx = link.rx_buffered();

        let got = drain(&mut rx, 2).await.expect("two batches");
        assert_eq!(got, vec![Vec::new(), next]);
    }

    /// A closed stream mid-batch is an error, not a spin: `read` returning 0
    /// must terminate the fold.
    #[tokio::test]
    async fn eof_mid_batch_is_an_error() {
        let b = body(3, 200);
        let wire = framed(&b);
        let (link, _probe) = transport_link(ScriptedLink::new(vec![wire[..50].to_vec()]));
        let mut rx = link.rx_buffered();

        let err = drain(&mut rx, 1)
            .await
            .expect_err("truncated batch must fail");
        assert!(
            err.to_string().contains("Connection closed"),
            "unexpected error: {err}"
        );
    }

    /// A batch longer than the receive buffer is rejected, exactly as the
    /// strict path rejects it — the fold must not read a batch it cannot hold.
    #[tokio::test]
    async fn oversized_batch_length_is_rejected() {
        let mut wire = (MTU).to_le_bytes().to_vec(); // len == MTU > MTU - L_LEN
        wire.extend_from_slice(&body(1, 16));
        let (link, _probe) = transport_link(ScriptedLink::new(vec![wire]));
        let mut rx = link.rx_buffered();

        let err = drain(&mut rx, 1)
            .await
            .expect_err("oversized batch must fail");
        assert!(
            err.to_string().contains("Invalid batch length"),
            "unexpected error: {err}"
        );
    }

    /// The per-priority RX fan-out clones the reader before it has read
    /// anything; a clone must start with an empty carry and still be buffered.
    #[tokio::test]
    async fn clone_of_a_fresh_buffered_reader_is_buffered_and_empty() {
        let bodies = [body(1, 10), body(2, 10)];
        let mut wire = Vec::new();
        for b in &bodies {
            wire.extend_from_slice(&framed(b));
        }
        let (link, probe) = transport_link(ScriptedLink::new(vec![wire]));
        let rx = link.rx_buffered();
        let mut clone = rx.clone();

        let got = drain(&mut clone, 2).await.expect("two batches");
        assert_eq!(got, bodies.to_vec());
        assert_eq!(
            probe.reads.load(Ordering::Relaxed),
            1,
            "the clone must still fold, not fall back to strict framing"
        );
    }
}
