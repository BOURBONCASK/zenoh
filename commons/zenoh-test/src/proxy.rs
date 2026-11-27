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

//! A TCP proxy whose forwarding is decided by the test.
//!
//! [`GatedProxy`] sits between two Zenoh endpoints. Each direction forwards bytes only
//! while it has credit: [`GatedProxy::open`] gives unlimited credit, [`GatedProxy::stall`]
//! takes it away (the proxy stops reading, the sender's socket buffer fills and its writes
//! stop completing, while the connection stays up), and [`GatedProxy::allow`] lets an exact
//! number of bytes through. A test thus drives a link that is frozen, or slow but
//! progressing, step by step, and observes what was forwarded with
//! [`GatedProxy::forwarded`] and [`GatedProxy::wait_forwarded`] instead of sleeping.

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpSocket, TcpStream,
    },
    sync::Notify,
    task::JoinHandle,
};

/// The direction of a flow through the proxy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    /// From the endpoint that connected to the proxy towards the proxy's target.
    Up,
    /// From the proxy's target towards the endpoint that connected to the proxy.
    Down,
}

const UNLIMITED: u64 = u64::MAX;
// Small socket buffers: a stalled direction blocks its sender after little data.
const SOCKET_BUFFER: u32 = 16 * 1024;

#[derive(Default)]
struct Gate {
    credit: AtomicU64,
    forwarded: AtomicU64,
    credit_changed: Notify,
    progress: Notify,
}

impl Gate {
    fn set_credit(&self, credit: u64) {
        self.credit.store(credit, Ordering::SeqCst);
        self.credit_changed.notify_waiters();
    }

    /// Changes a limited credit; an unlimited one stays unlimited.
    fn change_credit(&self, change: impl Fn(u64) -> u64) {
        let mut credit = self.credit.load(Ordering::SeqCst);
        while credit != UNLIMITED {
            match self.credit.compare_exchange_weak(
                credit,
                change(credit),
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(now) => credit = now,
            }
        }
    }

    fn add_credit(&self, bytes: u64) {
        self.change_credit(|c| c.saturating_add(bytes).min(UNLIMITED - 1));
        self.credit_changed.notify_waiters();
    }

    /// Accounts for `bytes` read from the sender and forwarded.
    fn forwarded(&self, bytes: usize) {
        self.change_credit(|c| c.saturating_sub(bytes as u64));
        self.forwarded.fetch_add(bytes as u64, Ordering::SeqCst);
        self.progress.notify_waiters();
    }
}

#[derive(Default)]
struct Gates {
    up: Gate,
    down: Gate,
    connections: AtomicU64,
    connected: Notify,
}

impl Gates {
    fn gate(&self, flow: Flow) -> &Gate {
        match flow {
            Flow::Up => &self.up,
            Flow::Down => &self.down,
        }
    }
}

/// A TCP proxy that forwards only what the test allows. See the module documentation.
pub struct GatedProxy {
    addr: SocketAddr,
    gates: Arc<Gates>,
    task: JoinHandle<()>,
}

impl GatedProxy {
    /// Starts a proxy on a free loopback port that forwards to `target` (`host:port`).
    /// Both directions start open. Every accepted connection gets its own connection to
    /// the target; all connections share the gates.
    pub async fn spawn(target: impl Into<String>) -> Self {
        let target = target.into();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let gates = Arc::new(Gates::default());
        gates.up.set_credit(UNLIMITED);
        gates.down.set_credit(UNLIMITED);
        let task = tokio::spawn({
            let gates = gates.clone();
            async move {
                loop {
                    let Ok((downstream, _)) = listener.accept().await else {
                        break;
                    };
                    let Ok(upstream) = connect(&target).await else {
                        continue;
                    };
                    gates.connections.fetch_add(1, Ordering::SeqCst);
                    gates.connected.notify_waiters();
                    let (down_rx, down_tx) = downstream.into_split();
                    let (up_rx, up_tx) = upstream.into_split();
                    let gates = gates.clone();
                    tokio::spawn(async move {
                        // When one direction ends (an endpoint closed), end the other one too.
                        tokio::select! {
                            _ = pump(down_rx, up_tx, &gates, Flow::Up) => {}
                            _ = pump(up_rx, down_tx, &gates, Flow::Down) => {}
                        }
                    });
                }
            }
        });
        Self { addr, gates, task }
    }

    /// The Zenoh endpoint of the proxy, e.g. `tcp/127.0.0.1:40123`.
    pub fn endpoint(&self) -> String {
        format!("tcp/{}", self.addr)
    }

    /// Lets everything through in both directions.
    pub fn open(&self) {
        self.open_flow(Flow::Up);
        self.open_flow(Flow::Down);
    }

    /// Lets everything through in one direction.
    pub fn open_flow(&self, flow: Flow) {
        self.gates.gate(flow).set_credit(UNLIMITED);
    }

    /// Stops forwarding in both directions. The connections stay up.
    pub fn stall(&self) {
        self.stall_flow(Flow::Up);
        self.stall_flow(Flow::Down);
    }

    /// Stops forwarding in one direction.
    pub fn stall_flow(&self, flow: Flow) {
        self.gates.gate(flow).set_credit(0);
    }

    /// Lets `bytes` more bytes through a stalled direction. With one connection the count is
    /// exact; several connections share the credit and may together exceed it by one read.
    pub fn allow(&self, flow: Flow, bytes: u64) {
        self.gates.gate(flow).add_credit(bytes);
    }

    /// The number of bytes forwarded so far in one direction.
    pub fn forwarded(&self, flow: Flow) -> u64 {
        self.gates.gate(flow).forwarded.load(Ordering::SeqCst)
    }

    /// Waits until at least `bytes` bytes have been forwarded in one direction.
    pub async fn wait_forwarded(&self, flow: Flow, bytes: u64) {
        let gate = self.gates.gate(flow);
        loop {
            let progress = gate.progress.notified();
            tokio::pin!(progress);
            // Register before reading the counter, so that a change in between is not missed.
            progress.as_mut().enable();
            if gate.forwarded.load(Ordering::SeqCst) >= bytes {
                return;
            }
            progress.await;
        }
    }

    /// The number of connections accepted so far.
    pub fn connections(&self) -> u64 {
        self.gates.connections.load(Ordering::SeqCst)
    }

    /// Waits until at least `count` connections have been accepted.
    pub async fn wait_connections(&self, count: u64) {
        loop {
            let connected = self.gates.connected.notified();
            tokio::pin!(connected);
            connected.as_mut().enable();
            if self.gates.connections.load(Ordering::SeqCst) >= count {
                return;
            }
            connected.await;
        }
    }
}

impl Drop for GatedProxy {
    fn drop(&mut self) {
        self.task.abort();
        // Let the pumps of the accepted connections end: they hold the sockets.
        self.open();
    }
}

async fn connect(target: &str) -> std::io::Result<TcpStream> {
    let addr: SocketAddr = target
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let socket = TcpSocket::new_v4()?;
    let _ = socket.set_recv_buffer_size(SOCKET_BUFFER);
    let _ = socket.set_send_buffer_size(SOCKET_BUFFER);
    socket.connect(addr).await
}

async fn pump(mut rx: OwnedReadHalf, mut tx: OwnedWriteHalf, gates: &Gates, flow: Flow) {
    let gate = gates.gate(flow);
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let changed = gate.credit_changed.notified();
        tokio::pin!(changed);
        // Register before reading the credit, so that a change in between is not missed.
        changed.as_mut().enable();
        let credit = gate.credit.load(Ordering::SeqCst);
        let max = credit.min(buf.len() as u64) as usize;
        if max == 0 {
            // Without credit the proxy does not read: the sender's socket buffer fills and
            // TCP flow control stops it, as with a peer that is up and does not read any more.
            changed.await;
            continue;
        }
        tokio::select! {
            biased;
            // The credit changed while waiting for data: look at it again before reading.
            _ = &mut changed => continue,
            read = rx.read(&mut buf[..max]) => match read {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if tx.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                    gate.forwarded(n);
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(30);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gated_proxy_forwards_what_is_allowed() {
        // The target reads everything it is sent.
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = target.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = target.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            while matches!(stream.read(&mut buf).await, Ok(n) if n > 0) {}
        });

        let proxy = GatedProxy::spawn(target_addr.to_string()).await;
        proxy.stall_flow(Flow::Up);
        let mut client = TcpStream::connect(proxy.addr).await.unwrap();
        tokio::time::timeout(TIMEOUT, proxy.wait_connections(1))
            .await
            .unwrap();
        client.write_all(&[7u8; 4096]).await.unwrap();

        proxy.allow(Flow::Up, 1000);
        tokio::time::timeout(TIMEOUT, proxy.wait_forwarded(Flow::Up, 1000))
            .await
            .unwrap();
        assert_eq!(proxy.forwarded(Flow::Up), 1000);

        proxy.allow(Flow::Up, 96);
        tokio::time::timeout(TIMEOUT, proxy.wait_forwarded(Flow::Up, 1096))
            .await
            .unwrap();
        assert_eq!(proxy.forwarded(Flow::Up), 1096);

        proxy.open_flow(Flow::Up);
        tokio::time::timeout(TIMEOUT, proxy.wait_forwarded(Flow::Up, 4096))
            .await
            .unwrap();
        assert_eq!(proxy.forwarded(Flow::Up), 4096);
        assert_eq!(proxy.forwarded(Flow::Down), 0);
    }
}
