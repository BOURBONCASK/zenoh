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
//! Per-face queueing wrapper for `EPrimitives`.
//!
//! ## Why
//!
//! With serial fan-out in `route_data`, a slow destination — a subscriber
//! callback holding a contended mutex, a peer whose `TransmissionPipeline`
//! is back-pressured — stalls the entire dispatch of *one* `publisher.put()`
//! call and therefore every other destination on that call as well. Our
//! production requirement (issue #2581 follow-up) is that any
//! single client / peer disconnect or slowdown must NOT affect unrelated
//! subscribers' delivery.
//!
//! Shared-thread-pool fan-out (we tried both `tokio::spawn_blocking` and
//! `rayon::ThreadPool`) makes things worse because the pool becomes a
//! contention point under concurrent fan-out from many publishers.
//!
//! This module wraps each destination face's primitives with a bounded
//! channel + dedicated worker task. Each face is then its own contention
//! domain: a slow consumer fills only its own queue; other faces' workers
//! continue draining independently. `route_data`'s dispatch becomes a
//! sequence of cheap `try_send` / `send` enqueues — typically µs per
//! destination, regardless of how slow any individual consumer is.
//!
//! ## Mechanics
//!
//! - A single `flume::bounded` channel per face carries *all* message types
//!   (`Push`, `Declare`, `Interest`, `Request`, `Response`, `ResponseFinal`)
//!   so per-face FIFO ordering — and the declare-before-push invariant — is
//!   preserved.
//! - A worker task spawned on `ZRuntime::Net` drains the channel and calls
//!   the inner primitives.
//! - When the wrapper is dropped (face removed from `tables.data.faces`),
//!   the channel sender drops; the receiver returns `Err(Disconnected)` and
//!   the worker exits naturally. No explicit join needed — the inner
//!   primitives keep the underlying transport alive only as long as the
//!   wrapper's `Arc<P>` is held by the worker.
//! - `Push` with `CongestionControl::Block` does `send` on full (back-pressure
//!   propagates only to the publisher that's targeting that one face). All
//!   other message types are control-plane and use `send` (blocking) so
//!   they are never dropped — declares must arrive for routing to remain
//!   correct.
//!
//! Gated by env var `ZENOH_PER_FACE_QUEUE=1` so production rollouts can
//! flip back to the unwrapped path if a regression is observed.

use std::{any::Any, cell::OnceCell, sync::Arc};

use flume::{Receiver, Sender, TrySendError};
use zenoh_protocol::{
    core::{CongestionControl, Reliability},
    network::{interest::Interest, Declare, Push, Request, Response, ResponseFinal},
};

use super::EPrimitives;
use crate::net::routing::RoutingContext;

/// Default per-face queue capacity. Override with `ZENOH_FACE_QUEUE_CAPACITY`.
const DEFAULT_CAPACITY: usize = 1024;

/// Items moving through the per-face queue. Owned copies of the original
/// messages plus any routing context (`full_expr` for control-plane).
enum QueuedMsg {
    Interest(Interest, Option<String>),
    Declare(Declare, Option<String>),
    Push(Push, Reliability),
    Request(Request),
    Response(Response),
    ResponseFinal(ResponseFinal),
}

/// Wrapper that buffers outgoing primitive calls per face and drains them on
/// a dedicated worker task. See module-level docs for the rationale.
///
/// The `_inner` Arc is held only to keep the underlying primitives alive for
/// as long as the wrapper exists; the worker owns its own clone for the
/// drain loop. We never call it through this field — kept under the
/// `_inner` name to suppress the unused-field warning.
pub(crate) struct QueueingPrimitives {
    #[allow(dead_code)]
    inner: Arc<dyn EPrimitives + Send + Sync>,
    tx: Sender<QueuedMsg>,
}

impl QueueingPrimitives {
    /// Build the wrapper, spawn the worker task. `inner` is held by both the
    /// wrapper and the worker; the worker exits when this wrapper is dropped.
    pub(crate) fn new(inner: Arc<dyn EPrimitives + Send + Sync>) -> Self {
        let capacity = std::env::var("ZENOH_FACE_QUEUE_CAPACITY")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n: &usize| n > 0 && n <= 1_000_000)
            .unwrap_or(DEFAULT_CAPACITY);
        let (tx, rx) = flume::bounded::<QueuedMsg>(capacity);
        let inner_for_worker = inner.clone();
        zenoh_runtime::ZRuntime::Net.spawn(async move {
            Self::drain(inner_for_worker, rx).await;
        });
        Self { inner, tx }
    }

    async fn drain(inner: Arc<dyn EPrimitives + Send + Sync>, rx: Receiver<QueuedMsg>) {
        while let Ok(item) = rx.recv_async().await {
            match item {
                QueuedMsg::Interest(mut m, expr) => {
                    let ctx = build_ctx(&mut m, expr);
                    let _ = inner.send_interest(ctx);
                }
                QueuedMsg::Declare(mut m, expr) => {
                    let ctx = build_ctx(&mut m, expr);
                    let _ = inner.send_declare(ctx);
                }
                QueuedMsg::Push(mut m, r) => {
                    let _ = inner.send_push(&mut m, r);
                }
                QueuedMsg::Request(mut m) => {
                    let _ = inner.send_request(&mut m);
                }
                QueuedMsg::Response(mut m) => {
                    let _ = inner.send_response(&mut m);
                }
                QueuedMsg::ResponseFinal(mut m) => {
                    let _ = inner.send_response_final(&mut m);
                }
            }
        }
    }

    /// Common enqueue path. `block_when_full = true` waits for capacity;
    /// `false` drops on full and returns `false` to the caller.
    fn enqueue(&self, msg: QueuedMsg, block_when_full: bool) -> bool {
        match self.tx.try_send(msg) {
            Ok(()) => true,
            Err(TrySendError::Full(item)) => {
                if block_when_full {
                    // `send` on a `flume::Sender` is blocking sync — safe to
                    // call from any context including a tokio task because
                    // it does not require the tokio runtime.
                    self.tx.send(item).is_ok()
                } else {
                    false
                }
            }
            Err(TrySendError::Disconnected(_)) => false,
        }
    }
}

#[inline]
fn build_ctx<T>(msg: &mut T, full_expr: Option<String>) -> RoutingContext<&mut T> {
    let cell = OnceCell::new();
    if let Some(s) = full_expr {
        let _ = cell.set(s);
    }
    RoutingContext {
        msg,
        full_expr: cell,
    }
}

impl EPrimitives for QueueingPrimitives {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn send_interest(&self, ctx: RoutingContext<&mut Interest>) -> bool {
        let msg = ctx.msg.clone();
        let expr = ctx.full_expr.into_inner();
        // Control plane: never drop.
        self.enqueue(QueuedMsg::Interest(msg, expr), true)
    }

    fn send_declare(&self, ctx: RoutingContext<&mut Declare>) -> bool {
        let msg = ctx.msg.clone();
        let expr = ctx.full_expr.into_inner();
        self.enqueue(QueuedMsg::Declare(msg, expr), true)
    }

    fn send_push(&self, msg: &mut Push, reliability: Reliability) -> bool {
        let owned = msg.clone();
        let block_when_full = matches!(
            owned.ext_qos.get_congestion_control(),
            CongestionControl::Block
        );
        self.enqueue(QueuedMsg::Push(owned, reliability), block_when_full)
    }

    fn send_request(&self, msg: &mut Request) -> bool {
        let owned = msg.clone();
        self.enqueue(QueuedMsg::Request(owned), true)
    }

    fn send_response(&self, msg: &mut Response) -> bool {
        let owned = msg.clone();
        self.enqueue(QueuedMsg::Response(owned), true)
    }

    fn send_response_final(&self, msg: &mut ResponseFinal) -> bool {
        let owned = msg.clone();
        self.enqueue(QueuedMsg::ResponseFinal(owned), true)
    }
}

/// `true` when `ZENOH_PER_FACE_QUEUE=1|true|yes`. Off by default to match
/// pre-change behavior so production rollouts can A/B test.
pub(crate) fn enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        matches!(
            std::env::var("ZENOH_PER_FACE_QUEUE").as_deref(),
            Ok("1") | Ok("true") | Ok("yes")
        )
    })
}
