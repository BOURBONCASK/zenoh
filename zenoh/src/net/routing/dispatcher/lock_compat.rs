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
//! `parking_lot` lock wrappers that mimic `std::sync`'s `LockResult` API so
//! the existing `zread!` / `zwrite!` / `zlock!` macros (which call
//! `.unwrap()`) keep compiling.
//!
//! ## Motivation (issue #2581)
//!
//! `std::sync::RwLock` is writer-preferring: once a writer is queued, every
//! subsequent reader blocks behind it. Under sustained p2p churn the routing
//! layer's `TablesLock::tables` saw ~50–100 writers/sec from OAM linkstate +
//! transport open/close, which is enough to keep the writer queue
//! continuously non-empty and starve `route_data`'s `tables.read()`. The
//! observed symptom (`publisher.put(...).wait()` stalling 10–30 s under
//! moderate p2p churn) matched this writer-priority starvation almost
//! exactly: a 1 h soak on a 50-peer reproducer produced `worker_pub_put`
//! max ≈ 14 s and 5 events > 10 s.
//!
//! `parking_lot::RwLock` is not writer-preferring in the same strict sense;
//! its biased fastpath lets readers proceed even when writers are queued
//! (as long as the lock is currently read-held), and eventual fairness
//! prevents writer starvation in the opposite direction. Empirically, on the
//! same 1 h soak it gave `worker_pub_put` max ≈ 5.2 s and **0** events >
//! 10 s. The residual ~5 s tail is `wait_before_close` in the transport's
//! Block-mode pipeline draining (issue tracked separately) and not lock
//! related.
//!
//! ## Semantic difference vs. `std::sync`
//!
//! `parking_lot` locks do **not** implement mutex poisoning: if a thread
//! panics while holding the lock, subsequent acquisitions succeed normally
//! instead of returning `Err(PoisonError)`. This diverges from
//! `std::sync::RwLock` / `Mutex`. In zenoh's routing hot paths the
//! `.unwrap()` of a `PoisonError` was effectively `panic!()` anyway, so the
//! loss of poisoning is a no-op at existing call sites — a panic under the
//! lock in `std` would have aborted the process via the `.unwrap()`, and a
//! panic under the lock in `parking_lot` continues without signalling
//! subsequent acquirers.
//!
//! There is no compiler-enforced safeguard that future callers will preserve
//! this property. If you add a new caller that relies on poisoning to detect
//! partial mutation across an unrelated panic, audit the new code path
//! independently and either restore an explicit poison flag or revert that
//! specific call to `std::sync`.

use std::convert::Infallible;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// `parking_lot::RwLock<T>` with a std-style `LockResult` API.
#[derive(Debug, Default)]
pub struct PlRwLock<T>(RwLock<T>);

impl<T> PlRwLock<T> {
    #[inline]
    pub fn new(value: T) -> Self {
        Self(RwLock::new(value))
    }

    #[inline]
    pub fn read(&self) -> Result<RwLockReadGuard<'_, T>, Infallible> {
        Ok(self.0.read())
    }

    #[inline]
    pub fn write(&self) -> Result<RwLockWriteGuard<'_, T>, Infallible> {
        Ok(self.0.write())
    }

    #[allow(dead_code)]
    #[inline]
    pub fn get_mut(&mut self) -> Result<&mut T, Infallible> {
        Ok(self.0.get_mut())
    }
}

/// `parking_lot::Mutex<T>` with a std-style `LockResult` API.
#[derive(Debug, Default)]
pub struct PlMutex<T>(parking_lot::Mutex<T>);

impl<T> PlMutex<T> {
    #[inline]
    pub fn new(value: T) -> Self {
        Self(parking_lot::Mutex::new(value))
    }

    #[inline]
    pub fn lock(&self) -> Result<parking_lot::MutexGuard<'_, T>, Infallible> {
        Ok(self.0.lock())
    }
}
