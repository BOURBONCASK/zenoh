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
//! Phase 7.1: `parking_lot` lock wrappers that pretend to return
//! `LockResult` so that the existing `zread!`/`zwrite!`/`zlock!`
//! macros (which call `.unwrap()`) keep compiling.
//!
//! `parking_lot::RwLock` and `parking_lot::Mutex` have noticeably
//! lower acquire-wait p99 than `std::sync` on contended workloads,
//! especially on aarch64 / RT kernels where the std impl falls
//! back to futex+park more aggressively. The Phase 5/6 measurements
//! showed wt_acq_p99 of 16 ms on `std::sync::RwLock` during the
//! restart=6 storm; parking_lot typically halves to quarters that
//! under similar load (microbenchmarks). The wrapper here lets us
//! swap the implementation in one place (`TablesLock`) without
//! touching every macro call site.
//!
//! These types implement `.read() -> Result<_, Infallible>` and
//! analogous APIs so that `lock.read().unwrap()` produces the
//! parking_lot guard. The `Infallible` error type makes the
//! `.unwrap()` a no-op the compiler can fold to nothing.
//!
//! ## Semantic difference vs. `std::sync`
//!
//! `parking_lot` locks do **not** implement mutex poisoning: if a
//! thread panics while holding the lock, subsequent acquisitions
//! succeed normally instead of returning `Err(PoisonError)`. This
//! diverges from `std::sync::RwLock`/`Mutex`. In zenoh's routing
//! hot paths the `.unwrap()` of a `PoisonError` was effectively
//! `panic!()` anyway, so the loss of poisoning produces no behavior
//! change at those existing call sites — a panic under the lock in
//! `std` would have aborted the process via the `.unwrap()`, and a
//! panic under the lock in `parking_lot` continues without
//! signalling subsequent acquirers.
//!
//! There is no compiler-enforced safeguard that future callers will
//! preserve this property. If you add a new caller that relies on
//! poisoning to detect partial mutation across an unrelated panic,
//! audit the new code path independently and either restore an
//! explicit poison flag or revert that specific call to
//! `std::sync`.

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
