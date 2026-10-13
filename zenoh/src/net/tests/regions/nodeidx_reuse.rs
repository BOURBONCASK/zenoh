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

//! Router-to-router replay of existing declarations to a neighbour whose link-state graph
//! `NodeIndex` reuses the slot of a neighbour removed in the same tree-computation window.
//!
//! Topology (all routers, one region):
//!
//! ```text
//!   P (provider: session declares queryable "k")
//!   |
//!   C (router under test, production async tree computation)
//!   |      \
//!   B / B'   N (noise neighbour)
//! ```
//!
//! Disconnecting `N` makes `C` compute its trees immediately; the tree worker then sleeps
//! `TREES_COMPUTATION_DELAY_MS` (100 ms). Replacing `B` with `B'` inside that sleep makes both
//! changes land in one computation, and `B'` takes `B`'s freed `NodeIndex`.

use std::{thread::sleep, time::Duration};

use zenoh_config::ZenohId;
use zenoh_protocol::{
    core::{Region, WhatAmI},
    network::declare::queryable::ext::QueryableInfoType,
};

use super::{
    try_init_tracing_subscriber, Connection, EstablishedConnection, FaceDef, Harness,
    HarnessBuilder,
};

fn router() -> Harness {
    HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
        .build()
}

fn router_with_zid(zid: ZenohId) -> Harness {
    HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .zid(zid)
        .subregions([Region::Local])
        .build()
}

fn connect<'a>(a: &'a Harness, b: &'a Harness) -> EstablishedConnection {
    Connection {
        a,
        b,
        a2b: FaceDef::default().mode(WhatAmI::Router),
        b2a: FaceDef::default().mode(WhatAmI::Router),
    }
    .establish()
}

/// Lets `C`'s delayed tree computation run, forwarding whatever it emits.
fn settle(conns: &mut [&mut EstablishedConnection]) {
    for _ in 0..4 {
        sleep(Duration::from_millis(250));
        for c in conns.iter_mut() {
            c.bi_fwd();
        }
    }
}

/// Number of `DeclareQueryable` that `C` has sent to the router on the other end of `c_x`.
fn qabls_sent(c_x: &EstablishedConnection) -> usize {
    c_x.a2b.recorder().queryables().len()
}

/// Runs the scenario and returns how many existing queryables `C` replayed to `B'`.
///
/// * `same_zid`: `B'` reuses `B`'s ZID (reconnect) instead of being a different router.
/// * `gap`: delay between losing `B` and connecting `B'`.
fn run(same_zid: bool, gap: Duration) -> (usize, usize) {
    try_init_tracing_subscriber();

    let p = router();
    let c = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
        .async_tree_computation(true)
        .build();
    let b = router();
    let n = router();

    let mut p_c = connect(&p, &c);
    let mut c_b = connect(&c, &b);
    let mut c_n = connect(&c, &n);
    settle(&mut [&mut p_c, &mut c_b, &mut c_n]);

    let ps = p.new_session();
    ps.declare_queryable(None, 1, "k", QueryableInfoType::DEFAULT);
    settle(&mut [&mut p_c, &mut c_b, &mut c_n]);
    assert_eq!(
        qabls_sent(&c_b),
        1,
        "baseline: C should propagate P's queryable to B"
    );

    // Open C's coalescing window: this disconnect is computed at once, then the worker sleeps.
    c_n.disconnect();
    sleep(Duration::from_millis(20));

    let b_zid: ZenohId = b.zid().into();
    c_b.disconnect();
    sleep(gap);
    let b2 = if same_zid {
        router_with_zid(b_zid)
    } else {
        router()
    };
    let mut c_b2 = connect(&c, &b2);
    c_b2.bi_fwd();
    p_c.bi_fwd();

    settle(&mut [&mut p_c, &mut c_b2]);
    let replayed = qabls_sent(&c_b2);

    // The link itself is healthy: a queryable declared now is propagated live.
    ps.declare_queryable(None, 2, "k2", QueryableInfoType::DEFAULT);
    settle(&mut [&mut p_c, &mut c_b2]);
    let fresh = qabls_sent(&c_b2) - replayed;

    (replayed, fresh)
}

#[test]
fn test_replay_to_new_router_reusing_freed_nodeindex_different_zid() {
    let (replayed, fresh) = run(false, Duration::ZERO);
    assert_eq!(fresh, 1, "a fresh declaration must reach B'");
    assert_eq!(replayed, 1, "existing queryable 'k' must be replayed to B'");
}

#[test]
fn test_replay_to_reconnected_router_same_zid() {
    let (replayed, fresh) = run(true, Duration::ZERO);
    assert_eq!(fresh, 1, "a fresh declaration must reach B'");
    assert_eq!(replayed, 1, "existing queryable 'k' must be replayed to B'");
}

/// Control: a tree computation runs between losing `B` and connecting `B'`.
#[test]
fn test_replay_to_new_router_after_slow_reconnect() {
    let (replayed, fresh) = run(false, Duration::from_millis(400));
    assert_eq!(fresh, 1);
    assert_eq!(replayed, 1);
}
