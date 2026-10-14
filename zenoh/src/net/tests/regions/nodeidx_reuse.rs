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

//! Router-to-router declarations when several topology changes are folded into one routing tree
//! computation, as the asynchronous computation does in production.
//!
//! The router under test (`C`) uses manual tree computation, so each test decides exactly which
//! topology changes happen between two computations.

use zenoh_config::ZenohId;
use zenoh_protocol::{
    core::{Region, WhatAmI, ZenohIdProto},
    network::declare::queryable::ext::QueryableInfoType,
};

use super::{
    try_init_tracing_subscriber, Connection, EstablishedConnection, FaceDef, Harness,
    HarnessBuilder, MockFace,
};

fn router_builder() -> HarnessBuilder {
    HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
}

fn router() -> Harness {
    router_builder().build()
}

fn manual_router() -> Harness {
    router_builder().manual_tree_computation(true).build()
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

/// Forwards messages and computes the trees of `manual` routers until the network is quiet.
fn settle(manual: &[&Harness], conns: &mut [&mut EstablishedConnection]) {
    for _ in 0..4 {
        for c in conns.iter_mut() {
            c.bi_fwd();
        }
        for h in manual {
            h.compute_trees();
        }
    }
    for c in conns.iter_mut() {
        c.bi_fwd();
    }
}

/// Index of `zid` in the routers graph of `h`.
fn node_index(h: &Harness, zid: &ZenohIdProto) -> Option<usize> {
    let mut idx = None;
    h.with_router_hats(|hat, _| {
        idx = idx.or(hat.net().get_idx(zid).map(|idx| idx.index()));
    });
    idx
}

/// Which of the queryable `k`, subscriber `s` and token `t` `h` knows from other routers.
fn sourced_kst(h: &Harness) -> [bool; 3] {
    let tables = h.gateway.tables.tables.read().unwrap();
    [
        tables.sourced_queryables().keys().any(|r| r.expr() == "k"),
        tables.sourced_subscribers().keys().any(|r| r.expr() == "s"),
        tables.sourced_tokens().keys().any(|r| r.expr() == "t"),
    ]
}

fn declare_kst(session: &MockFace) {
    session.declare_queryable(None, 1, "k", QueryableInfoType::DEFAULT);
    session.declare_subscriber(None, 2, "s");
    session.declare_token(None, 3, "t");
}

/// P (session declares `k`, `s`, `t`) – C – B. B leaves and B' joins with no tree computation
/// in between on C. Returns what B' knows of P's declarations once the network is quiet.
fn replay_after_neighbor_replaced(same_zid: bool, compute_in_between: bool) -> [bool; 3] {
    try_init_tracing_subscriber();

    let p = router();
    let c = manual_router();
    let b = router();
    let mut p_c = connect(&p, &c);
    let mut c_b = connect(&c, &b);
    settle(&[&c], &mut [&mut p_c, &mut c_b]);

    let session = p.new_session();
    declare_kst(&session);
    settle(&[&c], &mut [&mut p_c, &mut c_b]);
    assert_eq!(sourced_kst(&b), [true; 3], "B should know P's declarations");

    let b_idx = node_index(&c, &b.zid());
    c_b.disconnect();
    if compute_in_between {
        c.compute_trees();
    }
    let b2 = if same_zid {
        router_builder().zid(ZenohId::from(b.zid())).build()
    } else {
        router()
    };
    let mut c_b2 = connect(&c, &b2);
    c_b2.bi_fwd();
    assert_eq!(
        node_index(&c, &b2.zid()),
        b_idx,
        "B' should take B's index in C's routers graph"
    );

    settle(&[&c], &mut [&mut p_c, &mut c_b2]);
    sourced_kst(&b2)
}

#[test]
fn test_replay_to_router_reusing_freed_index() {
    assert_eq!(replay_after_neighbor_replaced(false, false), [true; 3]);
}

#[test]
fn test_replay_to_router_reconnecting_with_same_zid() {
    assert_eq!(replay_after_neighbor_replaced(true, false), [true; 3]);
}

/// Control: C computes its trees between losing B and connecting B'.
#[test]
fn test_replay_to_router_after_intermediate_computation() {
    assert_eq!(replay_after_neighbor_replaced(false, true), [true; 3]);
}

/// ```text
/// P – C – B
///     |   |
///     U – +
/// ```
///
/// The C–B link is lost and re-established before C computes its trees; B stays reachable
/// through U all along. Undeclarations made meanwhile must still reach B on the new link: C's
/// trees are the only path to B for them, since U does not have B as a child.
#[test]
fn test_undeclare_to_router_reconnecting_while_reachable() {
    try_init_tracing_subscriber();

    let p = router();
    let c = manual_router();
    let b = router();
    let u = manual_router();
    let mut p_c = connect(&p, &c);
    let c_b = connect(&c, &b);
    let mut c_u = connect(&c, &u);
    let mut u_b = connect(&u, &b);
    let mut c_b = c_b;
    settle(&[&c, &u], &mut [&mut p_c, &mut c_b, &mut c_u, &mut u_b]);

    let session = p.new_session();
    declare_kst(&session);
    settle(&[&c, &u], &mut [&mut p_c, &mut c_b, &mut c_u, &mut u_b]);
    assert_eq!(sourced_kst(&b), [true; 3], "B should know P's declarations");
    assert_eq!(
        u_b.a2b.recorder().queryables().len(),
        0,
        "B should not be a child of U in P's tree"
    );

    c_b.disconnect();
    let mut c_b2 = connect(&c, &b);
    c_b2.bi_fwd();
    assert!(node_index(&c, &b.zid()).is_some());

    session.undeclare_queryable(1);
    session.undeclare_subscriber(2);
    session.undeclare_token(3);
    settle(&[&c, &u], &mut [&mut p_c, &mut c_b2, &mut c_u, &mut u_b]);

    assert_eq!(
        sourced_kst(&b),
        [false; 3],
        "B must not keep undeclared resources"
    );
}
