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

//! Tests involving [`zenoh_protocol::network::declare`].

use std::sync::Arc;

use zenoh_protocol::{
    core::{Bound, Region, WhatAmI, WireExpr},
    network::{
        declare::queryable::ext::QueryableInfoType,
        interest::{InterestMode, InterestOptions},
        Mapping,
    },
};

use super::{
    try_init_tracing_subscriber, Connection, EstablishedConnection, FaceDef, Harness,
    HarnessBuilder,
};
use crate::net::routing::dispatcher::resource::Resource;

/// Tests that the gateway doesn't propagate tokens to the source router region.
#[test]
fn test_against_invalid_token_propagation_to_source_south_router_region() {
    try_init_tracing_subscriber();

    let n = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::default_south(WhatAmI::Router), Region::Local])
        .build();

    let s = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
        .build();

    let s_n = Connection {
        a: &s,
        b: &n,
        a2b: FaceDef::default()
            .mode(WhatAmI::Router)
            .remote_bound(Bound::South),
        b2a: FaceDef::default()
            .mode(WhatAmI::Router)
            .region(Region::default_south(WhatAmI::Router)),
    };

    let mut s_n = s_n.establish();
    s_n.bi_fwd();

    let ss = s.new_session();
    ss.declare_token(None, 1, "k");
    s_n.bi_fwd();

    assert!(s_n.b2a.recorder().tokens().is_empty());
}

/// Same as [`test_against_invalid_token_propagation_to_source_south_router_region`] but for north
/// router regions.
#[test]
fn test_against_invalid_token_propagation_to_source_north_router_region() {
    try_init_tracing_subscriber();

    let r0 = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
        .build();
    let r1 = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
        .build();

    let r0_r1 = Connection {
        a: &r0,
        b: &r1,
        a2b: FaceDef::default().mode(WhatAmI::Router),
        b2a: FaceDef::default().mode(WhatAmI::Router),
    };

    let mut r0_r1 = r0_r1.establish();
    r0_r1.bi_fwd();

    let r0s = r0.new_session();
    r0s.declare_token(None, 1, "k");
    r0_r1.bi_fwd();

    assert!(r0_r1.b2a.recorder().tokens().is_empty());
}

#[test]
fn test_multiple_gateways_r2r_token_propagation_upstream() {
    try_init_tracing_subscriber();

    let g1 = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::default_south(WhatAmI::Router)])
        .build();
    let g0 = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::default_south(WhatAmI::Router)])
        .build();
    let n = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
        .build();
    let s = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
        .build();

    let ss = s.new_session();

    let mut n_g1 = Connection {
        a: &n,
        b: &g1,
        a2b: FaceDef::default().mode(WhatAmI::Router),
        b2a: FaceDef::default().mode(WhatAmI::Router),
    }
    .establish();

    let mut n_g0 = Connection {
        a: &n,
        b: &g0,
        a2b: FaceDef::default().mode(WhatAmI::Router),
        b2a: FaceDef::default().mode(WhatAmI::Router),
    }
    .establish();

    let mut s_g1 = Connection {
        a: &s,
        b: &g1,
        a2b: FaceDef::default()
            .mode(WhatAmI::Router)
            .remote_bound(Bound::South),
        b2a: FaceDef::default()
            .mode(WhatAmI::Router)
            .region(Region::default_south(WhatAmI::Router)),
    }
    .establish();

    let mut s_g0 = Connection {
        a: &s,
        a2b: FaceDef::default()
            .mode(WhatAmI::Router)
            .remote_bound(Bound::South),
        b: &g0,
        b2a: FaceDef::default()
            .mode(WhatAmI::Router)
            .region(Region::default_south(WhatAmI::Router)),
    }
    .establish();

    let mut bi_fwd_all = || {
        EstablishedConnection::bi_fwd_many_unbounded([&mut n_g1, &mut n_g0, &mut s_g1, &mut s_g0])
    };

    bi_fwd_all();

    ss.declare_token(None, 1, "k");
    bi_fwd_all();

    // Only one gateway should forward the token
    assert!((n_g0.b2a.recorder().tokens().len() == 1) ^ (n_g1.b2a.recorder().tokens().len() == 1));

    // The token should not be re-propagated downstream
    assert!(s_g0.b2a.recorder().tokens().is_empty());
    assert!(s_g1.b2a.recorder().tokens().is_empty());

    assert!(n_g0.is_bi_complete());
    assert!(n_g1.is_bi_complete());
    assert!(s_g0.is_bi_complete());
    assert!(s_g1.is_bi_complete());
}

#[test]
fn test_multiple_gateways_r2r_token_propagation_downstream() {
    try_init_tracing_subscriber();

    let g1 = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::default_south(WhatAmI::Router)])
        .build();
    let g0 = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::default_south(WhatAmI::Router)])
        .build();
    let n = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
        .build();
    let s = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
        .build();

    let ns = n.new_session();

    let mut n_g1 = Connection {
        a: &n,
        b: &g1,
        a2b: FaceDef::default().mode(WhatAmI::Router),
        b2a: FaceDef::default().mode(WhatAmI::Router),
    }
    .establish();

    let mut n_g0 = Connection {
        a: &n,
        b: &g0,
        a2b: FaceDef::default().mode(WhatAmI::Router),
        b2a: FaceDef::default().mode(WhatAmI::Router),
    }
    .establish();

    let mut s_g1 = Connection {
        a: &s,
        b: &g1,
        a2b: FaceDef::default()
            .mode(WhatAmI::Router)
            .remote_bound(Bound::South),
        b2a: FaceDef::default()
            .mode(WhatAmI::Router)
            .region(Region::default_south(WhatAmI::Router)),
    }
    .establish();

    let mut s_g0 = Connection {
        a: &s,
        a2b: FaceDef::default()
            .mode(WhatAmI::Router)
            .remote_bound(Bound::South),
        b: &g0,
        b2a: FaceDef::default()
            .mode(WhatAmI::Router)
            .region(Region::default_south(WhatAmI::Router)),
    }
    .establish();

    let mut bi_fwd_all = || {
        EstablishedConnection::bi_fwd_many_unbounded([&mut n_g1, &mut n_g0, &mut s_g1, &mut s_g0])
    };

    bi_fwd_all();

    ns.declare_token(None, 1, "k");
    bi_fwd_all();

    // Only one gateway should forward the token
    assert!((s_g0.b2a.recorder().tokens().len() == 1) ^ (s_g1.b2a.recorder().tokens().len() == 1));

    // The token should not be re-propagated upstream
    assert!(n_g0.b2a.recorder().tokens().is_empty());
    assert!(n_g1.b2a.recorder().tokens().is_empty());

    assert!(n_g0.is_bi_complete());
    assert!(n_g1.is_bi_complete());
    assert!(s_g0.is_bi_complete());
    assert!(s_g1.is_bi_complete());
}

#[test]
fn test_client_token_repropagation() {
    let g = HarnessBuilder::new()
        .mode(WhatAmI::Client)
        .subregions([Region::Local, Region::default_south(WhatAmI::Client)])
        .start_runtime(false)
        .build();

    let s0 = g.new_session();

    let s1 = g.new_face(
        FaceDef::default()
            .region(Region::default_south(WhatAmI::Client))
            .mode(WhatAmI::Client),
    );

    s0.declare_token(None, 1, "k/a");
    s0.declare_token(None, 2, "k/b");

    s1.declare_token(None, 1, "k/b");
    s1.declare_token(None, 2, "k/c");

    let n = g.new_face(FaceDef::default().remote_bound(Bound::South));

    assert_eq!(n.recorder().tokens().len(), 3);
}

/// Tests that queryable declaration doesn't result in undeclaration of its duplicates.
#[test]
fn test_duplicate_queryable_undeclaration() {
    try_init_tracing_subscriber();

    let g = HarnessBuilder::new()
        .mode(WhatAmI::Client)
        .subregions([Region::default_south(WhatAmI::Client)])
        .start_runtime(false)
        .build();

    let c0 = g.new_face(
        FaceDef::default()
            .mode(WhatAmI::Client)
            .region(Region::default_south(WhatAmI::Client)),
    );

    c0.declare_keyexpr(None, 5, "k");

    c0.declare_queryable(
        None,
        1,
        WireExpr {
            scope: 5,
            suffix: "".into(),
            mapping: Mapping::Sender,
        },
        QueryableInfoType::DEFAULT,
    );

    c0.declare_queryable(None, 2, "k", QueryableInfoType::DEFAULT);

    c0.undeclare_queryable(1);

    let c1 = g.new_face(
        FaceDef::default()
            .mode(WhatAmI::Client)
            .region(Region::default_south(WhatAmI::Client)),
    );

    c1.query(1, "k");

    assert_eq!(c0.recorder().requests().len(), 1);
}

#[test]
fn test_router_disconnect_undeclares_subscriber() {
    const KEY_EXPR: &str = "k";

    try_init_tracing_subscriber();

    let r1 = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
        .build();
    let r2 = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
        .build();

    let r1s = r1.new_session();
    let r2s = r2.new_session();

    let mut r1_r2 = Connection {
        a: &r1,
        b: &r2,
        a2b: FaceDef::default().mode(WhatAmI::Router),
        b2a: FaceDef::default().mode(WhatAmI::Router),
    }
    .establish();
    r1_r2.bi_fwd();

    r2s.interest(
        1,
        InterestMode::Future,
        InterestOptions::SUBSCRIBERS,
        KEY_EXPR,
    );
    r1_r2.bi_fwd();

    r1s.declare_subscriber(None, 1, KEY_EXPR);
    r1_r2.bi_fwd();

    assert_eq!(r2s.recorder().subscribers().len(), 1);
    r2s.recorder().clear();

    r1_r2.disconnect();

    assert_eq!(r2s.recorder().undeclared_subscribers().len(), 1);
}

#[test]
fn test_router_disconnect_undeclares_queryable() {
    const KEY_EXPR: &str = "k";

    try_init_tracing_subscriber();

    let r1 = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
        .build();
    let r2 = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
        .build();

    let r1s = r1.new_session();
    let r2s = r2.new_session();

    let mut r1_r2 = Connection {
        a: &r1,
        b: &r2,
        a2b: FaceDef::default().mode(WhatAmI::Router),
        b2a: FaceDef::default().mode(WhatAmI::Router),
    }
    .establish();
    r1_r2.bi_fwd();

    r2s.interest(
        1,
        InterestMode::Future,
        InterestOptions::QUERYABLES,
        KEY_EXPR,
    );
    r1_r2.bi_fwd();

    r1s.declare_queryable(None, 1, KEY_EXPR, QueryableInfoType::DEFAULT);
    r1_r2.bi_fwd();

    assert_eq!(r2s.recorder().queryables().len(), 1);
    r2s.recorder().clear();

    r1_r2.disconnect();

    assert_eq!(r2s.recorder().undeclared_queryables().len(), 1);
}

#[test]
fn test_router_disconnect_undeclares_token() {
    const KEY_EXPR: &str = "k";

    try_init_tracing_subscriber();

    let r1 = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
        .build();
    let r2 = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
        .build();

    let r1s = r1.new_session();
    let r2s = r2.new_session();

    let mut r1_r2 = Connection {
        a: &r1,
        b: &r2,
        a2b: FaceDef::default().mode(WhatAmI::Router),
        b2a: FaceDef::default().mode(WhatAmI::Router),
    }
    .establish();
    r1_r2.bi_fwd();

    r2s.interest(1, InterestMode::Future, InterestOptions::TOKENS, KEY_EXPR);
    r1_r2.bi_fwd();

    r1s.declare_token(None, 1, KEY_EXPR);
    r1_r2.bi_fwd();

    assert_eq!(r2s.recorder().tokens().len(), 1);
    r2s.recorder().clear();

    r1_r2.disconnect();

    assert_eq!(r2s.recorder().undeclared_tokens().len(), 1);
}

/// Tests that once a face undeclares a keyexpr, the gateway stops using its id in routes cached on
/// child resources (e.g. `(id, "/c")` for `a/b/c` when the face declared `a/b`).
#[test]
fn test_undeclared_keyexpr_is_not_used_by_cached_child_route() {
    try_init_tracing_subscriber();

    let router = Harness::new_router();
    let client = || {
        router.new_face(
            FaceDef::default()
                .mode(WhatAmI::Client)
                .region(Region::default_south(WhatAmI::Client)),
        )
    };
    let subscriber = client();
    subscriber.declare_keyexpr(None, 5, "a/b");
    subscriber.declare_subscriber(None, 1, "a/**");
    let publisher = client();
    publisher.declare_keyexpr(None, 1, "a/b/c");
    let put = || {
        publisher.put(
            WireExpr {
                scope: 1,
                suffix: "".into(),
                mapping: Mapping::Sender,
            },
            vec![0],
        )
    };

    put();
    subscriber.undeclare_keyexpr(5);
    put();

    let sent: Vec<_> = subscriber
        .recorder()
        .pushes()
        .into_iter()
        .map(|p| (p.wire_expr.scope, p.wire_expr.mapping))
        .collect();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0], (5, Mapping::Receiver));
    assert_ne!(sent[1], (5, Mapping::Receiver), "{sent:?}");
}

/// Tests that a router releases the resources of a subscriber and a queryable that another router
/// undeclares by key expression (sourced declarations carry no id).
#[test]
fn test_undeclare_by_key_from_router_releases_resource() {
    try_init_tracing_subscriber();

    let r0 = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
        .build();
    let r1 = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local])
        .build();
    let mut r0_r1 = Connection {
        a: &r0,
        b: &r1,
        a2b: FaceDef::default().mode(WhatAmI::Router),
        b2a: FaceDef::default().mode(WhatAmI::Router),
    }
    .establish();
    r0_r1.bi_fwd();

    // r0's face on r1 declares and undeclares with full keys
    let from_r0 = &r0_r1.b2a;
    from_r0.declare_subscriber(None, 0, "x/subscriber");
    from_r0.declare_queryable(None, 0, "x/queryable", QueryableInfoType::DEFAULT);
    let weak = ["x/subscriber", "x/queryable"].map(|key| {
        let res = Resource::get_resource(zread!(r1.gateway.tables.tables).data._get_root(), key)
            .expect("resource should exist while declared");
        (key, Arc::downgrade(&res))
    });
    from_r0.undeclare_subscriber_by_key("x/subscriber");
    from_r0.undeclare_queryable_by_key("x/queryable");

    for (key, res) in weak {
        assert!(res.upgrade().is_none(), "resource {key} was not released");
    }
}
