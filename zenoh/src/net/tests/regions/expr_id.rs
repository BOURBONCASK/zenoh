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
use std::collections::HashMap;

use zenoh_core::{zread, zwrite};
use zenoh_protocol::{
    core::{Bound, ExprId, Region, WhatAmI, EMPTY_EXPR_ID},
    network::{
        interest::{InterestMode, InterestOptions},
        Declare, DeclareBody,
    },
};
use zenoh_sync::get_mut_unchecked;

use super::{try_init_tracing_subscriber, FaceDef, Harness, HarnessBuilder, Message, MockFace};
use crate::net::primitives::Primitives;

/// A peer gateway with one local session and one face towards a router, the shape of a
/// ROS 2 process running rmw_zenoh in peer mode next to `rmw_zenohd`.
fn peer_with_router_face() -> (Harness, MockFace, MockFace) {
    try_init_tracing_subscriber();
    let peer = HarnessBuilder::new()
        .mode(WhatAmI::Peer)
        .subregions([Region::Local])
        .build();
    let router = peer.new_face(
        FaceDef::default()
            .mode(WhatAmI::Router)
            .remote_bound(Bound::South),
    );
    let session = peer.new_session();
    (peer, router, session)
}

/// Take every remaining expr id of `face`, as if 65 535 key expressions were mapped on it.
fn exhaust_expr_ids(face: &MockFace) {
    let mut state = face.face.state.clone();
    let _tables = zwrite!(face.face.tables.tables);
    let ids = &mut get_mut_unchecked(&mut state).local_expr_ids;
    while ids.allocate(|_| false).is_some() {}
}

fn local_mappings(face: &MockFace) -> usize {
    let _tables = zread!(face.face.tables.tables);
    face.face.state.local_mappings.len()
}

/// Key expressions of the `UndeclareToken`s received by `face`, resolved against the mappings
/// live when each one arrived, or `None` when its scope was not mapped.
fn undeclared_token_keys(face: &MockFace) -> Vec<Option<String>> {
    face.recorder().with_messages(|msgs| {
        let mut live: HashMap<ExprId, String> = HashMap::new();
        let mut keys = Vec::new();
        for msg in msgs {
            let Message::Declare(Declare { body, .. }) = msg else {
                continue;
            };
            match body {
                DeclareBody::DeclareKeyExpr(d) => {
                    live.insert(d.id, d.wire_expr.suffix.to_string());
                }
                DeclareBody::UndeclareKeyExpr(u) => {
                    live.remove(&u.id);
                }
                DeclareBody::UndeclareToken(u) => {
                    let wire_expr = &u.ext_wire_expr.wire_expr;
                    keys.push(if wire_expr.scope == EMPTY_EXPR_ID {
                        Some(wire_expr.suffix.to_string())
                    } else {
                        live.get(&wire_expr.scope)
                            .map(|prefix| format!("{prefix}{}", wire_expr.suffix))
                    });
                }
                _ => {}
            }
        }
        keys
    })
}

/// Declare then undeclare `cycles` liveliness tokens, each on a key never used before.
fn churn_tokens(session: &MockFace, cycles: u32) {
    for i in 0..cycles {
        session.declare_token(None, i, format!("churn/k{i:09}/leaf"));
        session.undeclare_token(i);
    }
}

#[test]
fn should_not_keep_expr_ids_of_undeclared_tokens() {
    let (_peer, router, session) = peer_with_router_face();

    churn_tokens(&session, 1_000);

    assert_eq!(local_mappings(&router), 0);
}

#[test]
fn should_send_undeclare_keyexpr_after_the_mapped_token_is_undeclared() {
    let (_peer, router, session) = peer_with_router_face();

    churn_tokens(&session, 1);

    let mappings = router.recorder().keyexpr_mappings();
    let id = mappings[0].0;
    assert_eq!(
        mappings,
        vec![(id, Some("churn/k000000000/leaf".into())), (id, None)]
    );
}

#[test]
fn should_not_reuse_a_released_expr_id_for_the_next_declaration() {
    let (_peer, router, session) = peer_with_router_face();

    churn_tokens(&session, 2);

    let declared: Vec<ExprId> = router
        .recorder()
        .keyexpr_mappings()
        .into_iter()
        .filter_map(|(id, expr)| expr.map(|_| id))
        .collect();
    assert_ne!(declared[0], declared[1]);
}

#[test]
fn should_keep_routing_declarations_after_more_than_u16_max_churn_cycles() {
    let (_peer, router, session) = peer_with_router_face();

    churn_tokens(&session, u16::MAX as u32 + 1_000);

    assert_eq!(router.recorder().tokens().len(), u16::MAX as usize + 1_000);
}

#[test]
fn should_undeclare_an_expr_id_before_declaring_it_again() {
    let (_peer, router, session) = peer_with_router_face();

    churn_tokens(&session, u16::MAX as u32 + 1_000);

    let mut live: HashMap<ExprId, bool> = HashMap::new();
    for (id, expr) in router.recorder().keyexpr_mappings() {
        let was_live = live.insert(id, expr.is_some()).unwrap_or(false);
        assert!(
            !(was_live && expr.is_some()),
            "expr id {id} redeclared while still mapped"
        );
    }
}

#[test]
fn should_fall_back_to_the_full_key_when_every_expr_id_is_in_use() {
    let (_peer, router, session) = peer_with_router_face();
    exhaust_expr_ids(&router);

    session.declare_token(None, 0, "live/overflow/leaf");

    let token = router.recorder().tokens().pop().unwrap();
    assert_eq!(
        (token.wire_expr.scope, token.wire_expr.suffix.to_string()),
        (EMPTY_EXPR_ID, "live/overflow/leaf".to_string())
    );
}

#[test]
fn should_map_new_keys_again_once_an_expr_id_is_released() {
    let (_peer, router, session) = peer_with_router_face();
    session.declare_token(None, 0, "live/first/leaf");
    exhaust_expr_ids(&router);
    session.undeclare_token(0);

    session.declare_token(None, 1, "live/second/leaf");

    let token = router.recorder().tokens().pop().unwrap();
    assert_ne!(token.wire_expr.scope, EMPTY_EXPR_ID);
}

#[test]
fn should_not_send_undeclare_keyexpr_to_a_closing_face() {
    let (_peer, router, session) = peer_with_router_face();
    session.declare_token(None, 0, "a/b/c");
    session.declare_token(None, 1, "a/b");
    router.declare_subscriber(None, 0, "a/b/c");
    session.undeclare_token(0);
    session.undeclare_token(1);
    router.recorder().clear();

    router.face.send_close();

    assert!(router
        .recorder()
        .keyexpr_mappings()
        .iter()
        .all(|(_, expr)| expr.is_some()));
}

#[test]
fn should_resolve_the_key_of_a_token_undeclared_to_a_face_that_never_saw_it_declared() {
    try_init_tracing_subscriber();
    let router = Harness::new_router();
    let client = router.new_face(
        FaceDef::default()
            .mode(WhatAmI::Client)
            .region(Region::default_south(WhatAmI::Client)),
    );
    let session = router.new_session();
    session.declare_token(None, 0, "a/b");
    let options = InterestOptions::KEYEXPRS + InterestOptions::TOKENS;
    client.interest(1, InterestMode::Future, options, "a/**");
    client.interest(2, InterestMode::Current, options, "a/**");

    session.undeclare_token(0);

    assert_eq!(
        undeclared_token_keys(&client),
        vec![Some("a/b".to_string())]
    );
}

mod router_mesh {
    use std::sync::Arc;

    use zenoh_protocol::{
        core::WireExpr,
        network::{declare::queryable::ext::QueryableInfoType, Mapping},
    };

    use super::*;
    use crate::net::{
        routing::dispatcher::resource::Resource,
        tests::regions::{Connection, EstablishedConnection},
    };

    fn two_meshed_routers() -> (Harness, Harness, EstablishedConnection) {
        try_init_tracing_subscriber();
        let r0 = HarnessBuilder::new()
            .mode(WhatAmI::Router)
            .subregions([Region::Local])
            .build();
        let r1 = HarnessBuilder::new()
            .mode(WhatAmI::Router)
            .subregions([Region::Local])
            .build();
        let mut conn = Connection {
            a: &r0,
            b: &r1,
            a2b: FaceDef::default().mode(WhatAmI::Router),
            b2a: FaceDef::default().mode(WhatAmI::Router),
        }
        .establish();
        conn.bi_fwd();
        (r0, r1, conn)
    }

    fn weak_resource(h: &Harness, key: &str) -> std::sync::Weak<Resource> {
        Resource::get_resource(zread!(h.gateway.tables.tables).data._get_root(), key)
            .map(|res| Arc::downgrade(&res))
            .unwrap_or_else(|| panic!("resource {key} should exist while declared"))
    }

    /// Declare a subscriber, a queryable and a token on r0, undeclare them, and return the keys
    /// whose resources are still alive on either router.
    fn churn_one_of_each(
        r0: &Harness,
        r1: &Harness,
        conn: &mut EstablishedConnection,
    ) -> Vec<String> {
        const KEYS: [&str; 3] = ["m/sub/leaf", "m/qabl/leaf", "m/token/leaf"];
        let s = r0.new_session();
        s.declare_subscriber(None, 1, KEYS[0]);
        s.declare_queryable(None, 2, KEYS[1], QueryableInfoType::DEFAULT);
        s.declare_token(None, 3, KEYS[2]);
        conn.bi_fwd();
        let weak = [("r0", r0), ("r1", r1)]
            .map(|(name, h)| KEYS.map(|key| (format!("{name}:{key}"), weak_resource(h, key))));
        s.undeclare_subscriber(1);
        s.undeclare_queryable(2);
        s.undeclare_token(3);
        conn.bi_fwd();
        weak.into_iter()
            .flatten()
            .filter(|(_, res)| res.upgrade().is_some())
            .map(|(key, _)| key)
            .collect()
    }

    /// Resources of declarations forwarded over a router-to-router face are released on both
    /// routers once undeclared (the mapped path).
    #[test]
    fn should_release_resources_on_both_routers() {
        let (r0, r1, mut conn) = two_meshed_routers();

        let alive = churn_one_of_each(&r0, &r1, &mut conn);

        assert_eq!(alive, Vec::<String>::new());
        assert_eq!(local_mappings(&conn.a2b), 0);
    }

    /// Same, when the face fell back to full keys because its id space is exhausted.
    #[test]
    fn should_release_resources_on_both_routers_with_full_keys() {
        let (r0, r1, mut conn) = two_meshed_routers();
        exhaust_expr_ids(&conn.a2b);

        let alive = churn_one_of_each(&r0, &r1, &mut conn);

        assert_eq!(alive, Vec::<String>::new());
    }

    /// r1 routes `a/b/c` to r0's `a/**` subscriber using r0's id for `a/b` (mapping=Receiver).
    /// After r0 releases that id, r1 must stop using it.
    #[test]
    fn should_keep_delivering_a_child_key_after_its_parent_expr_id_is_released() {
        let (r0, r1, mut conn) = two_meshed_routers();
        let wild = r0.new_session();
        wild.declare_subscriber(None, 1, "a/**");
        let parent = r0.new_session();
        parent.declare_subscriber(None, 1, "a/b");
        let publisher = r1.new_session();
        publisher.declare_keyexpr(None, 1, "a/b/c");
        conn.bi_fwd();
        let put = |n: u8| {
            publisher.put(
                WireExpr {
                    scope: 1,
                    suffix: "".into(),
                    mapping: Mapping::Sender,
                },
                vec![n],
            )
        };

        put(0);
        conn.bi_fwd();
        assert_eq!(wild.recorder().pushes().len(), 1);

        parent.undeclare_subscriber(1);
        conn.bi_fwd();
        put(1);
        conn.bi_fwd();

        let sent: Vec<_> = conn
            .b2a
            .recorder()
            .pushes()
            .into_iter()
            .map(|p| p.wire_expr.to_owned())
            .collect();
        let undeclared = conn
            .a2b
            .recorder()
            .keyexpr_mappings()
            .into_iter()
            .filter(|(_, e)| e.is_none())
            .count();
        assert_eq!(
            wild.recorder().pushes().len(),
            2,
            "r1 sent {sent:?} to r0; r0 sent {undeclared} UndeclareKeyExpr"
        );
    }

    /// Same as above, once r0 has reused the released id for another key: r1's stale route then
    /// makes r0 deliver `a/b/c` samples as `z/y/c`.
    #[test]
    fn should_not_deliver_a_child_key_under_the_key_that_reused_its_parent_expr_id() {
        let (r0, r1, mut conn) = two_meshed_routers();
        let wild = r0.new_session();
        wild.declare_subscriber(None, 1, "a/**");
        let parent = r0.new_session();
        parent.declare_subscriber(None, 1, "a/b");
        let publisher = r1.new_session();
        publisher.declare_keyexpr(None, 1, "a/b/c");
        conn.bi_fwd();
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
        conn.bi_fwd();
        assert_eq!(wild.recorder().pushes().len(), 1);

        exhaust_expr_ids(&conn.a2b);
        parent.undeclare_subscriber(1);
        let victim = r0.new_session();
        victim.declare_subscriber(None, 1, "z/y/**");
        conn.bi_fwd();
        put();
        conn.bi_fwd();

        let misdelivered: Vec<_> = victim
            .recorder()
            .pushes()
            .into_iter()
            .map(|p| p.wire_expr.suffix.to_string())
            .collect();
        assert_eq!(misdelivered, Vec::<String>::new());
        assert_eq!(wild.recorder().pushes().len(), 2);
    }
}
