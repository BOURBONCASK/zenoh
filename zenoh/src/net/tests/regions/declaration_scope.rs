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

//! Declaration scope of a south router subregion (`gateway.south[].declaration_scope`).
//!
//! ```text
//!        N            north router
//!        |
//!        G            gateway under test, with sessions
//!      / | \
//!     A  B  U         routers of the scoped south subregion, with sessions
//! ```
//!
//! The Zenoh IDs of `A` and `B` yield the scopes [`SN_A`] and [`SN_B`]. The Zenoh ID of `U` does
//! not yield a scope. Some tests also link `A` and `B` together, which makes `A` a transit router.
//!
//! The clock of the gateway is stopped: a test moves it with [`elapse`], which also runs the
//! re-check of the faces in their grace period as its timer would. One test runs the timer itself
//! with the real clock ([`gateway_with_timer`]).

use std::{
    collections::{BTreeSet, HashMap},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use zenoh_config::{
    gateway::{DeclarationScopeMode, GatewaySouthConf},
    ZenohId,
};
use zenoh_protocol::{
    core::{Bound, Region, WhatAmI, WireExpr},
    network::declare::queryable::ext::QueryableInfoType,
};

use super::{
    try_init_tracing_subscriber, Connection, EstablishedConnection, FaceDef, Harness,
    HarnessBuilder, MockFace,
};
use crate::net::routing::{
    dispatcher::{resource::Resource, tables::TablesData},
    hat::{
        router::{Hat as RouterHat, TransitCounts, TRANSIT_GRACE as GRACE},
        Sources,
    },
};

const SCOPED: Region = Region::South {
    id: 0,
    mode: WhatAmI::Router,
};

const GATEWAY: &str = r#"{
    south: [{
        filters: [{ modes: ["router"] }],
        declaration_scope: {
            zid: {
                radix: 36,
                skip: 1,
                length: 19,
                alphabet: "0123456789ABCDEFGHJKLMNPQRSTUVWXYZ",
            },
            key_exprs: ["*/svc/**"],
        },
    }],
}"#;

const GATEWAY_OBSERVE: &str = r#"{
    south: [{
        filters: [{ modes: ["router"] }],
        declaration_scope: {
            zid: {
                radix: 36,
                skip: 1,
                length: 19,
                alphabet: "0123456789ABCDEFGHJKLMNPQRSTUVWXYZ",
            },
            key_exprs: ["*/svc/**"],
            mode: "observe",
        },
    }],
}"#;

const GATEWAY_WITHOUT_SCOPE: &str = r#"{ south: [{ filters: [{ modes: ["router"] }] }] }"#;

const SN_A: &str = "TEST000000000000001";
const SN_B: &str = "TEST000000000000002";
const SN_N: &str = "TEST000000000000003";

/// The Zenoh ID that yields the scope `sn`.
fn zid_of(sn: &str) -> ZenohId {
    let n = u128::from_str_radix(&format!("X{sn}"), 36).unwrap();
    ZenohId::from_str(&format!("{n:x}")).unwrap()
}

/// A gateway with the given `gateway` configuration, whose clock is stopped, and the grace period
/// of the router hat.
fn gateway(conf: &str) -> Harness {
    let g = gateway_with_timer(conf, GRACE);
    scoped_hat_mut(&g, |hat, _| hat.set_clock(Some(Instant::now())));
    g
}

/// A gateway with the given `gateway` configuration and grace period, whose clock runs.
fn gateway_with_timer(conf: &str, grace: Duration) -> Harness {
    let g = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([SCOPED, Region::Local])
        .gateway(conf)
        .build();
    scoped_hat_mut(&g, |hat, _| hat.set_transit_grace(grace));
    g
}

fn router(zid: Option<ZenohId>) -> Harness {
    let builder = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local]);
    match zid {
        Some(zid) => builder.zid(zid).build(),
        None => builder.build(),
    }
}

/// Connects `south` to the scoped subregion of `gateway`. `a2b` is the face of the gateway for
/// `south`: its recorder holds what the gateway sent to `south`.
fn connect_south<'a>(gateway: &'a Harness, south: &'a Harness) -> EstablishedConnection {
    Connection {
        a: gateway,
        a2b: FaceDef::default().mode(WhatAmI::Router).region(SCOPED),
        b: south,
        b2a: FaceDef::default()
            .mode(WhatAmI::Router)
            .remote_bound(Bound::South),
    }
    .establish()
}

/// Connects `north` to the north region of `gateway`.
fn connect_north<'a>(gateway: &'a Harness, north: &'a Harness) -> EstablishedConnection {
    Connection {
        a: gateway,
        a2b: FaceDef::default().mode(WhatAmI::Router),
        b: north,
        b2a: FaceDef::default().mode(WhatAmI::Router),
    }
    .establish()
}

/// Links two routers of the same region.
fn connect_routers<'a>(a: &'a Harness, b: &'a Harness) -> EstablishedConnection {
    Connection {
        a,
        a2b: FaceDef::default().mode(WhatAmI::Router),
        b,
        b2a: FaceDef::default().mode(WhatAmI::Router),
    }
    .establish()
}

fn scoped_hat<R>(gateway: &Harness, f: impl FnOnce(&RouterHat, &TablesData) -> R) -> R {
    let tables = gateway.gateway.tables.tables.read().unwrap();
    let hat = tables.hats[SCOPED]
        .as_any()
        .downcast_ref::<RouterHat>()
        .unwrap();
    f(hat, &tables.data)
}

fn scoped_hat_mut<R>(gateway: &Harness, f: impl FnOnce(&mut RouterHat, &mut TablesData) -> R) -> R {
    let mut guard = gateway.gateway.tables.tables.write().unwrap();
    let tables = &mut *guard;
    let hat = tables.hats[SCOPED]
        .as_any_mut()
        .downcast_mut::<RouterHat>()
        .unwrap();
    f(hat, &mut tables.data)
}

/// Moves the stopped clock of `gateway` forward by `by`, then runs the re-check of the faces in
/// their grace period, as its timer does when it is due. No topology changes.
fn elapse(gateway: &Harness, by: Duration) {
    scoped_hat_mut(gateway, |hat, tables| {
        hat.advance_clock(by);
        hat.recheck_transit_now(tables);
    });
}

/// For the face of the gateway towards the router of `sn`: the declarations and
/// undeclarations out of scope and the declarations of the router itself out of its scope;
/// whether it is a transit next hop; whether it is still filtered.
fn scope_state(gateway: &Harness, sn: &str) -> ([u64; 3], bool, bool) {
    scoped_hat(gateway, |hat, tables| {
        hat.declaration_scope_of(tables, &zid_of(sn).into())
            .expect("no scoped face")
    })
}

/// The scoped faces that are a transit next hop.
fn transit_faces(gateway: &Harness) -> TransitCounts {
    scoped_hat(gateway, |hat, _| hat.transit_counts())
}

/// Faces in their grace period, faces no longer filtered that are a transit next hop, how many
/// times a face was no longer filtered, how many times a face was a leaf again within its grace
/// period.
fn transit(
    pending: usize,
    violating: usize,
    failed_open: usize,
    transient: usize,
) -> TransitCounts {
    TransitCounts {
        pending,
        violating,
        failed_open,
        transient,
    }
}

fn settle(conns: &mut [&mut EstablishedConnection]) {
    loop {
        if conns
            .iter_mut()
            .all(|c| c.fwd1().is_none() && c.rev_fwd1().is_none())
        {
            break;
        }
    }
}

fn set<const N: usize>(keys: [&str; N]) -> BTreeSet<String> {
    keys.iter().map(|k| k.to_string()).collect()
}

/// The key of a declaration sent with no key expression id.
fn key(wire_expr: &WireExpr) -> String {
    assert_eq!(wire_expr.scope, 0, "{wire_expr:?} uses a key expression id");
    wire_expr.suffix.to_string()
}

/// What a face was sent, by kind. Each list is sorted and keeps duplicates.
#[derive(Debug, Default, PartialEq, Eq)]
struct Sent {
    subscribers: Vec<String>,
    queryables: Vec<String>,
    tokens: Vec<String>,
    undeclared_subscribers: Vec<String>,
    undeclared_queryables: Vec<String>,
    undeclared_tokens: Vec<String>,
    keyexpr_mappings: usize,
}

impl Sent {
    /// Every declaration and every undeclaration of each of `keys`, once per kind.
    fn declared_and_undeclared<const N: usize>(keys: [&str; N]) -> Self {
        Self {
            undeclared_subscribers: sorted(keys),
            undeclared_queryables: sorted(keys),
            undeclared_tokens: sorted(keys),
            ..Self::declared(keys)
        }
    }

    /// Every declaration of each of `keys`, once per kind.
    fn declared<const N: usize>(keys: [&str; N]) -> Self {
        Self {
            subscribers: sorted(keys),
            queryables: sorted(keys),
            tokens: sorted(keys),
            ..Self::default()
        }
    }
}

fn sorted<const N: usize>(keys: [&str; N]) -> Vec<String> {
    let mut keys = keys.iter().map(|k| k.to_string()).collect::<Vec<_>>();
    keys.sort();
    keys
}

/// What `face` was sent since its recorder was last cleared.
fn sent(face: &MockFace) -> Sent {
    sent_by(face, key)
}

/// [`sent`], for a face that may be sent key expression ids that its remote declared.
fn sent_with_ids(face: &MockFace) -> Sent {
    sent_by(face, |w| format!("{}:{}", w.scope, w.suffix))
}

fn sent_by(face: &MockFace, key: impl Fn(&WireExpr) -> String) -> Sent {
    fn sort(mut keys: Vec<String>) -> Vec<String> {
        keys.sort();
        keys
    }

    let r = face.recorder();
    Sent {
        subscribers: sort(r.subscribers().iter().map(|d| key(&d.wire_expr)).collect()),
        queryables: sort(r.queryables().iter().map(|d| key(&d.wire_expr)).collect()),
        tokens: sort(r.tokens().iter().map(|d| key(&d.wire_expr)).collect()),
        undeclared_subscribers: sort(
            r.undeclared_subscribers()
                .iter()
                .map(|u| key(&u.ext_wire_expr.wire_expr))
                .collect(),
        ),
        undeclared_queryables: sort(
            r.undeclared_queryables()
                .iter()
                .map(|u| key(&u.ext_wire_expr.wire_expr))
                .collect(),
        ),
        undeclared_tokens: sort(
            r.undeclared_tokens()
                .iter()
                .map(|u| key(&u.ext_wire_expr.wire_expr))
                .collect(),
        ),
        keyexpr_mappings: r.keyexpr_mappings().len(),
    }
}

/// What was sent between `before` and `after`, two [`sent`] of the same face. (Clearing the
/// recorder of a connected face would make it skip forwarding the next messages.)
fn since(after: Sent, before: &Sent) -> Sent {
    fn minus(mut after: Vec<String>, before: &[String]) -> Vec<String> {
        for key in before {
            if let Some(i) = after.iter().position(|k| k == key) {
                after.remove(i);
            }
        }
        after
    }
    Sent {
        subscribers: minus(after.subscribers, &before.subscribers),
        queryables: minus(after.queryables, &before.queryables),
        tokens: minus(after.tokens, &before.tokens),
        undeclared_subscribers: minus(after.undeclared_subscribers, &before.undeclared_subscribers),
        undeclared_queryables: minus(after.undeclared_queryables, &before.undeclared_queryables),
        undeclared_tokens: minus(after.undeclared_tokens, &before.undeclared_tokens),
        keyexpr_mappings: after.keyexpr_mappings - before.keyexpr_mappings,
    }
}

/// The subscribers, queryables and tokens `h` knows from other routers.
fn known(h: &Harness) -> [BTreeSet<String>; 3] {
    let zid = h.zid().to_string();
    let tables = h.gateway.tables.tables.read().unwrap();
    let from_others = |entities: HashMap<Arc<Resource>, Sources>| {
        entities
            .into_iter()
            .filter(|(_, sources)| {
                // `Sources` only exposes its content through serialization.
                serde_json::to_value(sources).unwrap()["routers"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|router| router.as_str().unwrap() != zid)
            })
            .map(|(res, _)| res.expr().to_string())
            .collect::<BTreeSet<_>>()
    };
    [
        from_others(tables.sourced_subscribers()),
        from_others(tables.sourced_queryables()),
        from_others(tables.sourced_tokens()),
    ]
}

fn known_all<const N: usize>(keys: [&str; N]) -> [BTreeSet<String>; 3] {
    [set(keys), set(keys), set(keys)]
}

/// Declares a subscriber, a queryable and a token on each of `keys`. The entity ids are
/// `first_id` + 3 * (index of the key) + 0, 1 and 2.
fn declare(session: &MockFace, first_id: u32, keys: &[&str]) {
    for (i, key) in keys.iter().enumerate() {
        let id = first_id + 3 * i as u32;
        session.declare_subscriber(None, id, key.to_string());
        session.declare_queryable(None, id + 1, key.to_string(), QueryableInfoType::DEFAULT);
        session.declare_token(None, id + 2, key.to_string());
    }
}

/// Undeclares what [`declare`] declared.
fn undeclare(session: &MockFace, first_id: u32, count: usize) {
    for i in 0..count {
        let id = first_id + 3 * i as u32;
        session.undeclare_subscriber(id);
        session.undeclare_queryable(id + 1);
        session.undeclare_token(id + 2);
    }
}

fn own(sn: &str) -> String {
    format!("{sn}/svc/default/pub/x")
}

/// Matches every scope.
const WILD: &str = "*/svc/default/pub/x";
/// Not included in the scoped key expression.
const UNSCOPED: &str = "fleet/status";

/// Declarations of other regions (here: sessions of the gateway) enter the scoped subregion as
/// declarations of the gateway itself.
#[test]
fn declarations_of_the_gateway_are_scoped() {
    try_init_tracing_subscriber();
    let (own_a, own_b) = (own(SN_A), own(SN_B));
    let (own_a, own_b) = (own_a.as_str(), own_b.as_str());

    let g = gateway(GATEWAY);
    let (a, b, u) = (
        router(Some(zid_of(SN_A))),
        router(Some(zid_of(SN_B))),
        router(None),
    );
    let (mut g_a, mut g_b, mut g_u) = (
        connect_south(&g, &a),
        connect_south(&g, &b),
        connect_south(&g, &u),
    );
    settle(&mut [&mut g_a, &mut g_b, &mut g_u]);

    let app = g.new_session();
    let keys = [own_a, own_b, WILD, UNSCOPED];
    declare(&app, 1, &keys);
    settle(&mut [&mut g_a, &mut g_b, &mut g_u]);

    assert_eq!(sent(&g_a.a2b), Sent::declared([own_a, WILD, UNSCOPED]));
    assert_eq!(sent(&g_b.a2b), Sent::declared([own_b, WILD, UNSCOPED]));
    // A router whose Zenoh ID yields no scope is not filtered.
    assert_eq!(sent(&g_u.a2b), Sent::declared(keys));
    assert_eq!(known(&a), known_all([own_a, WILD, UNSCOPED]));
    assert_eq!(known(&b), known_all([own_b, WILD, UNSCOPED]));
    assert_eq!(known(&u), known_all(keys));

    undeclare(&app, 1, keys.len());
    settle(&mut [&mut g_a, &mut g_b, &mut g_u]);

    // No undeclaration is sent for a declaration that was withheld.
    assert_eq!(
        sent(&g_a.a2b),
        Sent::declared_and_undeclared([own_a, WILD, UNSCOPED])
    );
    assert_eq!(
        sent(&g_b.a2b),
        Sent::declared_and_undeclared([own_b, WILD, UNSCOPED])
    );
    assert_eq!(sent(&g_u.a2b), Sent::declared_and_undeclared(keys));
    for h in [&a, &b, &u] {
        assert_eq!(known(h), known_all([]));
    }
}

/// Declarations of a router of the scoped subregion are forwarded to the other routers of the
/// subregion along the tree of their source.
#[test]
fn declarations_forwarded_within_the_subregion_are_scoped() {
    try_init_tracing_subscriber();
    let (own_a, own_b) = (own(SN_A), own(SN_B));
    let (own_a, own_b) = (own_a.as_str(), own_b.as_str());

    let g = gateway(GATEWAY);
    let (a, b, u) = (
        router(Some(zid_of(SN_A))),
        router(Some(zid_of(SN_B))),
        router(None),
    );
    let (mut g_a, mut g_b, mut g_u) = (
        connect_south(&g, &a),
        connect_south(&g, &b),
        connect_south(&g, &u),
    );
    settle(&mut [&mut g_a, &mut g_b, &mut g_u]);

    let robot = a.new_session();
    // A router may declare on the scope of another router: it reaches that router only.
    let keys = [own_a, own_b, WILD, UNSCOPED];
    declare(&robot, 1, &keys);
    settle(&mut [&mut g_a, &mut g_b, &mut g_u]);

    // The gateway knows everything: what it receives is not filtered.
    assert_eq!(known(&g), known_all(keys));
    assert_eq!(sent(&g_a.a2b), Sent::default());
    assert_eq!(sent(&g_b.a2b), Sent::declared([own_b, WILD, UNSCOPED]));
    assert_eq!(sent(&g_u.a2b), Sent::declared(keys));
    assert_eq!(known(&b), known_all([own_b, WILD, UNSCOPED]));
    assert_eq!(known(&u), known_all(keys));

    undeclare(&robot, 1, keys.len());
    settle(&mut [&mut g_a, &mut g_b, &mut g_u]);

    assert_eq!(sent(&g_a.a2b), Sent::default());
    assert_eq!(
        sent(&g_b.a2b),
        Sent::declared_and_undeclared([own_b, WILD, UNSCOPED])
    );
    assert_eq!(sent(&g_u.a2b), Sent::declared_and_undeclared(keys));
    for h in [&g, &b, &u] {
        assert_eq!(known(h), known_all([]));
    }
}

/// The declarations that exist when a router connects are replayed to it once it becomes a
/// child in the trees of the subregion; a router that reconnects gets the current ones again.
#[test]
fn declarations_replayed_to_a_new_router_are_scoped() {
    try_init_tracing_subscriber();
    let (own_a, own_b) = (own(SN_A), own(SN_B));
    let (own_a, own_b) = (own_a.as_str(), own_b.as_str());
    let later_b = format!("{SN_B}/svc/default/pub/later");
    let later_a = format!("{SN_A}/svc/default/pub/later");
    let (later_a, later_b) = (later_a.as_str(), later_b.as_str());

    let g = gateway(GATEWAY);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    let mut g_a = connect_south(&g, &a);
    settle(&mut [&mut g_a]);

    // Declarations of the gateway and of A, all made before B connects.
    let app = g.new_session();
    let robot = a.new_session();
    let app_keys = [own_a, own_b, UNSCOPED];
    declare(&app, 1, &app_keys);
    declare(&robot, 1, &[own_a, WILD]);
    settle(&mut [&mut g_a]);

    let mut g_b = connect_south(&g, &b);
    settle(&mut [&mut g_a, &mut g_b]);

    assert_eq!(sent(&g_b.a2b), Sent::declared([own_b, UNSCOPED, WILD]));
    assert_eq!(known(&b), known_all([own_b, UNSCOPED, WILD]));

    // B loses its link. Meanwhile declarations come and go.
    g_b.disconnect();
    assert_eq!(known(&b), known_all([]));
    undeclare(&app, 1, 2); // `own_a` and `own_b`
    declare(&app, 100, &[later_a, later_b]);
    settle(&mut [&mut g_a]);

    let mut g_b = connect_south(&g, &b);
    settle(&mut [&mut g_a, &mut g_b]);

    assert_eq!(sent(&g_b.a2b), Sent::declared([later_b, UNSCOPED, WILD]));
    assert_eq!(known(&b), known_all([later_b, UNSCOPED, WILD]));
    assert_eq!(
        known(&a),
        known_all([later_a, UNSCOPED]),
        "A should know the declarations of the gateway on its scope"
    );

    // Everything goes away: nothing is left on either side.
    undeclare(&app, 100, 2);
    undeclare(&app, 1 + 3 * 2, 1); // `UNSCOPED`
    undeclare(&robot, 1, 2);
    settle(&mut [&mut g_a, &mut g_b]);

    assert_eq!(
        sent(&g_b.a2b),
        Sent::declared_and_undeclared([later_b, UNSCOPED, WILD])
    );
    for h in [&g, &a, &b] {
        assert_eq!(known(h), known_all([]));
    }
}

/// Many declare/undeclare cycles, with the same keys declared on both sides, leave nothing
/// behind, and each face is sent as many undeclarations as declarations.
#[test]
fn churn_leaves_no_stale_declaration() {
    try_init_tracing_subscriber();
    let (own_a, own_b) = (own(SN_A), own(SN_B));
    let (own_a, own_b) = (own_a.as_str(), own_b.as_str());

    let g = gateway(GATEWAY);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    const CYCLES: usize = 20;

    let (mut g_a, mut g_b) = (connect_south(&g, &a), connect_south(&g, &b));
    settle(&mut [&mut g_a, &mut g_b]);

    let app = g.new_session();
    let robot = a.new_session();
    let keys = [own_a, own_b, WILD, UNSCOPED];
    for _ in 0..CYCLES {
        declare(&app, 1, &keys);
        declare(&robot, 1, &keys);
        settle(&mut [&mut g_a, &mut g_b]);
        undeclare(&app, 1, keys.len());
        settle(&mut [&mut g_a, &mut g_b]);
        undeclare(&robot, 1, keys.len());
        settle(&mut [&mut g_a, &mut g_b]);
    }

    for h in [&g, &a, &b] {
        assert_eq!(known(h), known_all([]));
    }
    // Per cycle and per kind, A is sent the gateway's declarations on its scope, on the wildcard
    // and on the unscoped key; B is sent those of the gateway and those of A. Keys are not
    // compared here: the gateway may name a key by an id that the router itself declared.
    for (face, per_cycle) in [(&g_a.a2b, 3), (&g_b.a2b, 6)] {
        let r = face.recorder();
        let n = CYCLES * per_cycle;
        assert_eq!(
            [
                r.subscribers().len(),
                r.queryables().len(),
                r.tokens().len(),
                r.undeclared_subscribers().len(),
                r.undeclared_queryables().len(),
                r.undeclared_tokens().len(),
            ],
            [n; 6]
        );
        assert!(
            r.keyexpr_mappings().is_empty(),
            "the gateway allocated a key expression id"
        );
    }
}

/// Without the option, every declaration reaches every router of the subregion.
#[test]
fn declarations_are_not_scoped_by_default() {
    try_init_tracing_subscriber();
    let (own_a, own_b) = (own(SN_A), own(SN_B));
    let (own_a, own_b) = (own_a.as_str(), own_b.as_str());

    let g = gateway(GATEWAY_WITHOUT_SCOPE);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    let (mut g_a, mut g_b) = (connect_south(&g, &a), connect_south(&g, &b));
    settle(&mut [&mut g_a, &mut g_b]);

    let app = g.new_session();
    let robot = a.new_session();
    let keys = [own_a, own_b, WILD, UNSCOPED];
    declare(&app, 1, &keys);
    settle(&mut [&mut g_a, &mut g_b]);
    assert_eq!(sent(&g_a.a2b), Sent::declared(keys));
    assert_eq!(sent(&g_b.a2b), Sent::declared(keys));

    undeclare(&app, 1, keys.len());
    declare(&robot, 1, &keys);
    settle(&mut [&mut g_a, &mut g_b]);
    assert_eq!(known(&b), known_all(keys));
}

/// Only the subregion that has the option is filtered: a north router gets every declaration,
/// even if its Zenoh ID yields a scope.
#[test]
fn declarations_to_other_regions_are_not_scoped() {
    try_init_tracing_subscriber();
    let (own_a, own_b) = (own(SN_A), own(SN_B));
    let (own_a, own_b) = (own_a.as_str(), own_b.as_str());

    let g = gateway(GATEWAY);
    let (a, n) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_N))));
    let (mut g_a, mut g_n) = (connect_south(&g, &a), connect_north(&g, &n));
    settle(&mut [&mut g_a, &mut g_n]);

    let app = g.new_session();
    let robot = a.new_session();
    declare(&app, 1, &[own_a, own_b]);
    declare(&robot, 1, &[UNSCOPED]);
    settle(&mut [&mut g_a, &mut g_n]);

    assert_eq!(known(&n), known_all([own_a, own_b, UNSCOPED]));
    assert_eq!(known(&a), known_all([own_a]));

    // Declarations of the north region enter the subregion scoped.
    let north_app = n.new_session();
    declare(&north_app, 1, &[own_b, WILD]);
    settle(&mut [&mut g_a, &mut g_n]);
    assert_eq!(known(&a), known_all([own_a, WILD]));
}

/// Data, queries and liveliness between the gateway side and a router of the scoped subregion
/// work in both directions: they rely on what that router declared to the gateway, which is not
/// filtered, and on the gateway's declarations on its scope, which pass.
#[test]
fn data_queries_and_liveliness_flow_within_a_scope() {
    try_init_tracing_subscriber();

    let g = gateway(GATEWAY);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    let (mut g_a, mut g_b) = (connect_south(&g, &a), connect_south(&g, &b));
    settle(&mut [&mut g_a, &mut g_b]);

    let app = g.new_session();
    let robot_a = a.new_session();
    let robot_b = b.new_session();

    let down = format!("{SN_A}/svc/default/sub/cmd");
    let up = format!("{SN_A}/svc/default/pub/state");
    let robot_rpc = format!("{SN_A}/svc/default/rpc/robot");
    let app_rpc = format!("{SN_A}/svc/default/rpc/app");
    let robot_alive = format!("{SN_A}/svc/default/alive/robot");
    let app_alive = format!("{SN_A}/svc/default/alive/app");

    robot_a.declare_subscriber(None, 1, down.clone());
    robot_a.declare_queryable(None, 2, robot_rpc.clone(), QueryableInfoType::DEFAULT);
    robot_a.declare_token(None, 3, robot_alive.clone());
    app.declare_subscriber(None, 1, up.clone());
    app.declare_queryable(None, 2, app_rpc.clone(), QueryableInfoType::DEFAULT);
    app.declare_token(None, 3, app_alive.clone());
    // B listens to everything it is allowed to hear about.
    robot_b.declare_subscriber(None, 1, format!("{SN_B}/svc/**"));
    settle(&mut [&mut g_a, &mut g_b]);

    // Liveliness, both directions.
    assert!(known(&g)[2].contains(&robot_alive));
    assert!(known(&a)[2].contains(&app_alive));
    assert_eq!(known(&b), known_all([]));

    // Data: gateway side to the robot, and back.
    app.put(down.clone(), vec![1]);
    settle(&mut [&mut g_a, &mut g_b]);
    assert_eq!(robot_a.recorder().pushes().len(), 1);

    robot_a.put(up.clone(), vec![2]);
    settle(&mut [&mut g_a, &mut g_b]);
    assert_eq!(app.recorder().pushes().len(), 1);

    // Queries: gateway side to the robot, and back.
    app.query(1, robot_rpc.clone());
    settle(&mut [&mut g_a, &mut g_b]);
    assert_eq!(robot_a.recorder().requests().len(), 1);

    robot_a.query(1, app_rpc.clone());
    settle(&mut [&mut g_a, &mut g_b]);
    assert_eq!(app.recorder().requests().len(), 1);

    // Nothing of the above reached the other robot.
    assert_eq!(robot_b.recorder().pushes().len(), 0);
    assert_eq!(robot_b.recorder().requests().len(), 0);
    assert_eq!(g_b.a2b.recorder().pushes().len(), 0);
    assert_eq!(g_b.a2b.recorder().requests().len(), 0);
    assert_eq!(sent(&g_b.a2b), Sent::default());
}

/// G—A—B, all declarations made once the graph has converged. A is the next hop towards B,
/// which is out of A's scope. The gateway detects it; at the end of the grace period it stops
/// filtering A and replays to it what it withheld, so B gets the declarations on its own scope
/// through A. Once the topology is a star again the detection clears; A stays unfiltered until
/// it reconnects.
#[test]
fn a_transit_router_is_detected_and_no_longer_filtered() {
    try_init_tracing_subscriber();
    let g = gateway(GATEWAY);
    let a = router(Some(zid_of(SN_A)));
    let b = router(Some(zid_of(SN_B)));
    let mut ga = connect_south(&g, &a);
    let mut ab = connect_routers(&a, &b);
    settle(&mut [&mut ga, &mut ab]);
    let app = g.new_session();
    let k = own(SN_B);
    declare(&app, 1, &[&k]); // subscriber + queryable + token
    settle(&mut [&mut ga, &mut ab]);

    // The detection. During the grace period A is still filtered, so B misses k.
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 0));
    assert_eq!(scope_state(&g, SN_A), ([3, 0, 0], true, true));
    assert_eq!(known(&b), known_all([]));

    // The end of the grace period, with no topology change.
    elapse(&g, GRACE);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(known(&b), known_all([k.as_str()]));
    assert_eq!(transit_faces(&g), transit(0, 1, 1, 0));
    assert_eq!(scope_state(&g, SN_A), ([3, 0, 0], true, false));

    // A declaration withheld while A was a leaf is replayed when A becomes a transit router.
    let own_a = own(SN_A);
    let later_b = format!("{SN_B}/svc/default/pub/later");
    let star_b = format!("{SN_B}/svc/default/pub/star");
    ab.disconnect();
    settle(&mut [&mut ga]);
    assert_eq!(transit_faces(&g), transit(0, 0, 1, 0), "a leaf again");
    assert!(
        !scope_state(&g, SN_A).2,
        "A stays unfiltered until it reconnects"
    );
    ga.disconnect();
    let mut ga = connect_south(&g, &a);
    settle(&mut [&mut ga]);
    assert!(scope_state(&g, SN_A).2, "a new face of A is filtered");
    declare(&app, 100, &[&later_b]);
    settle(&mut [&mut ga]);
    assert_eq!(known(&a), known_all([]));
    assert_eq!(
        scope_state(&g, SN_A).0,
        [6, 0, 0],
        "k and later_b withheld from A"
    );
    let before = sent(&ga.a2b);

    let mut ab = connect_routers(&a, &b);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(1, 0, 1, 0));
    assert_eq!(since(sent(&ga.a2b), &before), Sent::default());
    elapse(&g, GRACE);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(0, 1, 2, 0));
    assert_eq!(
        since(sent(&ga.a2b), &before),
        Sent::declared([k.as_str(), later_b.as_str()])
    );
    assert_eq!(known(&b), known_all([k.as_str(), later_b.as_str()]));

    // Star again: B connects to the gateway and the link between A and B goes away.
    let mut gb = connect_south(&g, &b);
    ab.disconnect();
    settle(&mut [&mut ga, &mut gb]);
    assert_eq!(transit_faces(&g), transit(0, 0, 2, 0));
    assert!(!scope_state(&g, SN_A).1);
    assert!(scope_state(&g, SN_B).2, "B is filtered");
    declare(&app, 200, &[&own_a, &star_b]);
    settle(&mut [&mut ga, &mut gb]);
    assert_eq!(
        known(&a),
        known_all([k.as_str(), later_b.as_str(), &own_a, &star_b]),
        "A is unfiltered"
    );
    assert_eq!(
        known(&b),
        known_all([k.as_str(), later_b.as_str(), &star_b])
    );

    // Undeclarations reach both and leave nothing behind. A is only sent undeclarations of what
    // it was sent.
    undeclare(&app, 1, 1);
    undeclare(&app, 100, 1);
    undeclare(&app, 200, 2);
    settle(&mut [&mut ga, &mut gb]);
    for h in [&a, &b] {
        assert_eq!(known(h), known_all([]));
    }
    let to_a = sent(&ga.a2b);
    for (declared, undeclared) in [
        (&to_a.subscribers, &to_a.undeclared_subscribers),
        (&to_a.queryables, &to_a.undeclared_queryables),
        (&to_a.tokens, &to_a.undeclared_tokens),
    ] {
        assert!(undeclared.iter().all(|k| declared.contains(k)), "{to_a:?}");
    }
}

/// G—A, G—B and A—B. A is a leaf until the link between G and B is lost; then B's publications
/// go through A, which must know the gateway's subscriber on B's scope. It gets it at the end of
/// the grace period: until then, B's publications do not reach the gateway.
#[test]
fn a_router_that_becomes_transit_after_a_link_loss() {
    try_init_tracing_subscriber();
    let g = gateway(GATEWAY);
    let a = router(Some(zid_of(SN_A)));
    let b = router(Some(zid_of(SN_B)));
    let mut ga = connect_south(&g, &a);
    let mut gb = connect_south(&g, &b);
    let mut ab = connect_routers(&a, &b);
    settle(&mut [&mut ga, &mut gb, &mut ab]);
    assert_eq!(transit_faces(&g), transit(0, 0, 0, 0));

    let app = g.new_session();
    let rb = b.new_session();
    let k = own(SN_B);
    app.declare_subscriber(None, 1, k.clone());
    settle(&mut [&mut ga, &mut gb, &mut ab]);
    rb.put(k.clone(), vec![1]);
    settle(&mut [&mut ga, &mut gb, &mut ab]);
    assert_eq!(app.recorder().pushes().len(), 1);
    assert_eq!(scope_state(&g, SN_A), ([1, 0, 0], false, true));
    gb.disconnect();
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 0));
    assert_eq!(scope_state(&g, SN_A), ([1, 0, 0], true, true));
    rb.put(k.clone(), vec![2]);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(
        app.recorder().pushes().len(),
        1,
        "lost during the grace period"
    );

    elapse(&g, GRACE);
    settle(&mut [&mut ga, &mut ab]);
    rb.put(k.clone(), vec![3]);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(app.recorder().pushes().len(), 2);
    assert_eq!(transit_faces(&g), transit(0, 1, 1, 0));
    assert_eq!(scope_state(&g, SN_A), ([1, 0, 0], true, false));

    // Star again.
    let mut gb = connect_south(&g, &b);
    ab.disconnect();
    settle(&mut [&mut ga, &mut gb]);
    assert_eq!(transit_faces(&g), transit(0, 0, 1, 0));
    assert_eq!(scope_state(&g, SN_A), ([1, 0, 0], false, false));
    rb.put(k.clone(), vec![4]);
    settle(&mut [&mut ga, &mut gb]);
    assert_eq!(app.recorder().pushes().len(), 3);

    app.undeclare_subscriber(1);
    settle(&mut [&mut ga, &mut gb]);
    for h in [&a, &b] {
        assert_eq!(known(h), known_all([]));
    }
}

/// The same declarations and undeclarations, in the given mode: what each of A and B was sent,
/// and the counts of their faces. A declares on B's scope, which the identity check reports; B
/// only declares on its own scope.
fn mode_scenario(conf: &str) -> ([Sent; 2], Option<[[u64; 3]; 2]>) {
    let (own_a, own_b) = (own(SN_A), own(SN_B));
    let (own_a, own_b) = (own_a.as_str(), own_b.as_str());

    let g = gateway(conf);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    let (mut ga, mut gb) = (connect_south(&g, &a), connect_south(&g, &b));
    settle(&mut [&mut ga, &mut gb]);

    let app = g.new_session();
    let (robot_a, robot_b) = (a.new_session(), b.new_session());
    declare(&app, 1, &[own_a, own_b, WILD, UNSCOPED]);
    declare(&robot_a, 1, &[own_a, own_b, WILD, UNSCOPED]);
    declare(&robot_b, 1, &[own_b, WILD, UNSCOPED]);
    settle(&mut [&mut ga, &mut gb]);
    undeclare(&app, 1, 4);
    undeclare(&robot_a, 1, 4);
    undeclare(&robot_b, 1, 3);
    settle(&mut [&mut ga, &mut gb]);
    for h in [&g, &a, &b] {
        assert_eq!(known(h), known_all([]));
    }

    let counts = scoped_hat(&g, |hat, tables| {
        let a = hat.declaration_scope_of(tables, &zid_of(SN_A).into())?;
        let b = hat.declaration_scope_of(tables, &zid_of(SN_B).into())?;
        Some([a.0, b.0])
    });
    ([sent_with_ids(&ga.a2b), sent_with_ids(&gb.a2b)], counts)
}

fn messages(sent: &Sent) -> u64 {
    [
        &sent.subscribers,
        &sent.queryables,
        &sent.tokens,
        &sent.undeclared_subscribers,
        &sent.undeclared_queryables,
        &sent.undeclared_tokens,
    ]
    .iter()
    .map(|v| v.len() as u64)
    .sum()
}

/// Observe mode sends what a gateway without the option sends, and counts exactly what enforce
/// mode withholds. The identity check reports A, which declares on B's scope, and not B, in
/// both modes.
#[test]
fn observe_mode_withholds_nothing_and_counts_what_enforce_mode_withholds() {
    try_init_tracing_subscriber();
    let (enforced, enforce_counts) = mode_scenario(GATEWAY);
    let (observed, observe_counts) = mode_scenario(GATEWAY_OBSERVE);
    let (unscoped, no_counts) = mode_scenario(GATEWAY_WITHOUT_SCOPE);

    assert_eq!(observed, unscoped);
    assert_eq!(no_counts, None);
    // A: the gateway's and B's declarations on B's scope, three kinds each; B: the gateway's
    // and A's on A's scope. A declared three entities on B's scope.
    assert_eq!(enforce_counts, Some([[6, 6, 3], [6, 6, 0]]));
    assert_eq!(observe_counts, enforce_counts);
    for i in 0..2 {
        let [declarations, undeclarations, _] = enforce_counts.unwrap()[i];
        assert_eq!(
            messages(&observed[i]),
            messages(&enforced[i]) + declarations + undeclarations
        );
    }
}

/// The mode is read when the router starts: changing it in the configuration of a running
/// router has no effect.
#[test]
fn the_mode_is_read_at_start_only() {
    try_init_tracing_subscriber();
    let own_b = own(SN_B);
    let g = gateway(GATEWAY);
    let a = router(Some(zid_of(SN_A)));
    let mut ga = connect_south(&g, &a);
    settle(&mut [&mut ga]);

    {
        let mut config = g.runtime().config().lock();
        let Some(GatewaySouthConf::Custom(south)) = config.gateway.south.as_mut() else {
            panic!("custom gateway.south expected");
        };
        south[0].declaration_scope.as_mut().unwrap().mode = DeclarationScopeMode::Observe;
    }
    g.runtime().update_network().unwrap();

    let app = g.new_session();
    declare(&app, 1, &[&own_b]);
    settle(&mut [&mut ga]);
    assert_eq!(sent(&ga.a2b), Sent::default());
    assert_eq!(scope_state(&g, SN_A).0, [3, 0, 0]);
}

/// G—A, then A—B, past the grace period: what the gateway sends to A across the transit
/// change, with the given option.
fn transit_change_output(conf: &str) -> (Sent, TransitCounts) {
    let g = gateway(conf);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    let mut ga = connect_south(&g, &a);
    settle(&mut [&mut ga]);
    let app = g.new_session();
    declare(&app, 1, &[&own(SN_B)]);
    settle(&mut [&mut ga]);
    let mut ab = connect_routers(&a, &b);
    settle(&mut [&mut ga, &mut ab]);
    elapse(&g, GRACE);
    settle(&mut [&mut ga, &mut ab]);
    (sent(&ga.a2b), transit_faces(&g))
}

/// Observe mode withheld nothing, so a face that becomes a transit next hop is not replayed
/// anything: the output is the same as without the option. The detection still happens.
#[test]
fn observe_mode_output_is_unchanged_across_a_transit_change() {
    try_init_tracing_subscriber();
    let (observed, transit) = transit_change_output(GATEWAY_OBSERVE);
    let (unscoped, _) = transit_change_output(GATEWAY_WITHOUT_SCOPE);
    assert_eq!(observed, unscoped);
    assert_eq!(observed, Sent::declared([own(SN_B).as_str()]));
    assert_eq!(transit, self::transit(0, 1, 1, 0));
}

/// A face closed while it is a transit next hop and no longer filtered: the detection clears; the
/// router reconnects into the same topology and its new face starts a new grace period, filtered,
/// and gets the replay at its end.
#[test]
fn a_face_closed_while_transit_is_detected_again_on_reconnect() {
    try_init_tracing_subscriber();
    let g = gateway(GATEWAY);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    let mut ga = connect_south(&g, &a);
    let mut ab = connect_routers(&a, &b);
    settle(&mut [&mut ga, &mut ab]);
    let app = g.new_session();
    let k = own(SN_B);
    declare(&app, 1, &[&k]);
    settle(&mut [&mut ga, &mut ab]);
    elapse(&g, GRACE);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(0, 1, 1, 0));

    ga.disconnect();
    settle(&mut [&mut ab]);
    assert_eq!(transit_faces(&g), transit(0, 0, 1, 0));
    let mut ga = connect_south(&g, &a);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(1, 0, 1, 0));
    assert_eq!(scope_state(&g, SN_A), ([3, 0, 0], true, true));
    assert_eq!(sent(&ga.a2b), Sent::default());
    elapse(&g, GRACE);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(0, 1, 2, 0));
    assert_eq!(scope_state(&g, SN_A), ([3, 0, 0], true, false));
    assert_eq!(sent(&ga.a2b), Sent::declared([k.as_str()]));
    assert_eq!(known(&b), known_all([k.as_str()]));
}

/// The replay to a face that becomes transit covers every source: the gateway (its sessions)
/// and another router of the subregion, with the current queryable information, including
/// keys the face already had. Withdrawing everything afterwards leaves nothing behind.
#[test]
fn the_replay_to_a_transit_router_covers_every_source() {
    try_init_tracing_subscriber();
    let g = gateway(GATEWAY);
    let (a, b, u) = (
        router(Some(zid_of(SN_A))),
        router(Some(zid_of(SN_B))),
        router(None),
    );
    let (mut ga, mut gu) = (connect_south(&g, &a), connect_south(&g, &u));
    settle(&mut [&mut ga, &mut gu]);
    let (app, robot_u) = (g.new_session(), u.new_session());
    let (own_a, own_b, from_u) = (own(SN_A), own(SN_B), format!("{SN_B}/svc/default/pub/u"));
    declare(&app, 1, &[&own_a, &own_b]);
    declare(&robot_u, 1, &[&from_u]);
    let complete = QueryableInfoType {
        complete: true,
        distance: 0,
    };
    app.declare_queryable(None, 5, own_b.clone(), complete); // info update
    settle(&mut [&mut ga, &mut gu]);
    assert_eq!(known(&a), known_all([own_a.as_str()]));
    let before = sent(&ga.a2b);

    let mut ab = connect_routers(&a, &b);
    settle(&mut [&mut ga, &mut gu, &mut ab]);
    elapse(&g, GRACE);
    settle(&mut [&mut ga, &mut gu, &mut ab]);
    let all = [own_a.as_str(), own_b.as_str(), from_u.as_str()];
    assert_eq!(since(sent(&ga.a2b), &before), Sent::declared(all));
    assert!(ga
        .a2b
        .recorder()
        .queryables()
        .iter()
        .any(|q| { q.wire_expr.suffix == own_b.as_str() && q.ext_info.complete }));
    assert_eq!(known(&a), known_all(all));
    assert_eq!(known(&b), known_all(all));

    undeclare(&app, 1, 2);
    undeclare(&robot_u, 1, 1);
    settle(&mut [&mut ga, &mut gu, &mut ab]);
    for h in [&g, &a, &b, &u] {
        assert_eq!(known(h), known_all([]));
    }
}

/// A—B appears, then goes away before the end of the grace period: A is never unfiltered, nothing
/// is replayed to it, and it is still filtered afterwards.
#[test]
fn a_transit_state_shorter_than_the_grace_period_is_ignored() {
    try_init_tracing_subscriber();
    let g = gateway(GATEWAY);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    let mut ga = connect_south(&g, &a);
    settle(&mut [&mut ga]);
    let app = g.new_session();
    declare(&app, 1, &[&own(SN_B)]);
    settle(&mut [&mut ga]);
    let before = sent(&ga.a2b);

    let mut ab = connect_routers(&a, &b);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 0));
    elapse(&g, GRACE - Duration::from_millis(1));
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 0));
    ab.disconnect();
    settle(&mut [&mut ga]);
    assert_eq!(transit_faces(&g), transit(0, 0, 0, 1));

    // Well past the end of the grace period that was cut short.
    elapse(&g, GRACE);
    settle(&mut [&mut ga]);
    assert_eq!(transit_faces(&g), transit(0, 0, 0, 1));
    assert_eq!(scope_state(&g, SN_A), ([3, 0, 0], false, true));
    assert_eq!(since(sent(&ga.a2b), &before), Sent::default());
    assert_eq!(known(&a), known_all([]));

    let later = format!("{SN_B}/svc/default/pub/later");
    declare(&app, 100, &[&later]);
    settle(&mut [&mut ga]);
    assert_eq!(since(sent(&ga.a2b), &before), Sent::default());
    assert_eq!(scope_state(&g, SN_A).0, [6, 0, 0]);
}

/// The shape seen when a gateway restarts: A was linked to the previous gateway `old`, with B
/// behind it, and connects to this gateway before it is done removing that link. Its first link
/// state makes it the next hop towards `old`, which has no scope and is reachable only through A,
/// and towards B, of another scope; its next link state withdraws them. A is never unfiltered.
#[test]
fn a_router_announcing_its_previous_links_when_it_connects_stays_filtered() {
    try_init_tracing_subscriber();
    let (own_a, own_b) = (own(SN_A), own(SN_B));
    let g = gateway(GATEWAY);
    let old = router(None);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    let mut old_a = connect_routers(&old, &a);
    let mut old_b = connect_routers(&old, &b);
    settle(&mut [&mut old_a, &mut old_b]);
    let app = g.new_session();
    declare(&app, 1, &[&own_a, &own_b]);

    let mut ga = connect_south(&g, &a);
    settle(&mut [&mut ga, &mut old_a, &mut old_b]);
    let behind_a = scoped_hat(&g, |hat, _| {
        [old.zid(), b.zid()].map(|zid| hat.net().get_idx(&zid).is_some())
    });
    assert_eq!(behind_a, [true, true], "the gateway sees old and B");
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 0));
    assert_eq!(scope_state(&g, SN_A), ([3, 0, 0], true, true));

    // The old link may only go away when its lease (20 s) expires.
    elapse(&g, Duration::from_secs(20));
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 0));
    old_a.disconnect();
    settle(&mut [&mut ga, &mut old_b]);
    assert_eq!(transit_faces(&g), transit(0, 0, 0, 1));
    elapse(&g, GRACE);
    settle(&mut [&mut ga]);
    assert_eq!(transit_faces(&g), transit(0, 0, 0, 1));
    assert_eq!(scope_state(&g, SN_A), ([3, 0, 0], false, true));
    assert_eq!(sent(&ga.a2b), Sent::declared([own_a.as_str()]));
    assert_eq!(known(&a), known_all([own_a.as_str()]));

    // B reconnects to this gateway too: a star, each router with its own scope.
    old_b.disconnect();
    let mut gb = connect_south(&g, &b);
    settle(&mut [&mut ga, &mut gb]);
    assert_eq!(transit_faces(&g), transit(0, 0, 0, 1));
    assert_eq!(sent(&gb.a2b), Sent::declared([own_b.as_str()]));
}

/// The grace period of the tests that use the timer of the re-check.
const TIMER_GRACE: Duration = Duration::from_millis(300);

/// Waits for the re-check timer of `gateway` to stop filtering the face of `sn`.
fn wait_unfiltered(gateway: &Harness, sn: &str) {
    let start = Instant::now();
    while scope_state(gateway, sn).2 {
        assert!(
            start.elapsed() < Duration::from_millis(1200),
            "the face of {sn} is still filtered"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A transit state that persists, with no topology change once it started: the re-check timer
/// stops filtering A at the end of the grace period and replays to it, exactly once, everything
/// withheld from it, including a declaration made during the grace period.
#[test]
fn a_persistent_transit_state_fails_open_at_the_end_of_the_grace_period() {
    try_init_tracing_subscriber();
    let g = gateway_with_timer(GATEWAY, TIMER_GRACE);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    let mut ga = connect_south(&g, &a);
    settle(&mut [&mut ga]);
    let app = g.new_session();
    let (k, during) = (own(SN_B), format!("{SN_B}/svc/default/pub/during"));
    declare(&app, 1, &[&k]);
    settle(&mut [&mut ga]);
    let before = sent(&ga.a2b);

    let start = Instant::now();
    let mut ab = connect_routers(&a, &b);
    settle(&mut [&mut ga, &mut ab]);
    declare(&app, 100, &[&during]);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 0));
    assert_eq!(scope_state(&g, SN_A), ([6, 0, 0], true, true));
    assert_eq!(since(sent(&ga.a2b), &before), Sent::default());
    assert!(scoped_hat(&g, |hat, _| hat.transit_recheck_due()).is_some());

    // No topology change from here on.
    wait_unfiltered(&g, SN_A);
    assert!(start.elapsed() >= TIMER_GRACE);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(0, 1, 1, 0));
    assert_eq!(scoped_hat(&g, |hat, _| hat.transit_recheck_due()), None);
    let replay = Sent::declared([k.as_str(), during.as_str()]);
    assert_eq!(since(sent(&ga.a2b), &before), replay);
    assert_eq!(known(&b), known_all([k.as_str(), during.as_str()]));

    std::thread::sleep(Duration::from_millis(100));
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(since(sent(&ga.a2b), &before), replay, "replayed once");
}

/// A persistent transit state in observe mode: nothing was withheld, so nothing is replayed at
/// the end of the grace period.
#[test]
fn a_persistent_transit_state_replays_nothing_in_observe_mode() {
    try_init_tracing_subscriber();
    let g = gateway(GATEWAY_OBSERVE);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    let mut ga = connect_south(&g, &a);
    settle(&mut [&mut ga]);
    let app = g.new_session();
    let (k, during) = (own(SN_B), format!("{SN_B}/svc/default/pub/during"));
    declare(&app, 1, &[&k]);
    settle(&mut [&mut ga]);

    let mut ab = connect_routers(&a, &b);
    settle(&mut [&mut ga, &mut ab]);
    elapse(&g, GRACE / 2);
    declare(&app, 100, &[&during]);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 0));
    assert_eq!(scope_state(&g, SN_A), ([6, 0, 0], true, true));
    let before = sent(&ga.a2b);
    assert_eq!(before, Sent::declared([k.as_str(), during.as_str()]));

    elapse(&g, GRACE / 2);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(0, 1, 1, 0));
    assert_eq!(scope_state(&g, SN_A), ([6, 0, 0], true, false));
    assert_eq!(since(sent(&ga.a2b), &before), Sent::default());
}

/// Leaf, transit, leaf, transit: each transit state starts a new, full grace period.
#[test]
fn a_router_flapping_between_leaf_and_transit_restarts_its_grace_period() {
    try_init_tracing_subscriber();
    let g = gateway(GATEWAY);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    let mut ga = connect_south(&g, &a);
    settle(&mut [&mut ga]);
    let app = g.new_session();
    let k = own(SN_B);
    declare(&app, 1, &[&k]);
    settle(&mut [&mut ga]);
    let before = sent(&ga.a2b);

    let mut ab = connect_routers(&a, &b);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 0));
    elapse(&g, GRACE / 2);
    ab.disconnect();
    settle(&mut [&mut ga]);
    assert_eq!(transit_faces(&g), transit(0, 0, 0, 1));
    elapse(&g, GRACE / 4);
    let mut ab = connect_routers(&a, &b);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 1));

    // 5/4 of the grace period after the first transit state, 1/2 after the second.
    elapse(&g, GRACE / 2);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 1));
    assert_eq!(scope_state(&g, SN_A), ([3, 0, 0], true, true));
    assert_eq!(since(sent(&ga.a2b), &before), Sent::default());

    // The end of the second grace period.
    elapse(&g, GRACE / 2);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(0, 1, 1, 1));
    assert_eq!(since(sent(&ga.a2b), &before), Sent::declared([k.as_str()]));
    assert_eq!(known(&b), known_all([k.as_str()]));
}

/// A face closed during its grace period leaves nothing behind: the counts drop at once and the
/// re-check that comes later does nothing. The router reconnects into the same topology: its new
/// face starts clean, with a full grace period of its own.
#[test]
fn a_face_closed_during_its_grace_period_leaves_nothing_behind() {
    try_init_tracing_subscriber();
    let g = gateway(GATEWAY);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    let mut ga = connect_south(&g, &a);
    let mut ab = connect_routers(&a, &b);
    settle(&mut [&mut ga, &mut ab]);
    let app = g.new_session();
    let k = own(SN_B);
    declare(&app, 1, &[&k]);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 0));
    elapse(&g, GRACE / 2);

    ga.disconnect();
    settle(&mut [&mut ab]);
    assert_eq!(transit_faces(&g), transit(0, 0, 0, 0));
    elapse(&g, GRACE);
    assert_eq!(transit_faces(&g), transit(0, 0, 0, 0));
    assert_eq!(scoped_hat(&g, |hat, _| hat.transit_recheck_due()), None);

    let mut ga = connect_south(&g, &a);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 0));
    assert_eq!(scope_state(&g, SN_A), ([3, 0, 0], true, true));
    elapse(&g, GRACE - Duration::from_millis(1));
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 0));
    assert_eq!(sent(&ga.a2b), Sent::default());
    elapse(&g, Duration::from_millis(1));
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(0, 1, 1, 0));
    assert_eq!(sent(&ga.a2b), Sent::declared([k.as_str()]));
    assert_eq!(known(&b), known_all([k.as_str()]));
}

/// One re-check per gateway, due at the earliest end of a grace period, whatever the number of
/// faces in their grace period.
#[test]
fn one_recheck_for_all_the_faces_in_their_grace_period() {
    try_init_tracing_subscriber();
    let g = gateway(GATEWAY);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    let (c, d) = (router(Some(zid_of(SN_N))), router(None));
    let due = || scoped_hat(&g, |hat, _| hat.transit_recheck_due());
    assert_eq!(due(), None);

    let (mut ga, mut ab) = (connect_south(&g, &a), connect_routers(&a, &b));
    settle(&mut [&mut ga, &mut ab]);
    let first = due().expect("a re-check");
    elapse(&g, GRACE / 2);
    let (mut gc, mut cd) = (connect_south(&g, &c), connect_routers(&c, &d));
    settle(&mut [&mut ga, &mut ab, &mut gc, &mut cd]);
    assert_eq!(transit_faces(&g), transit(2, 0, 0, 0));
    assert_eq!(due(), Some(first));
    // A re-check while both are in their grace period: the next one is due at the first end.
    elapse(&g, Duration::ZERO);
    assert_eq!(transit_faces(&g), transit(2, 0, 0, 0));
    assert_eq!(due(), Some(first));

    elapse(&g, GRACE / 2);
    assert_eq!(transit_faces(&g), transit(1, 1, 1, 0));
    assert_eq!(due(), Some(first + GRACE / 2));
    elapse(&g, GRACE / 2);
    assert_eq!(transit_faces(&g), transit(0, 2, 2, 0));
    assert_eq!(due(), None);
}

/// Waits for the tree worker of `gateway` to have computed the trees it was handed.
fn wait_for_tree_computation(gateway: &Harness) {
    let start = Instant::now();
    while scoped_hat(gateway, |hat, _| hat.trees_outdated()) {
        assert!(
            start.elapsed() < Duration::from_millis(1500),
            "no tree computation"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// The A—B link goes away at the end of A's grace period. The tree computation it schedules has
/// been taken by the worker, which waits for the tables lock, when the re-check runs: the
/// re-check must not judge A on the previous trees. A stays filtered, and the computation then
/// finds it a leaf.
#[test]
fn the_recheck_waits_for_a_tree_computation_taken_by_the_worker() {
    try_init_tracing_subscriber();
    let g = gateway(GATEWAY);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    let mut ga = connect_south(&g, &a);
    settle(&mut [&mut ga]);
    let app = g.new_session();
    declare(&app, 1, &[&own(SN_B)]);
    settle(&mut [&mut ga]);
    let before = sent(&ga.a2b);
    let mut ab = connect_routers(&a, &b);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 0));

    scoped_hat_mut(&g, |hat, _| hat.set_manual_tree_computation(true));
    ab.disconnect();
    settle(&mut [&mut ga]);
    scoped_hat_mut(&g, |hat, tables| {
        assert!(
            hat.net().get_idx(&b.zid()).is_none(),
            "B is out of the graph"
        );
        hat.advance_clock(GRACE);
        hat.hand_tree_computation_to_worker(g.gateway.tables.clone());
        hat.recheck_transit_now(tables);
        assert_eq!(hat.transit_recheck_due(), None);
    });
    assert!(scope_state(&g, SN_A).2, "unfiltered on outdated trees");

    wait_for_tree_computation(&g);
    settle(&mut [&mut ga]);
    assert_eq!(transit_faces(&g), transit(0, 0, 0, 1));
    assert_eq!(scope_state(&g, SN_A), ([3, 0, 0], false, true));
    assert_eq!(since(sent(&ga.a2b), &before), Sent::default());
}

/// A re-check that defers to a scheduled tree computation: before the end of the grace period
/// that computation schedules the re-check again; after it, the computation, which still finds
/// A a transit next hop, unfilters A and replays to it.
#[test]
fn a_deferred_recheck_is_completed_by_the_tree_computation() {
    try_init_tracing_subscriber();
    let g = gateway(GATEWAY);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    let mut ga = connect_south(&g, &a);
    settle(&mut [&mut ga]);
    let app = g.new_session();
    let k = own(SN_B);
    declare(&app, 1, &[&k]);
    settle(&mut [&mut ga]);
    let before = sent(&ga.a2b);
    let mut ab = connect_routers(&a, &b);
    settle(&mut [&mut ga, &mut ab]);
    let due = || scoped_hat(&g, |hat, _| hat.transit_recheck_due());
    let end = due().expect("a re-check");

    let defer = |by: Duration| {
        scoped_hat_mut(&g, |hat, tables| {
            hat.advance_clock(by);
            hat.hand_tree_computation_to_worker(g.gateway.tables.clone());
            hat.recheck_transit_now(tables);
            assert_eq!(hat.transit_recheck_due(), None, "deferred");
        });
        wait_for_tree_computation(&g);
    };
    defer(GRACE / 2);
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 0));
    assert_eq!(due(), Some(end), "scheduled again by the computation");

    defer(GRACE / 2);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), transit(0, 1, 1, 0));
    assert_eq!(due(), None);
    assert_eq!(since(sent(&ga.a2b), &before), Sent::declared([k.as_str()]));
    assert_eq!(known(&b), known_all([k.as_str()]));
}

/// A stays a transit next hop while the router behind it changes (B, then X of another scope):
/// no evaluation finds it a leaf, so its grace period goes on from the first detection.
#[test]
fn a_transit_router_whose_destination_changes_keeps_its_grace_period() {
    try_init_tracing_subscriber();
    let g = gateway(GATEWAY);
    let (a, b, x) = (
        router(Some(zid_of(SN_A))),
        router(Some(zid_of(SN_B))),
        router(Some(zid_of(SN_N))),
    );
    let mut ga = connect_south(&g, &a);
    settle(&mut [&mut ga]);
    let app = g.new_session();
    let (kb, kx) = (own(SN_B), own(SN_N));
    declare(&app, 1, &[&kb, &kx]);
    settle(&mut [&mut ga]);
    let before = sent(&ga.a2b);
    let towards = || {
        scoped_hat(&g, |hat, tables| {
            hat.transit_towards(tables, &zid_of(SN_A).into())
        })
    };

    let mut ab = connect_routers(&a, &b);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(towards(), Some(b.zid()));
    elapse(&g, GRACE / 2);
    let mut ax = connect_routers(&a, &x);
    settle(&mut [&mut ga, &mut ab, &mut ax]);
    ab.disconnect();
    settle(&mut [&mut ga, &mut ax]);
    assert_eq!(towards(), Some(x.zid()));
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 0));

    // The end of the grace period counted from the first detection.
    elapse(&g, GRACE / 2);
    settle(&mut [&mut ga, &mut ax]);
    assert_eq!(transit_faces(&g), transit(0, 1, 1, 0));
    assert_eq!(
        since(sent(&ga.a2b), &before),
        Sent::declared([kb.as_str(), kx.as_str()])
    );
    assert_eq!(known(&x), known_all([kb.as_str(), kx.as_str()]));
}

/// The face whose grace period ends first closes, while another face stays a transit next hop:
/// the re-check due at the first end does nothing for it and is scheduled again for its own end,
/// where it is unfiltered.
#[test]
fn the_recheck_goes_on_when_the_earliest_face_closes() {
    try_init_tracing_subscriber();
    let g = gateway(GATEWAY);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    let (c, d) = (router(Some(zid_of(SN_N))), router(None));
    let due = || scoped_hat(&g, |hat, _| hat.transit_recheck_due());

    let (mut ga, mut ab) = (connect_south(&g, &a), connect_routers(&a, &b));
    settle(&mut [&mut ga, &mut ab]);
    let first = due().expect("a re-check");
    elapse(&g, GRACE / 2);
    let (mut gc, mut cd) = (connect_south(&g, &c), connect_routers(&c, &d));
    settle(&mut [&mut ga, &mut ab, &mut gc, &mut cd]);
    assert_eq!(transit_faces(&g), transit(2, 0, 0, 0));

    ga.disconnect();
    settle(&mut [&mut gc, &mut cd]);
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 0));
    assert_eq!(due(), Some(first));

    elapse(&g, GRACE / 2);
    assert_eq!(transit_faces(&g), transit(1, 0, 0, 0));
    assert_eq!(due(), Some(first + GRACE / 2));
    elapse(&g, GRACE / 2);
    assert_eq!(transit_faces(&g), transit(0, 1, 1, 0));
    assert_eq!(due(), None);
}

/// The evaluation at the end of A's grace period is delayed, and a tree computation finds A a
/// leaf first: A is never unfiltered.
#[test]
fn a_leaf_found_after_a_delayed_end_of_grace_period_is_not_unfiltered() {
    try_init_tracing_subscriber();
    let g = gateway(GATEWAY);
    let (a, b) = (router(Some(zid_of(SN_A))), router(Some(zid_of(SN_B))));
    let mut ga = connect_south(&g, &a);
    settle(&mut [&mut ga]);
    let app = g.new_session();
    declare(&app, 1, &[&own(SN_B)]);
    settle(&mut [&mut ga]);
    let before = sent(&ga.a2b);
    let mut ab = connect_routers(&a, &b);
    settle(&mut [&mut ga, &mut ab]);

    // No re-check has run yet.
    scoped_hat_mut(&g, |hat, _| {
        hat.advance_clock(GRACE + Duration::from_secs(2))
    });
    ab.disconnect();
    settle(&mut [&mut ga]);
    assert_eq!(transit_faces(&g), transit(0, 0, 0, 1));
    assert_eq!(scope_state(&g, SN_A), ([3, 0, 0], false, true));
    elapse(&g, Duration::ZERO);
    assert_eq!(transit_faces(&g), transit(0, 0, 0, 1));
    assert_eq!(since(sent(&ga.a2b), &before), Sent::default());
}
