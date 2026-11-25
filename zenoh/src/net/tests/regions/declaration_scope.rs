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

use std::{
    collections::{BTreeSet, HashMap},
    str::FromStr,
    sync::Arc,
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
    hat::{router::Hat as RouterHat, Sources},
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

fn gateway(conf: &str) -> Harness {
    HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([SCOPED, Region::Local])
        .gateway(conf)
        .build()
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

/// For the face of the gateway towards the router of `sn`: the declarations and
/// undeclarations out of scope and the declarations of the router itself out of its scope;
/// whether it is a transit next hop; whether it is still filtered.
fn scope_state(gateway: &Harness, sn: &str) -> ([u64; 3], bool, bool) {
    scoped_hat(gateway, |hat, tables| {
        hat.declaration_scope_of(tables, &zid_of(sn).into())
            .expect("no scoped face")
    })
}

/// How many scoped faces are a transit next hop, and how many times a face became one.
fn transit_faces(gateway: &Harness) -> [usize; 2] {
    scoped_hat(gateway, |hat, _| hat.transit_faces())
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

/// The review's first probe: G—A—B, all declarations made once the graph has converged. A is the
/// next hop towards B, which is out of A's scope. The gateway detects it, stops filtering A and
/// replays to it what it withheld, so B gets the declarations on its own scope through A. Once
/// the topology is a star again the detection clears; A stays unfiltered until it reconnects.
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
    assert_eq!(known(&b), known_all([k.as_str()]));

    // The detection.
    assert_eq!(transit_faces(&g), [1, 1]);
    let (counts, transit, filtered) = scope_state(&g, SN_A);
    assert!(transit && !filtered);
    assert_eq!(counts, [0; 3], "nothing was withheld from A");

    // A declaration withheld while A was a leaf is replayed when A becomes a transit router.
    let own_a = own(SN_A);
    let later_b = format!("{SN_B}/svc/default/pub/later");
    let star_b = format!("{SN_B}/svc/default/pub/star");
    ab.disconnect();
    settle(&mut [&mut ga]);
    assert_eq!(transit_faces(&g), [0, 1], "a leaf again");
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
    ga.a2b.recorder().clear();

    let mut ab = connect_routers(&a, &b);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(transit_faces(&g), [1, 2]);
    assert_eq!(
        sent(&ga.a2b),
        Sent::declared([k.as_str(), later_b.as_str()])
    );
    assert_eq!(known(&b), known_all([k.as_str(), later_b.as_str()]));

    // Star again: B connects to the gateway and the link between A and B goes away.
    let mut gb = connect_south(&g, &b);
    ab.disconnect();
    settle(&mut [&mut ga, &mut gb]);
    assert_eq!(transit_faces(&g), [0, 2]);
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

/// The review's second probe: G—A, G—B and A—B. A is a leaf until the link between G and B is
/// lost; then B's publications go through A, which must know the gateway's subscriber on B's
/// scope.
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
    assert_eq!(transit_faces(&g), [0, 0]);

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
    rb.put(k.clone(), vec![2]);
    settle(&mut [&mut ga, &mut ab]);
    assert_eq!(app.recorder().pushes().len(), 2);

    assert_eq!(transit_faces(&g), [1, 1]);
    assert_eq!(scope_state(&g, SN_A), ([1, 0, 0], true, false));

    // Star again.
    let mut gb = connect_south(&g, &b);
    ab.disconnect();
    settle(&mut [&mut ga, &mut gb]);
    assert_eq!(transit_faces(&g), [0, 1]);
    assert_eq!(scope_state(&g, SN_A), ([1, 0, 0], false, false));
    rb.put(k.clone(), vec![3]);
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
