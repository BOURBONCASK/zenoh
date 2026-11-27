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

//! Leaf uplink of a router (`gateway.north.mode: "client"`).
//!
//! ```text
//!   phone ── C ─────────── svc        C: cloud router, with client sessions
//!           / \
//!     (leaf)   (leaf or router)
//!         B1   B2                     bridges: routers that are, or are not, leaves of C
//!         |    |
//!         S1   S2                     routers of the south router subregion of each bridge
//!         |    |
//!       app1  app2                    client sessions, two hops south of the uplink
//! ```
//!
//! A leaf bridge is a client face of `C` (it announces itself as one) and has the client hat for
//! its north region. Everything is asserted on what the sessions at both ends receive, and on what
//! crosses the uplink.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    time::Duration,
};

use zenoh_protocol::{
    core::{Bound, ExprId, Region, WhatAmI, WireExpr},
    network::{
        declare::queryable::ext::QueryableInfoType,
        ext,
        interest::{InterestMode, InterestOptions},
        request::ext::QueryTarget,
        response, DeclareBody, Interest, Mapping, RequestId, Response, ResponseFinal,
    },
    zenoh::{PushBody, Put, Reply, ResponseBody},
};

use super::{
    try_init_tracing_subscriber, Connection, EstablishedConnection, FaceDef, Harness,
    HarnessBuilder, Message, MockFace,
};
use crate::net::{
    primitives::Primitives,
    routing::hat::{
        client::{Hat as ClientHat, UplinkInterestState},
        router::Hat as RouterHat,
    },
};

/// On a bridge: the subregion of its south router.
const SOUTH_ROUTER: Region = Region::South {
    id: 0,
    mode: WhatAmI::Router,
};
/// On the cloud router: the subregion of client remotes, leaf bridges among them.
const CLIENTS: Region = Region::South {
    id: 0,
    mode: WhatAmI::Client,
};
/// On the cloud router: the subregion of the bridges that are routers of its north.
const ROUTER_BRIDGES: Region = Region::South {
    id: 1,
    mode: WhatAmI::Router,
};

const R1: &str = "robot1";
const R2: &str = "robot2";

fn leaf_conf(interests: &str) -> String {
    format!(
        r#"{{
            north: {{
                mode: "client",
                uplinks: [{{ link_protocols: ["quic"] }}],
                interests: [{interests}],
            }},
            south: [{{ filters: [{{ modes: ["router"] }}] }}],
        }}"#
    )
}

/// Everything of `scope`.
fn all_of(scope: &str) -> String {
    format!(r#"{{ key_expr: "{scope}/**", subscribers: true, queryables: true, tokens: true }}"#)
}

/// A bridge that is a leaf of its north region and asks for everything of `scope`.
fn leaf_bridge(scope: &str) -> Harness {
    leaf_bridge_with(&all_of(scope))
}

fn leaf_bridge_with(interests: &str) -> Harness {
    HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([SOUTH_ROUTER, Region::Local])
        .gateway(&leaf_conf(interests))
        .build()
}

/// The deadline of the configured interests of [`impatient_leaf_bridge_with`].
const DEADLINE: Duration = Duration::from_millis(300);

/// A leaf bridge whose configured interests are unanswered after [`DEADLINE`].
fn impatient_leaf_bridge_with(interests: &str) -> Harness {
    HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([SOUTH_ROUTER, Region::Local])
        .gateway(&leaf_conf(interests))
        .config(
            "routing/interests/timeout",
            &DEADLINE.as_millis().to_string(),
        )
        .build()
}

/// The state of each configured interest of a leaf bridge on its north face.
fn interest_states(bridge: &Harness) -> Vec<Option<UplinkInterestState>> {
    let tables = bridge.gateway.tables.tables.read().unwrap();
    let hat = tables.hats[Region::North]
        .as_any()
        .downcast_ref::<ClientHat>()
        .expect("the north hat of a leaf is the client hat");
    hat.uplink_interests_info(&tables.data)
        .into_iter()
        .map(|(_, _, state)| state)
        .collect()
}

/// Waits for the deadline of the configured interests declared before the call.
fn wait_for_the_deadline(bridge: &Harness, expected: &[Option<UplinkInterestState>]) {
    let until = std::time::Instant::now() + 5 * DEADLINE;
    while interest_states(bridge) != expected {
        assert!(
            std::time::Instant::now() < until,
            "{:?}",
            interest_states(bridge)
        );
        std::thread::sleep(DEADLINE / 10);
    }
}

/// A bridge that is a router of its north region, as without `gateway.north`.
fn router_bridge() -> Harness {
    HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([SOUTH_ROUTER, Region::Local])
        .gateway(r#"{ south: [{ filters: [{ modes: ["router"] }] }] }"#)
        .build()
}

fn cloud() -> Harness {
    HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([CLIENTS, ROUTER_BRIDGES, Region::Local])
        .build()
}

/// The router south of a bridge.
fn south_router() -> Harness {
    HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::default_south(WhatAmI::Client), Region::Local])
        .build()
}

/// A client session of `router`.
fn client(router: &Harness, region: Region) -> MockFace {
    router.new_face(FaceDef::default().mode(WhatAmI::Client).region(region))
}

/// A client session of a south router.
fn app(router: &Harness) -> MockFace {
    client(router, Region::default_south(WhatAmI::Client))
}

/// Links a leaf bridge to the cloud router: the cloud router sees a client. `a2b` is the face of
/// the bridge for the cloud router: its recorder holds what the bridge sent up.
fn leaf_uplink<'a>(bridge: &'a Harness, cloud: &'a Harness) -> EstablishedConnection {
    Connection {
        a: bridge,
        a2b: FaceDef::default()
            .mode(WhatAmI::Router)
            .remote_bound(Bound::South),
        b: cloud,
        b2a: FaceDef::default().mode(WhatAmI::Client).region(CLIENTS),
    }
    .establish()
}

/// Links a router bridge to the cloud router.
fn router_uplink<'a>(bridge: &'a Harness, cloud: &'a Harness) -> EstablishedConnection {
    Connection {
        a: bridge,
        a2b: FaceDef::default()
            .mode(WhatAmI::Router)
            .remote_bound(Bound::South),
        b: cloud,
        b2a: FaceDef::default()
            .mode(WhatAmI::Router)
            .region(ROUTER_BRIDGES),
    }
    .establish()
}

/// Links a bridge to its south router.
fn tether<'a>(bridge: &'a Harness, south: &'a Harness) -> EstablishedConnection {
    Connection {
        a: bridge,
        a2b: FaceDef::default()
            .mode(WhatAmI::Router)
            .region(SOUTH_ROUTER),
        b: south,
        b2a: FaceDef::default()
            .mode(WhatAmI::Router)
            .remote_bound(Bound::South),
    }
    .establish()
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

/// What a face was sent, with key expression ids resolved.
#[derive(Debug, Default)]
struct Seen {
    /// Declared and not undeclared.
    subscribers: BTreeSet<String>,
    queryables: BTreeSet<String>,
    tokens: BTreeSet<String>,
    /// Every declaration and undeclaration message, in order (`+sub key`, `-qabl key`, ...).
    declarations: Vec<String>,
    pushes: Vec<String>,
    requests: Vec<(RequestId, String)>,
    replies: Vec<String>,
    response_finals: usize,
    interests: Vec<(String, InterestMode, InterestOptions)>,
    declare_finals: usize,
    oams: usize,
}

/// What a client session was sent.
fn seen(face: &MockFace) -> Seen {
    seen_from(face, None)
}

/// What gateway `a` of a connection sent to gateway `b`.
fn a_sent(conn: &EstablishedConnection) -> Seen {
    seen_from(&conn.a2b, Some(&conn.b2a))
}

/// What gateway `b` of a connection sent to gateway `a`.
fn b_sent(conn: &EstablishedConnection) -> Seen {
    seen_from(&conn.b2a, Some(&conn.a2b))
}

/// What `face` was sent. `reverse` is the face of the other direction of the same link: a
/// message may use a key expression id that its receiver declared.
fn seen_from(face: &MockFace, reverse: Option<&MockFace>) -> Seen {
    type Ids = HashMap<ExprId, String>;

    fn full((ids, receiver_ids): (&Ids, &Ids), w: &WireExpr) -> String {
        let ids = match w.mapping {
            Mapping::Sender => ids,
            Mapping::Receiver => receiver_ids,
        };
        match w.scope {
            0 => w.suffix.to_string(),
            id => format!("{}{}", ids[&id], w.suffix),
        }
    }

    // the ids the receiver declared, whenever it did
    let mut receiver_ids = Ids::new();
    if let Some(reverse) = reverse {
        reverse.recorder().with_messages(|msgs| {
            for msg in msgs {
                if let Message::Declare(d) = msg {
                    if let DeclareBody::DeclareKeyExpr(k) = &d.body {
                        let expr = full((&receiver_ids, &Ids::new()), &k.wire_expr);
                        receiver_ids.insert(k.id, expr);
                    }
                }
            }
        });
    }

    /// Entities are undeclared by id (sessions) or by key (routers).
    fn forget(
        kind: &str,
        by_id: &mut BTreeMap<u32, String>,
        set: &mut BTreeSet<String>,
        log: &mut Vec<String>,
        id: u32,
        key: Option<String>,
    ) {
        let key = key
            .or_else(|| by_id.remove(&id))
            .unwrap_or_else(|| panic!("undeclaration of unknown {kind} {id}"));
        log.push(format!("-{kind} {key}"));
        set.remove(&key);
    }

    let mut s = Seen::default();
    let mut ids = Ids::new();
    let (mut subs, mut qabls, mut tokens) = (BTreeMap::new(), BTreeMap::new(), BTreeMap::new());
    face.recorder().with_messages(|msgs| {
        for msg in msgs {
            match msg {
                Message::Declare(d) => match &d.body {
                    DeclareBody::DeclareKeyExpr(k) => {
                        let expr = full((&ids, &receiver_ids), &k.wire_expr);
                        ids.insert(k.id, expr);
                    }
                    DeclareBody::UndeclareKeyExpr(k) => {
                        ids.remove(&k.id);
                    }
                    DeclareBody::DeclareSubscriber(m) => {
                        let key = full((&ids, &receiver_ids), &m.wire_expr);
                        s.declarations.push(format!("+sub {key}"));
                        subs.insert(m.id, key.clone());
                        s.subscribers.insert(key);
                    }
                    DeclareBody::UndeclareSubscriber(m) => {
                        let w = &m.ext_wire_expr.wire_expr;
                        let key = (!w.is_empty()).then(|| full((&ids, &receiver_ids), w));
                        let (set, log) = (&mut s.subscribers, &mut s.declarations);
                        forget("sub", &mut subs, set, log, m.id, key);
                    }
                    DeclareBody::DeclareQueryable(m) => {
                        let key = full((&ids, &receiver_ids), &m.wire_expr);
                        s.declarations.push(format!("+qabl {key}"));
                        qabls.insert(m.id, key.clone());
                        s.queryables.insert(key);
                    }
                    DeclareBody::UndeclareQueryable(m) => {
                        let w = &m.ext_wire_expr.wire_expr;
                        let key = (!w.is_empty()).then(|| full((&ids, &receiver_ids), w));
                        let (set, log) = (&mut s.queryables, &mut s.declarations);
                        forget("qabl", &mut qabls, set, log, m.id, key);
                    }
                    DeclareBody::DeclareToken(m) => {
                        let key = full((&ids, &receiver_ids), &m.wire_expr);
                        s.declarations.push(format!("+token {key}"));
                        tokens.insert(m.id, key.clone());
                        s.tokens.insert(key);
                    }
                    DeclareBody::UndeclareToken(m) => {
                        let w = &m.ext_wire_expr.wire_expr;
                        let key = (!w.is_empty()).then(|| full((&ids, &receiver_ids), w));
                        let (set, log) = (&mut s.tokens, &mut s.declarations);
                        forget("token", &mut tokens, set, log, m.id, key);
                    }
                    DeclareBody::DeclareFinal(_) => s.declare_finals += 1,
                },
                Message::Push(p) => s.pushes.push(full((&ids, &receiver_ids), &p.wire_expr)),
                Message::Request(r) => s
                    .requests
                    .push((r.id, full((&ids, &receiver_ids), &r.wire_expr))),
                Message::Response(r) => s.replies.push(full((&ids, &receiver_ids), &r.wire_expr)),
                Message::ResponseFinal(_) => s.response_finals += 1,
                Message::Interest(i) => s.interests.push((
                    i.wire_expr
                        .as_ref()
                        .map(|w| full((&ids, &receiver_ids), w))
                        .unwrap_or_default(),
                    i.mode,
                    i.options,
                )),
                Message::Oam(_) => s.oams += 1,
            }
        }
    });
    s
}

fn set<const N: usize>(keys: [&str; N]) -> BTreeSet<String> {
    keys.iter().map(|k| k.to_string()).collect()
}

/// Replies to the request `rid` that `responder` received, on `key`, then ends the replies.
fn reply(responder: &MockFace, rid: RequestId, key: &str) {
    responder.face.send_response(&mut Response {
        rid,
        wire_expr: key.to_string().into(),
        payload: ResponseBody::Reply(Reply {
            consolidation: Default::default(),
            ext_unknown: vec![],
            payload: PushBody::Put(Put::default()),
        }),
        ext_qos: response::ext::QoSType::DEFAULT,
        ext_tstamp: None,
        ext_respid: None,
        ext_ts_stack: None,
    });
    responder.face.send_response_final(&mut ResponseFinal {
        rid,
        ext_qos: ext::QoSType::DEFAULT,
        ext_tstamp: None,
    });
}

/// A robot: a bridge linked to its south router, with one application session on the latter.
struct Robot {
    bridge: Harness,
    south: Harness,
    tether: EstablishedConnection,
    app: MockFace,
}

impl Robot {
    fn new(bridge: Harness) -> Self {
        Self::with(bridge, south_router())
    }

    fn with(bridge: Harness, south: Harness) -> Self {
        let mut tether = tether(&bridge, &south);
        tether.bi_fwd();
        let app = app(&south);
        Self {
            bridge,
            south,
            tether,
            app,
        }
    }
}

/// The publications of an application two hops south of the uplink reach the subscribers of the
/// cloud: one for that robot, one for every robot. A publication that no one subscribed to does
/// not cross the uplink.
#[test]
fn robot_publication_reaches_cloud_subscribers() {
    try_init_tracing_subscriber();
    let c = cloud();
    let (phone, svc) = (client(&c, CLIENTS), client(&c, CLIENTS));
    phone.declare_subscriber(None, 1, format!("{R1}/tele/**"));
    svc.declare_subscriber(None, 1, "*/tele/**");
    let mut r = Robot::new(leaf_bridge(R1));
    let mut up = leaf_uplink(&r.bridge, &c);
    settle(&mut [&mut up, &mut r.tether]);

    r.app.put(format!("{R1}/tele/vital"), vec![1]);
    r.app.put(format!("{R1}/internal/joint"), vec![2]);
    settle(&mut [&mut r.tether, &mut up]);

    assert_eq!(seen(&phone).pushes, [format!("{R1}/tele/vital")]);
    assert_eq!(seen(&svc).pushes, [format!("{R1}/tele/vital")]);
    assert_eq!(a_sent(&up).pushes, [format!("{R1}/tele/vital")]);
}

/// A publication of the cloud reaches a subscriber two hops south of the uplink.
#[test]
fn cloud_publication_reaches_robot_subscriber() {
    try_init_tracing_subscriber();
    let c = cloud();
    let phone = client(&c, CLIENTS);
    let mut r = Robot::new(leaf_bridge(R1));
    let mut up = leaf_uplink(&r.bridge, &c);
    r.app
        .declare_subscriber(None, 1, format!("{R1}/control/down"));
    settle(&mut [&mut r.tether, &mut up]);

    phone.put(format!("{R1}/control/down"), vec![1]);
    settle(&mut [&mut up, &mut r.tether]);

    assert_eq!(seen(&r.app).pushes, [format!("{R1}/control/down")]);
}

/// The cloud queries a queryable two hops south of the uplink and gets its reply.
#[test]
fn cloud_query_reaches_robot_queryable() {
    try_init_tracing_subscriber();
    let c = cloud();
    let phone = client(&c, CLIENTS);
    let mut r = Robot::new(leaf_bridge(R1));
    let mut up = leaf_uplink(&r.bridge, &c);
    let key = format!("{R1}/srv/upload/list");
    r.app
        .declare_queryable(None, 1, key.clone(), QueryableInfoType::DEFAULT);
    settle(&mut [&mut r.tether, &mut up]);

    phone.query(7, key.clone());
    settle(&mut [&mut up, &mut r.tether]);
    let requests = seen(&r.app).requests;
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert_eq!(requests[0].1, key);
    reply(&r.app, requests[0].0, &key);
    settle(&mut [&mut r.tether, &mut up]);

    let phone = seen(&phone);
    assert_eq!(phone.replies, [key]);
    assert_eq!(phone.response_finals, 1);
}

/// An application two hops south of the uplink queries a queryable of the cloud declared for
/// every robot, and gets its reply.
#[test]
fn robot_query_reaches_cloud_queryable() {
    try_init_tracing_subscriber();
    let c = cloud();
    let svc = client(&c, CLIENTS);
    svc.declare_queryable(
        None,
        1,
        "**/srv/credential/issue",
        QueryableInfoType::DEFAULT,
    );
    let mut r = Robot::new(leaf_bridge(R1));
    let mut up = leaf_uplink(&r.bridge, &c);
    settle(&mut [&mut up, &mut r.tether]);
    let key = format!("{R1}/srv/credential/issue");

    r.app.query(3, key.clone());
    settle(&mut [&mut r.tether, &mut up]);
    let requests = seen(&svc).requests;
    assert_eq!(requests.len(), 1, "{requests:?}");
    assert_eq!(requests[0].1, key);
    reply(&svc, requests[0].0, &key);
    settle(&mut [&mut up, &mut r.tether]);

    let app = seen(&r.app);
    assert_eq!(app.replies, [key]);
    assert_eq!(app.response_finals, 1);
}

/// A query on a key that no uplink interest includes: a session of the leaf itself reaches the
/// queryable of the north, since the leaf sends north what it does not know about. A session two
/// hops south does not: its router only sends a query towards a queryable it was told of,
/// unless it has a default route (see `a_query_for_another_robot_reaches_it`).
#[test]
fn queries_outside_of_the_interests_only_leave_the_leaf_itself() {
    try_init_tracing_subscriber();
    let c = cloud();
    let svc = client(&c, CLIENTS);
    svc.declare_queryable(None, 1, "fleet/time", QueryableInfoType::DEFAULT);
    let mut r = Robot::new(leaf_bridge(R1));
    let local = r.bridge.new_session();
    let mut up = leaf_uplink(&r.bridge, &c);
    settle(&mut [&mut up, &mut r.tether]);

    local.query(3, "fleet/time");
    settle(&mut [&mut r.tether, &mut up]);
    assert_eq!(seen(&svc).requests.len(), 1);

    r.app.query(4, "fleet/time");
    settle(&mut [&mut r.tether, &mut up]);
    assert_eq!(seen(&svc).requests.len(), 1);
    assert_eq!(seen(&r.app).response_finals, 1);
}

/// Liveliness tokens cross the uplink both ways: a liveliness subscriber of the cloud gets the
/// token of an application two hops south, and a liveliness subscriber there gets the token the
/// cloud declared for that robot.
#[test]
fn liveliness_tokens_cross_the_uplink_both_ways() {
    try_init_tracing_subscriber();
    let c = cloud();
    let phone = client(&c, CLIENTS);
    let tokens = InterestOptions::KEYEXPRS + InterestOptions::TOKENS;
    phone.interest(
        1,
        InterestMode::CurrentFuture,
        tokens,
        format!("{R1}/alive/**"),
    );
    phone.declare_token(None, 1, format!("{R1}/alive/phone"));
    let mut r = Robot::new(leaf_bridge(R1));
    let mut up = leaf_uplink(&r.bridge, &c);
    settle(&mut [&mut up, &mut r.tether]);

    r.app.declare_token(None, 1, format!("{R1}/alive/app"));
    r.app.interest(
        1,
        InterestMode::CurrentFuture,
        tokens,
        format!("{R1}/alive/**"),
    );
    settle(&mut [&mut r.tether, &mut up]);

    assert!(
        seen(&phone).tokens.contains(&format!("{R1}/alive/app")),
        "{:?}",
        seen(&phone)
    );
    assert!(
        seen(&r.app).tokens.contains(&format!("{R1}/alive/phone")),
        "{:?}",
        seen(&r.app)
    );
}

/// Declarations made on either side after the uplink is up reach the other side, and are
/// withdrawn when they are undeclared.
#[test]
fn late_declarations_cross_the_uplink_and_are_withdrawn() {
    try_init_tracing_subscriber();
    let c = cloud();
    let phone = client(&c, CLIENTS);
    let mut r = Robot::new(leaf_bridge(R1));
    let mut up = leaf_uplink(&r.bridge, &c);
    settle(&mut [&mut up, &mut r.tether]);
    let (ctx, list) = (format!("{R1}/ctx/late"), format!("{R1}/srv/late"));

    // cloud subscriber declared late: robot publications start to reach it
    phone.declare_subscriber(None, 1, ctx.clone());
    settle(&mut [&mut up, &mut r.tether]);
    r.app.put(ctx.clone(), vec![1]);
    settle(&mut [&mut r.tether, &mut up]);
    assert_eq!(seen(&phone).pushes, std::slice::from_ref(&ctx));

    // and stop when it is undeclared
    phone.undeclare_subscriber(1);
    settle(&mut [&mut up, &mut r.tether]);
    r.app.put(ctx.clone(), vec![2]);
    settle(&mut [&mut r.tether, &mut up]);
    assert_eq!(seen(&phone).pushes, std::slice::from_ref(&ctx));
    assert_eq!(a_sent(&up).pushes, [ctx]);

    // robot queryable declared late: the cloud reaches it
    r.app
        .declare_queryable(None, 1, list.clone(), QueryableInfoType::DEFAULT);
    settle(&mut [&mut r.tether, &mut up]);
    phone.query(1, list.clone());
    settle(&mut [&mut up, &mut r.tether]);
    assert_eq!(seen(&r.app).requests.len(), 1);

    // and no longer does once it is undeclared
    r.app.undeclare_queryable(1);
    settle(&mut [&mut r.tether, &mut up]);
    phone.query(2, list);
    settle(&mut [&mut up, &mut r.tether]);
    assert_eq!(seen(&r.app).requests.len(), 1);
    assert_eq!(seen(&phone).response_finals, 1);
}

/// A wildcard subscriber and a wildcard query of the cloud reach every robot.
#[test]
fn cloud_wildcards_reach_every_robot() {
    try_init_tracing_subscriber();
    let c = cloud();
    let svc = client(&c, CLIENTS);
    svc.declare_subscriber(None, 1, "*/tele/**");
    let (mut r1, mut r2) = (Robot::new(leaf_bridge(R1)), Robot::new(leaf_bridge(R2)));
    let (mut up1, mut up2) = (leaf_uplink(&r1.bridge, &c), leaf_uplink(&r2.bridge, &c));
    for (r, sn) in [(&r1, R1), (&r2, R2)] {
        r.app.declare_queryable(
            None,
            1,
            format!("{sn}/srv/list"),
            QueryableInfoType::DEFAULT,
        );
    }
    settle(&mut [&mut r1.tether, &mut r2.tether, &mut up1, &mut up2]);

    r1.app.put(format!("{R1}/tele/a"), vec![1]);
    r2.app.put(format!("{R2}/tele/a"), vec![2]);
    svc.query(1, "*/srv/list");
    settle(&mut [&mut r1.tether, &mut r2.tether, &mut up1, &mut up2]);

    assert_eq!(
        seen(&svc).pushes.into_iter().collect::<BTreeSet<_>>(),
        set([&format!("{R1}/tele/a"), &format!("{R2}/tele/a")])
    );
    assert_eq!(seen(&r1.app).requests.len(), 1);
    assert_eq!(seen(&r2.app).requests.len(), 1);
}

/// What the routers of a robot hold of another robot, and of the cloud's declarations for
/// another robot: nothing with a leaf uplink, everything with a router uplink.
#[test]
fn a_leaf_holds_no_declaration_of_another_robot() {
    try_init_tracing_subscriber();

    /// The declarations the bridge sent to its south router, for a robot `R1` whose bridge is
    /// `bridge`, next to a robot `R2` with a leaf uplink that declares four entities, and a
    /// cloud that declares entities for both robots and for every robot.
    fn sent_south(
        bridge: Harness,
        uplink: fn(&Harness, &Harness) -> EstablishedConnection,
    ) -> Seen {
        let c = cloud();
        let phone = client(&c, CLIENTS);
        let mut r1 = Robot::new(bridge);
        let mut r2 = Robot::new(leaf_bridge(R2));
        let (mut up1, mut up2) = (uplink(&r1.bridge, &c), leaf_uplink(&r2.bridge, &c));
        for (i, sn) in [R1, R2].into_iter().enumerate() {
            phone.declare_subscriber(None, 10 + i as u32, format!("{sn}/tele/**"));
            phone.declare_token(None, 20 + i as u32, format!("{sn}/alive/phone"));
        }
        phone.declare_subscriber(None, 1, "*/log");
        r2.app.declare_subscriber(None, 1, format!("{R2}/control"));
        r2.app.declare_subscriber(None, 2, format!("{R2}/x"));
        r2.app.declare_queryable(
            None,
            3,
            format!("{R2}/srv/list"),
            QueryableInfoType::DEFAULT,
        );
        r2.app.declare_token(None, 4, format!("{R2}/alive/app"));
        settle(&mut [&mut r2.tether, &mut up2, &mut up1, &mut r1.tether]);
        a_sent(&r1.tether)
    }

    let leaf = sent_south(leaf_bridge(R1), leaf_uplink);
    assert_eq!(leaf.subscribers, set([&format!("{R1}/tele/**"), "*/log"]));
    assert_eq!(leaf.queryables, set([]));
    assert_eq!(leaf.tokens, set([&format!("{R1}/alive/phone")]));

    let router = sent_south(router_bridge(), router_uplink);
    assert_eq!(
        router.subscribers,
        set([
            &format!("{R1}/tele/**"),
            &format!("{R2}/tele/**"),
            "*/log",
            &format!("{R2}/control"),
            &format!("{R2}/x"),
        ])
    );
    assert_eq!(router.queryables, set([&format!("{R2}/srv/list")]));
    assert_eq!(
        router.tokens,
        set([
            &format!("{R1}/alive/phone"),
            &format!("{R2}/alive/phone"),
            &format!("{R2}/alive/app"),
        ])
    );
}

/// No link state crosses a leaf uplink, in either direction; it does cross a router uplink.
#[test]
fn no_link_state_crosses_a_leaf_uplink() {
    try_init_tracing_subscriber();
    let c = cloud();
    let mut leaf = Robot::new(leaf_bridge(R1));
    let mut up = leaf_uplink(&leaf.bridge, &c);
    settle(&mut [&mut up, &mut leaf.tether]);
    assert_eq!((a_sent(&up).oams, b_sent(&up).oams), (0, 0));

    let mut router = Robot::new(router_bridge());
    let mut up = router_uplink(&router.bridge, &c);
    settle(&mut [&mut up, &mut router.tether]);
    assert!(a_sent(&up).oams > 0 && b_sent(&up).oams > 0);
}

/// When the uplink of a leaf goes down and comes back, its south router is sent no link state
/// (its trees do not change) and only the cloud's declarations for that robot are withdrawn and
/// declared again; the flows work again afterwards. A session of the south router keeps
/// receiving the publications of another one all along.
#[test]
fn an_uplink_flap_does_not_disturb_the_south_region() {
    try_init_tracing_subscriber();
    let c = cloud();
    let phone = client(&c, CLIENTS);
    phone.declare_subscriber(None, 1, format!("{R1}/tele/**"));
    phone.declare_subscriber(None, 2, format!("{R2}/tele/**"));
    let mut r = Robot::new(leaf_bridge(R1));
    let other_app = app(&r.south);
    other_app.declare_subscriber(None, 1, format!("{R1}/internal/joint"));
    r.app.declare_subscriber(None, 1, format!("{R1}/control"));
    let mut up = leaf_uplink(&r.bridge, &c);
    settle(&mut [&mut up, &mut r.tether]);
    let before = a_sent(&r.tether);
    assert_eq!(before.subscribers, set([&format!("{R1}/tele/**")]));

    up.disconnect();
    settle(&mut [&mut r.tether]);
    r.app.put(format!("{R1}/internal/joint"), vec![1]);
    let down = a_sent(&r.tether);
    assert_eq!(down.subscribers, set([]));
    assert_eq!(seen(&other_app).pushes.len(), 1);

    let mut up = leaf_uplink(&r.bridge, &c);
    settle(&mut [&mut up, &mut r.tether]);
    r.app.put(format!("{R1}/internal/joint"), vec![2]);
    r.app.put(format!("{R1}/tele/vital"), vec![3]);
    phone.put(format!("{R1}/control"), vec![4]);
    settle(&mut [&mut r.tether, &mut up]);

    let after = a_sent(&r.tether);
    assert_eq!(
        after.oams, before.oams,
        "link state sent to the south router"
    );
    assert_eq!(
        after.declarations[before.declarations.len()..],
        [format!("-sub {R1}/tele/**"), format!("+sub {R1}/tele/**")]
    );
    assert_eq!(seen(&other_app).pushes.len(), 2);
    assert_eq!(seen(&phone).pushes, [format!("{R1}/tele/vital")]);
    assert_eq!(seen(&r.app).pushes, [format!("{R1}/control")]);
}

/// The interests of a leaf are sent each time its uplink is established, and answered.
#[test]
fn uplink_interests_are_declared_on_each_connection() {
    try_init_tracing_subscriber();
    let c = cloud();
    let r = Robot::new(leaf_bridge_with(&format!(
        r#"{{ key_expr: "{R1}/**", subscribers: true, queryables: true }},
           {{ key_expr: "{R1}/alive/**", tokens: true }}"#
    )));
    let entities =
        InterestOptions::KEYEXPRS + InterestOptions::SUBSCRIBERS + InterestOptions::QUERYABLES;
    let tokens = InterestOptions::KEYEXPRS + InterestOptions::TOKENS;
    let expected = [
        (format!("{R1}/**"), InterestMode::CurrentFuture, entities),
        (
            format!("{R1}/alive/**"),
            InterestMode::CurrentFuture,
            tokens,
        ),
    ];

    for _ in 0..2 {
        let mut up = leaf_uplink(&r.bridge, &c);
        up.bi_fwd();
        let sent = a_sent(&up);
        assert_eq!(sent.interests, expected);
        assert_eq!(b_sent(&up).declare_finals, 2);
        up.disconnect();
    }
}

/// Once an uplink interest is answered, a publication on a key it includes is only sent to the
/// uplink if the north declared a matching subscriber. A publication on a key that no interest
/// includes is sent anyway: the leaf does not know who subscribed to it.
#[test]
fn publications_without_a_north_subscriber_stay_south() {
    try_init_tracing_subscriber();
    let c = cloud();
    let mut r = Robot::new(leaf_bridge(R1));
    // the bridge itself subscribes: the publications of the application reach the bridge
    let local = r.bridge.new_session();
    local.declare_subscriber(None, 1, "**");
    let mut up = leaf_uplink(&r.bridge, &c);
    settle(&mut [&mut up, &mut r.tether]);

    r.app.put(format!("{R1}/internal/joint"), vec![1]);
    r.app.put("fleet/unscoped", vec![2]);
    settle(&mut [&mut r.tether, &mut up]);

    assert_eq!(seen(&local).pushes.len(), 2);
    assert_eq!(a_sent(&up).pushes, ["fleet/unscoped"]);
}

/// Before an uplink interest is answered the leaf does not know the subscribers of the north:
/// publications are sent to the uplink.
#[test]
fn publications_are_sent_north_until_the_interest_is_answered() {
    try_init_tracing_subscriber();
    let c = cloud();
    let mut r = Robot::new(leaf_bridge(R1));
    let local = r.bridge.new_session();
    local.declare_subscriber(None, 1, "**");
    r.tether.bi_fwd();
    // not forwarded: the cloud has not received the interests yet
    let up = leaf_uplink(&r.bridge, &c);

    r.app.put(format!("{R1}/internal/joint"), vec![1]);
    r.tether.bi_fwd();

    assert_eq!(a_sent(&up).pushes, [format!("{R1}/internal/joint")]);
}

/// Without an uplink interest the south router is not told of the subscribers of the north, so
/// a publication two hops south of the uplink never leaves it. This is what the interests are
/// for: a router subregion only sends data towards the subscribers its gateway declared.
#[test]
fn without_uplink_interests_the_south_router_ignores_the_north() {
    try_init_tracing_subscriber();
    let c = cloud();
    let phone = client(&c, CLIENTS);
    phone.declare_subscriber(None, 1, format!("{R1}/tele/**"));
    let mut r = Robot::new(leaf_bridge_with(""));
    let mut up = leaf_uplink(&r.bridge, &c);
    settle(&mut [&mut up, &mut r.tether]);

    r.app.put(format!("{R1}/tele/vital"), vec![1]);
    settle(&mut [&mut r.tether, &mut up]);

    assert_eq!(a_sent(&r.tether).subscribers, set([]));
    assert_eq!(seen(&phone).pushes, Vec::<String>::new());
}

/// A leaf bridge and a router bridge are linked to the same cloud router: the sessions behind
/// each exchange with the cloud, and a session behind the router bridge publishes to the robot
/// of the leaf. The reverse does not work without a default route: a publication for another
/// robot is outside of the interests of the leaf, so its south router has no subscriber for it
/// (see `robots_with_a_leaf_and_with_a_router_bridge_publish_to_each_other`).
#[test]
fn leaf_and_router_bridges_share_a_cloud_router() {
    try_init_tracing_subscriber();
    let c = cloud();
    let phone = client(&c, CLIENTS);
    phone.declare_subscriber(None, 1, "*/tele/**");
    let (mut leaf, mut router) = (Robot::new(leaf_bridge(R1)), Robot::new(router_bridge()));
    let mut up1 = leaf_uplink(&leaf.bridge, &c);
    let mut up2 = router_uplink(&router.bridge, &c);
    leaf.app
        .declare_subscriber(None, 1, format!("{R1}/control"));
    router
        .app
        .declare_subscriber(None, 1, format!("{R2}/control"));
    settle(&mut [&mut leaf.tether, &mut router.tether, &mut up1, &mut up2]);

    leaf.app.put(format!("{R1}/tele/a"), vec![1]);
    router.app.put(format!("{R2}/tele/a"), vec![2]);
    phone.put(format!("{R1}/control"), vec![3]);
    phone.put(format!("{R2}/control"), vec![4]);
    // a session behind the router bridge addresses the robot of the leaf bridge, and back
    router.app.put(format!("{R1}/control"), vec![5]);
    leaf.app.put(format!("{R2}/control"), vec![6]);
    settle(&mut [&mut leaf.tether, &mut router.tether, &mut up1, &mut up2]);

    assert_eq!(
        seen(&phone).pushes.into_iter().collect::<BTreeSet<_>>(),
        set([&format!("{R1}/tele/a"), &format!("{R2}/tele/a")])
    );
    assert_eq!(seen(&leaf.app).pushes, vec![format!("{R1}/control"); 2]);
    assert_eq!(seen(&router.app).pushes, [format!("{R2}/control")]);
}

/// The north region of a router with `gateway.north.mode: "client"` has the client hat; without
/// it, the router hat.
#[test]
fn north_mode_selects_the_north_hat() {
    let mode_of = |h: &Harness| {
        let tables = h.gateway.tables.tables.read().unwrap();
        tables.hats[Region::North].mode()
    };
    assert_eq!(mode_of(&leaf_bridge(R1)), WhatAmI::Client);
    assert_eq!(mode_of(&router_bridge()), WhatAmI::Router);
}

/// The kind of entity a configured interest asks for.
#[derive(Clone, Copy, Debug)]
enum Kind {
    Subscribers,
    Queryables,
    Tokens,
}

impl Kind {
    fn field(self) -> &'static str {
        match self {
            Kind::Subscribers => "subscribers",
            Kind::Queryables => "queryables",
            Kind::Tokens => "tokens",
        }
    }

    fn options(self) -> InterestOptions {
        InterestOptions::KEYEXPRS
            + match self {
                Kind::Subscribers => InterestOptions::SUBSCRIBERS,
                Kind::Queryables => InterestOptions::QUERYABLES,
                Kind::Tokens => InterestOptions::TOKENS,
            }
    }

    fn declare(self, face: &MockFace, id: u32, key: String) {
        match self {
            Kind::Subscribers => face.declare_subscriber(None, id, key),
            Kind::Queryables => face.declare_queryable(None, id, key, QueryableInfoType::DEFAULT),
            Kind::Tokens => face.declare_token(None, id, key),
        }
    }

    fn declared(self, seen: Seen) -> BTreeSet<String> {
        match self {
            Kind::Subscribers => seen.subscribers,
            Kind::Queryables => seen.queryables,
            Kind::Tokens => seen.tokens,
        }
    }
}

/// How a session stops being interested.
#[derive(Clone, Copy, Debug)]
enum Cancel {
    /// It undeclares its interest.
    Undeclare,
    /// It goes away.
    Close,
}

/// A session of the bridge declares the very interest that is configured for the uplink, then
/// stops being interested. The configured interest is not the session's: an entity the cloud
/// declares afterwards still reaches the south router, on this connection and on the next one.
fn configured_interest_outlives_the_same_interest_of_a_session(kind: Kind, cancel: Cancel) {
    try_init_tracing_subscriber();
    let c = cloud();
    let phone = client(&c, CLIENTS);
    let scope = format!("{R1}/x/**");
    let mut r = Robot::new(leaf_bridge_with(&format!(
        r#"{{ key_expr: "{scope}", {}: true }}"#,
        kind.field()
    )));
    let local = r.bridge.new_session();
    let mut up = leaf_uplink(&r.bridge, &c);
    settle(&mut [&mut up, &mut r.tether]);

    local.interest(
        52,
        InterestMode::CurrentFuture,
        kind.options(),
        scope.clone(),
    );
    settle(&mut [&mut up, &mut r.tether]);
    match cancel {
        Cancel::Undeclare => local.interest(52, InterestMode::Final, kind.options(), scope.clone()),
        Cancel::Close => local.face.send_close(),
    }
    settle(&mut [&mut up, &mut r.tether]);

    // both interests were declared to the cloud, and at most the session's one is cancelled
    let modes = |mode| {
        let sent = a_sent(&up).interests;
        sent.iter().filter(|(_, m, _)| *m == mode).count()
    };
    assert_eq!(modes(InterestMode::CurrentFuture), 2, "{kind:?} {cancel:?}");
    assert!(modes(InterestMode::Final) <= 1, "{kind:?} {cancel:?}");
    assert_eq!(
        interest_states(&r.bridge),
        [Some(UplinkInterestState::Finalized)]
    );

    kind.declare(&phone, 98, format!("{R1}/x/late"));
    settle(&mut [&mut up, &mut r.tether]);
    assert!(
        kind.declared(a_sent(&r.tether))
            .contains(&format!("{R1}/x/late")),
        "{kind:?} {cancel:?}: the configured interest was cancelled with the session's"
    );

    // on the next connection only the configured interest is declared, and it works
    up.disconnect();
    settle(&mut [&mut r.tether]);
    let mut up = leaf_uplink(&r.bridge, &c);
    settle(&mut [&mut up, &mut r.tether]);
    assert_eq!(
        a_sent(&up).interests,
        [(scope, InterestMode::CurrentFuture, kind.options())],
        "{kind:?} {cancel:?}"
    );
    kind.declare(&phone, 99, format!("{R1}/x/later"));
    settle(&mut [&mut up, &mut r.tether]);
    assert_eq!(
        kind.declared(a_sent(&r.tether)),
        set([&format!("{R1}/x/late"), &format!("{R1}/x/later")]),
        "{kind:?} {cancel:?}"
    );
}

#[test]
fn configured_subscriber_interest_outlives_a_session_that_undeclares_the_same() {
    configured_interest_outlives_the_same_interest_of_a_session(
        Kind::Subscribers,
        Cancel::Undeclare,
    );
}

#[test]
fn configured_queryable_interest_outlives_a_session_that_undeclares_the_same() {
    configured_interest_outlives_the_same_interest_of_a_session(
        Kind::Queryables,
        Cancel::Undeclare,
    );
}

#[test]
fn configured_token_interest_outlives_a_session_that_undeclares_the_same() {
    configured_interest_outlives_the_same_interest_of_a_session(Kind::Tokens, Cancel::Undeclare);
}

#[test]
fn configured_subscriber_interest_outlives_a_session_that_closes() {
    configured_interest_outlives_the_same_interest_of_a_session(Kind::Subscribers, Cancel::Close);
}

#[test]
fn configured_queryable_interest_outlives_a_session_that_closes() {
    configured_interest_outlives_the_same_interest_of_a_session(Kind::Queryables, Cancel::Close);
}

#[test]
fn configured_token_interest_outlives_a_session_that_closes() {
    configured_interest_outlives_the_same_interest_of_a_session(Kind::Tokens, Cancel::Close);
}

/// The north remote never answers the configured interest, here one that asks for subscribers,
/// queryables and tokens: nothing the bridge sends reaches the cloud, as when the access control
/// drops the interest. Until the deadline the bridge routes as any client whose interest is
/// pending: publications and queries on the keys of the interest go to the uplink. After the
/// deadline they stay south, and the interest is reported unanswered. A late answer finalizes
/// it.
#[test]
fn an_unanswered_interest_stops_sending_north_at_its_deadline() {
    try_init_tracing_subscriber();
    let c = cloud();
    let mut r = Robot::new(impatient_leaf_bridge_with(&all_of(R1)));
    // the bridge itself subscribes: the publications of the application reach the bridge
    let local = r.bridge.new_session();
    local.declare_subscriber(None, 1, "**");
    r.tether.bi_fwd();
    // not forwarded: the cloud never receives the interest
    let mut up = leaf_uplink(&r.bridge, &c);
    assert_eq!(
        interest_states(&r.bridge),
        [Some(UplinkInterestState::Pending)]
    );

    r.app.put(format!("{R1}/internal/joint"), vec![1]);
    r.tether.bi_fwd();
    local.query(1, format!("{R1}/srv/x"));
    let sent = a_sent(&up);
    assert_eq!(sent.pushes, [format!("{R1}/internal/joint")]);
    assert_eq!(sent.requests.len(), 1);

    wait_for_the_deadline(&r.bridge, &[Some(UplinkInterestState::Unanswered)]);
    r.app.put(format!("{R1}/internal/joint"), vec![2]);
    r.tether.bi_fwd();
    local.query(2, format!("{R1}/srv/x"));
    let sent = a_sent(&up);
    assert_eq!(
        sent.pushes.len(),
        1,
        "sent north after the deadline: {:?}",
        sent.pushes
    );
    assert_eq!(sent.requests.len(), 1, "{:?}", sent.requests);
    assert_eq!(seen(&local).pushes.len(), 2, "local delivery goes on");
    // the second query has no destination and is ended at once; the first one is on its way
    assert_eq!(seen(&local).response_finals, 1);

    // the answer arrives late
    settle(&mut [&mut up, &mut r.tether]);
    assert_eq!(
        interest_states(&r.bridge),
        [Some(UplinkInterestState::Finalized)]
    );
    assert_eq!(seen(&local).response_finals, 2);
}

/// The interest that asks for tokens is dropped on the way, as by an access control that denies
/// `declare_liveliness_subscriber` on its key, and the one that asks for subscribers and
/// queryables is answered. The first is reported unanswered after the deadline and the south
/// router does not get the cloud's tokens. The flows of the second are not affected.
#[test]
fn a_dropped_token_interest_is_reported_and_leaves_the_other_interests_alone() {
    try_init_tracing_subscriber();
    let c = cloud();
    let phone = client(&c, CLIENTS);
    phone.declare_subscriber(None, 1, format!("{R1}/tele/**"));
    phone.declare_token(None, 2, format!("{R1}/alive/phone"));
    let mut r = Robot::new(impatient_leaf_bridge_with(&format!(
        r#"{{ key_expr: "{R1}/**", subscribers: true, queryables: true }},
           {{ key_expr: "{R1}/alive/**", tokens: true }}"#
    )));
    let local = r.bridge.new_session();
    local.declare_subscriber(None, 1, "**");
    let mut up = leaf_uplink(&r.bridge, &c);
    let asks_for_tokens = |msg: &Message| matches!(msg, Message::Interest(Interest { options, .. }) if options.tokens());
    let mut dropped = 0;
    loop {
        let before = (
            a_sent(&up).declarations.len(),
            b_sent(&up).declarations.len(),
        );
        dropped += up.fwd_if(|msg| !asks_for_tokens(msg));
        up.rev_fwd();
        r.tether.bi_fwd();
        if before
            == (
                a_sent(&up).declarations.len(),
                b_sent(&up).declarations.len(),
            )
            && up.fwd_if(|msg| !asks_for_tokens(msg)) == 0
            && up.rev_fwd().is_empty()
        {
            break;
        }
    }
    assert_eq!(dropped, 1);
    assert_eq!(
        interest_states(&r.bridge),
        [
            Some(UplinkInterestState::Finalized),
            Some(UplinkInterestState::Pending)
        ]
    );

    wait_for_the_deadline(
        &r.bridge,
        &[
            Some(UplinkInterestState::Finalized),
            Some(UplinkInterestState::Unanswered),
        ],
    );
    let south = a_sent(&r.tether);
    assert_eq!(south.subscribers, set([&format!("{R1}/tele/**"), "**"]));
    assert_eq!(south.tokens, set([]));

    r.app.put(format!("{R1}/tele/vital"), vec![1]);
    r.app.put(format!("{R1}/internal/joint"), vec![2]);
    r.tether.bi_fwd();
    up.fwd_if(|msg| !asks_for_tokens(msg));
    assert_eq!(seen(&phone).pushes, [format!("{R1}/tele/vital")]);
    assert_eq!(a_sent(&up).pushes, [format!("{R1}/tele/vital")]);
}

/// A configured interest has no state when the leaf has no north remote, and is declared anew,
/// pending, on each connection.
#[test]
fn configured_interests_start_pending_on_each_connection() {
    try_init_tracing_subscriber();
    let c = cloud();
    let r = Robot::new(leaf_bridge(R1));
    assert_eq!(interest_states(&r.bridge), [None]);

    for _ in 0..2 {
        let mut up = leaf_uplink(&r.bridge, &c);
        assert_eq!(
            interest_states(&r.bridge),
            [Some(UplinkInterestState::Pending)]
        );
        up.bi_fwd();
        assert_eq!(
            interest_states(&r.bridge),
            [Some(UplinkInterestState::Finalized)]
        );
        up.disconnect();
        assert_eq!(interest_states(&r.bridge), [None]);
    }
}

/// Without `gateway.north`, a client routes as before: a finalized interest of one of its
/// sessions says nothing about a key that the key expression of that interest merely includes.
/// A publication or a query on such a key is still sent north.
#[test]
fn a_client_without_configured_interests_keeps_the_exact_resource_rule() {
    try_init_tracing_subscriber();
    let node = Harness::new_client();
    let north = node.new_face(
        FaceDef::default()
            .region(Region::North)
            .mode(WhatAmI::Router)
            .remote_bound(Bound::South),
    );
    let session = node.new_session();
    let options =
        InterestOptions::KEYEXPRS + InterestOptions::SUBSCRIBERS + InterestOptions::QUERYABLES;
    session.interest(1, InterestMode::CurrentFuture, options, "a/**");
    let interests = north.recorder().interests();
    assert_eq!(interests.len(), 1);
    north.declare_final(interests[0].id);

    session.put("a/b", vec![1]);
    session.query(1, "a/b");

    let sent = seen_from(&north, None);
    assert_eq!(sent.pushes, ["a/b"]);
    assert_eq!(
        sent.requests.iter().map(|(_, k)| k).collect::<Vec<_>>(),
        ["a/b"]
    );
}

// --------------------------------------------------------------------------------------------
// Default route (`gateway.north.default_route`): reaching another robot from any session.
// --------------------------------------------------------------------------------------------

/// The keys the robots of the fleet address each other with: `<robot>/app/...`.
const FLEET: &str = "*/app/**";

fn default_route_of(robot: &str) -> String {
    format!(r#"default_route: {{ key_exprs: ["{FLEET}"], except: ["{robot}/**"] }}"#)
}

/// A leaf bridge that asks for everything of `robot` and sends north, of the rest, the keys of
/// the fleet only.
fn leaf_bridge_with_default_route(robot: &str) -> HarnessBuilder {
    HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([SOUTH_ROUTER, Region::Local])
        .gateway(&format!(
            r#"{{
                north: {{
                    mode: "client",
                    uplinks: [{{ link_protocols: ["quic"] }}],
                    interests: [{}],
                    {},
                }},
                south: [{{ filters: [{{ modes: ["router"] }}] }}],
            }}"#,
            all_of(robot),
            default_route_of(robot)
        ))
}

/// The router south of a bridge, which sends the keys of the fleet that are not those of
/// `robot` towards its gateway.
fn south_router_with_default_route(robot: &str) -> Harness {
    HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::default_south(WhatAmI::Client), Region::Local])
        .gateway(&format!(
            r#"{{ north: {{ {} }} }}"#,
            default_route_of(robot)
        ))
        .build()
}

/// A robot whose two routers have the default route.
fn robot_with_default_route(robot: &str) -> Robot {
    Robot::with(
        leaf_bridge_with_default_route(robot).build(),
        south_router_with_default_route(robot),
    )
}

fn complete() -> QueryableInfoType {
    QueryableInfoType {
        complete: true,
        distance: 0,
    }
}

/// A session two hops south of the uplink of a robot publishes on a key of another robot, whose
/// session, two hops south of its own uplink, receives it once. Neither robot holds a
/// declaration of the other, and the second one needs no default route to receive.
#[test]
fn a_publication_for_another_robot_reaches_it() {
    try_init_tracing_subscriber();
    let c = cloud();
    let (mut r1, mut r2) = (robot_with_default_route(R1), Robot::new(leaf_bridge(R2)));
    let mut up1 = leaf_uplink(&r1.bridge, &c);
    let mut up2 = leaf_uplink(&r2.bridge, &c);
    let key = format!("{R2}/app/control");
    r2.app.declare_subscriber(None, 1, key.clone());
    settle(&mut [&mut r1.tether, &mut r2.tether, &mut up1, &mut up2]);

    r1.app.put(key.clone(), vec![1]);
    settle(&mut [&mut r1.tether, &mut r2.tether, &mut up1, &mut up2]);

    assert_eq!(seen(&r2.app).pushes, std::slice::from_ref(&key));
    assert_eq!(a_sent(&up1).pushes, [key]);
    // the first robot knows nothing of the second one
    assert_eq!(b_sent(&up1).subscribers, set([]));
    assert_eq!(a_sent(&r1.tether).subscribers, set([]));
}

/// The same for a query, with each target: the queryable of the other robot is reached once,
/// its reply comes back, and the query is ended once.
fn a_query_for_another_robot_reaches_it(target: QueryTarget) {
    try_init_tracing_subscriber();
    let c = cloud();
    let (mut r1, mut r2) = (robot_with_default_route(R1), Robot::new(leaf_bridge(R2)));
    let mut up1 = leaf_uplink(&r1.bridge, &c);
    let mut up2 = leaf_uplink(&r2.bridge, &c);
    let key = format!("{R2}/app/srv");
    r2.app.declare_queryable(None, 1, key.clone(), complete());
    settle(&mut [&mut r1.tether, &mut r2.tether, &mut up1, &mut up2]);

    r1.app.query_with_target(3, key.clone(), target);
    settle(&mut [&mut r1.tether, &mut r2.tether, &mut up1, &mut up2]);
    let requests = seen(&r2.app).requests;
    assert_eq!(requests.len(), 1, "{target:?} {requests:?}");
    assert_eq!(requests[0].1, key);
    reply(&r2.app, requests[0].0, &key);
    settle(&mut [&mut r1.tether, &mut r2.tether, &mut up1, &mut up2]);

    let app = seen(&r1.app);
    assert_eq!(app.replies, [key], "{target:?}");
    assert_eq!(app.response_finals, 1, "{target:?}");
    assert_eq!(b_sent(&up1).queryables, set([]));
    assert_eq!(a_sent(&r1.tether).queryables, set([]));
}

#[test]
fn a_query_for_another_robot_reaches_it_with_the_target_all() {
    a_query_for_another_robot_reaches_it(QueryTarget::All);
}

#[test]
fn a_query_for_another_robot_reaches_it_with_the_target_all_complete() {
    a_query_for_another_robot_reaches_it(QueryTarget::AllComplete);
}

#[test]
fn a_query_for_another_robot_reaches_it_with_the_target_best_matching() {
    a_query_for_another_robot_reaches_it(QueryTarget::BestMatching);
}

/// A query for another robot is ended once, without a reply, wherever it stops: no queryable in
/// the cloud, no uplink, no gateway.
#[test]
fn a_query_for_another_robot_that_nobody_answers_is_ended() {
    try_init_tracing_subscriber();
    let c = cloud();
    let mut r1 = robot_with_default_route(R1);
    let mut up1 = leaf_uplink(&r1.bridge, &c);
    settle(&mut [&mut r1.tether, &mut up1]);
    let key = format!("{R2}/app/srv");

    // the cloud has no queryable: it ends the query
    r1.app.query(1, key.clone());
    settle(&mut [&mut r1.tether, &mut up1]);
    assert_eq!(a_sent(&up1).requests.len(), 1);
    assert_eq!(b_sent(&up1).response_finals, 1);
    assert_eq!(seen(&r1.app).response_finals, 1);

    // no uplink: the bridge ends the query
    up1.disconnect();
    r1.app.query(2, key.clone());
    settle(&mut [&mut r1.tether]);
    assert_eq!(b_sent(&r1.tether).requests.len(), 2);
    assert_eq!(a_sent(&r1.tether).response_finals, 2);
    assert_eq!(seen(&r1.app).response_finals, 2);

    // no gateway: the south router ends the query
    r1.tether.disconnect();
    r1.app.query(3, key);
    assert_eq!(seen(&r1.app).response_finals, 3);
    assert_eq!(seen(&r1.app).replies, Vec::<String>::new());
}

/// Nothing local is pulled towards the uplink. Without any subscriber: a key of the robot itself
/// and a key outside of the fleet do not leave the south router, from which only the key of
/// another robot goes to the bridge and to the cloud. A session of the bridge is treated the same
/// way: with a default route, the bridge no longer sends north every key outside its interests.
#[test]
fn local_publications_are_not_pulled_towards_the_uplink() {
    try_init_tracing_subscriber();
    let c = cloud();
    let mut r1 = robot_with_default_route(R1);
    let local = r1.bridge.new_session();
    let mut up1 = leaf_uplink(&r1.bridge, &c);
    settle(&mut [&mut r1.tether, &mut up1]);
    let foreign = format!("{R2}/app/control");

    for session in [&r1.app, &local] {
        session.put(format!("{R1}/app/camera"), vec![1]);
        session.put(format!("{R1}/internal/joint"), vec![2]);
        session.put("rt/camera", vec![3]);
        session.put("app/unscoped", vec![4]);
        session.put(format!("{R2}/internal/joint"), vec![5]);
        session.put(foreign.clone(), vec![6]);
        settle(&mut [&mut r1.tether, &mut up1]);
    }

    // what the south router sent to the bridge, and the bridge to the cloud
    assert_eq!(b_sent(&r1.tether).pushes, std::slice::from_ref(&foreign));
    assert_eq!(a_sent(&up1).pushes, vec![foreign; 2]);
    assert_eq!(a_sent(&r1.tether).pushes, Vec::<String>::new());
}

/// The default gateway that the north hat of a router reaches, if it has one.
fn default_gateway(router: &Harness) -> Option<String> {
    let tables = router.gateway.tables.tables.read().unwrap();
    tables.hats[Region::North]
        .as_any()
        .downcast_ref::<RouterHat>()
        .expect("the north hat of a south router is the router hat")
        .reachable_default_gateway()
        .map(|zid| zid.to_string())
}

/// With a default route, a key of the robot never goes north without a declared destination,
/// whatever the state of the interest that includes it: pending on a first connection and on
/// a reconnection, then unanswered. That holds for a stream of the south router that a
/// subscriber of the bridge pulls to the bridge, for a session of the bridge and for a query.
/// The keys of the other robots keep going north, and a subscriber that the cloud declares is
/// served as soon as it is declared, before the interest is finalized.
#[test]
fn local_keys_do_not_go_north_while_their_interest_is_pending() {
    try_init_tracing_subscriber();
    let c = cloud();
    let phone = client(&c, CLIENTS);
    let (camera, foreign) = (format!("{R1}/app/camera"), format!("{R2}/app/control"));
    phone.declare_subscriber(None, 1, camera.clone());
    let mut r1 = Robot::with(
        leaf_bridge_with_default_route(R1)
            .config(
                "routing/interests/timeout",
                &DEADLINE.as_millis().to_string(),
            )
            .build(),
        south_router_with_default_route(R1),
    );
    let local = r1.bridge.new_session();
    // a recorder on the bridge: the south router sends it the stream of the application
    local.declare_subscriber(None, 1, format!("{R1}/app/**"));
    r1.tether.bi_fwd();

    let mut round = 0;
    let mut local_traffic = |r1: &mut Robot| {
        round += 1;
        r1.app.put(camera.clone(), vec![1]);
        local.put(format!("{R1}/app/state"), vec![2]);
        r1.app.query(round, format!("{R1}/app/srv"));
        local.query(round, format!("{R1}/app/srv"));
        r1.app.put(foreign.clone(), vec![3]);
        r1.tether.bi_fwd();
        // the recorder got the stream, and both queries ended without a reply
        assert_eq!(
            seen(&local).pushes.iter().filter(|k| **k == camera).count(),
            round as usize
        );
        assert_eq!(seen(&r1.app).response_finals, round as usize);
        assert_eq!(seen(&local).response_finals, round as usize);
    };

    // a connection, then a reconnection: the interest is pending, the cloud has not got it
    for _ in 0..2 {
        let up = leaf_uplink(&r1.bridge, &c);
        assert_eq!(
            interest_states(&r1.bridge),
            [Some(UplinkInterestState::Pending)]
        );
        local_traffic(&mut r1);
        assert_eq!(a_sent(&up).pushes, std::slice::from_ref(&foreign));
        assert_eq!(a_sent(&up).requests, vec![]);
        up.disconnect();
    }

    // unanswered
    let up = leaf_uplink(&r1.bridge, &c);
    wait_for_the_deadline(&r1.bridge, &[Some(UplinkInterestState::Unanswered)]);
    local_traffic(&mut r1);
    assert_eq!(a_sent(&up).pushes, std::slice::from_ref(&foreign));
    assert_eq!(a_sent(&up).requests, vec![]);
    up.disconnect();

    // the cloud declares its subscriber and the final is still on its way: the declared
    // subscriber is served, the rest of the robot's keys still stays south
    let mut up = leaf_uplink(&r1.bridge, &c);
    up.fwd();
    let held = up.rev_fwd_if(|msg| {
        !matches!(msg, Message::Declare(declare) if matches!(declare.body, DeclareBody::DeclareFinal(_)))
    });
    assert_eq!(held, 1);
    assert_eq!(
        interest_states(&r1.bridge),
        [Some(UplinkInterestState::Pending)]
    );
    r1.tether.bi_fwd();
    local_traffic(&mut r1);
    up.fwd();
    let mut north = a_sent(&up).pushes;
    north.sort();
    assert_eq!(north, [camera.clone(), foreign.clone()]);
    assert_eq!(a_sent(&up).requests, vec![]);
    assert_eq!(seen(&phone).pushes, [camera]);
}

/// Wildcard publications. One that is inside the keys of the fleet and wider than the robot is
/// delivered to the subscribers of the robot and sent north whole, once: the other robot gets it
/// and the cloud does not send it back. One that only covers keys of the robot stays on the
/// robot. One that is wider than the keys of the fleet is not eligible: it only reaches the
/// declared subscribers.
#[test]
fn a_wildcard_publication_inside_the_fleet_keys_reaches_local_and_remote_subscribers() {
    try_init_tracing_subscriber();
    let c = cloud();
    let (mut r1, mut r2) = (robot_with_default_route(R1), Robot::new(leaf_bridge(R2)));
    let neighbour = app(&r1.south);
    let mut up1 = leaf_uplink(&r1.bridge, &c);
    let mut up2 = leaf_uplink(&r2.bridge, &c);
    neighbour.declare_subscriber(None, 1, format!("{R1}/app/status"));
    r2.app
        .declare_subscriber(None, 1, format!("{R2}/app/status"));
    settle(&mut [&mut r1.tether, &mut r2.tether, &mut up1, &mut up2]);

    // inside the keys of the fleet, the robot's own and the others'
    r1.app.put("*/app/status", vec![1]);
    settle(&mut [&mut r1.tether, &mut r2.tether, &mut up1, &mut up2]);
    assert_eq!(seen(&neighbour).pushes, ["*/app/status"]);
    assert_eq!(seen(&r2.app).pushes, ["*/app/status"]);
    assert_eq!(a_sent(&up1).pushes, ["*/app/status"]);
    assert_eq!(b_sent(&up1).pushes, Vec::<String>::new());

    // only keys of the robot
    r1.app.put(format!("{R1}/app/*"), vec![2]);
    settle(&mut [&mut r1.tether, &mut r2.tether, &mut up1, &mut up2]);
    assert_eq!(seen(&neighbour).pushes.len(), 2);
    assert_eq!(b_sent(&r1.tether).pushes.len(), 1);

    // wider than the keys of the fleet
    r1.app.put("**", vec![3]);
    r1.app.put("*/**", vec![4]);
    settle(&mut [&mut r1.tether, &mut r2.tether, &mut up1, &mut up2]);
    assert_eq!(seen(&neighbour).pushes.len(), 4);
    assert_eq!(b_sent(&r1.tether).pushes.len(), 1);
    assert_eq!(a_sent(&up1).pushes.len(), 1);
    assert_eq!(seen(&r2.app).pushes.len(), 1);
}

/// Wildcard queries. One that is inside the keys of the fleet and wider than the robot reaches
/// the queryables of the robot and those of the other robot, once each; the querier gets every
/// reply and one final. One that is wider than the keys of the fleet stays on the robot.
#[test]
fn a_wildcard_query_inside_the_fleet_keys_reaches_local_and_remote_queryables() {
    try_init_tracing_subscriber();
    let c = cloud();
    let (mut r1, mut r2) = (robot_with_default_route(R1), Robot::new(leaf_bridge(R2)));
    let neighbour = app(&r1.south);
    let mut up1 = leaf_uplink(&r1.bridge, &c);
    let mut up2 = leaf_uplink(&r2.bridge, &c);
    let (status1, status2) = (format!("{R1}/app/status"), format!("{R2}/app/status"));
    neighbour.declare_queryable(None, 1, status1.clone(), complete());
    r2.app
        .declare_queryable(None, 1, status2.clone(), complete());
    settle(&mut [&mut r1.tether, &mut r2.tether, &mut up1, &mut up2]);

    r1.app.query(3, "*/app/status");
    settle(&mut [&mut r1.tether, &mut r2.tether, &mut up1, &mut up2]);
    let (local, remote) = (seen(&neighbour).requests, seen(&r2.app).requests);
    assert_eq!((local.len(), remote.len()), (1, 1), "{local:?} {remote:?}");
    assert_eq!(seen(&r1.app).response_finals, 0);
    reply(&neighbour, local[0].0, &status1);
    reply(&r2.app, remote[0].0, &status2);
    settle(&mut [&mut r1.tether, &mut r2.tether, &mut up1, &mut up2]);
    let app = seen(&r1.app);
    assert_eq!(
        app.replies.into_iter().collect::<BTreeSet<_>>(),
        set([&status1, &status2])
    );
    assert_eq!(app.response_finals, 1);

    // wider than the keys of the fleet: not eligible
    r1.app.query(4, "**");
    r1.app.query(5, "*/**");
    settle(&mut [&mut r1.tether, &mut r2.tether, &mut up1, &mut up2]);
    assert_eq!(seen(&neighbour).requests.len(), 3);
    assert_eq!(seen(&r2.app).requests.len(), 1);
    assert_eq!(b_sent(&r1.tether).requests.len(), 1);

    // only keys of the robot: excluded
    r1.app.query(6, format!("{R1}/app/*"));
    settle(&mut [&mut r1.tether, &mut r2.tether, &mut up1, &mut up2]);
    assert_eq!(seen(&neighbour).requests.len(), 4);
    assert_eq!(seen(&r2.app).requests.len(), 1);
    assert_eq!(b_sent(&r1.tether).requests.len(), 1);
    assert_eq!(a_sent(&up1).requests.len(), 1);
}

/// A publication that comes down from the gateway does not take the default route back, even
/// when its key is eligible: here a session of the bridge publishes on a key of another robot,
/// which a session two hops south subscribed to with a wildcard.
#[test]
fn a_publication_from_the_gateway_is_not_sent_back_to_it() {
    try_init_tracing_subscriber();
    let c = cloud();
    let mut r1 = robot_with_default_route(R1);
    let local = r1.bridge.new_session();
    let mut up1 = leaf_uplink(&r1.bridge, &c);
    r1.app.declare_subscriber(None, 1, FLEET);
    settle(&mut [&mut r1.tether, &mut up1]);
    let key = format!("{R2}/app/log");

    local.put(key.clone(), vec![1]);
    settle(&mut [&mut r1.tether, &mut up1]);

    assert_eq!(seen(&r1.app).pushes, std::slice::from_ref(&key));
    assert_eq!(a_sent(&r1.tether).pushes, std::slice::from_ref(&key));
    assert_eq!(b_sent(&r1.tether).pushes, Vec::<String>::new());
    assert_eq!(a_sent(&up1).pushes, [key]);
}

/// An eligible key that also has a subscriber on the robot goes to both.
#[test]
fn a_publication_for_another_robot_also_reaches_its_local_subscribers() {
    try_init_tracing_subscriber();
    let c = cloud();
    let mut r1 = robot_with_default_route(R1);
    let recorder = app(&r1.south);
    recorder.declare_subscriber(None, 1, FLEET);
    let mut up1 = leaf_uplink(&r1.bridge, &c);
    settle(&mut [&mut r1.tether, &mut up1]);
    let key = format!("{R2}/app/control");

    r1.app.put(key.clone(), vec![1]);
    settle(&mut [&mut r1.tether, &mut up1]);

    assert_eq!(seen(&recorder).pushes, std::slice::from_ref(&key));
    assert_eq!(a_sent(&up1).pushes, [key]);
}

/// The gateway goes away and comes back: a publication for another robot is dropped by the
/// south router in between, and takes the default route again afterwards.
#[test]
fn the_default_route_follows_the_gateway() {
    try_init_tracing_subscriber();
    let c = cloud();
    let mut r1 = robot_with_default_route(R1);
    let mut up1 = leaf_uplink(&r1.bridge, &c);
    settle(&mut [&mut r1.tether, &mut up1]);
    let key = format!("{R2}/app/control");

    r1.app.put(key.clone(), vec![1]);
    settle(&mut [&mut r1.tether, &mut up1]);
    assert_eq!(a_sent(&up1).pushes.len(), 1);
    // what the south router reports as its default gateway
    let bridge = Some(r1.bridge.zid().to_string());
    assert_eq!(default_gateway(&r1.south), bridge);

    r1.tether.disconnect();
    r1.app.put(key.clone(), vec![2]);
    assert_eq!(b_sent(&r1.tether).pushes.len(), 1);
    assert_eq!(default_gateway(&r1.south), None);

    let mut again = tether(&r1.bridge, &r1.south);
    settle(&mut [&mut again, &mut up1]);
    r1.app.put(key.clone(), vec![3]);
    settle(&mut [&mut again, &mut up1]);
    assert_eq!(b_sent(&again).pushes, [key]);
    assert_eq!(a_sent(&up1).pushes.len(), 2);
    assert_eq!(default_gateway(&r1.south), bridge);
}

/// The default gateway is chosen by its ZID, not by what it can reach. Two gateways in the
/// region of the south router, and only the one with the smallest ZID has an uplink:
/// publications and queries for another robot go to the other one and end there. They take the
/// gateway with the uplink while the greatest one is away, and go back to the greatest one
/// when it returns, also on keys whose routes were already computed.
#[test]
fn the_default_gateway_is_the_greatest_one_even_without_an_uplink() {
    try_init_tracing_subscriber();
    let c = cloud();
    let phone = client(&c, CLIENTS);
    let key = format!("{R2}/app/control");
    phone.declare_subscriber(None, 1, key.clone());
    phone.declare_queryable(None, 2, key.clone(), complete());
    let low = leaf_bridge_with_default_route(R1)
        .zid("b1".parse().unwrap())
        .build();
    let high = leaf_bridge_with_default_route(R1)
        .zid("b2".parse().unwrap())
        .build();
    let south = south_router_with_default_route(R1);
    let (mut to_low, mut to_high) = (tether(&low, &south), tether(&high, &south));
    let app = app(&south);
    let mut up_low = leaf_uplink(&low, &c);
    settle(&mut [&mut to_low, &mut to_high, &mut up_low]);
    assert_eq!(default_gateway(&south), Some(high.zid().to_string()));

    // the greatest gateway has no uplink: lost there, and the query ends without a reply
    app.put(key.clone(), vec![1]);
    app.query(1, key.clone());
    settle(&mut [&mut to_low, &mut to_high, &mut up_low]);
    assert_eq!(b_sent(&to_high).pushes, std::slice::from_ref(&key));
    assert_eq!(b_sent(&to_high).requests.len(), 1);
    assert_eq!(b_sent(&to_low).pushes, Vec::<String>::new());
    assert_eq!(b_sent(&to_low).requests, vec![]);
    assert_eq!(seen(&phone).pushes, Vec::<String>::new());
    assert_eq!(seen(&phone).requests, vec![]);
    assert_eq!(seen(&app).response_finals, 1);

    // it goes away: the other gateway, which has an uplink, becomes the default one
    to_high.disconnect();
    settle(&mut [&mut to_low, &mut up_low]);
    assert_eq!(default_gateway(&south), Some(low.zid().to_string()));
    app.put(key.clone(), vec![2]);
    app.query(2, key.clone());
    settle(&mut [&mut to_low, &mut up_low]);
    assert_eq!(seen(&phone).pushes, std::slice::from_ref(&key));
    assert_eq!(seen(&phone).requests.len(), 1);

    // it returns, still without an uplink: it is the default gateway again
    let mut back = tether(&high, &south);
    settle(&mut [&mut to_low, &mut back, &mut up_low]);
    assert_eq!(default_gateway(&south), Some(high.zid().to_string()));
    app.put(key.clone(), vec![3]);
    app.query(3, key.clone());
    settle(&mut [&mut to_low, &mut back, &mut up_low]);
    assert_eq!(b_sent(&back).pushes, std::slice::from_ref(&key));
    assert_eq!(b_sent(&back).requests.len(), 1);
    assert_eq!(seen(&phone).pushes.len(), 1);
    assert_eq!(seen(&phone).requests.len(), 1);
    assert_eq!(b_sent(&to_low).pushes.len(), 1);
}

/// Two gateways in the region of the south router: the default gateway is the one with the
/// greatest ZID, which is also the one that forwards to the north. The cloud gets one copy.
#[test]
fn the_default_gateway_is_the_one_with_the_greatest_zid() {
    try_init_tracing_subscriber();
    let c = cloud();
    let phone = client(&c, CLIENTS);
    let key = format!("{R2}/app/control");
    phone.declare_subscriber(None, 1, key.clone());
    let low = leaf_bridge_with_default_route(R1)
        .zid("b1".parse().unwrap())
        .build();
    let high = leaf_bridge_with_default_route(R1)
        .zid("b2".parse().unwrap())
        .build();
    let south = south_router_with_default_route(R1);
    let (mut to_low, mut to_high) = (tether(&low, &south), tether(&high, &south));
    let app = app(&south);
    let mut up_low = leaf_uplink(&low, &c);
    let mut up_high = leaf_uplink(&high, &c);
    settle(&mut [&mut to_low, &mut to_high, &mut up_low, &mut up_high]);

    app.put(key.clone(), vec![1]);
    settle(&mut [&mut to_low, &mut to_high, &mut up_low, &mut up_high]);

    assert_eq!(b_sent(&to_high).pushes, std::slice::from_ref(&key));
    assert_eq!(b_sent(&to_low).pushes, Vec::<String>::new());
    assert_eq!(seen(&phone).pushes, [key]);
}

/// A robot whose bridge is a leaf and a robot whose bridge is a router of the cloud publish to
/// each other. The south router of the second one has a default route too: its bridge already
/// declared the subscriber, and the publication is delivered once.
#[test]
fn robots_with_a_leaf_and_with_a_router_bridge_publish_to_each_other() {
    try_init_tracing_subscriber();
    let c = cloud();
    let mut leaf = robot_with_default_route(R1);
    let mut router = Robot::with(router_bridge(), south_router_with_default_route(R2));
    let mut up1 = leaf_uplink(&leaf.bridge, &c);
    let mut up2 = router_uplink(&router.bridge, &c);
    let (control1, control2) = (format!("{R1}/app/control"), format!("{R2}/app/control"));
    leaf.app.declare_subscriber(None, 1, control1.clone());
    router.app.declare_subscriber(None, 1, control2.clone());
    settle(&mut [&mut leaf.tether, &mut router.tether, &mut up1, &mut up2]);

    router.app.put(control1.clone(), vec![1]);
    leaf.app.put(control2.clone(), vec![2]);
    settle(&mut [&mut leaf.tether, &mut router.tether, &mut up1, &mut up2]);

    assert_eq!(seen(&leaf.app).pushes, [control1]);
    assert_eq!(seen(&router.app).pushes, [control2]);
}

/// The default direction is not a queryable. A region with a second router that has a complete
/// queryable for a key of another robot: a query with the target `BestMatching` goes to that
/// queryable only, and a query with the target `All` goes to the queryable and to the gateway.
#[test]
fn best_matching_prefers_a_complete_queryable_of_the_region_to_the_default_route() {
    try_init_tracing_subscriber();
    let c = cloud();
    let mut r1 = robot_with_default_route(R1);
    let other = south_router_with_default_route(R1);
    let mut to_other = tether(&r1.bridge, &other);
    // the two south routers are neighbours in their north region
    let mut neighbours = Connection {
        a: &r1.south,
        a2b: FaceDef::default().mode(WhatAmI::Router),
        b: &other,
        b2a: FaceDef::default().mode(WhatAmI::Router),
    }
    .establish();
    let cache = app(&other);
    cache.declare_queryable(None, 1, "*/app/time", complete());
    let mut up1 = leaf_uplink(&r1.bridge, &c);
    settle(&mut [&mut r1.tether, &mut to_other, &mut neighbours, &mut up1]);
    let key = format!("{R2}/app/time");

    r1.app
        .query_with_target(1, key.clone(), QueryTarget::BestMatching);
    settle(&mut [&mut r1.tether, &mut to_other, &mut neighbours, &mut up1]);
    assert_eq!(seen(&cache).requests.len(), 1);
    assert_eq!(b_sent(&r1.tether).requests, vec![]);

    r1.app.query_with_target(2, key, QueryTarget::All);
    settle(&mut [&mut r1.tether, &mut to_other, &mut neighbours, &mut up1]);
    assert_eq!(seen(&cache).requests.len(), 2);
    assert_eq!(b_sent(&r1.tether).requests.len(), 1);
    assert_eq!(a_sent(&up1).requests.len(), 1);
}

/// Without the option, the hats hold no default route.
#[test]
fn there_is_no_default_route_by_default() {
    try_init_tracing_subscriber();
    let c = cloud();
    let mut r1 = Robot::new(leaf_bridge(R1));
    let local = r1.bridge.new_session();
    let mut up1 = leaf_uplink(&r1.bridge, &c);
    settle(&mut [&mut r1.tether, &mut up1]);

    r1.app.put(format!("{R2}/app/control"), vec![1]);
    r1.app.query(1, format!("{R2}/app/srv"));
    local.put("rt/camera", vec![2]);
    settle(&mut [&mut r1.tether, &mut up1]);

    // the south router sends nothing to its gateway
    assert_eq!(b_sent(&r1.tether).pushes, Vec::<String>::new());
    assert_eq!(b_sent(&r1.tether).requests, vec![]);
    assert_eq!(seen(&r1.app).response_finals, 1);
    // a leaf sends north every key outside its interests
    assert_eq!(a_sent(&up1).pushes, ["rt/camera"]);
}
