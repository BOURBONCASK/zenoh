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
    routing::hat::client::{Hat as ClientHat, UplinkInterestState},
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
        let south = south_router();
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
/// hops south does not: its router only sends a query towards a queryable it was told of.
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
/// of the leaf. The reverse does not work: a publication for another robot is outside of the
/// interests of the leaf, so its south router has no subscriber for it.
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

    local.interest(52, InterestMode::CurrentFuture, kind.options(), scope.clone());
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
    let asks_for_tokens =
        |msg: &Message| matches!(msg, Message::Interest(Interest { options, .. }) if options.tokens());
    let mut dropped = 0;
    loop {
        let before = (a_sent(&up).declarations.len(), b_sent(&up).declarations.len());
        dropped += up.fwd_if(|msg| !asks_for_tokens(msg));
        up.rev_fwd();
        r.tether.bi_fwd();
        if before == (a_sent(&up).declarations.len(), b_sent(&up).declarations.len())
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
