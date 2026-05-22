//
// Copyright (c) 2023 ZettaScale Technology
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
use std::sync::Arc;

use zenoh_config::Config;
use zenoh_core::zlock;
use zenoh_protocol::{
    core::{key_expr::keyexpr, ExprId, Region, Reliability, WhatAmI, WireExpr, EMPTY_EXPR_ID},
    network::{
        declare::queryable::ext::QueryableInfoType,
        ext::{self, NodeIdType},
        request::ext::QueryTarget,
        Declare, DeclareBody, DeclareKeyExpr, Mapping, Push, Request, Response, UndeclareKeyExpr,
    },
    zenoh::{PushBody, Put, RequestBody, ResponseBody},
};

use crate::{
    key_expr::KeyExpr,
    net::{
        primitives::{DummyPrimitives, EPrimitives, Primitives},
        routing::{
            dispatcher::{
                face::{Face, FaceState},
                pubsub::SubscriberInfo,
                queries::route_send_response,
                tables::TablesData,
            },
            gateway::*,
            RoutingContext,
        },
    },
};

fn new_router() -> Gateway {
    let mut config = Config::default().expanded();
    config.set_mode(Some(WhatAmI::Client)).unwrap();
    GatewayBuilder::new(&config)
        .subregions(vec![Region::Local])
        .build()
        .unwrap()
}

#[test]
fn base_test() {
    let router = new_router();
    let tables = router.tables.clone();

    let primitives = Arc::new(DummyPrimitives {});
    let face = router.new_session(primitives);
    register_expr(&tables, &mut face.state.clone(), 1, &"one/two/three".into());

    register_expr(
        &tables,
        &mut face.state.clone(),
        2,
        &"one/deux/trois".into(),
    );

    let sub_info = SubscriberInfo;

    face.declare_subscriber(
        0,
        &WireExpr::from(1).with_suffix("four/five"),
        &sub_info,
        NodeId::default(),
        &mut |p, m| {
            m.with_mut(|m| {
                p.send_declare(m);
            })
        },
    );

    TablesData::print(&zread!(tables.tables).data);
}

#[test]
fn match_test() {
    let key_exprs = [
        "**",
        "a",
        "a/b",
        "*",
        "a/*",
        "a/b$*",
        "abc",
        "xx",
        "ab$*",
        "abcd",
        "ab$*d",
        "ab",
        "ab/*",
        "a/*/c/*/e",
        "a/b/c/d/e",
        "a/$*b/c/$*d/e",
        "a/xb/c/xd/e",
        "a/c/e",
        "a/b/c/d/x/e",
        "ab$*cd",
        "abxxcxxd",
        "abxxcxxcd",
        "abxxcxxcdx",
        "a/b/c",
        "ab/**",
        "**/xyz",
        "a/b/xyz/d/e/f/xyz",
        "**/xyz$*xyz",
        "a/b/xyz/d/e/f/xyz",
        "a/**/c/**/e",
        "a/b/b/b/c/d/d/d/e",
        "a/**/c/*/e/*",
        "a/b/b/b/c/d/d/c/d/e/f",
        "a/**/c/*/e/*",
        "x/abc",
        "x/*",
        "x/abc$*",
        "x/$*abc",
        "x/a$*",
        "x/a$*de",
        "x/abc$*de",
        "x/a$*d$*e",
        "x/a$*e",
        "x/a$*c$*e",
        "x/ade",
        "x/c$*",
        "x/$*d",
        "x/$*e",
        "@a",
        "**/@a",
        "@a/b",
    ]
    .map(|s| keyexpr::new(s).unwrap());

    let router = new_router();
    let tables = router.tables.clone();

    let primitives = Arc::new(DummyPrimitives {});
    let face = Arc::downgrade(&router.new_session(primitives).state);
    for (i, key_expr) in key_exprs.iter().enumerate() {
        register_expr(
            &tables,
            &mut face.upgrade().unwrap(),
            i.try_into().unwrap(),
            &(*key_expr).into(),
        );
    }

    for key_expr1 in key_exprs.iter() {
        let res_matches = Resource::get_matches(&zread!(tables.tables).data, key_expr1);
        for key_expr2 in key_exprs.iter() {
            if res_matches
                .iter()
                .any(|m| m.upgrade().unwrap().expr() == key_expr2.as_str())
            {
                assert!(dbg!(dbg!(key_expr1).intersects(dbg!(key_expr2))));
            } else {
                assert!(!dbg!(dbg!(key_expr1).intersects(dbg!(key_expr2))));
            }
        }
    }
}

#[test]
fn multisub_test() {
    let router = new_router();
    let tables = router.tables.clone();

    let primitives = Arc::new(DummyPrimitives {});
    let face0 = &router.new_session(primitives);

    // --------------
    let sub_info = SubscriberInfo;
    face0.declare_subscriber(
        0,
        &"sub".into(),
        &sub_info,
        NodeId::default(),
        &mut |p, m| {
            m.with_mut(|m| {
                p.send_declare(m);
            })
        },
    );
    let optres = Resource::get_resource(zread!(tables.tables).data._get_root(), "sub")
        .map(|res| Arc::downgrade(&res));
    assert!(optres.is_some());
    let res = optres.unwrap();
    assert!(res.upgrade().is_some());

    face0.declare_subscriber(
        1,
        &"sub".into(),
        &sub_info,
        NodeId::default(),
        &mut |p, m| {
            m.with_mut(|m| {
                p.send_declare(m);
            })
        },
    );
    assert!(res.upgrade().is_some());

    face0.undeclare_subscriber(0, &WireExpr::empty(), NodeId::default(), &mut |p, m| {
        m.with_mut(|m| {
            p.send_declare(m);
        })
    });
    assert!(res.upgrade().is_some());

    face0.undeclare_subscriber(1, &WireExpr::empty(), NodeId::default(), &mut |p, m| {
        m.with_mut(|m| {
            p.send_declare(m);
        })
    });
    assert!(res.upgrade().is_none());

    face0.send_close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn clean_test() {
    let router = new_router();
    let tables = router.tables.clone();

    let primitives = Arc::new(DummyPrimitives {});
    let face0 = &router.new_session(primitives);

    // --------------
    register_expr(&tables, &mut face0.state.clone(), 1, &"todrop1".into());
    let optres1 = Resource::get_resource(zread!(tables.tables).data._get_root(), "todrop1")
        .map(|res| Arc::downgrade(&res));
    assert!(optres1.is_some());
    let res1 = optres1.unwrap();
    assert!(res1.upgrade().is_some());

    register_expr(
        &tables,
        &mut face0.state.clone(),
        2,
        &"todrop1/todrop11".into(),
    );
    let optres2 =
        Resource::get_resource(zread!(tables.tables).data._get_root(), "todrop1/todrop11")
            .map(|res| Arc::downgrade(&res));
    assert!(optres2.is_some());
    let res2 = optres2.unwrap();
    assert!(res2.upgrade().is_some());

    register_expr(&tables, &mut face0.state.clone(), 3, &"**".into());
    let optres3 = Resource::get_resource(zread!(tables.tables).data._get_root(), "**")
        .map(|res| Arc::downgrade(&res));
    assert!(optres3.is_some());
    let res3 = optres3.unwrap();
    assert!(res3.upgrade().is_some());

    unregister_expr(&tables, &mut face0.state.clone(), 1);
    assert!(res1.upgrade().is_some());
    assert!(res2.upgrade().is_some());
    assert!(res3.upgrade().is_some());

    unregister_expr(&tables, &mut face0.state.clone(), 2);
    assert!(res1.upgrade().is_none());
    assert!(res2.upgrade().is_none());
    assert!(res3.upgrade().is_some());

    unregister_expr(&tables, &mut face0.state.clone(), 3);
    assert!(res1.upgrade().is_none());
    assert!(res2.upgrade().is_none());
    assert!(res3.upgrade().is_none());

    // --------------
    register_expr(&tables, &mut face0.state.clone(), 1, &"todrop1".into());
    let optres1 = Resource::get_resource(zread!(tables.tables).data._get_root(), "todrop1")
        .map(|res| Arc::downgrade(&res));
    assert!(optres1.is_some());
    let res1 = optres1.unwrap();
    assert!(res1.upgrade().is_some());

    let sub_info = SubscriberInfo;

    face0.declare_subscriber(
        0,
        &"todrop1/todrop11".into(),
        &sub_info,
        NodeId::default(),
        &mut |p, m| {
            m.with_mut(|m| {
                p.send_declare(m);
            })
        },
    );
    let optres2 =
        Resource::get_resource(zread!(tables.tables).data._get_root(), "todrop1/todrop11")
            .map(|res| Arc::downgrade(&res));
    assert!(optres2.is_some());
    let res2 = optres2.unwrap();
    assert!(res2.upgrade().is_some());

    face0.declare_subscriber(
        1,
        &WireExpr::from(1).with_suffix("/todrop12"),
        &sub_info,
        NodeId::default(),
        &mut |p, m| {
            m.with_mut(|m| {
                p.send_declare(m);
            })
        },
    );
    let optres3 =
        Resource::get_resource(zread!(tables.tables).data._get_root(), "todrop1/todrop12")
            .map(|res| Arc::downgrade(&res));
    assert!(optres3.is_some());
    let res3 = optres3.unwrap();
    println!("COUNT: {}", res3.strong_count());
    assert!(res3.upgrade().is_some());

    face0.undeclare_subscriber(1, &WireExpr::empty(), NodeId::default(), &mut |p, m| {
        m.with_mut(|m| {
            p.send_declare(m);
        })
    });

    println!("COUNT2: {}", res3.strong_count());

    assert!(res1.upgrade().is_some());
    assert!(res2.upgrade().is_some());
    assert!(res3.upgrade().is_none());

    face0.undeclare_subscriber(0, &WireExpr::empty(), NodeId::default(), &mut |p, m| {
        m.with_mut(|m| {
            p.send_declare(m);
        })
    });
    assert!(res1.upgrade().is_some());
    assert!(res2.upgrade().is_none());
    assert!(res3.upgrade().is_none());

    unregister_expr(&tables, &mut face0.state.clone(), 1);
    assert!(res1.upgrade().is_none());
    assert!(res2.upgrade().is_none());
    assert!(res3.upgrade().is_none());

    // --------------
    register_expr(&tables, &mut face0.state.clone(), 2, &"todrop3".into());
    face0.declare_subscriber(
        2,
        &"todrop3".into(),
        &sub_info,
        NodeId::default(),
        &mut |p, m| {
            m.with_mut(|m| {
                p.send_declare(m);
            })
        },
    );
    let optres1 = Resource::get_resource(zread!(tables.tables).data._get_root(), "todrop3")
        .map(|res| Arc::downgrade(&res));
    assert!(optres1.is_some());
    let res1 = optres1.unwrap();
    assert!(res1.upgrade().is_some());

    face0.undeclare_subscriber(2, &WireExpr::empty(), NodeId::default(), &mut |p, m| {
        m.with_mut(|m| {
            p.send_declare(m);
        })
    });
    assert!(res1.upgrade().is_some());

    unregister_expr(&tables, &mut face0.state.clone(), 2);
    assert!(res1.upgrade().is_none());

    // --------------
    register_expr(&tables, &mut face0.state.clone(), 3, &"todrop4".into());
    register_expr(&tables, &mut face0.state.clone(), 4, &"todrop5".into());
    register_expr(&tables, &mut face0.state.clone(), 5, &"todrop6".into());
    face0.declare_subscriber(
        3,
        &WireExpr::from(4),
        &sub_info,
        NodeId::default(),
        &mut |p, m| {
            m.with_mut(|m| {
                p.send_declare(m);
            })
        },
    );
    face0.declare_subscriber(
        4,
        &"todrop7".into(),
        &sub_info,
        NodeId::default(),
        &mut |p, m| {
            m.with_mut(|m| {
                p.send_declare(m);
            })
        },
    );
    face0.declare_token(
        5,
        &WireExpr::from(4),
        NodeId::default(),
        None,
        &mut |p, m| {
            m.with_mut(|m| {
                p.send_declare(m);
            })
        },
    );
    face0.declare_token(
        4,
        &"todrop8".into(),
        NodeId::default(),
        None,
        &mut |p, m| {
            m.with_mut(|m| {
                p.send_declare(m);
            })
        },
    );

    let optres1 = Resource::get_resource(zread!(tables.tables).data._get_root(), "todrop4")
        .map(|res| Arc::downgrade(&res));
    assert!(optres1.is_some());
    let res1 = optres1.unwrap();
    let optres2 = Resource::get_resource(zread!(tables.tables).data._get_root(), "todrop5")
        .map(|res| Arc::downgrade(&res));
    assert!(optres2.is_some());
    let res2 = optres2.unwrap();
    let optres3 = Resource::get_resource(zread!(tables.tables).data._get_root(), "todrop6")
        .map(|res| Arc::downgrade(&res));
    assert!(optres3.is_some());
    let res3 = optres3.unwrap();
    let optres4 = Resource::get_resource(zread!(tables.tables).data._get_root(), "todrop7")
        .map(|res| Arc::downgrade(&res));
    assert!(optres4.is_some());
    let res4 = optres4.unwrap();
    let optres5 = Resource::get_resource(zread!(tables.tables).data._get_root(), "todrop8")
        .map(|res| Arc::downgrade(&res));
    assert!(optres5.is_some());
    let res5 = optres5.unwrap();

    assert!(res1.upgrade().is_some());
    assert!(res2.upgrade().is_some());
    assert!(res3.upgrade().is_some());
    assert!(res4.upgrade().is_some());
    assert!(res5.upgrade().is_some());

    face0.send_close();
    assert!(res1.upgrade().is_none());
    assert!(res2.upgrade().is_none());
    assert!(res3.upgrade().is_none());
    assert!(res4.upgrade().is_none());
    assert!(res5.upgrade().is_none());
}

pub struct ClientPrimitives {
    data: std::sync::Mutex<Option<WireExpr<'static>>>,
    mapping: std::sync::Mutex<std::collections::HashMap<ExprId, String>>,
}

impl ClientPrimitives {
    pub fn new() -> ClientPrimitives {
        ClientPrimitives {
            data: std::sync::Mutex::new(None),
            mapping: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub fn clear_data(&self) {
        *self.data.lock().unwrap() = None;
    }
}

impl Default for ClientPrimitives {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientPrimitives {
    fn get_name(&self, key_expr: &WireExpr) -> String {
        let mapping = self.mapping.lock().unwrap();
        let (scope, suffix) = key_expr.as_id_and_suffix();
        if scope == EMPTY_EXPR_ID {
            suffix.to_string()
        } else if suffix.is_empty() {
            mapping.get(&scope).unwrap().clone()
        } else {
            format!("{}{}", mapping.get(&scope).unwrap(), suffix)
        }
    }

    fn get_last_name(&self) -> Option<String> {
        self.data
            .lock()
            .unwrap()
            .as_ref()
            .map(|data| self.get_name(data))
    }

    #[allow(dead_code)]
    fn get_last_key(&self) -> Option<WireExpr<'_>> {
        self.data.lock().unwrap().as_ref().cloned()
    }
}

impl Primitives for ClientPrimitives {
    fn send_interest(&self, _msg: &mut zenoh_protocol::network::Interest) {}

    fn send_declare(&self, msg: &mut zenoh_protocol::network::Declare) {
        match &msg.body {
            DeclareBody::DeclareKeyExpr(d) => {
                let name = self.get_name(&d.wire_expr);
                zlock!(self.mapping).insert(d.id, name);
            }
            DeclareBody::UndeclareKeyExpr(u) => {
                zlock!(self.mapping).remove(&u.id);
            }
            _ => (),
        }
    }

    fn send_push_consume(
        &self,
        msg: &mut zenoh_protocol::network::Push,
        _reliability: Reliability,
        _consume: bool,
    ) {
        *zlock!(self.data) = Some(msg.wire_expr.to_owned());
    }

    fn send_request(&self, msg: &mut zenoh_protocol::network::Request) {
        *zlock!(self.data) = Some(msg.wire_expr.to_owned())
    }

    fn send_response(&self, msg: &mut zenoh_protocol::network::Response) {
        *zlock!(self.data) = Some(msg.wire_expr.to_owned())
    }

    fn send_response_final(&self, _msg: &mut zenoh_protocol::network::ResponseFinal) {}

    fn send_close(&self) {}

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// `SlowPrimitives` is a deterministic stand-in for a back-pressured peer.
/// Every call to `send_push` sleeps for a configurable duration before
/// returning, simulating a `pipeline.push_network_message` that is waiting
/// out the configured `wait_before_close` for the peer's transmission
/// pipeline to drain. Combined with several fast destinations in the same
/// fan-out, this reproduces — deterministically and host-state-independent —
/// the worst-case publisher fan-out latency that issue #2581 surfaces in
/// the 50-peer p2p churn workload.
pub struct SlowPrimitives {
    delay: std::time::Duration,
    saw_push: std::sync::atomic::AtomicUsize,
}

impl SlowPrimitives {
    pub fn new(delay: std::time::Duration) -> Self {
        Self {
            delay,
            saw_push: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn push_count(&self) -> usize {
        self.saw_push.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Primitives for SlowPrimitives {
    fn send_interest(&self, _msg: &mut zenoh_protocol::network::Interest) {}
    fn send_declare(&self, _msg: &mut zenoh_protocol::network::Declare) {}
    fn send_push_consume(
        &self,
        _msg: &mut zenoh_protocol::network::Push,
        _reliability: Reliability,
        _consume: bool,
    ) {
        std::thread::sleep(self.delay);
        self.saw_push
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn send_request(&self, _msg: &mut zenoh_protocol::network::Request) {}
    fn send_response(&self, _msg: &mut zenoh_protocol::network::Response) {}
    fn send_response_final(&self, _msg: &mut zenoh_protocol::network::ResponseFinal) {}
    fn send_close(&self) {}
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl EPrimitives for SlowPrimitives {
    fn send_interest(&self, _ctx: RoutingContext<&mut zenoh_protocol::network::Interest>) -> bool {
        false
    }
    fn send_declare(&self, _ctx: RoutingContext<&mut zenoh_protocol::network::Declare>) -> bool {
        false
    }
    fn send_push(
        &self,
        _msg: &mut zenoh_protocol::network::Push,
        _reliability: Reliability,
    ) -> bool {
        std::thread::sleep(self.delay);
        self.saw_push
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        false
    }
    fn send_request(&self, _msg: &mut zenoh_protocol::network::Request) -> bool {
        false
    }
    fn send_response(&self, _msg: &mut zenoh_protocol::network::Response) -> bool {
        false
    }
    fn send_response_final(&self, _msg: &mut zenoh_protocol::network::ResponseFinal) -> bool {
        false
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl EPrimitives for ClientPrimitives {
    fn send_interest(&self, _ctx: RoutingContext<&mut zenoh_protocol::network::Interest>) -> bool {
        false
    }

    fn send_declare(&self, ctx: RoutingContext<&mut zenoh_protocol::network::Declare>) -> bool {
        match &ctx.msg.body {
            DeclareBody::DeclareKeyExpr(d) => {
                let name = self.get_name(&d.wire_expr);
                zlock!(self.mapping).insert(d.id, name);
            }
            DeclareBody::UndeclareKeyExpr(u) => {
                zlock!(self.mapping).remove(&u.id);
            }
            _ => (),
        }
        false
    }

    fn send_push(
        &self,
        msg: &mut zenoh_protocol::network::Push,
        _reliability: Reliability,
    ) -> bool {
        *zlock!(self.data) = Some(msg.wire_expr.to_owned());
        false
    }

    fn send_request(&self, msg: &mut zenoh_protocol::network::Request) -> bool {
        *zlock!(self.data) = Some(msg.wire_expr.to_owned());
        false
    }

    fn send_response(&self, msg: &mut zenoh_protocol::network::Response) -> bool {
        *zlock!(self.data) = Some(msg.wire_expr.to_owned());
        false
    }

    fn send_response_final(&self, _msg: &mut zenoh_protocol::network::ResponseFinal) -> bool {
        false
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[test]
fn test_response_wireexpr() {
    let router = new_router();
    let tables = router.tables.clone();
    let primitives0 = Arc::new(ClientPrimitives::new());
    let face0 = &router.new_session(primitives0.clone());

    let primitives1 = Arc::new(ClientPrimitives::new());
    let face1 = &router.new_session(primitives1.clone());

    let qinfo = QueryableInfoType {
        complete: true,
        distance: 1,
    };
    register_expr(
        &tables,
        &mut face0.state.clone(),
        11,
        &"test/queryable/reply".into(),
    );

    Primitives::send_declare(
        primitives0.as_ref(),
        &mut Declare {
            interest_id: None,
            ext_qos: ext::QoSType::DECLARE,
            ext_tstamp: None,
            ext_nodeid: ext::NodeIdType::DEFAULT,
            body: DeclareBody::DeclareKeyExpr(DeclareKeyExpr {
                id: 11,
                wire_expr: "test/queryable/reply".into(),
            }),
        },
    );

    register_expr(
        &tables,
        &mut face1.state.clone(),
        12,
        &"test/queryable".into(),
    );

    Primitives::send_declare(
        primitives1.as_ref(),
        &mut Declare {
            interest_id: None,
            ext_qos: ext::QoSType::DECLARE,
            ext_tstamp: None,
            ext_nodeid: ext::NodeIdType::DEFAULT,
            body: DeclareBody::DeclareKeyExpr(DeclareKeyExpr {
                id: 12,
                wire_expr: "test/queryable".into(),
            }),
        },
    );

    face0.declare_queryable(
        0,
        &WireExpr::from(11).with_suffix("/**"),
        &qinfo,
        NodeId::default(),
        &mut |p, m| {
            m.with_mut(|m| {
                p.send_declare(m);
            })
        },
    );

    face1.route_query(&mut Request {
        id: 1,
        wire_expr: "test/queryable/reply/*".into(),
        payload: RequestBody::Query(zenoh_protocol::zenoh::query::Query::default()),
        ext_qos: zenoh_protocol::network::request::ext::QoSType::default(),
        ext_tstamp: None,
        ext_nodeid: NodeIdType::DEFAULT,
        ext_target: QueryTarget::All,
        ext_budget: None,
        ext_timeout: None,
    });

    route_send_response(
        &tables,
        &mut face0.state.clone(),
        &mut Response {
            rid: 1,
            wire_expr: WireExpr {
                scope: 11,
                suffix: "/1".into(),
                mapping: Mapping::Sender,
            },
            payload: ResponseBody::Reply(zenoh_protocol::zenoh::reply::Reply {
                consolidation: zenoh_protocol::zenoh::ConsolidationMode::None,
                ext_unknown: Vec::default(),
                payload: PushBody::Put(zenoh_protocol::zenoh::put::Put::default()),
            }),
            ext_qos: zenoh_protocol::network::response::ext::QoSType::default(),
            ext_tstamp: None,
            ext_respid: None,
        },
    );
    assert_eq!(
        primitives1.get_last_name().unwrap(),
        "test/queryable/reply/1"
    );
    let we = primitives1.get_last_key().unwrap();
    // TODO: replace asserts with the ones commented below once optimization of reply wireexpr in route_send_response is enabled
    assert_eq!(we.suffix, "test/queryable/reply/1");
    assert_eq!(we.scope, 0);
    /*assert_eq!(we.suffix, "/reply/1");
    assert_eq!(we.scope, 12);
    assert_eq!(we.mapping, Mapping::Receiver);*/

    // unregister receiver mapping and validate that we is still correct
    unregister_expr(&tables, &mut face1.state.clone(), 12);

    Primitives::send_declare(
        primitives1.as_ref(),
        &mut Declare {
            interest_id: None,
            ext_qos: ext::QoSType::DECLARE,
            ext_tstamp: None,
            ext_nodeid: ext::NodeIdType::DEFAULT,
            body: DeclareBody::UndeclareKeyExpr(UndeclareKeyExpr { id: 12 }),
        },
    );

    route_send_response(
        &tables,
        &mut face0.state.clone(),
        &mut Response {
            rid: 1,
            wire_expr: WireExpr {
                scope: 11,
                suffix: "/1".into(),
                mapping: Mapping::Sender,
            },
            payload: ResponseBody::Reply(zenoh_protocol::zenoh::reply::Reply {
                consolidation: zenoh_protocol::zenoh::ConsolidationMode::None,
                ext_unknown: Vec::default(),
                payload: PushBody::Put(zenoh_protocol::zenoh::put::Put::default()),
            }),
            ext_qos: zenoh_protocol::network::response::ext::QoSType::default(),
            ext_tstamp: None,
            ext_respid: None,
        },
    );
    assert_eq!(
        primitives1.get_last_name().unwrap(),
        "test/queryable/reply/1"
    );
    let we = primitives1.get_last_key().unwrap();
    assert_eq!(we.suffix, "test/queryable/reply/1");
    assert_eq!(we.scope, 0);
}

#[test]
fn client_test() {
    let router = new_router();
    let tables = router.tables.clone();

    let sub_info = SubscriberInfo;

    let primitives0 = Arc::new(ClientPrimitives::new());
    let face0 = router.new_session(primitives0.clone());
    register_expr(&tables, &mut face0.state.clone(), 11, &"test/client".into());
    Primitives::send_declare(
        primitives0.as_ref(),
        &mut Declare {
            interest_id: None,
            ext_qos: ext::QoSType::DECLARE,
            ext_tstamp: None,
            ext_nodeid: ext::NodeIdType::DEFAULT,
            body: DeclareBody::DeclareKeyExpr(DeclareKeyExpr {
                id: 11,
                wire_expr: "test/client".into(),
            }),
        },
    );
    face0.declare_subscriber(
        0,
        &WireExpr::from(11).with_suffix("/**"),
        &sub_info,
        NodeId::default(),
        &mut |p, m| {
            m.with_mut(|m| {
                p.send_declare(m);
            })
        },
    );
    register_expr(
        &tables,
        &mut face0.state.clone(),
        12,
        &WireExpr::from(11).with_suffix("/z1_pub1"),
    );
    Primitives::send_declare(
        primitives0.as_ref(),
        &mut Declare {
            interest_id: None,
            ext_qos: ext::QoSType::DECLARE,
            ext_tstamp: None,
            ext_nodeid: ext::NodeIdType::DEFAULT,
            body: DeclareBody::DeclareKeyExpr(DeclareKeyExpr {
                id: 12,
                wire_expr: WireExpr::from(11).with_suffix("/z1_pub1"),
            }),
        },
    );

    let primitives1 = Arc::new(ClientPrimitives::new());
    let face1 = router.new_session(primitives1.clone());
    register_expr(&tables, &mut face1.state.clone(), 21, &"test/client".into());
    Primitives::send_declare(
        primitives1.as_ref(),
        &mut Declare {
            interest_id: None,
            ext_qos: ext::QoSType::DECLARE,
            ext_tstamp: None,
            ext_nodeid: ext::NodeIdType::DEFAULT,
            body: DeclareBody::DeclareKeyExpr(DeclareKeyExpr {
                id: 21,
                wire_expr: "test/client".into(),
            }),
        },
    );
    face1.declare_subscriber(
        0,
        &WireExpr::from(21).with_suffix("/**"),
        &sub_info,
        NodeId::default(),
        &mut |p, m| {
            m.with_mut(|m| {
                p.send_declare(m);
            })
        },
    );
    register_expr(
        &tables,
        &mut face1.state.clone(),
        22,
        &WireExpr::from(21).with_suffix("/z2_pub1"),
    );
    Primitives::send_declare(
        primitives1.as_ref(),
        &mut Declare {
            interest_id: None,
            ext_qos: ext::QoSType::DECLARE,
            ext_tstamp: None,
            ext_nodeid: ext::NodeIdType::DEFAULT,
            body: DeclareBody::DeclareKeyExpr(DeclareKeyExpr {
                id: 22,
                wire_expr: WireExpr::from(21).with_suffix("/z2_pub1"),
            }),
        },
    );

    let primitives2 = Arc::new(ClientPrimitives::new());
    let face2 = router.new_session(primitives2.clone());
    register_expr(&tables, &mut face2.state.clone(), 31, &"test/client".into());
    Primitives::send_declare(
        primitives2.as_ref(),
        &mut Declare {
            interest_id: None,
            ext_qos: ext::QoSType::DECLARE,
            ext_tstamp: None,
            ext_nodeid: ext::NodeIdType::DEFAULT,
            body: DeclareBody::DeclareKeyExpr(DeclareKeyExpr {
                id: 31,
                wire_expr: "test/client".into(),
            }),
        },
    );
    face2.declare_subscriber(
        0,
        &WireExpr::from(31).with_suffix("/**"),
        &sub_info,
        NodeId::default(),
        &mut |p, m| {
            m.with_mut(|m| {
                p.send_declare(m);
            })
        },
    );

    primitives0.clear_data();
    primitives1.clear_data();
    primitives2.clear_data();

    let route_dummy_data = |face: &Arc<FaceState>, wire_expr| {
        route_data(
            &tables,
            face,
            &mut Push {
                wire_expr,
                ..Put::default().into()
            },
            Reliability::Reliable,
            true,
        );
    };

    route_dummy_data(&face0.state, "test/client/z1_wr1".into());

    // functional check
    assert!(primitives1.get_last_name().is_some());
    assert_eq!(primitives1.get_last_name().unwrap(), "test/client/z1_wr1");
    // mapping strategy check
    // assert_eq!(primitives1.get_last_key().unwrap(), KeyExpr::IdWithSuffix(21, "/z1_wr1".to_string()));

    // functional check
    assert!(primitives2.get_last_name().is_some());
    assert_eq!(primitives2.get_last_name().unwrap(), "test/client/z1_wr1");
    // mapping strategy check
    // assert_eq!(primitives2.get_last_key().unwrap(), KeyExpr::IdWithSuffix(31, "/z1_wr1".to_string()));

    primitives0.clear_data();
    primitives1.clear_data();
    primitives2.clear_data();
    route_dummy_data(&face0.state, WireExpr::from(11).with_suffix("/z1_wr2"));

    // functional check
    assert!(primitives1.get_last_name().is_some());
    assert_eq!(primitives1.get_last_name().unwrap(), "test/client/z1_wr2");
    // mapping strategy check
    // assert_eq!(primitives1.get_last_key().unwrap(), KeyExpr::IdWithSuffix(21, "/z1_wr2".to_string()));

    // functional check
    assert!(primitives2.get_last_name().is_some());
    assert_eq!(primitives2.get_last_name().unwrap(), "test/client/z1_wr2");
    // mapping strategy check
    // assert_eq!(primitives2.get_last_key().unwrap(), KeyExpr::IdWithSuffix(31, "/z1_wr2".to_string()));

    primitives0.clear_data();
    primitives1.clear_data();
    primitives2.clear_data();
    route_dummy_data(&face1.state, "test/client/**".into());

    // functional check
    assert!(primitives0.get_last_name().is_some());
    assert_eq!(primitives0.get_last_name().unwrap(), "test/client/**");
    // mapping strategy check
    // assert_eq!(primitives1.get_last_key().unwrap(), KeyExpr::IdWithSuffix(11, "/**".to_string()));

    // functional check
    assert!(primitives2.get_last_name().is_some());
    assert_eq!(primitives2.get_last_name().unwrap(), "test/client/**");
    // mapping strategy check
    // assert_eq!(primitives2.get_last_key().unwrap(), KeyExpr::IdWithSuffix(31, "/**".to_string()));

    primitives0.clear_data();
    primitives1.clear_data();
    primitives2.clear_data();
    route_dummy_data(&face0.state, 12.into());

    // functional check
    assert!(primitives1.get_last_name().is_some());
    assert_eq!(primitives1.get_last_name().unwrap(), "test/client/z1_pub1");
    // mapping strategy check
    // assert_eq!(primitives1.get_last_key().unwrap(), KeyExpr::IdWithSuffix(21, "/z1_pub1".to_string()));

    // functional check
    assert!(primitives2.get_last_name().is_some());
    assert_eq!(primitives2.get_last_name().unwrap(), "test/client/z1_pub1");
    // mapping strategy check
    // assert_eq!(primitives2.get_last_key().unwrap(), KeyExpr::IdWithSuffix(31, "/z1_pub1".to_string()));

    primitives0.clear_data();
    primitives1.clear_data();
    primitives2.clear_data();
    route_dummy_data(&face1.state, 22.into());

    // functional check
    assert!(primitives0.get_last_name().is_some());
    assert_eq!(primitives0.get_last_name().unwrap(), "test/client/z2_pub1");
    // mapping strategy check
    // assert_eq!(primitives1.get_last_key().unwrap(), KeyExpr::IdWithSuffix(11, "/z2_pub1".to_string()));

    // functional check
    assert!(primitives2.get_last_name().is_some());
    assert_eq!(primitives2.get_last_name().unwrap(), "test/client/z2_pub1");
    // mapping strategy check
    // assert_eq!(primitives2.get_last_key().unwrap(), KeyExpr::IdWithSuffix(31, "/z2_pub1".to_string()));
}

#[test]
fn get_best_key_test() {
    let router = new_router();

    let primitives = Arc::new(DummyPrimitives {});
    let face1 = router.new_session(primitives.clone());
    let face2 = router.new_session(primitives.clone());
    let face3 = router.new_session(primitives);

    let root = zread!(router.tables.tables).data._get_root().clone();
    let register_expr = |face: &Face, id: ExprId, expr: &str| {
        register_expr(&router.tables, &mut face.state.clone(), id, &expr.into());
    };
    let get_best_key = |resource, suffix, face: &Face| {
        Resource::get_resource(&root, resource)
            .unwrap()
            .get_best_key(suffix, face.state.id)
    };

    register_expr(&face1, 1, "a");
    register_expr(&face2, 2, "a/b");
    register_expr(&face2, 3, "a/b/c");
    register_expr(&face3, 4, "a/d");

    macro_rules! assert_wire_expr {
        ($key:expr, {scope: $scope:expr, suffix: $suffix:expr}) => {
            assert_eq!($key.scope, $scope);
            assert_eq!($key.suffix, $suffix);
        };
    }
    assert_wire_expr!(get_best_key("", "a", &face1), { scope: 1, suffix: "" });
    assert_wire_expr!(get_best_key("", "a/b", &face1), { scope: 1, suffix: "/b" });
    assert_wire_expr!(get_best_key("a", "", &face1), { scope: 1, suffix: "" });
    assert_wire_expr!(get_best_key("a", "/b", &face1), { scope: 1, suffix: "/b" });
    assert_wire_expr!(get_best_key("a/b", "", &face1), { scope: 1, suffix: "/b" });
    assert_wire_expr!(get_best_key("", "e", &face1), { scope: 0, suffix: "e" });
    assert_wire_expr!(get_best_key("", "a", &face2), { scope: 0, suffix: "a" });
    assert_wire_expr!(get_best_key("", "a/b", &face2), { scope: 2, suffix: "" });
    assert_wire_expr!(get_best_key("", "a/b/c", &face2), { scope: 3, suffix: "" });
    assert_wire_expr!(get_best_key("", "a/b/c/d", &face2), { scope: 3, suffix: "/d" });
    assert_wire_expr!(get_best_key("a", "", &face2), { scope: 0, suffix: "a" });
    assert_wire_expr!(get_best_key("a", "/b", &face2), { scope: 2, suffix: "" });
    assert_wire_expr!(get_best_key("a", "/d", &face2), { scope: 0, suffix: "a/d" });
    assert_wire_expr!(get_best_key("a/b", "", &face2), { scope: 2, suffix: "" });
}

#[test]
fn big_key_expr() {
    let router = new_router();

    let primitives = Arc::new(DummyPrimitives {});
    let face = router.new_session(primitives.clone());

    let root = zread!(router.tables.tables).data._get_root().clone();
    let key_expr = KeyExpr::new(vec!["a/"; 10000].concat() + "a").unwrap();
    let wire_expr = WireExpr::from(&**key_expr);
    register_expr(&router.tables, &mut face.state.clone(), 1, &wire_expr);
    let res = Resource::get_resource(&root, &key_expr).unwrap();
    root.get_best_key(&key_expr, face.state.id);
    res.get_best_key("/a", face.state.id + 1);
    Resource::get_matches(&face.tables.tables.read().unwrap().data, &key_expr);
}

/// Deterministic microbench for the publisher fan-out path.
///
/// Builds N=24 destination faces — K=6 of them use `SlowPrimitives` with a
/// 200 ms artificial delay in `send_push` (simulating a back-pressured
/// peer's pipeline). Calls `route_data` once and measures wall time.
///
///   - With the upstream serial fan-out: 6 × 200 ms ≈ 1.2 s
///   - With concurrent fan-out (#9):    ≈ 200 ms (max across destinations)
///
/// This reproduces the architectural issue behind issue #2581's 30-second
/// `publisher.put().wait()` outliers without needing a live transport or
/// a specific host state. Marked `#[ignore]` because the slow path takes
/// >1 s; run with `cargo test --release route_data_parallel_fanout_microbench -- --ignored --nocapture`.
#[test]
#[ignore]
fn route_data_parallel_fanout_microbench() {
    use std::time::{Duration, Instant};

    let router = new_router();
    let tables_lock = router.tables.clone();
    let sub_info = SubscriberInfo;

    // Sender face — single fast publisher.
    let primitives_src = Arc::new(ClientPrimitives::new());
    let face_src = router.new_session(primitives_src.clone());
    register_expr(
        &tables_lock,
        &mut face_src.state.clone(),
        11,
        &"microbench".into(),
    );
    Primitives::send_declare(
        primitives_src.as_ref(),
        &mut Declare {
            interest_id: None,
            ext_qos: ext::QoSType::DECLARE,
            ext_tstamp: None,
            ext_nodeid: ext::NodeIdType::DEFAULT,
            body: DeclareBody::DeclareKeyExpr(DeclareKeyExpr {
                id: 11,
                wire_expr: "microbench".into(),
            }),
        },
    );

    const N_TOTAL: usize = 24;
    const N_SLOW: usize = 6;
    const SLOW_DELAY: Duration = Duration::from_millis(200);
    let mut slow_handles: Vec<Arc<SlowPrimitives>> = Vec::with_capacity(N_SLOW);
    let mut fast_handles: Vec<Arc<ClientPrimitives>> = Vec::with_capacity(N_TOTAL - N_SLOW);

    for i in 0..N_TOTAL {
        if i < N_SLOW {
            let p = Arc::new(SlowPrimitives::new(SLOW_DELAY));
            let face = router.new_session(p.clone());
            face.declare_subscriber(
                0,
                &"microbench/**".into(),
                &sub_info,
                NodeId::default(),
                &mut |p, m| {
                    m.with_mut(|m| {
                        p.send_declare(m);
                    })
                },
            );
            slow_handles.push(p);
        } else {
            let p = Arc::new(ClientPrimitives::new());
            let face = router.new_session(p.clone());
            face.declare_subscriber(
                0,
                &"microbench/**".into(),
                &sub_info,
                NodeId::default(),
                &mut |p, m| {
                    m.with_mut(|m| {
                        p.send_declare(m);
                    })
                },
            );
            fast_handles.push(p);
        }
    }

    // Warm up: prime data routes so the first put doesn't pay the
    // route-cache-miss cost. Use a separate key so the warm-up doesn't
    // hit our slow destinations.
    route_data(
        &tables_lock,
        &face_src.state.clone(),
        &mut Push {
            wire_expr: "microbench/warmup".into(),
            ..Put::default().into()
        },
        Reliability::Reliable,
        true,
    );

    // Actual measurement.
    let start = Instant::now();
    route_data(
        &tables_lock,
        &face_src.state.clone(),
        &mut Push {
            wire_expr: "microbench/x".into(),
            ..Put::default().into()
        },
        Reliability::Reliable,
        true,
    );
    let elapsed = start.elapsed();

    let serial_lower_bound = SLOW_DELAY * (N_SLOW as u32);
    let parallel_lower_bound = SLOW_DELAY;

    println!(
        "route_data fan-out to {N_TOTAL} dests ({N_SLOW} slow @ {SLOW_DELAY:?}): elapsed {elapsed:?}"
    );
    println!("  serial lower bound:   {serial_lower_bound:?}");
    println!("  parallel lower bound: {parallel_lower_bound:?}");
    println!(
        "  slow destinations actually received: {}",
        slow_handles
            .iter()
            .map(|p| p.push_count())
            .sum::<usize>()
    );

    // Either path should deliver to every slow destination at least once.
    for (i, slow) in slow_handles.iter().enumerate() {
        assert!(
            slow.push_count() >= 1,
            "slow destination {i} did not receive any push"
        );
    }
}
