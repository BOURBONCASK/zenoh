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

//! ⚠️ WARNING ⚠️
//!
//! This module is intended for Zenoh's internal use.
//!
//! [Click here for Zenoh's documentation](https://docs.rs/zenoh/latest/zenoh)
use std::{
    any::Any,
    collections::HashMap,
    mem,
    sync::{atomic::AtomicU32, Arc},
};

use token::{token_new_face, undeclare_simple_token};
use zenoh_config::{unwrap_or_default, ModeDependent, WhatAmI};
use zenoh_protocol::{
    common::ZExtBody,
    network::{
        declare::{
            ext::{NodeIdType, QoSType},
            queryable::ext::QueryableInfoType,
            QueryableId, SubscriberId, TokenId,
        },
        interest::{InterestId, InterestOptions},
        oam::id::OAM_LINKSTATE,
        Declare, DeclareBody, DeclareFinal, Oam,
    },
};
use zenoh_result::ZResult;
use zenoh_sync::get_mut_unchecked;
use zenoh_transport::unicast::TransportUnicast;

use self::{
    interests::interests_new_face,
    pubsub::{pubsub_new_face, undeclare_simple_subscription},
    queries::{queries_new_face, undeclare_simple_queryable},
};
use super::{
    super::dispatcher::{
        face::FaceState,
        interests as dispatcher_interests,
        resource::{
            clear_session_ctxs_for_face, collect_resource_stats, count_session_ctxs_for_face,
            count_tree, dump_session_ctxs_for_face,
        },
        tables::{NodeId, Resource, RoutingExpr, Tables, TablesLock},
    },
    HatBaseTrait, HatTrait, SendDeclare,
};
use crate::net::{
    codec::Zenoh080Routing,
    protocol::{gossip::Gossip, linkstate::LinkStateList},
    routing::{
        dispatcher::{
            face::{Face, InterestState},
            interests::RemoteInterest,
        },
        router::{LocalQueryables, LocalSubscribers},
        RoutingContext,
    },
    runtime::Runtime,
};

mod interests;
mod pubsub;
mod queries;
mod token;

macro_rules! hat {
    ($t:expr) => {
        $t.hat.downcast_ref::<HatTables>().unwrap()
    };
}

macro_rules! hat_mut {
    ($t:expr) => {
        $t.hat.downcast_mut::<HatTables>().unwrap()
    };
}

macro_rules! face_hat {
    ($f:expr) => {
        $f.hat.downcast_ref::<HatFace>().unwrap()
    };
}
use face_hat;

macro_rules! face_hat_mut {
    ($f:expr) => {
        get_mut_unchecked($f).hat.downcast_mut::<HatFace>().unwrap()
    };
}
use face_hat_mut;

use crate::net::common::AutoConnect;

struct HatTables {
    gossip: Option<Gossip>,
}

impl HatTables {
    fn new() -> Self {
        Self { gossip: None }
    }
}

pub(crate) struct HatCode {}

impl HatBaseTrait for HatCode {
    fn init(&self, tables: &mut Tables, runtime: Runtime) -> ZResult<()> {
        let config_guard = runtime.config().lock();
        let config = &config_guard.0;
        let whatami = tables.whatami;
        let gossip = unwrap_or_default!(config.scouting().gossip().enabled());
        let gossip_multihop = unwrap_or_default!(config.scouting().gossip().multihop());
        let gossip_target = *unwrap_or_default!(config.scouting().gossip().target().get(whatami));
        if gossip_target.matches(WhatAmI::Client) {
            bail!("\"client\" is not allowed as gossip target")
        }
        let autoconnect = if gossip {
            AutoConnect::gossip(config, whatami, runtime.zid().into())
        } else {
            AutoConnect::disabled()
        };
        let wait_declares = unwrap_or_default!(config.open().return_conditions().declares());
        let router_peers_failover_brokering =
            unwrap_or_default!(config.routing().router().peers_failover_brokering());
        drop(config_guard);

        if gossip {
            hat_mut!(tables).gossip = Some(Gossip::new(
                "[Gossip]".to_string(),
                tables.zid,
                runtime,
                router_peers_failover_brokering,
                gossip,
                gossip_multihop,
                gossip_target,
                autoconnect,
                wait_declares,
            ));
        }
        Ok(())
    }

    fn new_tables(&self, _router_peers_failover_brokering: bool) -> Box<dyn Any + Send + Sync> {
        Box::new(HatTables::new())
    }

    fn new_face(&self) -> Box<dyn Any + Send + Sync> {
        Box::new(HatFace::new())
    }

    fn new_resource(&self) -> Box<dyn Any + Send + Sync> {
        Box::new(HatContext::new())
    }

    fn new_local_face(
        &self,
        tables: &mut Tables,
        _tables_ref: &Arc<TablesLock>,
        face: &mut Face,
        send_declare: &mut SendDeclare,
    ) -> ZResult<()> {
        interests_new_face(tables, &mut face.state);
        pubsub_new_face(tables, &mut face.state, send_declare);
        queries_new_face(tables, &mut face.state, send_declare);
        token_new_face(tables, &mut face.state, send_declare);
        tables.disable_all_routes();
        Ok(())
    }

    fn new_transport_unicast_face(
        &self,
        tables: &mut Tables,
        _tables_ref: &Arc<TablesLock>,
        face: &mut Face,
        transport: &TransportUnicast,
        send_declare: &mut SendDeclare,
    ) -> ZResult<()> {
        if face.state.whatami != WhatAmI::Client {
            if let Some(net) = hat_mut!(tables).gossip.as_mut() {
                net.add_link(transport.clone());
            }
        }
        if face.state.whatami == WhatAmI::Peer {
            let face_id = face.state.id;
            get_mut_unchecked(&mut face.state).local_interests.insert(
                INITIAL_INTEREST_ID,
                InterestState::new(face_id, InterestOptions::ALL, None, false),
            );
        }

        interests_new_face(tables, &mut face.state);
        pubsub_new_face(tables, &mut face.state, send_declare);
        queries_new_face(tables, &mut face.state, send_declare);
        token_new_face(tables, &mut face.state, send_declare);
        tables.disable_all_routes();

        if face.state.whatami == WhatAmI::Peer {
            send_declare(
                &face.state.primitives,
                RoutingContext::new(Declare {
                    interest_id: Some(0),
                    ext_qos: QoSType::DECLARE,
                    ext_tstamp: None,
                    ext_nodeid: NodeIdType::default(),
                    body: DeclareBody::DeclareFinal(DeclareFinal),
                }),
            );
        }
        Ok(())
    }

    fn close_face(
        &self,
        tables: &TablesLock,
        _tables_ref: &Arc<TablesLock>,
        face: &mut Arc<FaceState>,
        send_declare: &mut SendDeclare,
    ) {
        let (interest_ids, debug_begin) = {
            let face = get_mut_unchecked(face);
            let hat_face = match face.hat.downcast_mut::<HatFace>() {
                Some(hat_face) => hat_face,
                None => {
                    tracing::error!("Error downcasting face hat in close_face!");
                    return;
                }
            };
            let debug = if tracing::enabled!(tracing::Level::DEBUG) {
                Some((
                    face.whatami,
                    face.local_interests.len(),
                    face.remote_key_interests.len(),
                    face.pending_current_interests.len(),
                    face.pending_queries.len(),
                    face.local_mappings.len(),
                    face.remote_mappings.len(),
                    hat_face.remote_interests.len(),
                    hat_face.remote_subs.len(),
                    hat_face.remote_qabls.len(),
                    hat_face.remote_tokens.len(),
                    hat_face.local_subs.simple_resources().count(),
                    hat_face.local_qabls.simple_resources().count(),
                ))
            } else {
                None
            };
            (
                hat_face.remote_interests.keys().cloned().collect::<Vec<_>>(),
                debug,
            )
        };
        if let Some((
            whatami,
            local_interests,
            remote_key_interests,
            pending_current_interests,
            pending_queries,
            local_mappings,
            remote_mappings,
            remote_interests,
            remote_subs,
            remote_qabls,
            remote_tokens,
            local_subs_simple,
            local_qabls_simple,
        )) = debug_begin
        {
            tracing::debug!(
                "{} Close face begin: whatami={:?} local_interests={} remote_key_interests={} pending_current_interests={} pending_queries={} local_mappings={} remote_mappings={} remote_interests={} remote_subs={} remote_qabls={} remote_tokens={} local_subs_simple={} local_qabls_simple={}",
                face,
                whatami,
                local_interests,
                remote_key_interests,
                pending_current_interests,
                pending_queries,
                local_mappings,
                remote_mappings,
                remote_interests,
                remote_subs,
                remote_qabls,
                remote_tokens,
                local_subs_simple,
                local_qabls_simple
            );
            let limit = 50usize;
            let total = face.local_mappings.len();
            tracing::debug!(
                "LOCAL_MAPPINGS face={} total={} limit={}",
                face.id,
                total,
                limit
            );
            for (expr_id, res) in face.local_mappings.iter().take(limit) {
                let ctx = res.session_ctxs.get(&face.id);
                let ctx_local = ctx.and_then(|c| c.local_expr_id);
                let ctx_remote = ctx.and_then(|c| c.remote_expr_id);
                tracing::debug!(
                    "LOCAL_MAPPING face={} expr_id={} res={} ctx_local={:?} ctx_remote={:?}",
                    face.id,
                    expr_id,
                    res.expr(),
                    ctx_local,
                    ctx_remote
                );
            }
        }
        for id in interest_ids {
            dispatcher_interests::undeclare_interest(self, tables, face, id);
        }

        let mut wtables = zwrite!(tables.tables);
        let pre_tree = if tracing::enabled!(tracing::Level::DEBUG) {
            Some(count_tree(&wtables.root_res))
        } else {
            None
        };
        let pre_face_ctx = if tracing::enabled!(tracing::Level::DEBUG) {
            Some(count_session_ctxs_for_face(&wtables.root_res, face.id))
        } else {
            None
        };
        let pre_stats = if tracing::enabled!(tracing::Level::DEBUG) {
            Some(collect_resource_stats(&wtables.root_res, 5))
        } else {
            None
        };
        let mut face_clone = face.clone();
        let face = get_mut_unchecked(face);
        let hat_face = match face.hat.downcast_mut::<HatFace>() {
            Some(hate_face) => hate_face,
            None => {
                tracing::error!("Error downcasting face hat in close_face!");
                return;
            }
        };

        hat_face.remote_interests.clear();
        hat_face.local_subs.clear();
        hat_face.local_qabls.clear();
        hat_face.local_tokens.clear();

        for res in face.remote_mappings.values_mut() {
            get_mut_unchecked(res).session_ctxs.remove(&face.id);
            Resource::clean(res);
        }
        face.remote_mappings.clear();
        for res in face.local_mappings.values_mut() {
            get_mut_unchecked(res).session_ctxs.remove(&face.id);
            Resource::clean(res);
        }
        face.local_mappings.clear();

        let mut subs_matches = vec![];
        for (_id, mut res) in hat_face.remote_subs.drain() {
            get_mut_unchecked(&mut res).session_ctxs.remove(&face.id);
            undeclare_simple_subscription(&mut wtables, &mut face_clone, &mut res, send_declare);

            if res.context.is_some() {
                for match_ in &res.context().matches {
                    let mut match_ = match_.upgrade().unwrap();
                    if !Arc::ptr_eq(&match_, &res) {
                        get_mut_unchecked(&mut match_)
                            .context_mut()
                            .disable_data_routes();
                        subs_matches.push(match_);
                    }
                }
                get_mut_unchecked(&mut res)
                    .context_mut()
                    .disable_data_routes();
                subs_matches.push(res);
            }
        }

        let mut qabls_matches = vec![];
        for (_id, (mut res, _)) in hat_face.remote_qabls.drain() {
            get_mut_unchecked(&mut res).session_ctxs.remove(&face.id);
            undeclare_simple_queryable(&mut wtables, &mut face_clone, &mut res, send_declare);

            if res.context.is_some() {
                for match_ in &res.context().matches {
                    let mut match_ = match_.upgrade().unwrap();
                    if !Arc::ptr_eq(&match_, &res) {
                        get_mut_unchecked(&mut match_)
                            .context_mut()
                            .disable_query_routes();
                        qabls_matches.push(match_);
                    }
                }
                get_mut_unchecked(&mut res)
                    .context_mut()
                    .disable_query_routes();
                qabls_matches.push(res);
            }
        }

        let mut tokens = vec![];
        for (_id, mut res) in hat_face.remote_tokens.drain() {
            get_mut_unchecked(&mut res).session_ctxs.remove(&face.id);
            undeclare_simple_token(&mut wtables, &mut face_clone, &mut res, send_declare);
            tokens.push(res);
        }

        for mut res in subs_matches {
            get_mut_unchecked(&mut res)
                .context_mut()
                .disable_data_routes();
            Resource::clean(&mut res);
        }
        for mut res in qabls_matches {
            get_mut_unchecked(&mut res)
                .context_mut()
                .disable_query_routes();
            Resource::clean(&mut res);
        }
        for mut res in tokens {
            Resource::clean(&mut res);
        }

        let sweep_removed = clear_session_ctxs_for_face(&mut wtables.root_res, face.id);
        if sweep_removed > 0 && tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                "SESSION_CTX_SWEEP face={} removed={}",
                face.id,
                sweep_removed
            );
        }
        wtables.faces.remove(&face.id);
        if let Some((nodes, contexts, session_ctxs)) = pre_tree {
            let (post_nodes, post_contexts, post_session_ctxs) = count_tree(&wtables.root_res);
            let (pre_face_total, pre_face_unused) = pre_face_ctx.unwrap_or((0, 0));
            let (post_face_total, post_face_unused) =
                count_session_ctxs_for_face(&wtables.root_res, face.id);
            tracing::debug!(
                "{} Close face end: faces={} res_nodes={} res_ctxs={} res_session_ctxs={} post_res_nodes={} post_res_ctxs={} post_res_session_ctxs={} face_ctxs={} face_unused_ctxs={} post_face_ctxs={} post_face_unused_ctxs={}",
                face,
                wtables.faces.len(),
                nodes,
                contexts,
                session_ctxs,
                post_nodes,
                post_contexts,
                post_session_ctxs,
                pre_face_total,
                pre_face_unused,
                post_face_total,
                post_face_unused
            );
            if let Some(pre_stats) = pre_stats {
                let post_stats = collect_resource_stats(&wtables.root_res, 5);
                tracing::debug!(
                    "RES_STATS pre: nodes={} ctxs={} sess={} ros2_lv_nodes={} ros2_lv_ctxs={} ros2_lv_sess={} ros2_lv_guids={} ros2_lv_guid_min={} ros2_lv_guid_max={} ros2_lv_guid_top={:?} ros2_nodes={} ros2_ctxs={} ros2_sess={} data_nodes={} data_ctxs={} data_sess={}",
                    pre_stats.nodes,
                    pre_stats.contexts,
                    pre_stats.session_ctxs,
                    pre_stats.ros2_lv_nodes,
                    pre_stats.ros2_lv_contexts,
                    pre_stats.ros2_lv_session_ctxs,
                    pre_stats.ros2_lv_guid_total,
                    pre_stats.ros2_lv_guid_min,
                    pre_stats.ros2_lv_guid_max,
                    pre_stats.ros2_lv_guid_top,
                    pre_stats.ros2_nodes,
                    pre_stats.ros2_contexts,
                    pre_stats.ros2_session_ctxs,
                    pre_stats.data_nodes,
                    pre_stats.data_contexts,
                    pre_stats.data_session_ctxs
                );
                tracing::debug!(
                    "RES_STATS post: nodes={} ctxs={} sess={} ros2_lv_nodes={} ros2_lv_ctxs={} ros2_lv_sess={} ros2_lv_guids={} ros2_lv_guid_min={} ros2_lv_guid_max={} ros2_lv_guid_top={:?} ros2_nodes={} ros2_ctxs={} ros2_sess={} data_nodes={} data_ctxs={} data_sess={}",
                    post_stats.nodes,
                    post_stats.contexts,
                    post_stats.session_ctxs,
                    post_stats.ros2_lv_nodes,
                    post_stats.ros2_lv_contexts,
                    post_stats.ros2_lv_session_ctxs,
                    post_stats.ros2_lv_guid_total,
                    post_stats.ros2_lv_guid_min,
                    post_stats.ros2_lv_guid_max,
                    post_stats.ros2_lv_guid_top,
                    post_stats.ros2_nodes,
                    post_stats.ros2_contexts,
                    post_stats.ros2_session_ctxs,
                    post_stats.data_nodes,
                    post_stats.data_contexts,
                    post_stats.data_session_ctxs
                );
            }
            if post_face_total > 0 {
                let limit = 50;
                let total = dump_session_ctxs_for_face(&wtables.root_res, face.id, limit);
                tracing::debug!(
                    "SESSION_CTX_REMAIN_SUMMARY face={} total={} limit={}",
                    face.id,
                    total,
                    limit
                );
            }
        }

        if face.whatami != WhatAmI::Client {
            if let Some(net) = hat_mut!(wtables).gossip.as_mut() {
                net.remove_link(&face.zid);
            }
        };
        drop(wtables);
    }

    fn handle_oam(
        &self,
        tables: &mut Tables,
        _tables_ref: &Arc<TablesLock>,
        oam: &mut Oam,
        transport: &TransportUnicast,
        _send_declare: &mut SendDeclare,
    ) -> ZResult<()> {
        if oam.id == OAM_LINKSTATE {
            if let ZExtBody::ZBuf(buf) = mem::take(&mut oam.body) {
                if let Ok(zid) = transport.get_zid() {
                    let whatami = transport.get_whatami()?;
                    if whatami != WhatAmI::Client {
                        if let Some(net) = hat_mut!(tables).gossip.as_mut() {
                            use zenoh_buffers::reader::HasReader;
                            use zenoh_codec::RCodec;
                            let codec = Zenoh080Routing::new();
                            let mut reader = buf.reader();
                            let Ok(list): Result<LinkStateList, _> = codec.read(&mut reader) else {
                                bail!("failed to decode link state");
                            };

                            net.link_states(list.link_states, zid, whatami);
                        }
                    };
                }
            }
        }

        Ok(())
    }

    #[inline]
    fn map_routing_context(
        &self,
        _tables: &Tables,
        _face: &FaceState,
        _routing_context: NodeId,
    ) -> NodeId {
        0
    }

    #[inline]
    fn ingress_filter(&self, _tables: &Tables, _face: &FaceState, _expr: &RoutingExpr) -> bool {
        true
    }

    #[inline]
    fn egress_filter(
        &self,
        _tables: &Tables,
        src_face: &FaceState,
        out_face: &Arc<FaceState>,
        _expr: &RoutingExpr,
    ) -> bool {
        src_face.id != out_face.id
            && (out_face.mcast_group.is_none()
                || (src_face.whatami == WhatAmI::Client && src_face.mcast_group.is_none()))
    }

    fn info(&self, tables: &Tables, _kind: WhatAmI) -> String {
        hat!(tables)
            .gossip
            .as_ref()
            .map(|net| net.dot())
            .unwrap_or_else(|| "graph {}".to_string())
    }
}

struct HatContext {}

impl HatContext {
    fn new() -> Self {
        Self {}
    }
}

struct HatFace {
    next_id: AtomicU32, // @TODO: manage rollover and uniqueness
    remote_interests: HashMap<InterestId, RemoteInterest>,
    local_subs: LocalSubscribers,
    remote_subs: HashMap<SubscriberId, Arc<Resource>>,
    local_tokens: HashMap<Arc<Resource>, TokenId>,
    remote_tokens: HashMap<TokenId, Arc<Resource>>,
    local_qabls: LocalQueryables,
    remote_qabls: HashMap<QueryableId, (Arc<Resource>, QueryableInfoType)>,
}

impl HatFace {
    fn new() -> Self {
        Self {
            next_id: AtomicU32::new(1), // In p2p, id 0 is erserved for initial interest
            remote_interests: HashMap::new(),
            local_subs: LocalSubscribers::new(),
            remote_subs: HashMap::new(),
            local_tokens: HashMap::new(),
            remote_tokens: HashMap::new(),
            local_qabls: LocalQueryables::new(),
            remote_qabls: HashMap::new(),
        }
    }
}

impl HatTrait for HatCode {}

// In p2p, at connection, while no interest is sent on the network,
// peers act as if they received an interest CurrentFuture with id 0
// and send back a DeclareFinal with interest_id 0.
// This 'ghost' interest is registered locally to allow tracking if
// the DeclareFinal has been received or not (finalized).

const INITIAL_INTEREST_ID: u32 = 0;

#[inline]
fn initial_interest(face: &FaceState) -> Option<&InterestState> {
    face.local_interests.get(&INITIAL_INTEREST_ID)
}

#[inline]
pub(super) fn push_declaration_profile(face: &FaceState) -> bool {
    face.whatami != WhatAmI::Client
}
