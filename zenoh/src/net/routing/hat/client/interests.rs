//
// Copyright (c) 2024 ZettaScale Technology
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
use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::Ordering, Arc, Weak},
    time::Duration,
};

#[allow(unused_imports)]
use zenoh_core::polyfill::*;
use zenoh_keyexpr::keyexpr;
use zenoh_protocol::{
    core::Region,
    network::{
        declare::queryable::ext::QueryableInfoType,
        interest::{self, InterestId, InterestMode, InterestOptions},
        Interest,
    },
};
use zenoh_sync::get_mut_unchecked;

use super::{FaceUplinkInterest, Hat, HatFace, UplinkInterestState};
use crate::net::routing::{
    dispatcher::{
        face::{FaceState, InterestState},
        interests::{
            CurrentInterest, CurrentInterestCleanup, PendingCurrentInterest, RemoteInterest,
        },
        region::RegionMap,
        resource::Resource,
        tables::{TablesData, TablesLock},
    },
    gateway::SubscriberInfo,
    hat::{
        DispatcherContext, HatBaseTrait, HatInterestTrait, HatTrait, Remote,
        RouteCurrentDeclareResult, RouteInterestResult,
    },
    RoutingContext,
};
impl Hat {
    /// Declares the configured interests to the new north face and starts their deadline.
    #[tracing::instrument(level = "debug", skip_all, ret)]
    pub(super) fn propagate_uplink_interests(&self, ctx: DispatcherContext) {
        for (res, options) in &self.uplink_interests {
            let id = self
                .face_hat(ctx.src_face)
                .next_id
                .fetch_add(1, Ordering::SeqCst);
            self.face_hat_mut(ctx.src_face)
                .uplink_interests
                .push(FaceUplinkInterest {
                    id,
                    res: res.clone(),
                    options: *options,
                    state: UplinkInterestState::Pending,
                });
            let wire_expr = Resource::decl_key(res, ctx.src_face);
            tracing::debug!(dst = %ctx.src_face, expr = res.expr(), %options, "Uplink interest");
            let sent = ctx
                .src_face
                .primitives
                .send_interest(RoutingContext::with_expr(
                    &mut Interest {
                        id,
                        mode: InterestMode::CurrentFuture,
                        options: *options,
                        wire_expr: Some(wire_expr),
                        ext_qos: interest::ext::QoSType::INTEREST,
                        ext_tstamp: None,
                        ext_nodeid: interest::ext::NodeIdType::DEFAULT,
                    },
                    res.expr().to_string(),
                ));
            if !sent {
                tracing::error!(
                    "Uplink interest '{}' ({}) was not sent to {}: the access control of this \
                     node denies it on egress (an interest that asks for tokens is checked as \
                     declare_liveliness_subscriber on its key expression). This node will miss \
                     the declarations it asks for",
                    res.expr(),
                    options,
                    ctx.src_face.zid,
                );
                if let Some(interest) = self
                    .face_hat_mut(ctx.src_face)
                    .uplink_interests
                    .last_mut()
                {
                    interest.state = UplinkInterestState::Denied;
                }
            }
        }

        UplinkInterestsDeadline::spawn(
            ctx.src_face,
            ctx.tables_lock,
            self.region,
            ctx.tables.interests_timeout,
        );
    }

    /// Returns `true` if a publication (resp. a query) on `key_expr` is only sent to `face`, the
    /// north face, towards a matching subscriber (resp. queryable) that the north remote
    /// declared. `asks_for` tells if an interest asks for that kind of entity.
    ///
    /// + A configured interest includes `key_expr`, asks for the kind and is no longer pending:
    ///   `true`. Finalized, every matching entity of the north remote has been declared to this
    ///   node. Denied, or unanswered at its deadline, this node does not know the entities of
    ///   the north remote, but south traffic must not flow to the uplink for as long as an
    ///   interest stays without an answer.
    /// + Such an interest is pending: `false`, the entities are not known yet.
    /// + No configured interest includes `key_expr`: `false`, unless a default route is
    ///   configured and `key_expr` is not eligible to it.
    ///
    /// Without configured interests and default route, this returns `false`: the hat routes as
    /// the one of any client.
    pub(super) fn stays_south(
        &self,
        face: &FaceState,
        key_expr: &keyexpr,
        asks_for: impl Fn(&InterestOptions) -> bool,
    ) -> bool {
        if self.uplink_interests.is_empty() && self.default_route.is_none() {
            return false;
        }
        let Some(hat_face) = face.hats[self.region].downcast_ref::<HatFace>() else {
            return false;
        };
        let mut pending = false;
        for interest in hat_face.uplink_interests.iter().filter(|interest| {
            asks_for(&interest.options)
                && interest
                    .res
                    .keyexpr()
                    .is_some_and(|interest_ke| interest_ke.includes(key_expr))
        }) {
            if interest.state != UplinkInterestState::Pending {
                return true;
            }
            pending = true;
        }
        !pending
            && self
                .default_route
                .as_ref()
                .is_some_and(|default_route| !default_route.is_eligible(key_expr))
    }

    /// Marks the configured interests of `face` that are still pending as unanswered.
    pub(crate) fn expire_uplink_interests(
        &mut self,
        tables: &mut TablesData,
        face: &mut Arc<FaceState>,
        timeout: Duration,
    ) {
        let mut expired = false;
        let remote = face.zid;
        for interest in &mut self.face_hat_mut(face).uplink_interests {
            if interest.state == UplinkInterestState::Pending {
                interest.state = UplinkInterestState::Unanswered;
                expired = true;
                tracing::error!(
                    "Uplink interest '{}' ({}) was not answered by {} within {:?}: this node may \
                     miss declarations of its north region. Publications and queries on the keys \
                     it includes are now only sent to the uplink towards the declarations received",
                    interest.res.expr(),
                    interest.options,
                    remote,
                    timeout,
                );
            }
        }
        if expired {
            self.disable_all_routes(tables);
        }
    }

    #[tracing::instrument(level = "debug", skip_all, ret)]
    pub(super) fn repropagate_interests(
        &self,
        ctx: DispatcherContext,
        other_hats: &RegionMap<&dyn HatTrait>,
    ) {
        for RemoteInterest { res, options, .. } in other_hats
            .values()
            .flat_map(|hat| hat.remote_interests(ctx.tables))
        {
            let id = self
                .face_hat(ctx.src_face)
                .next_id
                .fetch_add(1, Ordering::SeqCst);
            let face_id = ctx.src_face.id;
            get_mut_unchecked(ctx.src_face)
                .local_interests
                .insert(id, InterestState::new(face_id, options, res.clone(), false));
            let wire_expr = res
                .as_ref()
                .map(|res| Resource::decl_key(res, ctx.src_face));
            ctx.src_face
                .primitives
                .send_interest(RoutingContext::with_expr(
                    &mut Interest {
                        id,
                        mode: InterestMode::CurrentFuture,
                        options,
                        wire_expr,
                        ext_qos: interest::ext::QoSType::INTEREST,
                        ext_tstamp: None,
                        ext_nodeid: interest::ext::NodeIdType::DEFAULT,
                    },
                    res.as_ref()
                        .map(|res| res.expr().to_string())
                        .unwrap_or_default(),
                ));
        }
    }
}

/// Deadline of the configured interests of a north face: those that are still pending when it
/// fires become [`UplinkInterestState::Unanswered`].
struct UplinkInterestsDeadline;

impl UplinkInterestsDeadline {
    fn spawn(
        face: &Arc<FaceState>,
        tables_ref: &Arc<TablesLock>,
        region: Region,
        timeout: Duration,
    ) {
        let tables_ref = tables_ref.clone();
        let weak_face: Weak<FaceState> = Arc::downgrade(face);
        let cancellation_token = face.task_controller.get_cancellation_token();
        face.task_controller
            .spawn_with_rt(zenoh_runtime::ZRuntime::Net, async move {
                tokio::select! {
                    _ = tokio::time::sleep(timeout) => {}
                    _ = cancellation_token.cancelled() => { return; }
                }
                let Some(mut face) = weak_face.upgrade() else {
                    return;
                };
                let _ctrl_lock = zlock!(tables_ref.ctrl_lock);
                let mut wtables = zwrite!(tables_ref.tables);
                let tables = &mut *wtables;
                if !tables.data.faces.contains_key(&face.id) {
                    return;
                }
                if let Some(hat) = tables.hats[region].as_any_mut().downcast_mut::<Hat>() {
                    hat.expire_uplink_interests(&mut tables.data, &mut face, timeout);
                }
            });
    }
}

impl HatInterestTrait for Hat {
    #[tracing::instrument(level = "debug", skip(ctx, msg), fields(%src), ret)]
    fn route_interest(
        &mut self,
        ctx: DispatcherContext,
        msg: &Interest,
        res: Option<Arc<Resource>>,
        src: &Remote,
    ) -> RouteInterestResult {
        use RouteInterestResult::*;

        debug_assert!(self.region().bound().is_north());
        debug_assert!(ctx.src_face.region.bound().is_south());

        let interest = Arc::new(CurrentInterest {
            src: src.clone(),
            src_region: ctx.src_face.region,
            src_interest_id: msg.id,
            mode: msg.mode,
        });

        let interests_timeout = ctx.tables.interests_timeout;

        if let Some(mut dst_face) = self.owned_faces(ctx.tables).next().cloned() {
            let id = self
                .face_hat(&dst_face)
                .next_id
                .fetch_add(1, Ordering::SeqCst);

            if msg.mode.is_future() {
                let dst_face_id = dst_face.id;
                let is_finalized = msg.mode == InterestMode::Future;
                get_mut_unchecked(&mut dst_face).local_interests.insert(
                    id,
                    InterestState::new(dst_face_id, msg.options, res.clone(), is_finalized),
                );
            }

            if msg.mode.is_current() {
                let dst_face_mut = get_mut_unchecked(&mut dst_face);
                let cancellation_token = dst_face_mut.task_controller.get_cancellation_token();
                let rejection_token = dst_face_mut.task_controller.get_cancellation_token();
                dst_face_mut.pending_current_interests.insert(
                    id,
                    PendingCurrentInterest {
                        interest: interest.clone(),
                        cancellation_token,
                        rejection_token,
                    },
                );
                CurrentInterestCleanup::spawn_interest_clean_up_task(
                    &dst_face,
                    ctx.tables_lock,
                    id,
                    interests_timeout,
                );
            }

            let wire_expr = res
                .as_ref()
                .map(|res| Resource::decl_key(res, &mut dst_face));
            dst_face.primitives.send_interest(RoutingContext::with_expr(
                &mut Interest {
                    id,
                    mode: msg.mode,
                    options: msg.options,
                    wire_expr,
                    ext_qos: interest::ext::QoSType::INTEREST,
                    ext_tstamp: None,
                    ext_nodeid: interest::ext::NodeIdType::DEFAULT,
                },
                res.as_ref()
                    .map(|res| res.expr().to_string())
                    .unwrap_or_default(),
            ));
        } else {
            tracing::debug!("Client region is empty");
        }

        if msg.mode.is_current() && Arc::into_inner(interest).is_some() {
            ResolvedCurrentInterest
        } else {
            Noop
        }
    }

    #[tracing::instrument(level = "debug", skip(ctx, _msg), ret)]
    fn route_interest_final(
        &mut self,
        ctx: DispatcherContext,
        _msg: &Interest,
        remote_interest: &RemoteInterest,
    ) {
        debug_assert!(self.region().bound().is_north());
        debug_assert!(ctx.src_face.region.bound().is_south());

        // NOTE(regions): clients have at most one north-bound remote, this invariant is enforced in
        // the orchestrator.
        if let Some(dst_face) = self
            .owned_faces_mut(ctx.tables)
            .next()
            .map(get_mut_unchecked)
        {
            dst_face.local_interests.retain(|id, local_interest| {
                if local_interest == remote_interest {
                    dst_face.primitives.send_interest(RoutingContext::with_expr(
                        &mut Interest {
                            id: *id,
                            mode: InterestMode::Final,
                            // NOTE: InterestMode::Final options are undefined in the current protocol specification,
                            // they are initialized here for internal use by local egress interceptors.
                            options: remote_interest.options,
                            wire_expr: None,
                            ext_qos: interest::ext::QoSType::INTEREST,
                            ext_tstamp: None,
                            ext_nodeid: interest::ext::NodeIdType::DEFAULT,
                        },
                        local_interest
                            .res
                            .as_ref()
                            .map(|res| res.expr().to_string())
                            .unwrap_or_default(),
                    ));
                    local_interest.unset_finalized();
                    return false;
                }
                true
            });
        }
    }

    #[tracing::instrument(level = "debug", skip(ctx), ret)]
    fn route_declare_final(
        &mut self,
        ctx: DispatcherContext,
        interest_id: InterestId,
    ) -> RouteCurrentDeclareResult {
        use RouteCurrentDeclareResult::*;

        debug_assert!(self.region().bound().is_north());
        debug_assert!(ctx.src_face.region.bound().is_north());

        let remote = ctx.src_face.zid;
        if let Some(interest) = self
            .face_hat_mut(ctx.src_face)
            .uplink_interests
            .iter_mut()
            .find(|i| i.id == interest_id)
        {
            match interest.state {
                UplinkInterestState::Pending => tracing::info!(
                    "Uplink interest '{}' ({}) answered by {}",
                    interest.res.expr(),
                    interest.options,
                    remote
                ),
                UplinkInterestState::Unanswered => tracing::warn!(
                    "Uplink interest '{}' ({}) answered by {} after its deadline",
                    interest.res.expr(),
                    interest.options,
                    remote
                ),
                UplinkInterestState::Finalized | UplinkInterestState::Denied => {}
            }
            interest.state = UplinkInterestState::Finalized;
            return NoBreadcrumb;
        }

        let has_local_interest = get_mut_unchecked(ctx.src_face)
            .local_interests
            .get_mut(&interest_id)
            .map(|i| i.set_finalized())
            .is_some();

        let breadcrumb = get_mut_unchecked(ctx.src_face)
            .pending_current_interests
            .remove(&interest_id);

        match (has_local_interest, breadcrumb) {
            (false, None) => {
                tracing::error!("Unknown interest");
                Noop
            }
            (true, None) => NoBreadcrumb,
            (_, Some(breadcrumb)) => {
                let Some(interest) = Arc::into_inner(breadcrumb.interest) else {
                    bug!("Pending current interest with strong count > 1");
                    return Noop;
                };

                breadcrumb.cancellation_token.cancel();
                Breadcrumb { interest }
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(ctx), ret)]
    fn route_current_token(
        &mut self,
        ctx: DispatcherContext,
        interest_id: InterestId,
        _res: Arc<Resource>,
    ) -> RouteCurrentDeclareResult {
        use RouteCurrentDeclareResult::*;

        debug_assert!(self.region().bound().is_north());
        debug_assert!(ctx.src_face.region.bound().is_north());

        let has_local_interest = ctx.src_face.local_interests.contains_key(&interest_id)
            || self
                .face_hat(ctx.src_face)
                .uplink_interests
                .iter()
                .any(|i| i.id == interest_id);

        let breadcrumb = ctx
            .src_face
            .pending_current_interests
            .get(&interest_id)
            .map(|i| i.interest.as_ref().clone());

        match (has_local_interest, breadcrumb) {
            (false, None) => {
                tracing::error!("Unknown interest");
                Noop
            }
            (true, None) => NoBreadcrumb,
            (_, Some(interest)) => Breadcrumb { interest },
        }
    }

    fn send_current_subscribers(
        &self,
        _ctx: DispatcherContext,
        _msg: &Interest,
        _res: Option<Arc<Resource>>,
        _other_matches: HashMap<Arc<Resource>, SubscriberInfo>,
    ) {
        unreachable!("south-bound client hat")
    }

    fn send_current_queryables(
        &self,
        _ctx: DispatcherContext,
        _msg: &Interest,
        _res: Option<Arc<Resource>>,
        _other_matches: HashMap<Arc<Resource>, QueryableInfoType>,
    ) {
        unreachable!("south-bound client hat")
    }

    fn send_current_tokens(
        &self,
        _ctx: DispatcherContext,
        _msg: &Interest,
        _res: Option<Arc<Resource>>,
        _other_matches: HashSet<Arc<Resource>>,
    ) {
        unreachable!("south-bound client hat")
    }

    fn propagate_current_token(
        &self,
        _ctx: DispatcherContext,
        _res: Arc<Resource>,
        _interest: CurrentInterest,
    ) {
        unreachable!("south-bound client hat")
    }

    fn send_declare_final(&mut self, _ctx: DispatcherContext, _id: InterestId, _src: &Remote) {
        unreachable!("south-bound client hat")
    }

    fn register_interest(
        &mut self,
        _ctx: DispatcherContext,
        _msg: &Interest,
        _res: Option<Arc<Resource>>,
    ) {
        unreachable!("south-bound client hat")
    }

    fn unregister_interest(
        &mut self,
        _ctx: DispatcherContext,
        _msg: &Interest,
    ) -> Option<RemoteInterest> {
        unreachable!("south-bound client hat");
    }

    #[tracing::instrument(level = "trace", skip(_tables), ret)]
    fn remote_interests(&self, _tables: &TablesData) -> HashSet<RemoteInterest> {
        HashSet::default()
    }
}
