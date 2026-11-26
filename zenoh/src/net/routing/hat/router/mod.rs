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
    collections::{HashMap, HashSet},
    fmt::Debug,
    iter, mem,
    sync::{atomic::Ordering::Relaxed, Arc, Weak},
    time::{Duration, Instant},
};

use itertools::Itertools;
use petgraph::graph::NodeIndex;
use zenoh_config::{gateway::GatewaySouthConf, unwrap_or_default, ModeDependent, WhatAmI};
use zenoh_protocol::{
    common::ZExtBody,
    core::{Region, ZenohIdProto},
    network::{declare::queryable::ext::QueryableInfoType, oam::id::OAM_LINKSTATE, Oam},
};
use zenoh_result::ZResult;
use zenoh_sync::get_mut_unchecked;
use zenoh_task::TerminatableTask;
use zenoh_transport::unicast::TransportUnicast;

use super::{
    super::dispatcher::{
        face::FaceState,
        tables::{NodeId, Resource, TablesData, TablesLock},
    },
    HatBaseTrait, HatTrait,
};
use crate::net::{
    codec::Zenoh080Routing,
    protocol::{
        linkstate::{link_weights_from_config, LinkStateList},
        network::{LinkId, Network},
        ROUTERS_NET_NAME,
    },
    routing::{
        dispatcher::{queries::merge_qabl_infos, region::RegionMap},
        gateway::DEFAULT_NODE_ID,
        hat::{
            DispatcherContext, Remote, UnregisterFaceEntitiesResult, TREES_COMPUTATION_DELAY_MS,
        },
    },
    runtime::Runtime,
};

mod interests;
mod pubsub;
mod queries;
mod scope;
mod token;

#[cfg(all(test, feature = "test"))]
pub(crate) use scope::TRANSIT_GRACE;

use crate::net::{common::AutoConnect, protocol::network::SuccessorEntry};

struct TreesComputationWorker {
    _task: TerminatableTask,
    tx: flume::Sender<Arc<TablesLock>>,
}

impl TreesComputationWorker {
    fn new(region: Region) -> Self {
        let (tx, rx) = flume::bounded::<Arc<TablesLock>>(1);
        let task = TerminatableTask::spawn_abortable(zenoh_runtime::ZRuntime::Net, async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(
                    *TREES_COMPUTATION_DELAY_MS,
                ))
                .await;

                if let Ok(tables_ref) = rx.recv_async().await {
                    let mut wtables = zwrite!(tables_ref.tables);
                    let tables = &mut *wtables;

                    tables.hats[region]
                        .as_any_mut()
                        .downcast_mut::<Hat>()
                        .unwrap()
                        .do_compute_trees(&mut tables.data);
                }
            }
        });
        Self { _task: task, tx }
    }
}

pub(crate) struct Hat {
    region: Region,
    router_subs: HashSet<Arc<Resource>>,
    /// All tokens declared by routers in the network.
    router_tokens: HashSet<Arc<Resource>>,
    router_qabls: HashSet<Arc<Resource>>,
    routers_net: Option<Network>,
    /// The declaration scope of this region (`gateway.south[].declaration_scope`), if any.
    declaration_scope: Option<scope::DeclarationScope>,
    /// The scoped faces that are a transit next hop, see [`Hat::check_leaves`].
    transit: TransitCounts,
    /// How long a scoped face that is a transit next hop stays filtered:
    /// [`scope::TRANSIT_GRACE`], or what a test sets.
    transit_grace: Duration,
    /// When the pending re-check of the faces in their grace period is due, if one is pending.
    /// There is at most one per hat; see [`Hat::schedule_transit_recheck`].
    transit_recheck: Option<Instant>,
    /// The tables of this hat, for the re-check. Set when a scoped face is created.
    tables_ref: Weak<TablesLock>,
    /// A computation of the trees is scheduled and has not run yet: the trees and the transit
    /// state of the faces may be outdated. Only maintained with a declaration scope; set by
    /// [`Hat::compute_trees_async`], cleared by [`Hat::do_compute_trees`], both under the tables
    /// lock.
    trees_outdated: bool,
    routers_trees_worker: TreesComputationWorker,
    /// Tests only: the time, if the test controls it.
    #[cfg(test)]
    clock: Option<Instant>,
    #[cfg(test)]
    disable_async_tree_computation: bool,
    /// Tests only: never compute trees on topology changes; the test runs
    /// [`Hat::compute_trees_now`] itself.
    #[cfg(test)]
    manual_tree_computation: bool,
}

/// The scoped faces that are a transit next hop towards a router outside of their scope, see
/// [`Hat::check_leaves`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(all(test, feature = "test")), allow(dead_code))]
pub(crate) struct TransitCounts {
    /// Faces in their grace period: they are still filtered.
    pub(crate) pending: usize,
    /// Faces that are no longer filtered.
    pub(crate) violating: usize,
    /// How many times a face was no longer filtered at the end of its grace period.
    pub(crate) failed_open: usize,
    /// How many times a face was a leaf again before it was unfiltered: normally within its grace
    /// period, later if the evaluation at its end was delayed.
    pub(crate) transient: usize,
}

impl Debug for Hat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.region)
    }
}

impl Hat {
    pub(crate) fn new(region: Region) -> Self {
        Self {
            region,
            router_subs: HashSet::new(),
            router_qabls: HashSet::new(),
            router_tokens: HashSet::new(),
            routers_net: None,
            declaration_scope: None,
            transit: TransitCounts::default(),
            transit_grace: scope::TRANSIT_GRACE,
            transit_recheck: None,
            tables_ref: Weak::new(),
            trees_outdated: false,
            routers_trees_worker: TreesComputationWorker::new(region),
            #[cfg(test)]
            clock: None,
            #[cfg(test)]
            disable_async_tree_computation: false,
            #[cfg(test)]
            manual_tree_computation: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_disable_async_tree_computation(&mut self, value: bool) {
        self.disable_async_tree_computation = value;
    }

    #[cfg(all(test, feature = "test"))]
    pub(crate) fn set_manual_tree_computation(&mut self, value: bool) {
        self.manual_tree_computation = value;
    }

    #[cfg(all(test, feature = "test"))]
    pub(crate) fn compute_trees_now(&mut self, tables: &mut TablesData) {
        self.do_compute_trees(tables);
    }

    /// Tests only: the counts of the scoped face of `zid` (see [`scope::FaceScope::counts`]),
    /// whether it is a transit next hop and whether it is still filtered.
    #[cfg(all(test, feature = "test"))]
    pub(crate) fn declaration_scope_of(
        &self,
        tables: &TablesData,
        zid: &ZenohIdProto,
    ) -> Option<([u64; 3], bool, bool)> {
        let scope = self.face_hat(self.face(tables, zid)?).scope.as_ref()?;
        Some((scope.counts(), scope.transit.is_some(), !scope.failed_open))
    }

    /// Tests only: the scoped faces that are a transit next hop.
    #[cfg(all(test, feature = "test"))]
    pub(crate) fn transit_counts(&self) -> TransitCounts {
        self.transit
    }

    /// Tests only: when the pending re-check of the faces in their grace period is due.
    #[cfg(all(test, feature = "test"))]
    pub(crate) fn transit_recheck_due(&self) -> Option<Instant> {
        self.transit_recheck
    }

    /// Tests only: set the grace period of the faces that are a transit next hop.
    #[cfg(all(test, feature = "test"))]
    pub(crate) fn set_transit_grace(&mut self, grace: Duration) {
        self.transit_grace = grace;
    }

    /// Tests only: stop the clock of this hat at `now` (it then only moves with
    /// [`Hat::advance_clock`]), or let it run again (`None`).
    #[cfg(all(test, feature = "test"))]
    pub(crate) fn set_clock(&mut self, now: Option<Instant>) {
        self.clock = now;
    }

    /// Tests only: move the stopped clock of this hat forward.
    #[cfg(all(test, feature = "test"))]
    pub(crate) fn advance_clock(&mut self, by: Duration) {
        self.clock = Some(self.now() + by);
    }

    /// Tests only: run the re-check of the faces in their grace period now, as its timer does.
    #[cfg(all(test, feature = "test"))]
    pub(crate) fn recheck_transit_now(&mut self, tables: &mut TablesData) {
        self.recheck_transit(tables);
    }

    /// Tests only: schedule a computation of the trees as a topology change does, and wait until
    /// the worker has taken it. The caller holds the tables lock, so the worker then waits for
    /// it and computes the trees once it is released.
    #[cfg(all(test, feature = "test"))]
    pub(crate) fn hand_tree_computation_to_worker(&mut self, tables_ref: Arc<TablesLock>) {
        self.compute_trees_async(tables_ref);
        let start = Instant::now();
        while !self.routers_trees_worker.tx.is_empty() {
            assert!(
                start.elapsed() < Duration::from_secs(1),
                "the worker is stuck"
            );
            std::thread::yield_now();
        }
    }

    /// Tests only: whether a computation of the trees is scheduled and has not run yet.
    #[cfg(all(test, feature = "test"))]
    pub(crate) fn trees_outdated(&self) -> bool {
        self.trees_outdated
    }

    /// Tests only: the router out of scope behind the scoped face of `zid`, if it is a transit
    /// next hop.
    #[cfg(all(test, feature = "test"))]
    pub(crate) fn transit_towards(
        &self,
        tables: &TablesData,
        zid: &ZenohIdProto,
    ) -> Option<ZenohIdProto> {
        let scope = self.face_hat(self.face(tables, zid)?).scope.as_ref()?;
        scope.transit.map(|transit| transit.towards)
    }

    fn now(&self) -> Instant {
        #[cfg(test)]
        if let Some(now) = self.clock {
            return now;
        }
        Instant::now()
    }

    pub(crate) fn net(&self) -> &Network {
        self.routers_net
            .as_ref()
            .expect("router Network should be initialized")
    }

    pub(crate) fn net_mut(&mut self) -> &mut Network {
        self.routers_net
            .as_mut()
            .expect("router Network should be initialized")
    }

    pub(self) fn res_hat<'r>(&self, res: &'r Resource) -> &'r HatContext {
        res.context().hats[self.region].ctx.downcast_ref().unwrap()
    }

    pub(self) fn res_hat_mut<'r>(&self, res: &'r mut Arc<Resource>) -> &'r mut HatContext {
        get_mut_unchecked(res).context_mut().hats[self.region]
            .ctx
            .downcast_mut()
            .unwrap()
    }

    pub(self) fn face_hat<'f>(&self, face_state: &'f FaceState) -> &'f HatFace {
        face_state.hats[self.region].downcast_ref().unwrap()
    }

    pub(self) fn face_hat_mut<'f>(&self, face_state: &'f mut Arc<FaceState>) -> &'f mut HatFace {
        get_mut_unchecked(face_state).hats[self.region]
            .downcast_mut()
            .unwrap()
    }

    pub(crate) fn face<'t>(
        &self,
        tables: &'t TablesData,
        zid: &ZenohIdProto,
    ) -> Option<&'t Arc<FaceState>> {
        tables.faces.values().find(|face| face.zid == *zid)
    }

    /// Returns `true` if declarations on `res` are subject to this region's declaration scope.
    ///
    /// Always `false` when the region has no declaration scope.
    #[inline]
    pub(self) fn is_scoped(&self, res: &Resource) -> bool {
        match (&self.declaration_scope, res.keyexpr()) {
            (Some(scope), Some(key_expr)) => scope.is_scoped(key_expr),
            _ => false,
        }
    }

    /// Returns `true` if a declaration or undeclaration on `res`, for which
    /// [`Hat::is_scoped`] returned `scoped`, must not be sent to `face`.
    ///
    /// This has to be checked before anything about `res` is sent to `face`, key expression
    /// declaration included. The answer is the same for the whole life of the face, so a
    /// declaration and its undeclaration are either both sent or both withheld, unless the face
    /// stays a transit next hop for the grace period (see [`Hat::check_leaves`]): it then gets
    /// everything.
    ///
    /// What is out of scope is counted, and only withheld in enforce mode.
    #[inline]
    pub(self) fn withholds(
        &self,
        scoped: bool,
        face: &FaceState,
        res: &Resource,
        undeclaration: bool,
    ) -> bool {
        if !scoped {
            return false;
        }
        let (Some(declaration_scope), Some(scope), Some(key_expr)) = (
            &self.declaration_scope,
            &self.face_hat(face).scope,
            res.keyexpr(),
        ) else {
            return false;
        };
        if scope.failed_open || scope.concerns(key_expr) {
            return false;
        }
        let first = scope.count_out_of_scope(undeclaration);
        if !declaration_scope.observe() {
            return true;
        }
        if first && !undeclaration {
            tracing::info!(
                "Declaration scope (observe): router {} ({}) would not get {} (out of its scope \
                 {})",
                face.zid,
                face,
                key_expr,
                scope.scope()
            );
        }
        false
    }

    /// Counts, and logs once per face, a declaration that the router of `face` makes itself on a
    /// scoped key expression out of its own scope: its Zenoh ID may not match the scope that its
    /// applications use.
    #[inline]
    pub(self) fn check_identity(&self, face: &FaceState, router: &ZenohIdProto, res: &Resource) {
        if !self.is_scoped(res) || *router != face.zid {
            return;
        }
        if let (Some(scope), Some(key_expr)) = (&self.face_hat(face).scope, res.keyexpr()) {
            if !scope.concerns(key_expr) && scope.foreign.fetch_add(1, Relaxed) == 0 {
                tracing::warn!(
                    "Declaration scope: router {} ({}) declares {}, which is out of its scope {}: \
                     its Zenoh ID may not match the scope its applications use",
                    face.zid,
                    face,
                    key_expr,
                    scope.scope()
                );
            }
        }
    }

    /// Detects the scoped faces that are a transit next hop towards a router of the region
    /// outside of their scope: these routers would miss what is withheld from the face.
    ///
    /// Such a face stays filtered for a grace period first ([`Hat::transit_grace`]): a router that
    /// reconnects may announce for a moment the links of its previous connection, which makes it
    /// look like a transit next hop until its next link state (see [`scope::TRANSIT_GRACE`]). The
    /// grace period ends at the first evaluation at or after its end, which can be later than its
    /// end if that evaluation is delayed: a face that is a leaf again before it was unfiltered is
    /// never unfiltered for that episode.
    ///
    /// The face is evaluated after each computation of the trees, and by the re-check at the end
    /// of a grace period, which reuses the result of the last computation unless another one is
    /// scheduled ([`Hat::recheck_transit`]). A
    /// grace period starts at the first evaluation that finds the face a transit next hop after
    /// an evaluation that found it a leaf (or after it connected), and lasts as long as every
    /// evaluation finds it a transit next hop: an evaluation that finds it a leaf ends it, and the
    /// next one that finds it a transit next hop starts a new, full grace period. A leaf state
    /// that starts and ends between two computations of the trees is not seen. If an evaluation
    /// at or after the end of the grace period still finds it a transit next hop, the face is no
    /// longer filtered for the rest of its life and, in enforce mode, becomes a new child in every
    /// tree, so that everything withheld from it, during the grace period included, is replayed
    /// (observe mode withheld nothing).
    fn check_leaves(&mut self, tables: &mut TablesData, new_children: &mut [Vec<NodeIndex>]) {
        let (Some(declaration_scope), Some(net)) = (&self.declaration_scope, &self.routers_net)
        else {
            return;
        };
        let idx_of = net
            .graph
            .node_indices()
            .map(|idx| (net.graph[idx].zid, idx))
            .collect::<HashMap<_, _>>();
        // For each node, a router behind it that is out of its scope, if it is a scoped face.
        let mut behind = vec![None; net.trees.len()];
        {
            let mut scopes = vec![None; net.trees.len()];
            for face in tables.faces.values() {
                if let (Some(scope), Some(idx)) =
                    (&self.face_hat(face).scope, idx_of.get(&face.zid))
                {
                    scopes[idx.index()] = Some(scope);
                }
            }
            for tree in &net.trees {
                for (dst, hop) in tree.directions.iter().enumerate() {
                    // `hop` is a child of this router in `tree`, and `dst` is behind it.
                    let Some(hop) = *hop else { continue };
                    if hop.index() == dst
                        || Some(hop) == tree.parent
                        || behind[hop.index()].is_some()
                    {
                        continue;
                    }
                    let dst = &net.graph[NodeIndex::new(dst)].zid;
                    if let Some(Some(scope)) = scopes.get(hop.index()) {
                        if !declaration_scope.has_scope_of(dst, scope) {
                            behind[hop.index()] = Some(*dst);
                        }
                    }
                }
            }
        }
        let (now, observe) = (self.now(), declaration_scope.observe());
        let mut transient = 0;
        for face in tables.faces.values_mut() {
            let dst = idx_of.get(&face.zid).and_then(|idx| behind[idx.index()]);
            let Some(scope) = self.face_hat_mut(face).scope.as_mut() else {
                continue;
            };
            let since = scope.transit.map(|transit| transit.since);
            scope.transit = dst.map(|towards| scope::Transit {
                towards,
                since: since.unwrap_or(now),
            });
            let failed_open = scope.failed_open;
            if since.is_some() && dst.is_none() && !failed_open {
                scope.transient += 1;
                transient += 1;
            }
            let scope = self.face_hat(face).scope.as_ref().map_or("", |s| s.scope());
            match (since, dst) {
                (None, Some(dst)) if failed_open => tracing::info!(
                    "Declaration scope: router {} ({}) is a transit next hop towards {}, which is \
                     out of its scope {}, again (it is unfiltered until it reconnects)",
                    face.zid,
                    face,
                    dst,
                    scope
                ),
                (None, Some(dst)) => tracing::info!(
                    "Declaration scope: router {} ({}) is a transit next hop towards {}, which is \
                     out of its scope {}. {} {:.1} s, {}",
                    face.zid,
                    face,
                    dst,
                    scope,
                    if observe {
                        "Observe mode filters nothing; if it is still one in"
                    } else {
                        "It stays filtered; if it is still one in"
                    },
                    self.transit_grace.as_secs_f64(),
                    if observe {
                        "it will be reported as no longer filtered"
                    } else {
                        "it will no longer be filtered"
                    }
                ),
                (Some(_), None) if failed_open => tracing::info!(
                    "Declaration scope: router {} ({}) is a leaf again (it stays unfiltered until \
                     it reconnects)",
                    face.zid,
                    face
                ),
                (Some(since), None) => tracing::info!(
                    "Declaration scope: router {} ({}) is a leaf again after {:.1} s, before it was \
                     unfiltered{}",
                    face.zid,
                    face,
                    now.saturating_duration_since(since).as_secs_f64(),
                    if observe {
                        " (observe mode)"
                    } else {
                        ": it stays filtered"
                    }
                ),
                _ => {}
            }
        }
        self.transit.transient += transient;
        self.expire_transit(tables, new_children);
    }

    /// Ends the grace period of the scoped faces that have been a transit next hop for
    /// [`Hat::transit_grace`], as of the last evaluation (see [`Hat::check_leaves`]): they are no
    /// longer filtered and, in enforce mode, are added to `new_children` in every tree in which
    /// they are a child. Then counts the faces in each state and schedules the re-check of the
    /// faces still in their grace period.
    fn expire_transit(&mut self, tables: &mut TablesData, new_children: &mut [Vec<NodeIndex>]) {
        let Some(declaration_scope) = &self.declaration_scope else {
            return;
        };
        let (now, grace) = (self.now(), self.transit_grace);
        let (mut pending, mut violating, mut failed_open) = (0, 0, 0);
        let mut due = None::<Instant>;
        let mut replay = vec![];
        for face in tables.faces.values_mut() {
            let Some(scope) = self.face_hat_mut(face).scope.as_mut() else {
                continue;
            };
            let Some(transit) = scope.transit else {
                continue;
            };
            let elapsed = now.saturating_duration_since(transit.since);
            if scope.failed_open {
                violating += 1;
                continue;
            } else if elapsed < grace {
                pending += 1;
                let end = transit.since + grace;
                due = Some(due.map_or(end, |due| due.min(end)));
                continue;
            }
            scope.failed_open = true;
            violating += 1;
            failed_open += 1;
            // Observe mode withheld nothing: there is nothing to replay.
            if !declaration_scope.observe() {
                replay.push(face.zid);
            }
            let scope = self.face_hat(face).scope.as_ref().map_or("", |s| s.scope());
            tracing::error!(
                "Declaration scope: router {} ({}) has been a transit next hop towards {}, which \
                 is out of its scope {}, for {:.1} s; the routers of a scoped subregion must be \
                 leaves. It is no longer filtered until it reconnects",
                face.zid,
                face,
                transit.towards,
                scope,
                elapsed.as_secs_f64()
            );
        }
        self.transit.pending = pending;
        self.transit.violating = violating;
        self.transit.failed_open += failed_open;
        let net = self.net();
        for zid in replay {
            let Some(idx) = net.get_idx(&zid) else {
                continue;
            };
            for (tree, children) in net.trees.iter().zip(new_children.iter_mut()) {
                if tree.children.contains(&idx) && !children.contains(&idx) {
                    children.push(idx);
                }
            }
        }
        if let Some(due) = due {
            self.schedule_transit_recheck(due);
        }
    }

    /// Schedules the re-check of the faces in their grace period at `due`, the earliest end of
    /// their grace periods, unless one is pending already. The pending one is then due first: all
    /// grace periods have the same length, so a grace period that starts later ends later. When it
    /// runs, it schedules the next one if faces are still in their grace period.
    fn schedule_transit_recheck(&mut self, due: Instant) {
        if self.transit_recheck.is_some() {
            return;
        }
        self.transit_recheck = Some(due);
        let delay = due.saturating_duration_since(self.now());
        let (tables_ref, region) = (self.tables_ref.clone(), self.region);
        zenoh_runtime::ZRuntime::Net.spawn(async move {
            tokio::time::sleep(delay).await;
            // The tables, and the hat with them, may be gone.
            let Some(tables_ref) = tables_ref.upgrade() else {
                return;
            };
            let mut wtables = zwrite!(tables_ref.tables);
            let tables = &mut *wtables;
            if let Some(hat) = tables
                .hats
                .get_mut(&region)
                .and_then(|hat| hat.as_any_mut().downcast_mut::<Hat>())
            {
                hat.recheck_transit(&mut tables.data);
            }
        });
    }

    /// The re-check at the end of a grace period: what [`Hat::check_leaves`] does once the trees
    /// are computed, without computing them. The transit state of each face is the result of the
    /// last computation; it is only current if no computation is scheduled since (the
    /// `trees_outdated` marker), which covers a computation that the worker has already taken
    /// and that waits for the tables lock. Then this ends the grace periods that are over
    /// ([`Hat::expire_transit`]) and replays to the faces that are no longer filtered.
    /// Otherwise it does nothing: the scheduled computation evaluates the faces on the new
    /// topology, ends the grace periods that are over and schedules the next re-check.
    fn recheck_transit(&mut self, tables: &mut TablesData) {
        self.transit_recheck = None;
        if self.trees_outdated {
            return;
        }
        let mut new_children = vec![vec![]; self.net().trees.len()];
        self.expire_transit(tables, &mut new_children);
        if new_children.iter().any(|children| !children.is_empty()) {
            self.pubsub_tree_change(tables, &new_children);
            self.queries_tree_change(tables, &new_children);
            self.token_tree_change(tables, &new_children);
        }
    }

    /// Schedules a computation of the trees. Every production path that changes the topology or
    /// the link weights goes through here.
    fn compute_trees_async(&mut self, tables_ref: Arc<TablesLock>) {
        tracing::trace!("Schedule trees computation");
        if self.declaration_scope.is_some() {
            self.trees_outdated = true;
        }
        if let Err(err) = self.routers_trees_worker.tx.try_send(tables_ref) {
            tracing::trace!(%err, "Failed to schedule routing tree computation");
        }
    }

    fn compute_trees(&mut self, ctx: DispatcherContext) {
        #[cfg(test)]
        {
            if self.manual_tree_computation {
                tracing::trace!("Trees computation left to the test");
                if self.declaration_scope.is_some() {
                    self.trees_outdated = true;
                }
            } else if self.disable_async_tree_computation {
                self.do_compute_trees(ctx.tables);
            } else {
                self.compute_trees_async(ctx.tables_lock.clone());
            }
        }

        #[cfg(not(test))]
        self.compute_trees_async(ctx.tables_lock.clone());
    }

    fn do_compute_trees(&mut self, tables: &mut TablesData) {
        tracing::trace!("Compute trees");
        let mut new_children = self.net_mut().compute_trees();
        self.trees_outdated = false;
        self.check_leaves(tables, &mut new_children);
        tracing::trace!("Compute routes");
        self.pubsub_tree_change(tables, &new_children);
        self.queries_tree_change(tables, &new_children);
        self.token_tree_change(tables, &new_children);
        self.disable_all_routes(tables);
    }

    fn get_router(&self, face: &FaceState, nodeid: NodeId) -> Option<ZenohIdProto> {
        match self
            .routers_net
            .as_ref()
            .unwrap()
            .get_link(self.face_hat(face).link_id)
        {
            Some(link) => match link.get_zid(&(nodeid as u64)) {
                Some(router) => Some(*router),
                None => {
                    tracing::error!(
                        "Received router declaration with unknown routing context id {}",
                        nodeid
                    );
                    None
                }
            },
            None => {
                tracing::error!(
                    "Could not find corresponding link in routers network for {}",
                    face
                );
                None
            }
        }
    }

    fn unregister_router_entities(
        &mut self,
        removed_routers: HashSet<ZenohIdProto>,
    ) -> UnregisterFaceEntitiesResult {
        let mut removed_subscribers = HashSet::new();
        for mut res in self.router_subs.iter().cloned().collect_vec() {
            self.res_hat_mut(&mut res)
                .router_subs
                .retain(|router| !removed_routers.contains(router));

            if self.res_hat(&res).router_subs.is_empty() {
                self.router_subs.retain(|r| !Arc::ptr_eq(r, &res));
                removed_subscribers.insert(res);
            }
        }

        let mut removed_queryables = HashSet::new();
        for mut res in self.router_qabls.iter().cloned().collect_vec() {
            self.res_hat_mut(&mut res)
                .router_qabls
                .retain(|router, _| !removed_routers.contains(router));

            if self.res_hat(&res).router_qabls.is_empty() {
                self.router_qabls.retain(|r| !Arc::ptr_eq(r, &res));
                removed_queryables.insert(res);
            }
        }

        let mut removed_tokens = HashSet::new();
        for mut res in self.router_tokens.iter().cloned().collect_vec() {
            self.res_hat_mut(&mut res)
                .router_tokens
                .retain(|router| !removed_routers.contains(router));

            if self.res_hat(&res).router_tokens.is_empty() {
                self.router_tokens.retain(|r| !Arc::ptr_eq(r, &res));
                removed_tokens.insert(res);
            }
        }

        UnregisterFaceEntitiesResult {
            removed_queryables,
            removed_subscribers,
            removed_tokens,
        }
    }
}

impl HatBaseTrait for Hat {
    fn init(&mut self, tables: &mut TablesData, runtime: Runtime) -> ZResult<()> {
        let config_guard = runtime.config().lock();
        let config = &config_guard;
        let whatami = config.mode();
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

        let router_link_weights = config
            .routing()
            .router()
            .linkstate()
            .transport_weights()
            .clone();
        let declaration_scope = match (self.region, config.gateway.south.as_ref()) {
            (Region::South { id, .. }, Some(GatewaySouthConf::Custom(subregions))) => subregions
                .get(id)
                .and_then(|subregion| subregion.declaration_scope.clone()),
            _ => None,
        };
        drop(config_guard);

        self.declaration_scope = declaration_scope
            .as_ref()
            .map(scope::DeclarationScope::new)
            .transpose()?;
        if let Some(conf) = &declaration_scope {
            tracing::info!(
                region = %self.region,
                key_exprs = ?conf.key_exprs.iter().map(|ke| ke.as_str()).collect_vec(),
                mode = ?conf.mode,
                "Declaration scope enabled"
            );
        }

        self.routers_net = Some(Network::new(
            ROUTERS_NET_NAME.to_string(),
            tables.zid,
            runtime.clone(),
            true,
            gossip,
            gossip_multihop,
            gossip_target,
            autoconnect,
            link_weights_from_config(router_link_weights, ROUTERS_NET_NAME)?,
            self.region().bound(),
        ));
        Ok(())
    }

    fn new_face(&self) -> Box<dyn Any + Send + Sync> {
        Box::new(HatFace::new())
    }

    fn new_resource(&self) -> Box<dyn Any + Send + Sync> {
        Box::new(HatContext::new())
    }

    fn new_remote(&self, face: &Arc<FaceState>, nid: NodeId) -> Option<Remote> {
        self.get_router(face, nid).map(|zid| Remote(Box::new(zid)))
    }

    fn new_local_face(
        &mut self,
        _ctx: DispatcherContext,
        _tables_ref: &Arc<TablesLock>,
    ) -> ZResult<()> {
        bail!("Local sessions should not be bound to router hats");
    }

    #[tracing::instrument(level = "debug", skip(ctx, transport, _other_hats), fields(src = %ctx.src_face), ret)]
    fn new_transport_unicast_face(
        &mut self,
        ctx: DispatcherContext,
        transport: &TransportUnicast,
        _other_hats: RegionMap<&dyn HatTrait>,
    ) -> ZResult<()> {
        debug_assert!(self.owns(ctx.src_face));

        let link_id = self.net_mut().add_link(transport.clone());
        self.face_hat_mut(ctx.src_face).link_id = link_id;

        if let Some(declaration_scope) = &self.declaration_scope {
            match declaration_scope.face_scope(&ctx.src_face.zid) {
                Ok(scope) => {
                    tracing::debug!(face = %ctx.src_face, ?scope, "Declarations are scoped");
                    self.face_hat_mut(ctx.src_face).scope = Some(scope);
                    self.tables_ref = Arc::downgrade(ctx.tables_lock);
                }
                // Fail open: a router that has no scope gets every declaration.
                Err(reason) => tracing::warn!(
                    "{} has no declaration scope ({}): declarations to it are not filtered",
                    ctx.src_face,
                    reason
                ),
            }
        }

        self.compute_trees(ctx);

        Ok(())
    }

    fn close_face(&mut self, ctx: DispatcherContext) {
        debug_assert!(self.owns(ctx.src_face));

        if let Some(scope) = self.face_hat_mut(ctx.src_face).scope.as_mut() {
            // The face leaves its transit state, if any, with nothing left to re-check.
            if scope.transit.take().is_some() {
                let count = if scope.failed_open {
                    &mut self.transit.violating
                } else {
                    &mut self.transit.pending
                };
                *count = count.saturating_sub(1);
            }
        }
        if let (Some(declaration_scope), Some(scope)) =
            (&self.declaration_scope, &self.face_hat(ctx.src_face).scope)
        {
            let [declarations, undeclarations, foreign] = scope.counts();
            tracing::info!(
                "Declaration scope: router {} ({}) closed (scope {}{}{}): {} declarations and {} \
                 undeclarations out of scope {}, {} of its own declarations out of its scope",
                ctx.src_face.zid,
                ctx.src_face,
                scope.scope(),
                if scope.failed_open {
                    ", unfiltered since it stayed a transit next hop for the grace period"
                } else {
                    ""
                },
                match scope.transient {
                    0 => String::new(),
                    n => format!(", a leaf again {n} times after being a transit next hop"),
                },
                declarations,
                undeclarations,
                if declaration_scope.observe() {
                    "sent (observe mode)"
                } else {
                    "withheld"
                },
                foreign
            );
        }

        self.net_mut().remove_link(&ctx.src_face.zid);
        self.compute_trees(ctx);
    }

    #[tracing::instrument(level = "debug", skip(ctx, other_hats), ret)]
    fn handle_oam(
        &mut self,
        mut ctx: DispatcherContext,
        oam: &mut Oam,
        other_hats: RegionMap<&mut dyn HatTrait>,
    ) -> ZResult<()> {
        if oam.id == OAM_LINKSTATE {
            debug_assert_implies!(
                !ctx.src_face.whatami.is_router(),
                ctx.src_face.remote_bound.is_south()
            );

            if let ZExtBody::ZBuf(buf) = mem::take(&mut oam.body) {
                use zenoh_buffers::reader::HasReader;
                use zenoh_codec::RCodec;
                let codec = Zenoh080Routing::new();
                let mut reader = buf.reader();
                let Ok(list): Result<LinkStateList, _> = codec.read(&mut reader) else {
                    bail!("failed to decode link state");
                };

                tracing::trace!(linkstate = ?list);

                let removed_routers = self
                    .net_mut()
                    .link_states(list.link_states, ctx.src_face.zid)
                    .removed_nodes
                    .into_iter()
                    .map(|(_, zid)| zid)
                    .collect();

                let UnregisterFaceEntitiesResult {
                    removed_subscribers,
                    removed_queryables,
                    removed_tokens,
                } = self.unregister_router_entities(removed_routers);

                let region = self.region();

                let mut hats = other_hats
                    .into_iter()
                    .chain(iter::once((self.region, self as &mut dyn HatTrait)))
                    .collect::<RegionMap<_>>();

                for mut res in removed_subscribers {
                    hats[region].disable_data_routes(&mut res);

                    let mut remaining = hats
                        .values_mut()
                        .filter(|hat| hat.remote_subscribers_of(ctx.tables, &res).is_some())
                        .collect_vec();

                    if remaining.is_empty() {
                        for hat in hats.values_mut() {
                            hat.unpropagate_subscriber(ctx.reborrow(), res.clone());
                        }
                        Resource::clean(&mut res);
                    } else if let [last_owner] = &mut *remaining {
                        last_owner
                            .unpropagate_last_non_owned_subscriber(ctx.reborrow(), res.clone())
                    }
                }

                for mut res in removed_queryables {
                    hats[region].disable_query_routes(&mut res);

                    let remaining = hats
                        .iter()
                        .filter_map(|(rgn, hat)| {
                            hat.remote_queryables_of(ctx.tables, &res)
                                .map(|info| (rgn, info))
                        })
                        .collect_vec();

                    match &*remaining {
                        [] => {
                            for hat in hats.values_mut() {
                                hat.unpropagate_queryable(ctx.reborrow(), res.clone());
                            }
                            Resource::clean(&mut res);
                        }
                        [(last_owner, _)] => hats[last_owner]
                            .unpropagate_last_non_owned_queryable(ctx.reborrow(), res.clone()),
                        _ => {
                            for hat in hats.values_mut() {
                                let other_info = remaining
                                    .iter()
                                    .filter_map(|(region, info)| {
                                        (region != &hat.region()).then_some(*info)
                                    })
                                    .reduce(merge_qabl_infos);

                                hat.propagate_queryable(ctx.reborrow(), res.clone(), other_info);
                            }
                        }
                    }
                }

                for mut res in removed_tokens {
                    let mut remaining = hats
                        .values_mut()
                        .filter(|hat| hat.remote_tokens_of(ctx.tables, &res))
                        .collect_vec();

                    if remaining.is_empty() {
                        for hat in hats.values_mut() {
                            hat.unpropagate_token(ctx.reborrow(), res.clone());
                        }
                        Resource::clean(&mut res);
                    } else if let [last_owner] = &mut *remaining {
                        last_owner.unpropagate_last_non_owned_token(ctx.reborrow(), res.clone())
                    }
                }

                hats[region]
                    .as_any_mut()
                    .downcast_mut::<Self>()
                    .unwrap()
                    .compute_trees(ctx);
            }
        }

        Ok(())
    }

    #[inline]
    fn map_routing_context(&self, _tables: &TablesData, face: &FaceState, nid: NodeId) -> NodeId {
        if self.owns(face) {
            self.net()
                .get_local_context(nid, self.face_hat(face).link_id)
        } else {
            DEFAULT_NODE_ID
        }
    }

    fn info(&self) -> String {
        self.net().dot()
    }

    fn update_from_config(
        &mut self,
        tables_ref: &Arc<TablesLock>,
        runtime: &Runtime,
    ) -> ZResult<()> {
        let config = runtime.config().lock();
        let router_link_weights = link_weights_from_config(
            config
                .routing()
                .router()
                .linkstate()
                .transport_weights()
                .clone(),
            ROUTERS_NET_NAME,
        )?;
        drop(config);
        if let Some(net) = self.routers_net.as_mut() {
            if net.update_link_weights(router_link_weights) {
                self.compute_trees_async(tables_ref.clone());
            }
        }
        Ok(())
    }

    fn links_info(&self) -> HashMap<ZenohIdProto, crate::net::protocol::linkstate::LinkInfo> {
        if let Some(net) = &self.routers_net {
            net.links_info()
        } else {
            HashMap::new()
        }
    }

    fn route_successor(&self, src: ZenohIdProto, dst: ZenohIdProto) -> Option<ZenohIdProto> {
        self.net().route_successor(src, dst)
    }

    fn route_successors(&self) -> Vec<SuccessorEntry> {
        self.net().route_successors()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn mode(&self) -> WhatAmI {
        WhatAmI::Router
    }

    fn region(&self) -> Region {
        self.region
    }

    fn remote_node_id_to_zid(&self, src: &FaceState, node_id: NodeId) -> Option<ZenohIdProto> {
        self.get_router(src, node_id)
    }

    #[tracing::instrument(level = "trace", skip(_tables), ret)]
    fn gateways_of(&self, _tables: &TablesData, zid: &ZenohIdProto) -> Option<Vec<ZenohIdProto>> {
        debug_assert!(self.region().bound().is_south());

        let node = self.net().graph.node_weights().find(|n| &n.zid == zid)?;

        let gwys = self
            .net()
            .graph
            .node_weights()
            .filter_map(|n| (n.is_gateway && node.links.contains_key(&n.zid)).then_some(n.zid))
            .collect_vec();

        Some(gwys)
    }

    #[tracing::instrument(level = "trace", skip(_tables), ret)]
    fn gateways(&self, _tables: &TablesData) -> Option<Vec<ZenohIdProto>> {
        debug_assert!(self.region().bound().is_south());

        let gwys = self
            .net()
            .graph
            .node_weights()
            .filter_map(|n| n.is_gateway.then_some(n.zid))
            .collect();

        Some(gwys)
    }
}

struct HatContext {
    router_subs: HashSet<ZenohIdProto>,
    router_qabls: HashMap<ZenohIdProto, QueryableInfoType>,
    /// Routers that have declared a token on the given resource.
    router_tokens: HashSet<ZenohIdProto>,
}

impl HatContext {
    fn new() -> Self {
        Self {
            router_subs: HashSet::new(),
            router_qabls: HashMap::new(),
            router_tokens: HashSet::new(),
        }
    }
}

struct HatFace {
    link_id: LinkId,
    /// The declaration scope of the face. [`None`] if the region has no declaration scope or if
    /// the Zenoh ID of the remote does not yield one: the face is then not filtered.
    scope: Option<scope::FaceScope>,
}

impl HatFace {
    fn new() -> Self {
        Self {
            link_id: LinkId::default(),
            scope: None,
        }
    }
}

impl HatTrait for Hat {
    fn unregister_face_entities(&mut self, ctx: DispatcherContext) -> UnregisterFaceEntitiesResult {
        let removed_routers = self
            // NOTE: a &mut is not taken here and the link graph is not yet modified.
            .net()
            .find_disconnected_nodes_after_removing_link(&ctx.src_face.zid)
            .into_iter()
            .map(|(_, node)| node)
            .collect::<HashSet<_>>();

        self.unregister_router_entities(removed_routers)
    }
}
