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
use nonempty_collections::NEVec;
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use zenoh_core::polyfill::*;
use zenoh_keyexpr::OwnedKeyExpr;
use zenoh_protocol::core::{RegionName, WhatAmIMatcher};

use crate::{Interface, ZenohId};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayConf {
    pub south: Option<GatewaySouthConf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GatewaySouthConf {
    Preset(GatewayPresetConf),
    Custom(Vec<GatewayExplicitSouthConf>),
}

impl Default for GatewaySouthConf {
    fn default() -> Self {
        GatewaySouthConf::Preset(GatewayPresetConf::default())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GatewayPresetConf {
    #[default]
    Auto,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayExplicitSouthConf {
    /// Bound filters.
    ///
    /// If [`Some`], a subject matches this filter list iff it matches _any_ of the individual
    /// filters. Thus if the list is empty no subject ever matches.
    ///
    /// If [`None`], a subject _always_ matches.
    pub filters: Option<Vec<GatewayFiltersConf>>,
    /// Declaration scope of the routers of this subregion.
    ///
    /// If [`None`] (the default), every declaration is propagated to every router of the
    /// subregion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declaration_scope: Option<DeclarationScopeConf>,
}

/// Declaration scope of a south router subregion.
///
/// Each router of the subregion is given a _scope_: a key expression chunk derived from its
/// Zenoh ID. A subscriber, queryable or token declaration is withheld from a router iff its key
/// expression is included in one of [`DeclarationScopeConf::key_exprs`] and intersects none of
/// them once their leading `*` is replaced by the scope of that router. Declarations _received_
/// from the routers of the subregion are not filtered.
///
/// A router whose Zenoh ID does not yield a valid scope is not filtered.
///
/// **Prerequisite: the routers of the subregion must be leaves.** A router is only given what
/// concerns its own scope, so it must not be a transit next hop, as seen from this router, towards
/// a router of the same subregion outside of that scope: its routers would miss what was
/// withheld. A router behind it in its own, separate south subregion is fine.
///
/// A router that becomes a transit next hop stays filtered for a grace period of 30 seconds: a
/// router that reconnects, for instance when this router restarts, can announce the links of its
/// previous connection for a moment. If it is a leaf again before it is unfiltered, nothing
/// changes. If it is still a transit next hop at the end, this is logged at error level and it is
/// no longer filtered until it reconnects: in enforce mode, what was withheld from it is then sent
/// to it. The grace period is a debounce chosen from measurements (such episodes lasted up to a
/// few seconds), not a bound: a router still transit after it is not necessarily mis-wired, and a
/// router that really is a transit router keeps missing what is withheld for that long.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclarationScopeConf {
    /// How the scope of a router is derived from its Zenoh ID.
    pub zid: ZidScopeConf,
    /// The scoped key expressions. The first chunk of each must be `*`: it stands for the scope
    /// (e.g. `*/telemetry/**`).
    pub key_exprs: NEVec<OwnedKeyExpr>,
    /// What is done with the declarations that are out of scope. Read at start only.
    #[serde(default)]
    pub mode: DeclarationScopeMode,
}

/// What is done with the declarations that are out of the scope of a router.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeclarationScopeMode {
    /// They are withheld.
    #[default]
    Enforce,
    /// They are sent, and counted per router as if they had been withheld.
    Observe,
}

/// Derivation of a scope from a Zenoh ID.
///
/// The Zenoh ID is taken as an unsigned integer and written in [`ZidScopeConf::radix`] without
/// leading zeros, using `0-9` then `A-Z` as digits. That text must be exactly
/// [`ZidScopeConf::skip`] + [`ZidScopeConf::length`] characters long. The scope is what follows
/// the first [`ZidScopeConf::skip`] characters; it must only contain characters of
/// [`ZidScopeConf::alphabet`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ZidScopeConf {
    /// Radix of the text, from 2 to 36.
    pub radix: u32,
    /// Number of leading characters of the text that are not part of the scope.
    #[serde(default)]
    pub skip: usize,
    /// Exact number of characters of a scope.
    pub length: usize,
    /// The characters a scope may contain. Each must be a digit of [`ZidScopeConf::radix`].
    pub alphabet: String,
}

/// Bound filter.
///
/// A subject matches this filter iff it matches _all_ of its individual filter fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayFiltersConf {
    pub modes: Option<WhatAmIMatcher>,
    pub interfaces: Option<NEVec<Interface>>,
    pub zids: Option<NEVec<ZenohId>>,
    pub region_names: Option<NEVec<RegionName>>,
    #[serde(default)]
    pub negated: bool,
}
