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
use zenoh_protocol::core::{RegionName, WhatAmI, WhatAmIMatcher};

use crate::{Interface, ZenohId};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayConf {
    pub south: Option<GatewaySouthConf>,
    /// North region options. Unset by default: the node takes its own mode in its north region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub north: Option<GatewayNorthConf>,
}

/// North region options of a gateway.
///
/// With `mode: "client"` a router is a _leaf_ of its north region: it announces itself as a
/// client on its uplinks, exchanges no link state with its north remote and only gets the
/// declarations it asks for with [`GatewayNorthConf::interests`], while it remains a router for
/// its south subregions.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayNorthConf {
    /// The mode this node takes in its north region, and announces on its uplinks. Only
    /// `"client"` differs from the default, which is the mode of the node.
    pub mode: Option<WhatAmI>,
    /// Which of the links this node opens are uplinks. A link is an uplink iff it matches
    /// _any_ of the filters; no link is an uplink by default, and a link this node accepts
    /// never is. Required with `mode: "client"`. The mode is announced before the remote is
    /// known, so only properties of the link itself can be used.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub uplinks: Vec<GatewayUplinkFilterConf>,
    /// Declarations requested from the north remote each time an uplink is established, on
    /// behalf of the south router subregions, which have no interest protocol of their own.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub interests: Vec<GatewayNorthInterestConf>,
    /// Key expressions that are routed towards the north without a matching declaration. Unset
    /// by default. Does not need `mode`: it also applies to a router whose north region is a
    /// router region with a gateway.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_route: Option<GatewayDefaultRouteConf>,
}

/// Default route towards the north.
///
/// A publication or a query is _eligible_ iff its key expression is included in one of
/// `key_exprs` and in none of `except`. An eligible message is sent towards the north in
/// addition to its declared destinations:
/// + by a router of a router region, to the gateway of the region;
/// + by a leaf (`mode: "client"`), to its north remote. With this option set, a leaf does not
///   send to its north remote the other key expressions that no configured interest includes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayDefaultRouteConf {
    /// The key expressions that may have destinations this node does not know. At least one.
    pub key_exprs: Vec<OwnedKeyExpr>,
    /// The key expressions whose destinations this node knows, typically its own.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub except: Vec<OwnedKeyExpr>,
}

/// Uplink filter. A link matches iff it matches _all_ of the given fields; at least one is
/// required.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayUplinkFilterConf {
    /// Link protocols (e.g. `"quic"`).
    pub link_protocols: Option<NEVec<String>>,
    /// Network interfaces: every interface of the link must be one of them. A link whose
    /// interfaces cannot be resolved does not match.
    pub interfaces: Option<NEVec<Interface>>,
}

/// An interest declared to the north remote.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayNorthInterestConf {
    /// Only declarations whose key expression intersects this one are requested.
    pub key_expr: OwnedKeyExpr,
    #[serde(default)]
    pub subscribers: bool,
    #[serde(default)]
    pub queryables: bool,
    /// Liveliness tokens. The access control checks an interest that asks for tokens as a
    /// liveliness subscriber declaration on `key_expr`.
    #[serde(default)]
    pub tokens: bool,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_route_is_parsed() {
        let conf: GatewayConf = json5::from_str(
            r#"{
                north: {
                    default_route: { key_exprs: ["*/fleet/**"], except: ["r1/**"] },
                },
            }"#,
        )
        .unwrap();
        let north = conf.north.unwrap();
        assert!(north.mode.is_none());
        let default_route = north.default_route.unwrap();
        assert_eq!(default_route.key_exprs[0].as_str(), "*/fleet/**");
        assert_eq!(default_route.except[0].as_str(), "r1/**");

        let conf: GatewayConf =
            json5::from_str(r#"{ north: { default_route: { key_exprs: ["a/**"] } } }"#).unwrap();
        assert!(conf.north.unwrap().default_route.unwrap().except.is_empty());

        assert!(json5::from_str::<GatewayConf>(r#"{ north: { default_route: {} } }"#).is_err());
        assert!(json5::from_str::<GatewayConf>(
            r#"{ north: { default_route: { key_exprs: ["a/**"], local: ["a/b"] } } }"#
        )
        .is_err());
        assert!(
            json5::from_str::<GatewayConf>(r#"{ north: { mode: "client" } }"#)
                .unwrap()
                .north
                .unwrap()
                .default_route
                .is_none()
        );
    }

    #[test]
    fn north_section_is_parsed() {
        let conf: GatewayConf = json5::from_str(
            r#"{
                north: {
                    mode: "client",
                    uplinks: [{ link_protocols: ["quic"] }, { interfaces: ["wwan0"] }],
                    interests: [
                        { key_expr: "sn/**", subscribers: true, queryables: true },
                        { key_expr: "sn/alive/**", tokens: true },
                    ],
                },
            }"#,
        )
        .unwrap();
        let north = conf.north.unwrap();
        assert_eq!(north.mode, Some(WhatAmI::Client));
        assert_eq!(north.uplinks.len(), 2);
        assert_eq!(north.interests.len(), 2);
        assert!(north.interests[0].subscribers && north.interests[0].queryables);
        assert!(!north.interests[0].tokens && north.interests[1].tokens);
    }

    #[test]
    fn north_section_is_optional_and_strict() {
        let conf: GatewayConf = json5::from_str("{}").unwrap();
        assert!(conf.north.is_none());
        let conf: GatewayConf = json5::from_str(r#"{ north: { mode: "client" } }"#).unwrap();
        assert!(conf.north.unwrap().uplinks.is_empty());
        assert!(json5::from_str::<GatewayConf>(r#"{ north: { uplink: [] } }"#).is_err());
        assert!(json5::from_str::<GatewayConf>(
            r#"{ north: { interests: [{ subscribers: true }] } }"#
        )
        .is_err());
    }
}
