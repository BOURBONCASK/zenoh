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
use zenoh_config::{
    gateway::{GatewayFiltersConf, GatewayPresetConf, GatewaySouthConf},
    ExpandedConfig, Interface, WhatAmI,
};
#[allow(unused_imports)]
use zenoh_core::polyfill::*;
use zenoh_link::Link;
use zenoh_protocol::core::{Bound, Region};
use zenoh_result::ZResult;
use zenoh_transport::TransportPeer;

/// Computes the _transient_ [`Bound`] of a remote.
///
/// This method is used during the Open phase of establishment to decide whether a remote is
/// south-bound using the [`zenoh_protocol::transport::open::ext::RemoteBound`] extension.
#[tracing::instrument(level = "debug", skip(peer, config), fields(peer.zid = %peer.zid.short(), peer.mode = %peer.whatami), ret)]
pub(crate) fn compute_transient_bound_of(
    peer: &TransportPeer,
    config: &ExpandedConfig,
) -> ZResult<Option<Bound>> {
    Ok(compute_transient_region_of(peer, config)?.map(|r| r.bound()))
}

/// Computes the [`Region`] and _remote_ [`Bound`] of a remote.
#[tracing::instrument(level = "debug", skip(peer, config), fields(peer.zid = %peer.zid.short(), peer.mode = %peer.whatami), ret)]
pub(crate) fn compute_region_of(
    peer: &TransportPeer,
    config: &ExpandedConfig,
    transient_remote_bound: Option<&Bound>,
) -> ZResult<(Region, Bound)> {
    let mode = config.mode();
    let remote_mode = peer.whatami;
    let transient_region = compute_transient_region_of(peer, config)?;

    match (transient_region.map(|r| r.bound()), transient_remote_bound) {
        (None, None) => compute_auto_region(mode, remote_mode),
        (None, Some(Bound::South)) => Ok((Region::North, Bound::South)),
        (Some(Bound::South), None) => Ok((transient_region.unwrap(), Bound::North)),
        (None, Some(Bound::North)) => {
            let (auto_region, auto_remote_bound) = compute_auto_region(mode, remote_mode)?;

            // NOTE(regions): in this case, I am not configured to be a gateway, nor does the auto
            // mode allow me to put the remote in the north region. We _could_ in the future adjust
            // this to put the remote in a subregion.
            if !auto_remote_bound.is_north() {
                bail!("Remote's custom configuration conflicts with auto preset")
            } else {
                Ok((auto_region, Bound::North))
            }
        }
        (Some(Bound::North), None) => {
            let (auto_region, auto_remote_bound) = compute_auto_region(mode, remote_mode)?;

            // NOTE(regions): see above comment for the symmetric case.
            if auto_region.bound().is_south() {
                bail!("Remote's auto preset conflicts with custom configuration")
            } else {
                Ok((Region::North, auto_remote_bound))
            }
        }
        (Some(Bound::North), Some(Bound::North)) => {
            if mode != remote_mode {
                bail!("North-north {mode}-{remote_mode} configuration (invalid)")
            } else {
                Ok((Region::North, Bound::North))
            }
        }
        (Some(Bound::South), Some(Bound::South)) => {
            bail!("South-south configuration (invalid)")
        }
        (Some(Bound::North), Some(Bound::South)) | (Some(Bound::South), Some(Bound::North)) => {
            Ok((transient_region.unwrap(), *transient_remote_bound.unwrap()))
        }
    }
}

/// The mode this node takes in its north region: `gateway.north.mode`, or its own mode.
pub(crate) fn north_mode(config: &ExpandedConfig) -> WhatAmI {
    config
        .gateway
        .north
        .as_ref()
        .and_then(|north| north.mode)
        .unwrap_or(config.mode())
}

/// Returns `true` if this node is a leaf of its north region, i.e. takes another mode there
/// than its own.
pub(crate) fn is_leaf(config: &ExpandedConfig) -> bool {
    north_mode(config) != config.mode()
}

/// Checks the `gateway.north` section against the mode of the node.
pub(crate) fn check_north_config(config: &ExpandedConfig) -> ZResult<()> {
    if let Some(north) = &config.gateway.north {
        if is_leaf(config) && !(config.mode().is_router() && north_mode(config).is_client()) {
            bail!(
                "gateway.north.mode '{}' is not supported in '{}' mode: only a router can be a \
                 client of its north region",
                north_mode(config),
                config.mode()
            )
        }
        if !is_leaf(config) && !north.interests.is_empty() {
            bail!("gateway.north.interests requires gateway.north.mode to be 'client'")
        }
    }
    Ok(())
}

/// Returns `true` if `link` is an uplink according to `gateway.north.uplinks`.
#[allow(clippy::incompatible_msrv)]
fn is_uplink(link: &Link, config: &ExpandedConfig) -> bool {
    let Some(north) = &config.gateway.north else {
        return false;
    };
    north.uplinks.as_ref().is_none_or(|filters| {
        filters.iter().any(|filter| {
            filter.link_protocols.as_ref().is_none_or(|protocols| {
                protocols
                    .iter()
                    .any(|p| p.as_str() == link.dst.protocol().as_str())
            }) && filter.interfaces.as_ref().is_none_or(|ifaces| {
                link.interfaces
                    .iter()
                    .all(|iface| ifaces.contains(&Interface(iface.to_owned())))
            })
        })
    })
}

/// The mode this node announces on `link` when it differs from its own: a leaf announces its
/// north mode on its uplinks. The mode is sent before the remote is known, hence the decision
/// only looks at the link.
pub(crate) fn announced_mode_on(link: &Link, config: &ExpandedConfig) -> Option<WhatAmI> {
    (is_leaf(config) && is_uplink(link, config)).then(|| north_mode(config))
}

/// Refuses a remote of a leaf whose region disagrees with the mode announced on its links: a
/// north remote that was told this node is a router would handle it as one, and a south remote
/// that was told it is a client would not exchange link state with it.
pub(crate) fn check_announced_mode(
    peer: &TransportPeer,
    config: &ExpandedConfig,
    region: &Region,
) -> ZResult<()> {
    if !is_leaf(config) {
        return Ok(());
    }
    let over_uplink = !peer.links.is_empty() && peer.links.iter().all(|l| is_uplink(l, config));
    match (region.bound(), over_uplink) {
        (Bound::North, true) | (Bound::South, false) => Ok(()),
        (Bound::North, false) => bail!(
            "Remote {} is in the north region of this leaf but none of gateway.north.uplinks \
             matches its link(s): it was announced the '{}' mode instead of '{}'",
            peer.zid,
            config.mode(),
            north_mode(config)
        ),
        (Bound::South, true) => bail!(
            "Remote {} is in the south region {} of this leaf but its link(s) match \
             gateway.north.uplinks: it was announced the '{}' mode instead of '{}'",
            peer.zid,
            region,
            north_mode(config),
            config.mode()
        ),
    }
}

const MULTICAST_LIMITATION_NOTICE: &str =
    "Multicast is only supported in north peer & south router regions";

/// Computes the [`Region`] of a multicast transport.
#[tracing::instrument(level = "debug", skip(config), ret)]
pub(crate) fn compute_multicast_region(config: &ExpandedConfig) -> ZResult<Region> {
    match config.mode() {
        WhatAmI::Peer => Ok(Region::North),
        WhatAmI::Router => Ok(Region::default_south(WhatAmI::Peer)),
        WhatAmI::Client => bail!("{MULTICAST_LIMITATION_NOTICE}"),
    }
}

/// Computes the [`Region`] and _remote_ [`Bound`] of a multicast remote.
#[tracing::instrument(level = "debug", skip(config), ret)]
pub(crate) fn compute_multicast_region_of(
    peer: &TransportPeer,
    config: &ExpandedConfig,
) -> ZResult<(Region, Bound)> {
    match (config.mode(), peer.whatami) {
        (WhatAmI::Peer, WhatAmI::Peer) => Ok((Region::North, Bound::North)),
        (WhatAmI::Router, WhatAmI::Peer) => {
            Ok((Region::default_south(WhatAmI::Peer), Bound::North))
        }
        (WhatAmI::Peer, WhatAmI::Router) => Ok((Region::North, Bound::South)),
        _ => bail!("{MULTICAST_LIMITATION_NOTICE}"),
    }
}

fn compute_transient_region_of(
    peer: &TransportPeer,
    config: &ExpandedConfig,
) -> ZResult<Option<Region>> {
    match config.gateway.south.clone().unwrap_or_default() {
        GatewaySouthConf::Preset(GatewayPresetConf::Auto) => Ok(None),
        GatewaySouthConf::Custom(subregions) => {
            if let Some(id) = subregions
                .iter()
                .position(|s| is_match(s.filters.as_deref(), peer))
            {
                if peer.whatami.is_router() && !config.mode().is_router() {
                    bail!("Router regions cannot be subregions of non-router regions (unsupported)")
                }

                Ok(Some(Region::South {
                    id,
                    mode: peer.whatami,
                }))
            } else {
                Ok(Some(Region::North))
            }
        }
    }
}

/// Computes the _auto_ [`Region`] and _remote_ [`Bound`] of a remote.
fn compute_auto_region(mode: WhatAmI, remote_mode: WhatAmI) -> ZResult<(Region, Bound)> {
    match (mode, remote_mode) {
        (WhatAmI::Router, WhatAmI::Peer | WhatAmI::Client) | (WhatAmI::Peer, WhatAmI::Client) => {
            Ok((Region::default_south(remote_mode), Bound::North))
        }
        (WhatAmI::Router, WhatAmI::Router) | (WhatAmI::Peer, WhatAmI::Peer) => {
            Ok((Region::North, Bound::North))
        }
        (WhatAmI::Peer | WhatAmI::Client, WhatAmI::Router) | (WhatAmI::Client, WhatAmI::Peer) => {
            Ok((Region::North, Bound::South))
        }
        (WhatAmI::Client, WhatAmI::Client) => {
            bail!("North-north client-client configuration (invalid)")
        }
    }
}

#[allow(clippy::incompatible_msrv)]
fn is_match(filter: Option<&[GatewayFiltersConf]>, peer: &TransportPeer) -> bool {
    filter.is_none_or(|filters| {
        filters.iter().any(|filter| {
            let value = filter
                .zids
                .as_ref()
                .is_none_or(|zid| zid.contains(&peer.zid.into()))
                && filter.interfaces.as_ref().is_none_or(|ifaces| {
                    peer.links
                        .iter()
                        .flat_map(|link| {
                            link.interfaces
                                .iter()
                                .map(|iface| Interface(iface.to_owned()))
                        })
                        .all(|iface| ifaces.contains(&iface))
                })
                && filter
                    .modes
                    .as_ref()
                    .is_none_or(|mode| mode.matches(peer.whatami))
                && filter.region_names.as_ref().is_none_or(|region_names| {
                    peer.region_name
                        .as_ref()
                        .is_some_and(|region_name| region_names.iter().any(|n| n == region_name))
                });

            if filter.negated {
                !value
            } else {
                value
            }
        })
    })
}
