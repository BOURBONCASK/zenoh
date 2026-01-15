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
use zenoh_config::{gateway::BoundFilterConf, ExpandedConfig, Interface, ModeDependent};
#[allow(unused_imports)]
use zenoh_core::polyfill::*;
use zenoh_result::ZResult;
use zenoh_transport::{Bound, TransportPeer};

use crate::net::routing::dispatcher::region::Region;

/// Computes the _transient_ [`Region`] of a remote.
///
/// This method is used during the Open phase of establishment to decide whether a remote is
/// south-bound using the [`zenoh_protocol::transport::open::ext::South`] extension.
#[tracing::instrument(level = "debug", skip(config), ret)]
pub(crate) fn compute_transient_region_of(
    config: &ExpandedConfig,
    peer: &TransportPeer,
) -> ZResult<Region> {
    const ROUTER_REGION_LIMITATION_ERROR: &str =
        "Router regions cannot be subregions of non-router regions (unsupported)";

    let mode = config.mode();

    let south = config.gateway_south().get(mode).ok_or_else(|| {
        zerror!("`mode` is set to `{mode}` but `gateway.south.{mode}` is not set")
    })?;

    if let Some(number) = south
        .iter()
        .position(|s| is_match(s.filters.as_ref(), peer))
    {
        if peer.whatami.is_router() && !mode.is_router() {
            bail!("{}", ROUTER_REGION_LIMITATION_ERROR)
        }

        return Ok(Region::South {
            number,
            mode: peer.whatami,
        });
    }

    let north = config.gateway_north().get(mode).ok_or_else(|| {
        zerror!("`mode` is set to `{mode}` but `gateway.north.{mode}` is not set")
    })?;

    if is_match(north.filters.as_ref(), peer) {
        return Ok(Region::North);
    }

    if config.gateway.fallback.as_ref().unwrap().enabled {
        if peer.whatami.is_router() && !mode.is_router() {
            bail!("{}", ROUTER_REGION_LIMITATION_ERROR)
        }

        Ok(Region::Fallback { mode: peer.whatami })
    } else {
        Err(zerror!("Fallback region is disabled and north/south filters don't match").into())
    }
}

/// Computes the [`Region`] of a remote.
#[tracing::instrument(level = "debug", ret)]
pub(crate) fn compute_region_of(
    transient_region: &Region,
    remote_bound: &Bound,
) -> ZResult<Region> {
    match (transient_region.bound(), remote_bound) {
        (Bound::South, Bound::North) => Ok(*transient_region),
        (Bound::South, Bound::South) => {
            bail!("Invalid gateway configuration: both the local bound and the remote bound are south")
        }
        (Bound::North, Bound::North) => Ok(Region::North),
        (Bound::North, Bound::South) => Ok(Region::North),
    }
}

#[allow(clippy::incompatible_msrv)]
fn is_match(filter: Option<&Vec<BoundFilterConf>>, peer: &TransportPeer) -> bool {
    filter.is_none_or(|filters| {
        filters.iter().any(|filter| {
            filter
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
                })
        })
    })
}
