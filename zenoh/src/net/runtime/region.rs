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
use zenoh_link::{Link, BIND_INTERFACE};
use zenoh_protocol::core::{Bound, EndPoint, Region};
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
///
/// `announced_mode` is the mode this node announced to that remote. It is the mode of the node,
/// except on the uplinks of a leaf router (see [`announced_mode_on`]). The relationship is
/// inferred from what the remote was told, since this is what the remote infers it from.
#[tracing::instrument(level = "debug", skip(peer, config), fields(peer.zid = %peer.zid.short(), peer.mode = %peer.whatami), ret)]
pub(crate) fn compute_region_of(
    peer: &TransportPeer,
    config: &ExpandedConfig,
    transient_remote_bound: Option<&Bound>,
    announced_mode: WhatAmI,
) -> ZResult<(Region, Bound)> {
    let mode = announced_mode;
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
    let Some(north) = &config.gateway.north else {
        return Ok(());
    };
    if let Some(default_route) = &north.default_route {
        if !config.mode().is_router() {
            bail!(
                "gateway.north.default_route only applies to a router (mode is '{}')",
                config.mode()
            )
        }
        if default_route.key_exprs.is_empty() {
            bail!(
                "gateway.north.default_route.key_exprs is empty: no key expression would take \
                 the default route"
            )
        }
    }
    if !is_leaf(config) {
        if !north.uplinks.is_empty() || !north.interests.is_empty() {
            bail!(
                "gateway.north.uplinks and gateway.north.interests only apply to a router whose \
                 gateway.north.mode is 'client' (mode is '{}', gateway.north.mode is '{}')",
                config.mode(),
                north_mode(config)
            )
        }
        return Ok(());
    }
    if !(config.mode().is_router() && north_mode(config).is_client()) {
        bail!(
            "gateway.north.mode '{}' is not supported in '{}' mode: only a router can be a \
             client of its north region",
            north_mode(config),
            config.mode()
        )
    }
    if north.uplinks.is_empty() {
        bail!(
            "gateway.north.uplinks is required when gateway.north.mode is 'client': it tells on \
             which of the links this router opens it announces itself as a client. No link is an \
             uplink by default"
        )
    }
    for (i, filter) in north.uplinks.iter().enumerate() {
        if filter.link_protocols.is_none() && filter.interfaces.is_none() {
            bail!(
                "gateway.north.uplinks[{i}] has neither link_protocols nor interfaces: it would \
                 match every link"
            )
        }
    }
    for interest in &north.interests {
        if !(interest.subscribers || interest.queryables || interest.tokens) {
            bail!(
                "gateway.north.interests: '{}' asks for nothing: set subscribers, queryables \
                 or tokens",
                interest.key_expr
            )
        }
    }
    Ok(())
}

/// Returns `true` if `link`, opened by this node, is an uplink according to
/// `gateway.north.uplinks`.
#[allow(clippy::incompatible_msrv)]
fn is_uplink(link: &Link, config: &ExpandedConfig) -> bool {
    let Some(north) = &config.gateway.north else {
        return false;
    };
    north.uplinks.iter().any(|filter| {
        filter.link_protocols.as_ref().is_none_or(|protocols| {
            protocols
                .iter()
                .any(|p| p.as_str() == link.dst.protocol().as_str())
        }) && filter.interfaces.as_ref().is_none_or(|ifaces| {
            // NOTE: `all` is true on an empty list. A link whose interfaces could not be
            // resolved must not match an interface filter.
            !link.interfaces.is_empty()
                && link
                    .interfaces
                    .iter()
                    .all(|iface| ifaces.contains(&Interface(iface.to_owned())))
        })
    })
}

/// Returns `true` if a link opened to `endpoint` is an uplink whatever its interfaces turn out
/// to be: a filter of `gateway.north.uplinks` selects its protocol and no interface, or selects
/// the interface the endpoint is bound to (`iface`).
#[allow(clippy::incompatible_msrv)]
pub(crate) fn is_uplink_endpoint(endpoint: &EndPoint, config: &ExpandedConfig) -> bool {
    let Some(north) = config.gateway.north.as_ref().filter(|_| is_leaf(config)) else {
        return false;
    };
    let protocol = endpoint.protocol();
    let endpoint_config = endpoint.config();
    let iface = endpoint_config.get(BIND_INTERFACE);
    north.uplinks.iter().any(|filter| {
        filter.link_protocols.as_ref().is_none_or(|protocols| {
            protocols.iter().any(|p| p.as_str() == protocol.as_str())
        }) && match (&filter.interfaces, iface) {
            (None, _) => filter.link_protocols.is_some(),
            (Some(ifaces), Some(iface)) => ifaces.contains(&Interface(iface.to_owned())),
            (Some(_), None) => false,
        }
    })
}

/// The mode this node announces on `link`, which it opens, when it differs from its own: a
/// leaf announces its north mode on its uplinks. The mode is sent before the remote is known,
/// hence the decision only looks at the link.
pub(crate) fn announced_mode_on(link: &Link, config: &ExpandedConfig) -> Option<WhatAmI> {
    (is_leaf(config) && is_uplink(link, config)).then(|| north_mode(config))
}

/// Refuses a remote of a leaf whose relationship disagrees with the mode it was announced.
///
/// + A north remote must have been announced the north mode of this leaf (otherwise it handles
///   this node as a router), and must put this node south of itself: the hat of a `client`
///   north region requires it.
/// + A south remote must have been announced the mode of the node (otherwise it would not
///   exchange link state with it).
///
/// These are errors in release builds too: nothing else stops a face with the wrong relationship
/// from being created.
pub(crate) fn check_leaf_relationship(
    peer: &TransportPeer,
    config: &ExpandedConfig,
    region: &Region,
    remote_bound: &Bound,
    announced_mode: WhatAmI,
) -> ZResult<()> {
    if !is_leaf(config) {
        return Ok(());
    }
    let links = || {
        peer.links
            .iter()
            .map(|l| l.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    };
    match region.bound() {
        Bound::North => {
            if announced_mode != north_mode(config) {
                bail!(
                    "Remote {} is in the north region of this leaf but was announced the '{}' \
                     mode instead of '{}': its link ({}) is not an uplink. An uplink is a link \
                     this node opens and that matches gateway.north.uplinks",
                    peer.zid,
                    announced_mode,
                    north_mode(config),
                    links()
                )
            }
            if !remote_bound.is_south() {
                bail!(
                    "Remote {} is in the north region of this leaf but does not put this node \
                     south of itself: a '{}' north region needs a remote that handles this node \
                     as one of its south '{}' nodes",
                    peer.zid,
                    north_mode(config),
                    north_mode(config)
                )
            }
            Ok(())
        }
        Bound::South => {
            if announced_mode != config.mode() {
                bail!(
                    "Remote {} is in the south region {} of this leaf but was announced the \
                     '{}' mode instead of '{}': its link ({}) matches gateway.north.uplinks",
                    peer.zid,
                    region,
                    announced_mode,
                    config.mode(),
                    links()
                )
            }
            Ok(())
        }
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

#[cfg(test)]
mod tests {
    use zenoh_protocol::core::ZenohIdProto;

    use super::*;

    /// A router with `gateway.north.mode: "client"` and a south router subregion.
    fn leaf() -> ExpandedConfig {
        let mut config = zenoh_config::Config::default().expanded();
        config.set_mode(Some(WhatAmI::Router)).unwrap();
        config.gateway = json5::from_str(
            r#"{
                north: { mode: "client", uplinks: [{ link_protocols: ["quic"] }] },
                south: [{ filters: [{ modes: ["router"], zids: ["5100"] }] }],
            }"#,
        )
        .unwrap();
        config
    }

    fn remote(mode: WhatAmI, zid: &str) -> TransportPeer {
        TransportPeer {
            zid: zid.parse::<ZenohIdProto>().unwrap(),
            whatami: mode,
            links: vec![],
            is_qos: false,
            #[cfg(feature = "shared-memory")]
            is_shm: false,
            region_name: None,
        }
    }

    /// The relationship with `cloud`, a router that sent `bound`, when this leaf announced
    /// `announced` to it, and whether the leaf accepts it.
    fn relationship(bound: Option<Bound>, announced: WhatAmI) -> ZResult<(Region, Bound)> {
        let (config, cloud) = (leaf(), remote(WhatAmI::Router, "c10d"));
        let (region, remote_bound) =
            compute_region_of(&cloud, &config, bound.as_ref(), announced)?;
        check_leaf_relationship(&cloud, &config, &region, &remote_bound, announced)?;
        Ok((region, remote_bound))
    }

    /// A router that uses the `auto` gateway preset, or that does not send the bound extension,
    /// sends no bound. It was told that this node is a client, so it put it south: the leaf
    /// infers the same from the mode it announced, not from its own mode.
    #[test]
    fn north_relationship_is_inferred_from_the_announced_mode() {
        assert_eq!(
            relationship(None, WhatAmI::Client).unwrap(),
            (Region::North, Bound::South)
        );
    }

    /// A router with a custom configuration that puts clients south says so.
    #[test]
    fn north_remote_that_puts_the_leaf_south_is_accepted() {
        assert_eq!(
            relationship(Some(Bound::South), WhatAmI::Client).unwrap(),
            (Region::North, Bound::South)
        );
    }

    /// A router whose custom configuration leaves the leaf in its north region is refused:
    /// the hat of a client north region needs a remote that put this node south.
    #[test]
    fn north_remote_that_keeps_the_leaf_north_is_refused() {
        let e = relationship(Some(Bound::North), WhatAmI::Client).unwrap_err();
        assert!(e.to_string().contains("North-north"), "{e}");
    }

    /// The north remote was reached over a link that is not an uplink: it was told that this
    /// node is a router. Whatever it answers, it is refused.
    #[test]
    fn north_remote_that_was_announced_the_mode_of_the_node_is_refused() {
        for bound in [None, Some(Bound::North), Some(Bound::South)] {
            let e = relationship(bound, WhatAmI::Router).unwrap_err();
            assert!(e.to_string().contains("not an uplink"), "{bound:?}: {e}");
        }
    }

    /// A router of a south subregion keeps the relationship routers have, and is refused if it
    /// was reached over an uplink, i.e. told that this node is a client.
    #[test]
    fn south_relationship_needs_the_mode_of_the_node() {
        let (config, south) = (leaf(), remote(WhatAmI::Router, "5100"));
        let check = |announced| {
            let (region, remote_bound) = compute_region_of(&south, &config, None, announced)?;
            check_leaf_relationship(&south, &config, &region, &remote_bound, announced)?;
            Ok::<_, zenoh_result::Error>((region, remote_bound))
        };

        let (region, remote_bound) = check(WhatAmI::Router).unwrap();
        assert!(region.bound().is_south() && remote_bound.is_north());
        let e = check(WhatAmI::Client).unwrap_err();
        assert!(e.to_string().contains("matches gateway.north.uplinks"), "{e}");
    }

    /// A release build has no assertion to stop a client north face whose remote is not south
    /// of it: the check is explicit.
    #[test]
    fn client_north_face_requires_a_south_remote() {
        let (config, cloud) = (leaf(), remote(WhatAmI::Router, "c10d"));
        let e = check_leaf_relationship(
            &cloud,
            &config,
            &Region::North,
            &Bound::North,
            WhatAmI::Client,
        )
        .unwrap_err();
        assert!(e.to_string().contains("does not put this node south"), "{e}");
    }

    fn link(protocol: &str, interfaces: &[&str]) -> Link {
        Link {
            src: format!("{protocol}/127.0.0.1:1").parse().unwrap(),
            dst: format!("{protocol}/127.0.0.1:2").parse().unwrap(),
            group: None,
            mtu: 1500,
            is_streamed: false,
            interfaces: interfaces.iter().map(|i| i.to_string()).collect(),
            auth_identifier: zenoh_link::LinkAuthId::Tcp,
            priorities: None,
            reliability: None,
        }
    }

    fn with_uplinks(uplinks: &str) -> ExpandedConfig {
        let mut config = leaf();
        config.gateway.north = Some(
            json5::from_str(&format!(r#"{{ mode: "client", uplinks: {uplinks} }}"#)).unwrap(),
        );
        config
    }

    /// An interface filter does not match a link whose interfaces could not be resolved.
    #[test]
    fn interface_filter_does_not_match_unresolved_interfaces() {
        let config = with_uplinks(r#"[{ interfaces: ["wwan0"] }]"#);

        assert_eq!(
            announced_mode_on(&link("tcp", &["wwan0"]), &config),
            Some(WhatAmI::Client)
        );
        assert_eq!(announced_mode_on(&link("tcp", &["eth0"]), &config), None);
        assert_eq!(announced_mode_on(&link("tcp", &[]), &config), None);
        assert_eq!(
            announced_mode_on(&link("tcp", &["wwan0", "eth0"]), &config),
            None
        );
    }

    /// A filter matches a link iff all its criteria do.
    #[test]
    fn uplink_filter_criteria_are_combined() {
        let config =
            with_uplinks(r#"[{ link_protocols: ["quic"], interfaces: ["wwan0"] }, { link_protocols: ["tls"] }]"#);

        assert!(announced_mode_on(&link("quic", &["wwan0"]), &config).is_some());
        assert!(announced_mode_on(&link("quic", &["eth0"]), &config).is_none());
        assert!(announced_mode_on(&link("quic", &[]), &config).is_none());
        assert!(announced_mode_on(&link("tls", &[]), &config).is_some());
        assert!(announced_mode_on(&link("tcp", &["wwan0"]), &config).is_none());
    }

    /// Without `gateway.north`, no link is announced another mode.
    #[test]
    fn nothing_is_an_uplink_without_the_north_section() {
        let mut config = leaf();
        config.gateway.north = None;

        assert_eq!(announced_mode_on(&link("quic", &["wwan0"]), &config), None);
        assert!(!is_leaf(&config));
        assert!(check_north_config(&config).is_ok());
    }

    /// The `gateway.north` section is checked when the node starts.
    #[test]
    fn north_section_is_checked() {
        let check = |mode: WhatAmI, north: &str| {
            let mut config = leaf();
            config.set_mode(Some(mode)).unwrap();
            config.gateway.north = Some(json5::from_str(north).unwrap());
            check_north_config(&config).map_err(|e| e.to_string())
        };
        let (router, quic) = (WhatAmI::Router, r#"uplinks: [{ link_protocols: ["quic"] }]"#);

        assert!(check(router, &format!(r#"{{ mode: "client", {quic} }}"#)).is_ok());
        // `uplinks` omitted or empty does not mean every link
        let e = check(router, r#"{ mode: "client" }"#).unwrap_err();
        assert!(e.contains("gateway.north.uplinks is required"), "{e}");
        let e = check(router, r#"{ mode: "client", uplinks: [] }"#).unwrap_err();
        assert!(e.contains("gateway.north.uplinks is required"), "{e}");
        // a filter without a criterion would match every link
        let e = check(router, r#"{ mode: "client", uplinks: [{}] }"#).unwrap_err();
        assert!(e.contains("would match every link"), "{e}");
        // an interest must ask for something
        let nothing = format!(r#"{{ mode: "client", {quic}, interests: [{{ key_expr: "a/**" }}] }}"#);
        let e = check(router, &nothing).unwrap_err();
        assert!(e.contains("asks for nothing"), "{e}");
        // only a router can be a leaf, and only a leaf has uplinks and interests
        let e = check(WhatAmI::Peer, &format!(r#"{{ mode: "client", {quic} }}"#)).unwrap_err();
        assert!(e.contains("only a router"), "{e}");
        let e = check(router, &format!("{{ {quic} }}")).unwrap_err();
        assert!(e.contains("only apply to a router"), "{e}");
        let interests = r#"{ interests: [{ key_expr: "a/**", subscribers: true }] }"#;
        let e = check(router, interests).unwrap_err();
        assert!(e.contains("only apply to a router"), "{e}");
        // a default route does not need a leaf, but needs a router and a key expression
        let default_route = r#"default_route: { key_exprs: ["*/app/**"], except: ["r1/**"] }"#;
        assert!(check(router, &format!("{{ {default_route} }}")).is_ok());
        assert!(check(router, &format!(r#"{{ mode: "client", {quic}, {default_route} }}"#)).is_ok());
        let e = check(WhatAmI::Peer, &format!("{{ {default_route} }}")).unwrap_err();
        assert!(e.contains("default_route only applies to a router"), "{e}");
        let e = check(router, r#"{ default_route: { key_exprs: [] } }"#).unwrap_err();
        assert!(e.contains("default_route.key_exprs is empty"), "{e}");
    }

    /// An endpoint is known to be an uplink before it is dialed only when no interface has to be
    /// resolved for it.
    #[test]
    fn uplink_endpoints_are_recognised_without_guessing_interfaces() {
        let by_protocol = with_uplinks(r#"[{ link_protocols: ["quic"] }]"#);
        let by_interface = with_uplinks(r#"[{ interfaces: ["wwan0"] }]"#);
        let endpoint = |e: &str| e.parse::<EndPoint>().unwrap();

        assert!(is_uplink_endpoint(&endpoint("quic/cloud:443"), &by_protocol));
        assert!(!is_uplink_endpoint(&endpoint("tcp/192.168.1.2:7447"), &by_protocol));
        assert!(!is_uplink_endpoint(&endpoint("tcp/cloud:443"), &by_interface));
        assert!(is_uplink_endpoint(&endpoint("tcp/cloud:443#iface=wwan0"), &by_interface));
        assert!(!is_uplink_endpoint(&endpoint("tcp/cloud:443#iface=eth0"), &by_interface));
    }
}
