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

//! Gossip locator freshness.
//!
//! These tests describe the behaviour expected when a node's routable IPv4
//! address appears *after* the node has bound its wildcard listener -- the
//! normal situation on an embedded board where the network interface is
//! configured by a service that races the zenoh applications at boot.
//!
//! * T1 -- interface enumeration must observe an address added after the
//!   first enumeration.
//! * T2 -- a peer that bound before its address existed must become
//!   reachable to a peer that joins afterwards.
//! * T3 -- an advertisement that carries no locators must not stop a later
//!   advertisement with a good locator from being dialled.
//! * T4 -- a production-shaped startup storm before the address exists must
//!   still converge once the address is there.
//! * T5 -- a peer that has nothing of its own to dial must become reachable
//!   too.
//!
//! Every test except T3 needs a private network namespace whose only interface
//! is a `dummy0` link without an IPv4 address; they are `#[ignore]`d so that a
//! plain `cargo test` skips them. Run them with `ci/netns-check/run.sh`,
//! which builds this target and gives each test its own namespace and its
//! own process.
//!
//! Peers that are supposed to join *after* the address exists are started as
//! **child processes** of the test binary. That is deliberate: interface
//! enumeration is cached per process, so a peer sharing the test's process
//! would inherit the test's own (pre-address) view and stop being a valid
//! control -- which it would not, in a deployment where peers are separate
//! processes.

#![cfg(all(feature = "unstable", feature = "transport_tcp"))]

use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr},
    process::{Child, Command, Stdio},
    str::FromStr,
    time::{Duration, Instant},
};

use zenoh::{
    config::{WhatAmI, ZenohId},
    Session,
};
use zenoh_config::Config;
use zenoh_link::EndPoint;
use zenoh_protocol::core::ZenohIdProto;
use zenoh_test::get_free_tcp_port;

// The interface the namespace exposes, and the address that shows up late.
const NETNS_IFACE: &str = "dummy0";
const NETNS_ADDR: &str = "10.99.0.1";
const NETNS_CIDR: &str = "10.99.0.1/24";

// Generous, deliberately loose budgets: they must never be the reason a test
// goes red on a loaded machine, so a failure at these budgets is evidence of
// permanence rather than of slowness.
const OPEN_TIMEOUT: Duration = Duration::from_secs(30);
const ROUTER_LINK_TIMEOUT: Duration = Duration::from_secs(30);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);
// Long enough to outlive every budget above, taken twice.
const CHILD_LIFETIME_SECS: u64 = 300;

// ---------------------------------------------------------------------------
// network namespace helpers
// ---------------------------------------------------------------------------

/// `ip addr show` over every address family: `ip -4 addr show` hides
/// interfaces that have no IPv4 address, which is exactly the state we assert
/// about before the address is added.
fn ip_addr_show() -> String {
    let out = Command::new("ip")
        .args(["addr", "show"])
        .output()
        .expect("`ip` must be available (iproute2); run via ci/netns-check/run.sh");
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Fails loudly unless we are inside the expected namespace: `dummy0` present
/// and no routable IPv4 anywhere yet.
fn assert_netns_precondition() {
    let shown = ip_addr_show();
    assert!(
        shown.contains(NETNS_IFACE),
        "netns precondition: interface {NETNS_IFACE} not found. This test must run as root \
         inside `unshare -n` with lo+{NETNS_IFACE} up. Current state:\n{shown}"
    );
    assert!(
        !shown.contains(NETNS_ADDR),
        "netns precondition: {NETNS_ADDR} is already configured; the namespace must start \
         without any routable IPv4. Current state:\n{shown}"
    );
}

/// Adds the late address and returns only once *the kernel* reports it, so no
/// test ever sleeps to guess when the address became real.
fn add_netns_address() {
    let status = Command::new("ip")
        .args(["addr", "add", NETNS_CIDR, "dev", NETNS_IFACE])
        .status()
        .expect("`ip addr add` could not be spawned");
    assert!(
        status.success(),
        "`ip addr add {NETNS_CIDR} dev {NETNS_IFACE}` failed -- the test process must be root \
         inside its own network namespace"
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if ip_addr_show().contains(NETNS_ADDR) {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!(
        "kernel never reported {NETNS_ADDR} on {NETNS_IFACE}:\n{}",
        ip_addr_show()
    );
}

// ---------------------------------------------------------------------------
// configuration helpers (production shape lives in `peer_config`)
// ---------------------------------------------------------------------------

fn credentials_file() -> String {
    let path = std::env::temp_dir().join("gossip_locator_freshness_credentials.txt");
    std::fs::write(&path, "u:p\n").unwrap();
    path.to_string_lossy().into_owned()
}

#[cfg(feature = "auth_usrpwd")]
fn add_usrpwd(config: &mut Config) {
    config
        .insert_json5(
            "transport",
            r#"{ "auth": { usrpwd: { user: "u", password: "p" } } }"#,
        )
        .unwrap();
    config
        .transport
        .auth
        .usrpwd
        .set_dictionary_file(Some(credentials_file()))
        .unwrap();
}

#[cfg(not(feature = "auth_usrpwd"))]
fn add_usrpwd(_config: &mut Config) {
    panic!("this test requires the `auth_usrpwd` feature");
}

fn router_config(port: u16, auth: bool) -> Config {
    let mut config = Config::default();
    config.set_mode(Some(WhatAmI::Router)).unwrap();
    config
        .listen
        .endpoints
        .set(vec![format!("tcp/127.0.0.1:{port}")
            .parse::<EndPoint>()
            .unwrap()])
        .unwrap();
    config.scouting.multicast.set_enabled(Some(false)).unwrap();
    config.scouting.gossip.set_enabled(Some(true)).unwrap();
    if auth {
        add_usrpwd(&mut config);
    }
    config
}

/// A peer shaped like a deployed one: wildcard listener, one configured
/// connect endpoint (the router), multicast scouting off, gossip fanned out by
/// the router, peer-to-peer autoconnect with the `greater-zid` strategy.
fn peer_config(zid: &ZenohId, router_port: u16, auth: bool) -> Config {
    let mut config = Config::default();
    config.set_mode(Some(WhatAmI::Peer)).unwrap();
    config.set_id(Some(*zid)).unwrap();
    config
        .listen
        .endpoints
        .set(vec!["tcp/0.0.0.0:0".parse::<EndPoint>().unwrap()])
        .unwrap();
    config
        .connect
        .endpoints
        .set(vec![format!("tcp/127.0.0.1:{router_port}")
            .parse::<zenoh_config::EndPoints>()
            .unwrap()])
        .unwrap();
    config
        .insert_json5(
            "scouting",
            r#"{
                multicast: { enabled: false },
                gossip: {
                    enabled: true,
                    multihop: false,
                    target: { router: ["router", "peer"], peer: ["router"] },
                    autoconnect: { router: [], peer: ["peer"] },
                    autoconnect_strategy: { peer: { to_peer: "greater-zid" } },
                },
            }"#,
        )
        .unwrap();
    if auth {
        add_usrpwd(&mut config);
    }
    config
}

/// `n` zids in ascending `ZenohIdProto` order, so a test can pick who dials
/// whom under `greater-zid` without guessing the byte order.
fn ascending_zids(n: usize) -> Vec<ZenohId> {
    // ZenohId rejects leading zeros, so the first nibble must be non-zero.
    assert!((1..=15).contains(&n), "ascending_zids supports 1..=15 ids");
    let mut zids: Vec<ZenohId> = (1..=n)
        .map(|i| ZenohId::from_str(&format!("{i:x}{i:x}")).unwrap())
        .collect();
    zids.sort_by_key(|z| ZenohIdProto::from(*z));
    zids.dedup_by_key(|z| ZenohIdProto::from(*z));
    assert_eq!(zids.len(), n, "zid generator produced duplicates");
    zids
}

// ---------------------------------------------------------------------------
// out-of-process peers
// ---------------------------------------------------------------------------

const ENV_ZID: &str = "ZENOH_REPRO_CHILD_ZID";
const ENV_ROUTER_PORT: &str = "ZENOH_REPRO_CHILD_ROUTER_PORT";
const ENV_AUTH: &str = "ZENOH_REPRO_CHILD_AUTH";
const ENV_LIFETIME: &str = "ZENOH_REPRO_CHILD_LIFETIME";

/// A peer running in its own process, inside the same network namespace.
struct ChildPeer {
    name: String,
    zid: ZenohId,
    child: Child,
}

impl Drop for ChildPeer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Starts `child_peer_process` in a new process. Used for every peer that is
/// supposed to see the interface address that the parent has just added.
fn spawn_child_peer(name: &str, zid: ZenohId, router_port: u16, auth: bool) -> ChildPeer {
    let exe = std::env::current_exe().expect("current_exe");
    let child = Command::new(exe)
        .args([
            "--exact",
            "child_peer_process",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(ENV_ZID, zid.to_string())
        .env(ENV_ROUTER_PORT, router_port.to_string())
        .env(ENV_AUTH, if auth { "1" } else { "0" })
        .env(ENV_LIFETIME, CHILD_LIFETIME_SECS.to_string())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|e| panic!("could not spawn child peer {name}: {e}"));
    eprintln!(
        "[parent] spawned child peer {name} zid={zid} pid={}",
        child.id()
    );
    ChildPeer {
        name: name.to_string(),
        zid,
        child,
    }
}

/// The body of an out-of-process peer. Never run directly; `spawn_child_peer`
/// re-executes the test binary with this name and the `ZENOH_REPRO_CHILD_*`
/// environment set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "internal helper: re-executed as a child process by T2/T4"]
async fn child_peer_process() {
    let zid = ZenohId::from_str(
        &std::env::var(ENV_ZID)
            .expect("child_peer_process is a helper; it is spawned by T2/T4, not run directly"),
    )
    .expect("child zid");
    let router_port: u16 = std::env::var(ENV_ROUTER_PORT)
        .expect(ENV_ROUTER_PORT)
        .parse()
        .expect("child router port");
    let auth = std::env::var(ENV_AUTH).as_deref() == Ok("1");
    let lifetime: u64 = std::env::var(ENV_LIFETIME)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(CHILD_LIFETIME_SECS);

    zenoh::init_log_from_env_or("error");
    eprintln!(
        "[child {zid}] pid={} starting; interfaces:\n{}",
        std::process::id(),
        ip_addr_show()
    );
    let session = open_session(peer_config(&zid, router_port, auth), "child").await;
    eprintln!("[child {zid}] session open");
    tokio::time::sleep(Duration::from_secs(lifetime)).await;
    let peers: Vec<String> = session
        .info()
        .peers_zid()
        .await
        .map(|z| z.to_string())
        .collect();
    eprintln!("[child {zid}] lifetime elapsed, peers={peers:?}");
}

// ---------------------------------------------------------------------------
// waiting / observation helpers
// ---------------------------------------------------------------------------

async fn wait_until<F, Fut>(budget: Duration, mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if cond().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn open_session(config: Config, who: &str) -> Session {
    match tokio::time::timeout(OPEN_TIMEOUT, zenoh::open(config)).await {
        Ok(res) => res.unwrap_or_else(|e| panic!("zenoh::open failed for {who}: {e}")),
        Err(_) => panic!("zenoh::open for {who} did not return within {OPEN_TIMEOUT:?}"),
    }
}

/// True when `session` holds a direct peer transport to `other`.
async fn sees(session: &Session, other: ZenohId) -> bool {
    session.info().peers_zid().await.any(|z| z == other)
}

async fn sees_router(session: &Session, router: ZenohId) -> bool {
    session.info().routers_zid().await.any(|z| z == router)
}

async fn linked(a: &Session, b: &Session) -> bool {
    sees(a, b.zid()).await && sees(b, a.zid()).await
}

async fn dump(label: &str, sessions: &[(&str, &Session)], children: &[&ChildPeer]) {
    eprintln!("---- {label} ----");
    for (name, s) in sessions {
        let peers: Vec<String> = s.info().peers_zid().await.map(|z| z.to_string()).collect();
        let routers: Vec<String> = s
            .info()
            .routers_zid()
            .await
            .map(|z| z.to_string())
            .collect();
        eprintln!(
            "  in-process {name} zid={} peers={peers:?} routers={routers:?}",
            s.zid()
        );
    }
    for c in children {
        eprintln!("  child {} zid={} pid={}", c.name, c.zid, c.child.id());
    }
    eprintln!("  interfaces:\n{}", ip_addr_show());
}

// ---------------------------------------------------------------------------
// T1 -- interface enumeration must observe an address added after first touch
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires a private network namespace; run via ci/netns-check/run.sh"]
fn t1_enumeration_observes_address_added_after_first_touch() {
    assert_netns_precondition();

    // First enumeration happens while the namespace has no routable IPv4 --
    // exactly what a process that starts before the network service sees.
    let before = zenoh_util::net::get_ipv4_ipaddrs(None, true);
    assert!(
        before.is_empty(),
        "precondition: expected no non-loopback IPv4 before the address is added, got {before:?}"
    );

    add_netns_address();

    let expected = IpAddr::V4(Ipv4Addr::new(10, 99, 0, 1));

    let after = zenoh_util::net::get_ipv4_ipaddrs(None, true);
    assert!(
        after.contains(&expected),
        "get_ipv4_ipaddrs did not observe {expected} after it was added to {NETNS_IFACE}; \
         it returned {after:?} while the kernel reports:\n{}",
        ip_addr_show()
    );

    let all = zenoh_util::net::get_local_addresses(None).expect("get_local_addresses");
    assert!(
        all.contains(&expected),
        "get_local_addresses did not observe {expected} after it was added to {NETNS_IFACE}; \
         it returned {all:?}"
    );
}

// ---------------------------------------------------------------------------
// T2 -- a peer that bound before its address existed must become reachable
// ---------------------------------------------------------------------------

/// One peer (`early`) binds `tcp/0.0.0.0:0` before the namespace has any
/// routable IPv4. The address then appears and two further peers join, each in
/// its own process:
///
/// * `low`  has a *smaller* zid, so under `greater-zid` **`early` dials it**.
///   This is the control: it exercises the same router, the same gossip and
///   the same moment, and only needs `low`'s locators, which are fresh.
/// * `high` has a *greater* zid, so **it must dial `early`**, which needs
///   `early`'s advertised locators.
///
/// Both are observed from `early`'s side, so the only difference between
/// control and subject is whose locators the dial depends on.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires a private network namespace; run via ci/netns-check/run.sh"]
async fn t2_peer_bound_before_address_becomes_reachable() {
    zenoh::init_log_from_env_or("error");
    assert_netns_precondition();

    let router_port = get_free_tcp_port();
    let router = open_session(router_config(router_port, false), "router").await;
    let router_zid = router.zid();

    let zids = ascending_zids(3);
    let (zid_low, zid_early, zid_high) = (zids[0], zids[1], zids[2]);

    // `early` binds its wildcard listener while no routable IPv4 exists.
    let early = open_session(peer_config(&zid_early, router_port, false), "early").await;
    assert!(
        wait_until(ROUTER_LINK_TIMEOUT, || sees_router(&early, router_zid)).await,
        "setup: the early peer never connected to the router"
    );

    // The interface address appears. This is the only independent variable.
    add_netns_address();

    // Control: a fresh process whose zid is smaller, so `early` dials it.
    let low = spawn_child_peer("low", zid_low, router_port, false);
    if !wait_until(CONVERGENCE_TIMEOUT, || sees(&early, zid_low)).await {
        dump("T2 control failure", &[("early", &early)], &[&low]).await;
        panic!(
            "CONTROL FAILED: the early peer did not even dial a peer that joined after \
             {NETNS_ADDR} existed, so this run says nothing about the reverse direction"
        );
    }
    eprintln!("[parent] control ok: early dialled the low-zid child");

    // Subject: a fresh process whose zid is greater, so it must dial `early`.
    let high = spawn_child_peer("high", zid_high, router_port, false);
    if !wait_until(CONVERGENCE_TIMEOUT, || sees(&early, zid_high)).await {
        dump("T2 failure", &[("early", &early)], &[&low, &high]).await;
        panic!(
            "a peer that bound its wildcard listener before {NETNS_ADDR} existed never became \
             reachable: {CONVERGENCE_TIMEOUT:?} after the address was configured, a peer that \
             joined afterwards still has no transport to it, while the same peer is reachable \
             in the opposite direction (control above)"
        );
    }

    drop(high);
    drop(low);
    early.close().await.unwrap();
    router.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// T3 -- an advertisement without locators must not disable later dials
// ---------------------------------------------------------------------------

#[cfg(feature = "internal")]
mod connect_bookkeeping {
    use zenoh::internal::runtime::{Runtime, RuntimeBuilder};
    use zenoh_link::Locator;

    use super::*;

    /// A peer with no scouting at all, reachable at an explicit loopback
    /// endpoint. Stands in for the neighbour whose gossip advertisement is
    /// first seen without locators and later seen with a good one.
    async fn open_target(port: u16) -> Session {
        let mut config = Config::default();
        config.set_mode(Some(WhatAmI::Peer)).unwrap();
        config
            .listen
            .endpoints
            .set(vec![format!("tcp/127.0.0.1:{port}")
                .parse::<EndPoint>()
                .unwrap()])
            .unwrap();
        config.scouting.multicast.set_enabled(Some(false)).unwrap();
        config.scouting.gossip.set_enabled(Some(false)).unwrap();
        open_session(config, "target").await
    }

    /// An outbound-only runtime: no listener, no scouting, no configured
    /// connect endpoints, so the only dials it makes are the ones the test
    /// asks for.
    async fn open_prober(name: &str) -> (Runtime, Session) {
        let mut config = Config::default();
        config.set_mode(Some(WhatAmI::Peer)).unwrap();
        config.listen.endpoints.set(vec![]).unwrap();
        config.connect.endpoints.set(vec![]).unwrap();
        config.scouting.multicast.set_enabled(Some(false)).unwrap();
        config.scouting.gossip.set_enabled(Some(false)).unwrap();
        let mut runtime = RuntimeBuilder::new(config.into())
            .build()
            .await
            .unwrap_or_else(|e| panic!("could not build runtime for {name}: {e}"));
        let session = zenoh::session::init(runtime.clone().into())
            .await
            .unwrap_or_else(|e| panic!("could not init session for {name}: {e}"));
        runtime
            .start()
            .await
            .unwrap_or_else(|e| panic!("could not start runtime for {name}: {e}"));
        (runtime, session)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn t3_empty_locator_advertisement_does_not_block_later_dial() {
        zenoh::init_log_from_env_or("error");

        let port = get_free_tcp_port();
        let target = open_target(port).await;
        let target_zid = ZenohIdProto::from(target.zid());
        let good: Locator = format!("tcp/127.0.0.1:{port}")
            .parse()
            .expect("locator must parse");

        // Control: a runtime that only ever sees the good locator dials it.
        // Without this a red result below could just mean "locator unusable".
        let (control_rt, control_session) = open_prober("control").await;
        assert!(
            control_rt
                .connect_peer(&target_zid, std::slice::from_ref(&good))
                .await,
            "CONTROL FAILED: connect_peer refused a plainly good locator {good}; the rest of \
             this test says nothing"
        );
        assert!(
            wait_until(Duration::from_secs(10), || sees(
                &control_session,
                target.zid()
            ))
            .await,
            "CONTROL FAILED: connect_peer reported success but no transport to the target exists"
        );

        // Subject: the first advertisement for this zid carries no locators,
        // which is what a peer that bound before its address existed sends.
        let (subject_rt, subject_session) = open_prober("subject").await;
        assert!(
            !subject_rt.connect_peer(&target_zid, &[]).await,
            "sanity: an advertisement with no locators cannot produce a transport"
        );

        // The neighbour's address has since appeared and gossip now carries a
        // usable locator; this dial must happen.
        assert!(
            subject_rt
                .connect_peer(&target_zid, std::slice::from_ref(&good))
                .await,
            "connect_peer ignored a good locator for a zid whose earlier advertisement had no \
             locators: the pending-connection entry taken on the empty attempt is never released"
        );
        assert!(
            wait_until(Duration::from_secs(10), || sees(
                &subject_session,
                target.zid()
            ))
            .await,
            "no transport to the target after a good locator was offered following an \
             empty-locator advertisement for the same zid"
        );

        subject_session.close().await.unwrap();
        control_session.close().await.unwrap();
        target.close().await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// T4 -- production-shaped startup storm before the address exists
// ---------------------------------------------------------------------------

/// Five peers open at once against one router, all before the namespace has a
/// routable IPv4 -- the shape of a restart storm. The address then appears
/// and a sixth peer joins from its own process with the *smallest* zid, so
/// the early cohort is the side that dials it.
///
/// * control -- every early peer reaches the late one, proving the topology,
///   the credentials and the gossip fan-out all work after the address exists.
/// * subject -- every pair inside the early cohort must converge too.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires a private network namespace; run via ci/netns-check/run.sh"]
async fn t4_startup_storm_before_address_converges_after_address() {
    zenoh::init_log_from_env_or("error");
    assert_netns_precondition();

    const EARLY: usize = 5;

    let router_port = get_free_tcp_port();
    let router = open_session(router_config(router_port, true), "router").await;
    let router_zid = router.zid();

    let zids = ascending_zids(EARLY + 1);
    let late_zid = zids[0];
    let early_zids: Vec<ZenohId> = zids[1..].to_vec();

    // Storm: every early peer opens concurrently, all before any routable IPv4.
    let mut handles = Vec::new();
    for (i, zid) in early_zids.iter().enumerate() {
        let cfg = peer_config(zid, router_port, true);
        handles.push(tokio::spawn(async move {
            match tokio::time::timeout(OPEN_TIMEOUT, zenoh::open(cfg)).await {
                Ok(res) => res.unwrap_or_else(|e| panic!("zenoh::open failed for early{i}: {e}")),
                Err(_) => panic!("zenoh::open for early{i} did not return within {OPEN_TIMEOUT:?}"),
            }
        }));
    }
    let mut early = Vec::new();
    for h in handles {
        early.push(h.await.expect("early peer open task panicked"));
    }
    for (i, s) in early.iter().enumerate() {
        assert!(
            wait_until(ROUTER_LINK_TIMEOUT, || sees_router(s, router_zid)).await,
            "setup: early peer {i} never connected to the router"
        );
    }

    // The interface address appears.
    add_netns_address();

    // The late joiner starts afterwards, in its own process, as a service
    // restarted after boot would.
    let late = spawn_child_peer("late", late_zid, router_port, true);

    let named: Vec<(String, &Session)> = early
        .iter()
        .enumerate()
        .map(|(i, s)| (format!("early{i}"), s))
        .collect();
    let named_refs: Vec<(&str, &Session)> = named.iter().map(|(n, s)| (n.as_str(), *s)).collect();

    // Control: every early peer must reach the late joiner.
    if !wait_until(CONVERGENCE_TIMEOUT, || async {
        for s in &early {
            if !sees(s, late_zid).await {
                return false;
            }
        }
        true
    })
    .await
    {
        dump("T4 control failure", &named_refs, &[&late]).await;
        panic!(
            "CONTROL FAILED: the early cohort did not reach the peer that started after \
             {NETNS_ADDR} existed, so this run says nothing about the early cohort itself"
        );
    }
    eprintln!("[parent] control ok: the whole early cohort reached the late joiner");

    // Subject: every pair inside the early cohort must converge too. They all
    // bound their wildcard listener before the address existed; once the
    // address is there nothing about the topology keeps them apart.
    if !wait_until(CONVERGENCE_TIMEOUT, || async {
        for i in 0..early.len() {
            for j in (i + 1)..early.len() {
                if !linked(&early[i], &early[j]).await {
                    return false;
                }
            }
        }
        true
    })
    .await
    {
        dump("T4 failure", &named_refs, &[&late]).await;
        let mut missing = Vec::new();
        for i in 0..early.len() {
            for j in (i + 1)..early.len() {
                if !linked(&early[i], &early[j]).await {
                    missing.push(format!("early{i}<->early{j}"));
                }
            }
        }
        panic!(
            "the {EARLY} peers that started before {NETNS_ADDR} existed never converged: \
             {} of {} pairs still have no direct transport {CONVERGENCE_TIMEOUT:?} after the \
             address was configured, even though every one of them reached the peer that \
             started later (missing: {missing:?})",
            missing.len(),
            EARLY * (EARLY - 1) / 2
        );
    }

    drop(late);
    for s in early {
        s.close().await.unwrap();
    }
    router.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// T5 -- a peer with nothing of its own to dial must still become reachable
// ---------------------------------------------------------------------------

/// Builds the T5 universe: one router, one subject peer holding the smallest
/// zid there is, and later one peer holding a greater zid, in its own process.
///
/// Under `greater-zid` the subject never dials anybody, and there is no other
/// peer for it to dial in any case. Its only transport is the router link. So
/// unlike T2 and T4, nothing here can hand the subject a transport as a side
/// effect: the one peer that could dial it is the one that needs the locators
/// the subject is failing to advertise.
///
/// `address_first` selects the control (the address exists before anything
/// binds) or the subject case (the address arrives after the subject bound).
async fn run_isolated_peer_case(address_first: bool) {
    assert_netns_precondition();
    if address_first {
        add_netns_address();
    }

    let router_port = get_free_tcp_port();
    let router = open_session(router_config(router_port, false), "router").await;
    let router_zid = router.zid();

    let zids = ascending_zids(2);
    let (zid_subject, zid_later) = (zids[0], zids[1]);

    let subject = open_session(peer_config(&zid_subject, router_port, false), "subject").await;
    assert!(
        wait_until(ROUTER_LINK_TIMEOUT, || sees_router(&subject, router_zid)).await,
        "setup: the subject never connected to the router"
    );
    // The premise of this test: the subject holds no peer transport, and will
    // never open one of its own.
    let already: Vec<String> = subject
        .info()
        .peers_zid()
        .await
        .map(|z| z.to_string())
        .collect();
    assert!(
        already.is_empty(),
        "setup: the subject is supposed to be alone, but it already sees {already:?}"
    );

    if !address_first {
        add_netns_address();
    }

    // The only other peer in the universe. Greater zid, so it is the one that
    // must dial; own process, so its interface view is its own.
    let later = spawn_child_peer("later", zid_later, router_port, false);

    if !wait_until(CONVERGENCE_TIMEOUT, || sees(&subject, zid_later)).await {
        dump("T5 failure", &[("subject", &subject)], &[&later]).await;
        let when = if address_first {
            "with the address configured before it bound"
        } else {
            "having bound its wildcard listener before the address existed"
        };
        panic!(
            "the only peer that could reach the subject never did: the subject {when} still \
             has no transport {CONVERGENCE_TIMEOUT:?} after a greater-zid peer joined. Nothing \
             in this topology gives the subject a transport of its own, so a locator refresh \
             driven by transport establishment cannot fire here"
        );
    }

    drop(later);
    subject.close().await.unwrap();
    router.close().await.unwrap();
}

/// Control: identical topology, but the address is configured before anything
/// binds. Establishes that a lone subject with the smallest zid is reachable
/// at all, that the `greater-zid` direction works with a single dialer, and
/// that the child-process peer can reach it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires a private network namespace; run via ci/netns-check/run.sh"]
async fn t5_control_isolated_peer_bound_after_address_is_reachable() {
    zenoh::init_log_from_env_or("error");
    run_isolated_peer_case(true).await;
}

/// Subject: the same universe, with the address arriving after the subject
/// bound. A node in this position reports zero peer links and makes no
/// outbound dial of its own, so nothing about it ever changes again.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires a private network namespace; run via ci/netns-check/run.sh"]
async fn t5_isolated_peer_bound_before_address_becomes_reachable() {
    zenoh::init_log_from_env_or("error");
    run_isolated_peer_case(false).await;
}
