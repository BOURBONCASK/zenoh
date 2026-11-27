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

//! Which links get an owned sender: the rules of the configuration, checked at start-up, and
//! the reasons for which a selected link is refused when it is activated.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use zenoh_config::{ExpandedConfig, HandoverLinkConf};
use zenoh_result::{bail, ZResult};

use super::{CloseReason, ProcessPool};

/// The link protocols on which a sender is supported.
#[cfg(not(feature = "test"))]
const SUPPORTED_PROTOCOLS: &[&str] = &["quic"];
/// Tests drive links over TCP through a proxy that they control.
#[cfg(feature = "test")]
const SUPPORTED_PROTOCOLS: &[&str] = &["quic", "tcp"];

/// Smallest minimum reserve: room for the records of lost replies and a few control messages.
const RESERVE_FLOOR: usize = 16 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct Rule {
    pub(crate) id: String,
    pub(crate) link_protocols: Vec<String>,
    pub(crate) interfaces: Vec<String>,
    pub(crate) data_bytes: usize,
    pub(crate) large_bytes: usize,
    pub(crate) reserve_min_bytes: usize,
    pub(crate) reserve_max_bytes: usize,
    pub(crate) max_age: Duration,
    pub(crate) control_max_age: Duration,
}

impl Rule {
    fn matches(&self, protocol: &str, interfaces: &[String]) -> bool {
        self.link_protocols.iter().any(|p| p == protocol)
            && (self.interfaces.is_empty()
                || interfaces.iter().any(|i| self.interfaces.contains(i)))
    }
}

/// Why a selected link was not activated. The link is refused: it is never run without its
/// sender.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActivationRefusal {
    /// Shared memory was negotiated on the transport.
    Shm,
    /// QoS was not negotiated on the transport.
    Qos,
    /// The transport has, or would get, more than one link.
    Multilink,
    /// The link is not a single reliable stream.
    Carrier,
    /// The process pool cannot give the link its minimum reserve.
    ReserveUnavailable,
}

impl ActivationRefusal {
    pub(crate) const ALL: [Self; 5] = [
        Self::Shm,
        Self::Qos,
        Self::Multilink,
        Self::Carrier,
        Self::ReserveUnavailable,
    ];

    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Shm => "shm",
            Self::Qos => "qos",
            Self::Multilink => "multilink",
            Self::Carrier => "carrier",
            Self::ReserveUnavailable => "reserve_unavailable",
        }
    }
}

/// What the selected links of a process hold and what happened to them, for the admin space.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RulesStatus {
    /// Bytes reserved by all selected links.
    pub pool_bytes: usize,
    pub total_max_bytes: usize,
    /// Selected links refused when they were added, by reason.
    pub refused_links: std::collections::BTreeMap<&'static str, usize>,
    /// Transports closed by their sender, by reason.
    pub closed_transports: std::collections::BTreeMap<&'static str, usize>,
}

/// The hand-over rules of a process, with the budget its selected links share.
#[derive(Debug)]
pub struct Rules {
    pub(crate) rules: Vec<Rule>,
    pub(crate) pool: Arc<ProcessPool>,
    pub(crate) wait_before_close: Duration,
    pub(crate) default_query_timeout: Duration,
    refusals: [AtomicUsize; ActivationRefusal::ALL.len()],
    closes: [AtomicUsize; CloseReason::ALL.len()],
}

impl Rules {
    /// Reads and checks the rules. `Ok(None)` when there is none: nothing of the hand-over
    /// exists then.
    pub fn from_config(config: &ExpandedConfig) -> ZResult<Option<Arc<Self>>> {
        let queue = config.transport().link().tx().queue();
        let handover = queue.handover();
        let links: &Vec<HandoverLinkConf> = handover.links();
        if links.is_empty() {
            return Ok(None);
        }
        let wait_before_close = *config
            .transport()
            .link()
            .tx()
            .queue()
            .congestion_control()
            .block()
            .wait_before_close();
        if wait_before_close <= 0 {
            bail!(
                "transport/link/tx/queue/handover: wait_before_close must be positive and \
                 finite on a process with hand-over rules (it is {wait_before_close})"
            );
        }
        let unicast = config.transport().unicast();
        if *unicast.lowlatency() {
            bail!(
                "transport/link/tx/queue/handover: not supported with transport/unicast/lowlatency"
            );
        }
        if !*unicast.qos().enabled() {
            bail!("transport/link/tx/queue/handover: not supported with transport/unicast/qos/enabled: false");
        }
        if *unicast.max_links() != 1 {
            bail!(
                "transport/link/tx/queue/handover: not supported with transport/unicast/max_links: {} \
                 (one link per transport)",
                unicast.max_links()
            );
        }
        Self::new(
            *handover.total_max_bytes(),
            links,
            Duration::from_micros(wait_before_close as u64),
            Duration::from_millis(
                config
                    .queries_default_timeout()
                    .unwrap_or(zenoh_config::defaults::queries_default_timeout),
            ),
        )
        .map(Some)
    }

    /// Checks the rules themselves. What they require of the rest of the configuration is
    /// checked by [`Rules::from_config`], and again for each link when it is added.
    pub fn new(
        total: usize,
        links: &[HandoverLinkConf],
        wait_before_close: Duration,
        default_query_timeout: Duration,
    ) -> ZResult<Arc<Self>> {
        let mut rules: Vec<Rule> = Vec::with_capacity(links.len());
        for (n, link) in links.iter().enumerate() {
            let id = link.id.clone().unwrap_or_else(|| format!("#{n}"));
            let at = format!("transport/link/tx/queue/handover/links[{id}]");
            if link.link_protocols.is_empty() {
                bail!("{at}: link_protocols must name the protocols of the selected links");
            }
            for protocol in &link.link_protocols {
                if !SUPPORTED_PROTOCOLS.contains(&protocol.as_str()) {
                    bail!(
                        "{at}: link protocol {protocol:?} is not supported (supported: {SUPPORTED_PROTOCOLS:?})"
                    );
                }
                if let Some(other) = rules.iter().find(|r| r.link_protocols.contains(protocol)) {
                    bail!(
                        "{at}: link protocol {protocol:?} is also selected by rule {:?}; a link \
                         must match one rule at most",
                        other.id
                    );
                }
            }
            if link.data_bytes == 0 || link.reserve_max_bytes == 0 {
                bail!("{at}: data_bytes and reserve_max_bytes must be positive");
            }
            if link.reserve_min_bytes < RESERVE_FLOOR {
                bail!("{at}: reserve_min_bytes must be at least {RESERVE_FLOOR}");
            }
            if link.reserve_min_bytes > link.reserve_max_bytes {
                bail!("{at}: reserve_min_bytes is larger than reserve_max_bytes");
            }
            let all = link
                .data_bytes
                .checked_mul(zenoh_protocol::core::Priority::NUM)
                .and_then(|sum| sum.checked_add(link.large_bytes))
                .and_then(|sum| sum.checked_add(link.reserve_max_bytes));
            if !all.is_some_and(|all| all <= total) {
                bail!(
                    "{at}: the budgets of one link (data_bytes for each priority, large_bytes, \
                     reserve_max_bytes) exceed total_max_bytes ({total})"
                );
            }
            let max_age = match link.max_age_ms {
                Some(0) => bail!("{at}: max_age_ms must be positive"),
                Some(ms) => Duration::from_millis(ms),
                None => wait_before_close,
            };
            let control_max_age = match link.control_max_age_ms {
                Some(ms) => Duration::from_millis(ms),
                None => wait_before_close.max(Duration::from_secs(10)).max(max_age),
            };
            if control_max_age < wait_before_close {
                bail!("{at}: control_max_age_ms must not be shorter than wait_before_close");
            }
            if control_max_age < max_age {
                bail!("{at}: control_max_age_ms must not be shorter than the age limit of data (max_age_ms)");
            }
            rules.push(Rule {
                id,
                link_protocols: link.link_protocols.clone(),
                interfaces: link.interfaces.clone(),
                data_bytes: link.data_bytes,
                large_bytes: link.large_bytes,
                reserve_min_bytes: link.reserve_min_bytes,
                reserve_max_bytes: link.reserve_max_bytes,
                max_age,
                control_max_age,
            });
        }
        Ok(Arc::new(Self {
            rules,
            pool: ProcessPool::new(total),
            wait_before_close,
            default_query_timeout,
            refusals: Default::default(),
            closes: Default::default(),
        }))
    }

    /// The rule that selects a link, if any. Rules select disjoint protocols, so there is one
    /// at most.
    pub(crate) fn find(&self, protocol: &str, interfaces: &[String]) -> Option<&Rule> {
        self.rules.iter().find(|r| r.matches(protocol, interfaces))
    }

    pub(crate) fn count_refusal(&self, refusal: ActivationRefusal) {
        self.refusals[refusal as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// The selected links refused at activation so far, by reason.
    pub fn refusals(&self) -> impl Iterator<Item = (&'static str, usize)> + '_ {
        ActivationRefusal::ALL.iter().map(|r| {
            (
                r.as_str(),
                self.refusals[*r as usize].load(Ordering::Relaxed),
            )
        })
    }

    pub(crate) fn count_close(&self, reason: CloseReason) {
        self.closes[reason as usize - 1].fetch_add(1, Ordering::Relaxed);
    }

    /// The transports closed by their sender so far, by reason.
    pub fn closes(&self) -> impl Iterator<Item = (&'static str, usize)> + '_ {
        CloseReason::ALL.iter().map(|r| {
            (
                r.as_str(),
                self.closes[*r as usize - 1].load(Ordering::Relaxed),
            )
        })
    }

    pub fn status(&self) -> RulesStatus {
        let (pool_bytes, total_max_bytes) = self.pool_usage();
        RulesStatus {
            pool_bytes,
            total_max_bytes,
            refused_links: self.refusals().collect(),
            closed_transports: self.closes().collect(),
        }
    }

    /// Bytes reserved by all selected links, and the limit.
    pub fn pool_usage(&self) -> (usize, usize) {
        (self.pool.in_use(), self.pool.total())
    }
}

#[cfg(test)]
mod tests {
    use zenoh_config::Config;

    use super::*;

    const HANDOVER: &str = "transport/link/tx/queue/handover";
    const CLOSE: &str = "transport/link/tx/queue/congestion_control/block/wait_before_close";

    /// The rules of a default configuration with these changes.
    fn rules(changes: &[(&str, &str)]) -> ZResult<Option<Arc<Rules>>> {
        let mut config = Config::default();
        for (key, value) in changes {
            config
                .insert_json5(key, value)
                .unwrap_or_else(|e| panic!("{key}: {e}"));
        }
        Rules::from_config(&config.expanded())
    }

    fn refused(changes: &[(&str, &str)]) -> String {
        rules(changes).expect_err("accepted").to_string()
    }

    const QUIC: &str = r#"{ links: [{ link_protocols: ["quic"] }] }"#;

    // Without rules nothing of the hand-over exists, whatever the rest of the configuration.
    #[test]
    fn without_rules_nothing_exists() {
        assert!(rules(&[]).unwrap().is_none());
        assert!(rules(&[("transport/unicast/qos/enabled", "false")])
            .unwrap()
            .is_none());
        assert!(
            rules(&[("transport/unicast/max_links", "4"), (CLOSE, "-1")])
                .unwrap()
                .is_none()
        );
        assert!(rules(&[(HANDOVER, "{ total_max_bytes: 1, links: [] }")])
            .unwrap()
            .is_none());
    }

    // A message that is not started within the close period is dropped, unless the rule says
    // otherwise: a link is not asked to be fresher than the period after which it is closed.
    #[test]
    fn the_default_age_is_the_close_period() {
        let default = rules(&[(HANDOVER, QUIC)]).unwrap().unwrap();
        assert_eq!(default.wait_before_close, Duration::from_secs(5));
        assert_eq!(default.rules[0].max_age, Duration::from_secs(5));
        assert_eq!(default.rules[0].control_max_age, Duration::from_secs(10));
        assert_eq!(default.rules[0].link_protocols, ["quic"]);
        assert!(default.rules[0].interfaces.is_empty());

        let long = rules(&[(HANDOVER, QUIC), (CLOSE, "20000000")])
            .unwrap()
            .unwrap();
        assert_eq!(long.rules[0].max_age, Duration::from_secs(20));
        assert_eq!(long.rules[0].control_max_age, Duration::from_secs(20));

        let set = rules(&[(
            HANDOVER,
            r#"{ links: [{ link_protocols: ["quic"], max_age_ms: 800, control_max_age_ms: 7000 }] }"#,
        )])
        .unwrap()
        .unwrap();
        assert_eq!(set.rules[0].max_age, Duration::from_millis(800));
        assert_eq!(set.rules[0].control_max_age, Duration::from_secs(7));
    }

    // A control message is never held for less than the close period, nor for less than a
    // publication: it would close a link that the close period still gives time.
    #[test]
    fn a_control_age_shorter_than_the_close_period_is_refused() {
        let e = refused(&[(
            HANDOVER,
            r#"{ links: [{ link_protocols: ["quic"], control_max_age_ms: 4999 }] }"#,
        )]);
        assert!(e.contains("control_max_age_ms"), "{e}");
        // Also when publications are dropped sooner than the close period.
        let e = refused(&[(
            HANDOVER,
            r#"{ links: [{ link_protocols: ["quic"], max_age_ms: 800, control_max_age_ms: 3000 }] }"#,
        )]);
        assert!(e.contains("wait_before_close"), "{e}");
        let e = refused(&[(
            HANDOVER,
            r#"{ links: [{ link_protocols: ["quic"], max_age_ms: 60000, control_max_age_ms: 30000 }] }"#,
        )]);
        assert!(e.contains("control_max_age_ms"), "{e}");
        let e = refused(&[(
            HANDOVER,
            r#"{ links: [{ link_protocols: ["quic"], max_age_ms: 0 }] }"#,
        )]);
        assert!(e.contains("max_age_ms"), "{e}");
    }

    // The sender exists for one carrier. A rule for any other is refused when the process
    // starts, not when a link of that kind comes up.
    #[test]
    fn only_supported_protocols_can_be_selected() {
        for protocol in [
            "tls",
            "udp",
            "ws",
            "unixsock-stream",
            "quic_datagram",
            "serial",
        ] {
            let e = refused(&[(
                HANDOVER,
                &format!(r#"{{ links: [{{ link_protocols: ["quic", "{protocol}"] }}] }}"#),
            )]);
            assert!(e.contains("not supported"), "{protocol}: {e}");
        }
        let e = refused(&[(HANDOVER, r#"{ links: [{ link_protocols: [] }] }"#)]);
        assert!(e.contains("link_protocols"), "{e}");
        // TCP is for the tests of this repository, which drive links through a proxy.
        let tcp = rules(&[(HANDOVER, r#"{ links: [{ link_protocols: ["tcp"] }] }"#)]);
        assert_eq!(tcp.is_ok(), cfg!(feature = "test"));
    }

    #[test]
    fn a_protocol_is_selected_by_one_rule_at_most() {
        let e = refused(&[(
            HANDOVER,
            r#"{ links: [
                { id: "a", link_protocols: ["quic"], interfaces: ["eth0"] },
                { id: "b", link_protocols: ["quic"], interfaces: ["wlan0"] },
            ] }"#,
        )]);
        assert!(e.contains("one rule at most"), "{e}");
    }

    // What the sender does not support is refused with the rules, at start-up.
    #[test]
    fn rules_are_refused_with_what_the_sender_does_not_support() {
        let e = refused(&[(HANDOVER, QUIC), ("transport/unicast/qos/enabled", "false")]);
        assert!(e.contains("qos"), "{e}");
        let e = refused(&[(HANDOVER, QUIC), ("transport/unicast/max_links", "2")]);
        assert!(e.contains("max_links"), "{e}");
        let e = refused(&[(HANDOVER, QUIC), ("transport/unicast/lowlatency", "true")]);
        assert!(e.contains("lowlatency"), "{e}");
        for never in ["-1", "0"] {
            let e = refused(&[(HANDOVER, QUIC), (CLOSE, never)]);
            assert!(e.contains("wait_before_close"), "{e}");
        }
    }

    #[test]
    fn the_budgets_of_a_link_fit_the_process_limit() {
        let e = refused(&[(
            HANDOVER,
            r#"{ total_max_bytes: 1048576, links: [{ link_protocols: ["quic"] }] }"#,
        )]);
        assert!(e.contains("total_max_bytes"), "{e}");
        let e = refused(&[(
            HANDOVER,
            r#"{ links: [{ link_protocols: ["quic"], reserve_min_bytes: 1024 }] }"#,
        )]);
        assert!(e.contains("reserve_min_bytes"), "{e}");
        let e = refused(&[(
            HANDOVER,
            r#"{ links: [{ link_protocols: ["quic"], reserve_min_bytes: 2097152 }] }"#,
        )]);
        assert!(e.contains("reserve_min_bytes"), "{e}");
        let ok = rules(&[(
            HANDOVER,
            r#"{ total_max_bytes: 4194304, links: [{ link_protocols: ["quic"], data_bytes: 65536, large_bytes: 1048576 }] }"#,
        )])
        .unwrap()
        .unwrap();
        assert_eq!(ok.pool_usage(), (0, 4194304));
    }

    // A key that the rule does not have is an error, not a silently ignored setting.
    #[test]
    fn a_rule_has_no_unknown_keys() {
        let mut config = Config::default();
        assert!(config
            .insert_json5(
                HANDOVER,
                r#"{ links: [{ link_protocols: ["quic"], max_age: 250 }] }"#
            )
            .is_err());
    }

    // Replies and the final of their query take their priority from the query. The sender
    // announces dropped replies in front of the final on that assumption. A QoS overwrite
    // rule cannot name replies today; the day it can, rules that select a link must refuse
    // it, and this test says so.
    #[test]
    fn a_qos_overwrite_rule_cannot_name_replies() {
        let rule = |message: &str| {
            format!(
                r#"[{{ messages: ["{message}"], overwrite: {{ priority: "data_low" }}, flows: ["egress"] }}]"#
            )
        };
        let mut config = Config::default();
        config.insert_json5("qos/network", &rule("query")).unwrap();
        assert!(config.insert_json5("qos/network", &rule("reply")).is_err());
    }
}
