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

//! Queries that a transport took from routing and did not send: what routing gives the
//! transport with a query, and how the branch of the query ends when the transport reports it.

use std::time::{Duration, Instant};

use zenoh_protocol::{
    core::{Region, WhatAmI},
    network::{
        declare::queryable::ext::QueryableInfoType,
        request::ext::{NodeIdType, QueryTarget},
        response, Request, RequestId, ResponseFinal,
    },
    zenoh::{Query, RequestBody, ResponseBody},
};
use zenoh_sync::get_mut_unchecked;
use zenoh_transport::{
    unicast::{
        RequestLifetime, Undelivered, UndeliveredReason, QUERY_EXPIRED, QUERY_NOT_DELIVERED,
    },
    TransportPeerEventHandler,
};

use super::{try_init_tracing_subscriber, FaceDef, Harness, HarnessBuilder, MockFace};
use crate::net::{
    primitives::Primitives,
    routing::dispatcher::queries::{
        claim_undelivered_queries, complete_claimed_queries, run_query_timeout, ClaimedQuery,
    },
};

const KEY: &str = "test/undelivered/probe";

/// A router with a client that asks and clients that own a queryable on [`KEY`].
struct Setup {
    _gateway: Harness,
    asker: MockFace,
    owners: Vec<MockFace>,
}

fn setup(owners: usize) -> Setup {
    try_init_tracing_subscriber();
    setup_with(owners, true)
}

/// `handover`: the configuration has a rule that selects links whose messages are handed over
/// to a sender, as a process has whose links can refuse a query.
fn setup_with(owners: usize, handover: bool) -> Setup {
    let region = Region::default_south(WhatAmI::Client);
    let gateway = HarnessBuilder::new()
        .mode(WhatAmI::Router)
        .subregions([Region::Local, region])
        .handover(handover)
        .build();
    let client = || gateway.new_face(FaceDef::default().mode(WhatAmI::Client).region(region));
    let asker = client();
    let owners = (0..owners)
        .map(|_| {
            let owner = client();
            owner.declare_queryable(
                None,
                1,
                "test/undelivered/**",
                QueryableInfoType {
                    complete: true,
                    distance: 0,
                },
            );
            owner
        })
        .collect();
    Setup {
        _gateway: gateway,
        asker,
        owners,
    }
}

impl Setup {
    /// The asker sends a query to every owner. Returns, for each owner, the request id on its
    /// face and what routing gave its transport with the request.
    fn ask(&self, id: RequestId, timeout: Option<Duration>) -> Vec<(RequestId, RequestLifetime)> {
        let before = self
            .owners
            .iter()
            .map(|o| o.transport().request_lifetimes().len())
            .collect::<Vec<_>>();
        self.asker.face.send_request(&mut Request {
            id,
            wire_expr: KEY.into(),
            ext_qos: response::ext::QoSType::DEFAULT,
            ext_tstamp: None,
            ext_nodeid: NodeIdType::DEFAULT,
            ext_target: QueryTarget::All,
            ext_budget: None,
            ext_timeout: timeout,
            ext_ts_stack: None,
            payload: RequestBody::Query(Query::default()),
        });
        self.owners
            .iter()
            .zip(before)
            .map(|(owner, before)| {
                let lifetimes = owner.transport().request_lifetimes();
                assert_eq!(lifetimes.len(), before + 1, "one request for each owner");
                lifetimes.last().unwrap().clone()
            })
            .collect()
    }

    fn claim(&self, owner: usize, records: Vec<Undelivered>) -> Vec<ClaimedQuery> {
        let face = &self.owners[owner].face;
        claim_undelivered_queries(&face.tables, &mut face.state.clone(), records)
    }

    fn timeout(&self, owner: usize, rid: RequestId) {
        let face = &self.owners[owner].face;
        run_query_timeout(
            &face.tables,
            &face.state,
            rid,
            response::ext::QoSType::DEFAULT,
        );
    }

    fn final_from(&self, owner: usize, rid: RequestId) {
        self.owners[owner]
            .face
            .send_response_final(&mut ResponseFinal {
                rid,
                ext_qos: response::ext::QoSType::DEFAULT,
                ext_tstamp: None,
            });
    }

    fn pending(&self, owner: usize) -> usize {
        self.owners[owner].face.state.pending_queries.len()
    }

    /// The texts of the error replies and the number of finals that the asker received.
    fn asker_saw(&self) -> (Vec<String>, usize) {
        let errors = self
            .asker
            .recorder()
            .responses()
            .into_iter()
            .filter_map(|r| match r.payload {
                ResponseBody::Err(e) => {
                    let text: Vec<u8> = e
                        .payload
                        .zslices()
                        .flat_map(|s| s.as_slice().to_vec())
                        .collect();
                    Some(String::from_utf8_lossy(&text).into_owned())
                }
                ResponseBody::Reply(_) => None,
            })
            .collect();
        (errors, self.asker.recorder().response_finals().len())
    }

    /// The key expressions of the error replies that the asker received.
    fn asker_saw_keys(&self) -> Vec<(u16, String)> {
        self.asker
            .recorder()
            .responses()
            .into_iter()
            .filter(|r| matches!(r.payload, ResponseBody::Err(_)))
            .map(|r| (r.wire_expr.scope, r.wire_expr.suffix.to_string()))
            .collect()
    }
}

fn record(
    (rid, lifetime): &(RequestId, RequestLifetime),
    reason: UndeliveredReason,
) -> Undelivered {
    Undelivered {
        rid: *rid,
        reason,
        branch: lifetime.branch.clone(),
    }
}

fn not_delivered(branch: &(RequestId, RequestLifetime)) -> Undelivered {
    record(branch, UndeliveredReason::NotDelivered)
}

// A query that routing forwards carries the end of its branch (now plus its own timeout, or
// plus the default one) and a handle that identifies the query while it is pending, without
// keeping it.
#[test]
fn a_routed_query_carries_the_end_of_its_branch() {
    let s = setup(1);
    let before = Instant::now();
    let (rid, lifetime) = s.ask(1, Some(Duration::from_millis(300))).remove(0);
    let after = Instant::now();
    assert!(lifetime.expiry >= before + Duration::from_millis(300));
    assert!(lifetime.expiry <= after + Duration::from_millis(300));
    let branch = lifetime.branch.clone().expect("a handle on the query");
    assert!(branch.upgrade().is_some());
    s.final_from(0, rid);
    assert!(branch.upgrade().is_none(), "the handle kept the query");
    assert_eq!(s.asker_saw(), (vec![], 1));

    let default_timeout = Duration::from_millis(zenoh_config::defaults::queries_default_timeout);
    let before = Instant::now();
    let (_, lifetime) = s.ask(2, None).remove(0);
    let after = Instant::now();
    assert!(lifetime.expiry >= before + default_timeout);
    assert!(lifetime.expiry <= after + default_timeout);
}

// A query that the transport reports as not sent ends at once for the one who asked: an
// error reply that says why, then the final. A second report of it does nothing.
#[test]
fn an_unsent_query_gets_an_error_reply_and_one_final() {
    for (reason, text) in [
        (UndeliveredReason::NotDelivered, QUERY_NOT_DELIVERED),
        (UndeliveredReason::Expired, QUERY_EXPIRED),
    ] {
        let s = setup(1);
        let branch = s.ask(1, None).remove(0);
        assert_eq!(s.pending(0), 1);
        let claimed = s.claim(0, vec![record(&branch, reason)]);
        assert_eq!(claimed.len(), 1);
        assert_eq!(s.pending(0), 0);
        // Taking the branch sends nothing.
        assert_eq!(s.asker_saw(), (vec![], 0));
        complete_claimed_queries(claimed);
        assert_eq!(s.asker_saw(), (vec![text.to_string()], 1));
        // The error reply carries the key expression of the query, as the error reply of a
        // queryable does: a session with a namespace and the access control drop an error
        // reply without a key.
        assert_eq!(s.asker_saw_keys(), [(0, KEY.to_string())]);

        assert!(s.claim(0, vec![record(&branch, reason)]).is_empty());
        assert_eq!(s.asker_saw(), (vec![text.to_string()], 1));
    }
}

// Without a rule that selects links, no link of the process can refuse a query: routing sends
// the query as it always did, and keeps nothing for it beyond what it always kept.
#[test]
fn without_rules_a_query_is_routed_as_before() {
    let s = setup_with(1, false);
    s.asker.face.send_request(&mut Request {
        id: 1,
        wire_expr: KEY.into(),
        ext_qos: response::ext::QoSType::DEFAULT,
        ext_tstamp: None,
        ext_nodeid: NodeIdType::DEFAULT,
        ext_target: QueryTarget::All,
        ext_budget: None,
        ext_timeout: None,
        ext_ts_stack: None,
        payload: RequestBody::Query(Query::default()),
    });
    assert_eq!(s.pending(0), 1);
    assert!(s.owners[0].transport().request_lifetimes().is_empty());
}

// C6. A report names its query, not only a request id: when the branch has ended and its id
// is in use again for another query on the same face, the report does nothing to that query.
#[test]
fn a_completion_for_a_reused_request_id_does_nothing() {
    let s = setup(1);
    let first = s.ask(1, None).remove(0);
    s.final_from(0, first.0);
    assert_eq!(s.asker_saw(), (vec![], 1));

    // The next query on the face of the owner gets the same id again.
    let owner = &s.owners[0].face;
    get_mut_unchecked(&mut owner.state.clone()).next_qid = first.0.wrapping_sub(1);
    let second = s.ask(2, None).remove(0);
    assert_eq!(second.0, first.0, "the request id was not used again");

    // A late report for the first query.
    assert!(s.claim(0, vec![not_delivered(&first)]).is_empty());
    assert_eq!(s.pending(0), 1);
    assert_eq!(s.asker_saw(), (vec![], 1));

    // The second query ends as any query does.
    s.final_from(0, second.0);
    assert_eq!(s.asker_saw(), (vec![], 2));
}

// C6. Whatever else ends the branch (the final of the queryable, the timeout of the branch,
// the close of the face), before the report, or between the moment the branch is taken and
// the moment its error reply is sent: the one who asked gets one final, and at most one
// error reply.
#[test]
fn refusal_against_timeout_final_and_close_gives_one_final() {
    let not_sent = || vec![QUERY_NOT_DELIVERED.to_string()];

    // The final first: the report does nothing.
    let s = setup(1);
    let branch = s.ask(1, None).remove(0);
    s.final_from(0, branch.0);
    assert!(s.claim(0, vec![not_delivered(&branch)]).is_empty());
    assert_eq!(s.asker_saw(), (vec![], 1));

    // The final after the branch was taken: it finds nothing.
    let s = setup(1);
    let branch = s.ask(1, None).remove(0);
    let claimed = s.claim(0, vec![not_delivered(&branch)]);
    s.final_from(0, branch.0);
    assert_eq!(s.asker_saw(), (vec![], 0));
    complete_claimed_queries(claimed);
    assert_eq!(s.asker_saw(), (not_sent(), 1));

    // The timeout first: its error reply and its final; the report does nothing.
    let s = setup(1);
    let branch = s.ask(1, None).remove(0);
    s.timeout(0, branch.0);
    assert!(s.claim(0, vec![not_delivered(&branch)]).is_empty());
    assert_eq!(s.asker_saw(), (vec!["Timeout".to_string()], 1));

    // The timeout after the branch was taken: it finds nothing.
    let s = setup(1);
    let branch = s.ask(1, None).remove(0);
    let claimed = s.claim(0, vec![not_delivered(&branch)]);
    s.timeout(0, branch.0);
    assert_eq!(s.asker_saw(), (vec![], 0));
    complete_claimed_queries(claimed);
    assert_eq!(s.asker_saw(), (not_sent(), 1));

    // The close of the face first: the report does nothing.
    let s = setup(1);
    let branch = s.ask(1, None).remove(0);
    s.owners[0].face.send_close();
    assert!(s.claim(0, vec![not_delivered(&branch)]).is_empty());
    assert_eq!(s.asker_saw(), (vec![], 1));

    // The close of the face after the branch was taken: it finds nothing.
    let s = setup(1);
    let branch = s.ask(1, None).remove(0);
    let claimed = s.claim(0, vec![not_delivered(&branch)]);
    s.owners[0].face.send_close();
    assert_eq!(s.asker_saw(), (vec![], 0));
    complete_claimed_queries(claimed);
    assert_eq!(s.asker_saw(), (not_sent(), 1));

    // Two branches: the final goes out when the last of them ends, whichever it is.
    let s = setup(2);
    let branches = s.ask(1, None);
    complete_claimed_queries(s.claim(0, vec![not_delivered(&branches[0])]));
    assert_eq!(s.asker_saw(), (not_sent(), 0));
    s.final_from(1, branches[1].0);
    assert_eq!(s.asker_saw(), (not_sent(), 1));

    let s = setup(2);
    let branches = s.ask(1, None);
    s.final_from(1, branches[1].0);
    assert_eq!(s.asker_saw(), (vec![], 0));
    complete_claimed_queries(s.claim(0, vec![not_delivered(&branches[0])]));
    assert_eq!(s.asker_saw(), (not_sent(), 1));
}

// C6. A transport that is closed by force reports its unsent queries and then says that it
// is closed. The handler takes the branches before it returns, so the close of the face,
// which comes right behind, does not end them without their error reply.
#[test]
fn forced_close_claims_before_the_face_closes() {
    let s = setup(1);
    let branch = s.ask(1, None).remove(0);
    let demux = s.owners[0].demux.as_ref().unwrap();
    demux.handle_undelivered(vec![not_delivered(&branch)]);
    demux.closed();
    // The error reply and the final are sent from a task.
    let start = Instant::now();
    while s.asker_saw().1 == 0 {
        assert!(start.elapsed() < Duration::from_secs(30), "no final");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(s.asker_saw(), (vec![QUERY_NOT_DELIVERED.to_string()], 1));
    assert_eq!(s.pending(0), 0);
}
