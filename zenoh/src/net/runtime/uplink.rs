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

//! Admission of the north remote of a leaf router (`gateway.north.mode: "client"`).
//!
//! The hat of a `client` north region has one north face. A leaf router has several ways to get
//! a second north remote: more than one endpoint leads north, or it reconnects, to another
//! remote, as soon as its uplink loses its link, which is before the transport of that uplink is
//! closed. [`NorthAdmission`] decides, atomically, which remote owns the north region:
//!
//! + while the owner has a link, any other candidate is refused;
//! + two candidates that complete at the same time cannot refuse each other: the first to ask
//!   becomes the owner at once, before its face is created;
//! + once the owner has lost its last link it is _closing_, and the next candidate takes over
//!   without waiting for the close of the old transport to complete, however long that takes. The
//!   caller closes the face of the previous owner before it creates the new one.

use std::sync::Mutex;

use tokio::sync::Notify;
use zenoh_protocol::core::ZenohIdProto;

/// Identifies one admission: the same remote gets a new one each time it is admitted.
pub(crate) type NorthTicket = u64;

struct Owner<S> {
    ticket: NorthTicket,
    zid: ZenohIdProto,
    /// The owner has lost its last link: its transport is closing.
    closing: bool,
    /// What the caller needs to close the face of the owner (see [`NorthAdmission::bind`]).
    session: Option<S>,
}

struct State<S> {
    owner: Option<Owner<S>>,
    next_ticket: NorthTicket,
}

/// The outcome of a granted admission.
pub(crate) struct Granted<S> {
    pub(crate) ticket: NorthTicket,
    /// The previous owner, which is closing: its face must be closed before the face of the new
    /// owner is created.
    pub(crate) previous: Option<S>,
}

/// The remote that owns the north region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NorthOwner {
    pub(crate) zid: ZenohIdProto,
    pub(crate) closing: bool,
}

pub(crate) struct NorthAdmission<S> {
    state: Mutex<State<S>>,
    /// Notified when the north region gets vacant (no owner, or a closing one).
    vacant: Notify,
}

impl<S> Default for NorthAdmission<S> {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                owner: None,
                next_ticket: 0,
            }),
            vacant: Notify::new(),
        }
    }
}

impl<S> NorthAdmission<S> {
    /// Asks for the north region on behalf of `zid`.
    ///
    /// Granted if there is no owner, or if the owner is closing. Refused, with the owner,
    /// otherwise.
    pub(crate) fn admit(&self, zid: ZenohIdProto) -> Result<Granted<S>, NorthOwner> {
        let mut state = self.state.lock().unwrap();
        let previous = match state.owner.take() {
            Some(owner) if !owner.closing => {
                let refused = NorthOwner {
                    zid: owner.zid,
                    closing: false,
                };
                state.owner = Some(owner);
                return Err(refused);
            }
            Some(owner) => owner.session,
            None => None,
        };
        let ticket = state.next_ticket;
        state.next_ticket += 1;
        state.owner = Some(Owner {
            ticket,
            zid,
            closing: false,
            session: None,
        });
        Ok(Granted { ticket, previous })
    }

    /// Attaches to the admission `ticket` what a later owner needs to close this one's face.
    pub(crate) fn bind(&self, ticket: NorthTicket, session: S) {
        let mut state = self.state.lock().unwrap();
        if let Some(owner) = state.owner.as_mut().filter(|o| o.ticket == ticket) {
            owner.session = Some(session);
        }
    }

    /// The owner of admission `ticket` has lost its last link.
    pub(crate) fn link_lost(&self, ticket: NorthTicket) {
        let mut state = self.state.lock().unwrap();
        if let Some(owner) = state.owner.as_mut().filter(|o| o.ticket == ticket) {
            owner.closing = true;
            drop(state);
            self.vacant.notify_waiters();
        }
    }

    /// The transport of admission `ticket` is closed, or was never set up. Does nothing if the
    /// north region has been taken over since. Returns `true` if `ticket` was the owner.
    pub(crate) fn release(&self, ticket: NorthTicket) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.owner.as_ref().is_some_and(|o| o.ticket == ticket) {
            state.owner = None;
            drop(state);
            self.vacant.notify_waiters();
            true
        } else {
            false
        }
    }

    pub(crate) fn owner(&self) -> Option<NorthOwner> {
        self.state.lock().unwrap().owner.as_ref().map(|o| NorthOwner {
            zid: o.zid,
            closing: o.closing,
        })
    }

    fn is_vacant(&self) -> bool {
        !matches!(self.owner(), Some(owner) if !owner.closing)
    }

    /// Waits until a candidate would be admitted.
    pub(crate) async fn wait_vacant(&self) {
        loop {
            let notified = self.vacant.notified();
            tokio::pin!(notified);
            // registers for the notification before the check, not to miss one in between
            notified.as_mut().enable();
            if self.is_vacant() {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier},
        time::Duration,
    };

    use zenoh_protocol::core::ZenohIdProto;

    use super::NorthAdmission;

    fn zid(id: u8) -> ZenohIdProto {
        ZenohIdProto::try_from([id, 1]).unwrap()
    }

    #[test]
    fn a_second_remote_is_refused_while_the_owner_has_a_link() {
        let north = NorthAdmission::<&str>::default();
        north.admit(zid(1)).ok().unwrap();

        let refused = north.admit(zid(2)).err().unwrap();

        assert_eq!((refused.zid, refused.closing), (zid(1), false));
        assert_eq!(north.owner().unwrap().zid, zid(1));
    }

    /// Two candidates that complete at the same time: exactly one is admitted, in every round.
    #[test]
    fn two_simultaneous_candidates_do_not_refuse_each_other() {
        for round in 0..200 {
            let north = Arc::new(NorthAdmission::<()>::default());
            let barrier = Arc::new(Barrier::new(2));
            let candidates = [1, 2].map(|id| {
                let (north, barrier) = (north.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    north.admit(zid(id)).is_ok()
                })
            });

            let admitted = candidates
                .into_iter()
                .map(|c| c.join().unwrap())
                .filter(|admitted| *admitted)
                .count();

            assert_eq!(admitted, 1, "round {round}");
            assert!(north.owner().is_some_and(|o| !o.closing));
        }
    }

    /// The owner lost its link and the close of its transport is delayed: the next candidate
    /// is admitted at once and is handed the previous owner. The late close of the old
    /// transport does not release the new owner.
    #[test]
    fn a_closing_owner_is_taken_over_without_waiting_for_its_close() {
        let north = NorthAdmission::<&str>::default();
        let old = north.admit(zid(1)).ok().unwrap();
        north.bind(old.ticket, "old");
        north.link_lost(old.ticket);
        assert!(north.owner().unwrap().closing);

        let new = north.admit(zid(2)).ok().unwrap();
        assert_eq!(new.previous, Some("old"));
        north.bind(new.ticket, "new");

        // the delayed close of the old transport
        assert!(!north.release(old.ticket));
        assert_eq!(
            north.owner().map(|o| (o.zid, o.closing)),
            Some((zid(2), false))
        );
        // a third remote is refused: the new owner has its link
        assert!(north.admit(zid(3)).is_err());
        assert!(north.release(new.ticket));
        assert!(north.owner().is_none());
    }

    /// The same remote comes back while its previous transport is closing.
    #[test]
    fn the_same_remote_is_admitted_again_with_a_new_ticket() {
        let north = NorthAdmission::<&str>::default();
        let first = north.admit(zid(1)).ok().unwrap();
        north.link_lost(first.ticket);

        let second = north.admit(zid(1)).ok().unwrap();

        assert_ne!(first.ticket, second.ticket);
        assert!(!north.release(first.ticket));
        assert!(north.owner().is_some());
    }

    /// A candidate that was admitted and whose transport was never set up frees the region.
    #[test]
    fn a_released_admission_frees_the_north_region() {
        let north = NorthAdmission::<()>::default();
        let granted = north.admit(zid(1)).ok().unwrap();

        assert!(north.release(granted.ticket));

        assert!(north.admit(zid(2)).is_ok());
    }

    /// A connector that waits for the north region to be vacant is woken when the owner loses
    /// its link, not only when its transport is closed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn waiters_are_woken_when_the_owner_loses_its_link() {
        let north = Arc::new(NorthAdmission::<()>::default());
        tokio::time::timeout(Duration::from_secs(5), north.wait_vacant())
            .await
            .expect("no owner: vacant");
        let granted = north.admit(zid(1)).ok().unwrap();
        let waiter = tokio::spawn({
            let north = north.clone();
            async move { north.wait_vacant().await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished());

        north.link_lost(granted.ticket);

        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("woken by the loss of the link")
            .unwrap();
    }
}
