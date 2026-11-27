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

//! The receiving end of a link, for tests.
//!
//! [`TestReceiver`] reads batches as the unicast RX path does (`unicast/universal/rx.rs`:
//! the channel is chosen from the priority and the reliability of the frame, sequence numbers
//! are checked per channel, fragments are reassembled with the negotiated markers or without
//! them) and collects the network messages in the order in which a peer would deliver them.

use zenoh_buffers::reader::{DidntRead, HasReader};
use zenoh_codec::{RCodec, Zenoh080};
use zenoh_protocol::{
    core::{Bits, ExprId, Priority, Reliability},
    network::{DeclareBody, NetworkBody, NetworkMessage, RequestId},
    transport::{Fragment, Frame, TransportBody, TransportMessage},
    zenoh::{PushBody, ResponseBody},
};

use crate::common::priority::TransportChannelRx;

/// A delivered network message, reduced to what the tests compare.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Seen {
    /// A publication: priority, payload size, scope of its key expression.
    Put(Priority, usize, ExprId),
    DeclareKey(ExprId),
    UndeclareKey(ExprId),
    Request(RequestId),
    Reply(RequestId),
    /// An error reply and its text.
    ReplyErr(RequestId, String),
    Final(RequestId),
    Other,
}

pub(crate) struct TestReceiver {
    channels: Vec<[TransportChannelRx; 2]>,
    fragmentation_markers: bool,
    pub(crate) delivered: Vec<(Priority, NetworkMessage)>,
    /// Frames and fragments refused for their sequence number, reassembly errors, fragments
    /// without a start: anything a healthy sender must not produce.
    pub(crate) errors: Vec<String>,
}

impl TestReceiver {
    pub(crate) fn new(resolution: Bits, fragmentation_markers: bool) -> Self {
        let channels = (0..Priority::NUM)
            .map(|_| {
                [Reliability::BestEffort, Reliability::Reliable].map(|r| {
                    let mut c = TransportChannelRx::make(r, resolution, 1 << 30).unwrap();
                    c.sync(0).unwrap();
                    c
                })
            })
            .collect();
        Self {
            channels,
            fragmentation_markers,
            delivered: Vec::new(),
            errors: Vec::new(),
        }
    }

    /// Reads the content of one batch (not finalized: without length prefix or compression).
    pub(crate) fn read(&mut self, batch: &[u8]) {
        let mut reader = batch.reader();
        let codec = Zenoh080::new();
        loop {
            let res: Result<TransportMessage, DidntRead> = codec.read(&mut reader);
            let Ok(msg) = res else { break };
            match msg.body {
                TransportBody::Frame(Frame {
                    reliability,
                    sn,
                    ext_qos,
                    payload,
                }) => {
                    let priority = ext_qos.priority();
                    let c = &mut self.channels[priority as usize]
                        [(reliability == Reliability::Reliable) as usize];
                    if !c.sn.roll(sn).unwrap() {
                        self.errors.push(format!("frame sn {sn} refused"));
                        continue;
                    }
                    for m in payload {
                        self.delivered.push((priority, m));
                    }
                }
                TransportBody::Fragment(Fragment {
                    reliability,
                    more,
                    sn,
                    ext_qos,
                    ext_first,
                    ext_drop,
                    payload,
                }) => {
                    let priority = ext_qos.priority();
                    let c = &mut self.channels[priority as usize]
                        [(reliability == Reliability::Reliable) as usize];
                    if !c.sn.roll(sn).unwrap() {
                        self.errors.push(format!("fragment sn {sn} refused"));
                        continue;
                    }
                    if self.fragmentation_markers {
                        if ext_first.is_some() {
                            c.defrag.clear();
                        } else if c.defrag.is_empty() {
                            self.errors
                                .push(format!("fragment sn {sn} without a start"));
                            continue;
                        }
                        if ext_drop.is_some() {
                            c.defrag.clear();
                            continue;
                        }
                    }
                    if c.defrag.is_empty() {
                        let _ = c.defrag.sync(sn);
                    }
                    if let Err(e) = c.defrag.push(sn, payload) {
                        self.errors.push(format!("fragment sn {sn}: {e}"));
                        continue;
                    }
                    if !more {
                        match c.defrag.defragment() {
                            Some(m) => self.delivered.push((priority, m)),
                            None => self.errors.push(format!("reassembly ending at sn {sn}")),
                        }
                    }
                }
                other => self
                    .errors
                    .push(format!("unexpected transport message {other:?}")),
            }
        }
    }

    /// The messages delivered so far, in order, reduced to [`Seen`].
    pub(crate) fn seen(&self) -> Vec<Seen> {
        self.delivered
            .iter()
            .map(|(priority, m)| match &m.body {
                NetworkBody::Push(p) => {
                    let size = match &p.payload {
                        PushBody::Put(put) => put.payload.zslices().map(|s| s.len()).sum(),
                        PushBody::Del(_) => 0,
                    };
                    Seen::Put(*priority, size, p.wire_expr.scope)
                }
                NetworkBody::Declare(d) => match &d.body {
                    DeclareBody::DeclareKeyExpr(k) => Seen::DeclareKey(k.id),
                    DeclareBody::UndeclareKeyExpr(k) => Seen::UndeclareKey(k.id),
                    _ => Seen::Other,
                },
                NetworkBody::Request(r) => Seen::Request(r.id),
                NetworkBody::Response(r) => match &r.payload {
                    ResponseBody::Reply(_) => Seen::Reply(r.rid),
                    ResponseBody::Err(e) => {
                        let text: Vec<u8> = e
                            .payload
                            .zslices()
                            .flat_map(|s| s.as_slice().to_vec())
                            .collect();
                        Seen::ReplyErr(r.rid, String::from_utf8_lossy(&text).into_owned())
                    }
                },
                NetworkBody::ResponseFinal(f) => Seen::Final(f.rid),
                _ => Seen::Other,
            })
            .collect()
    }

    /// The payload sizes of the publications delivered so far, in order.
    pub(crate) fn put_sizes(&self) -> Vec<usize> {
        self.seen()
            .into_iter()
            .filter_map(|s| match s {
                Seen::Put(_, size, _) => Some(size),
                _ => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use zenoh_config::{QueueAllocConf, QueueAllocMode};
    use zenoh_protocol::{
        core::CongestionControl,
        network::{ext, NetworkMessageExt, Push},
        transport::{BatchSize, TransportSn},
    };
    use zenoh_result::ZResult;

    use super::*;
    use crate::common::{
        batch::BatchConfig,
        pipeline::{PipelineConsumer, TransmissionPipeline, TransmissionPipelineConf},
        priority::TransportPriorityTx,
    };

    // The test receiver delivers what a pipeline produced, in order, whole and fragmented.
    #[test]
    fn test_receiver_reassembles_pipeline_output() -> ZResult<()> {
        let mtu = BatchSize::MAX;
        let config = TransmissionPipelineConf {
            batch: BatchConfig {
                mtu,
                is_streamed: false,
                #[cfg(feature = "transport_compression")]
                is_compression: false,
            },
            queue_size: [16; Priority::NUM],
            batching_enabled: true,
            wait_before_drop: Duration::from_millis(1),
            max_wait_before_drop_fragments: Duration::from_millis(1024),
            wait_before_close: Duration::from_secs(5),
            disable_on_failed_block: false,
            batching_time_limit: Duration::from_micros(1),
            queue_alloc: QueueAllocConf {
                mode: QueueAllocMode::Init,
            },
        };
        let priorities = vec![TransportPriorityTx::make(Bits::from(TransportSn::MAX))?];
        let (producer, mut consumer) = TransmissionPipeline::make(config, &priorities, false);
        let sizes = [8usize, 1_000, 3 * mtu as usize, 64, mtu as usize + 1, 17];
        for size in sizes {
            let message = NetworkMessage::from(Push {
                wire_expr: "test".into(),
                ext_qos: ext::QoSType::new(Priority::DEFAULT, CongestionControl::Block, false),
                ..Push::from(vec![0_u8; size])
            });
            assert!(producer.push_network_message(message.as_ref())?);
        }
        let mut rx = TestReceiver::new(Bits::from(TransportSn::MAX), true);
        while let Ok((batch, prio)) = consumer.stage_pull() {
            rx.read(batch.as_slice());
            consumer.refill(batch, prio);
        }
        assert!(rx.errors.is_empty(), "{:?}", rx.errors);
        assert_eq!(rx.put_sizes(), sizes);
        Ok(())
    }
}
