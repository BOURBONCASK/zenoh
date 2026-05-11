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

//! Lightweight runtime counters used to attribute p2p routing performance issues.
//!
//! These counters are always-on (atomic relaxed adds) and a background thread
//! prints aggregated per-second snapshots as `metric name=routing_diag ...`
//! lines on stdout, matching the existing benchmark log format.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Once,
    },
    thread,
    time::{Duration, Instant},
};

// ----- Round-1 metrics (unchanged) -----

pub(crate) static DISABLE_ALL_ROUTES_DECLARE_FINAL: AtomicU64 = AtomicU64::new(0);
pub(crate) static DISABLE_ALL_ROUTES_PEER_INIT: AtomicU64 = AtomicU64::new(0);
pub(crate) static DISABLE_ALL_ROUTES_OTHER: AtomicU64 = AtomicU64::new(0);

pub(crate) static COMPUTE_DATA_ROUTE_COUNT: AtomicU64 = AtomicU64::new(0);
pub(crate) static COMPUTE_DATA_ROUTE_TOTAL_US: AtomicU64 = AtomicU64::new(0);
pub(crate) static COMPUTE_DATA_ROUTE_MAX_US: AtomicU64 = AtomicU64::new(0);

pub(crate) static ROUTE_CACHE_HIT: AtomicU64 = AtomicU64::new(0);
pub(crate) static ROUTE_CACHE_MISS: AtomicU64 = AtomicU64::new(0);

// ----- v3 wtables metrics: acquire_wait + hold per site -----

const WT_SITE_COUNT: usize = 5;
const WT_SITE_NAMES: [&str; WT_SITE_COUNT] = [
    "peer_init",
    "declare_final",
    "register_expr",
    "interest_final",
    "send_close",
];
macro_rules! atomics {
    ($n:expr) => {
        [const { AtomicU64::new(0) }; $n]
    };
}

// Histogram bucket count for acquire_wait timings. Each bucket i covers the
// range `[2^(i-1), 2^i)` microseconds, except bucket 0 which captures 0 µs.
// With 25 buckets the last range is `[2^23, 2^24)` µs ≈ `[8.4 s, 16.8 s)`,
// which comfortably covers `wait_before_close = 5 s` outliers.
const HIST_BUCKETS: usize = 25;

macro_rules! hist_atomics_2d {
    ($rows:expr) => {
        [const { [const { AtomicU64::new(0) }; HIST_BUCKETS] }; $rows]
    };
}

#[inline]
fn hist_bucket_for(v_us: u64) -> usize {
    if v_us == 0 {
        return 0;
    }
    let bit = (64 - v_us.leading_zeros()) as usize;
    bit.min(HIST_BUCKETS - 1)
}

#[inline]
fn hist_record(buckets: &[AtomicU64; HIST_BUCKETS], v_us: u64) {
    buckets[hist_bucket_for(v_us)].fetch_add(1, Ordering::Relaxed);
}

/// Drain a histogram into a local snapshot and emit p50/p90/p99/p999.
/// Returns the (snapshot, total) for the caller to use.
#[inline]
fn hist_drain(buckets: &[AtomicU64; HIST_BUCKETS]) -> ([u64; HIST_BUCKETS], u64) {
    let mut snap = [0u64; HIST_BUCKETS];
    let mut total = 0u64;
    for (i, b) in buckets.iter().enumerate() {
        let v = b.swap(0, Ordering::Relaxed);
        snap[i] = v;
        total += v;
    }
    (snap, total)
}

/// Linear-interpolate inside the picked bucket for a percentile value (µs).
fn hist_percentile(snap: &[u64; HIST_BUCKETS], total: u64, p: f64) -> u64 {
    if total == 0 {
        return 0;
    }
    let target = ((total as f64) * p).ceil().max(1.0) as u64;
    let mut cum: u64 = 0;
    for (i, &count) in snap.iter().enumerate() {
        if count == 0 {
            continue;
        }
        let new_cum = cum + count;
        if new_cum >= target {
            let lo: u64 = if i == 0 { 0 } else { 1u64 << (i - 1) };
            let hi: u64 = if i == 0 { 1 } else { 1u64 << i };
            let span = hi - lo;
            let pos_in_bucket = (target - cum) as f64 / count as f64;
            return lo + (span as f64 * pos_in_bucket) as u64;
        }
        cum = new_cum;
    }
    1u64 << (HIST_BUCKETS - 1)
}
pub(crate) static WT_COUNT: [AtomicU64; WT_SITE_COUNT] = atomics!(WT_SITE_COUNT);
pub(crate) static WT_ACQUIRE_WAIT_TOTAL_US: [AtomicU64; WT_SITE_COUNT] = atomics!(WT_SITE_COUNT);
pub(crate) static WT_ACQUIRE_WAIT_MAX_US: [AtomicU64; WT_SITE_COUNT] = atomics!(WT_SITE_COUNT);
pub(crate) static WT_ACQUIRE_WAIT_HIST: [[AtomicU64; HIST_BUCKETS]; WT_SITE_COUNT] =
    hist_atomics_2d!(WT_SITE_COUNT);
pub(crate) static WT_HOLD_TOTAL_US: [AtomicU64; WT_SITE_COUNT] = atomics!(WT_SITE_COUNT);
pub(crate) static WT_HOLD_MAX_US: [AtomicU64; WT_SITE_COUNT] = atomics!(WT_SITE_COUNT);

// v3 flush metrics: post-release send_declare loop time per site
pub(crate) static WT_FLUSH_COUNT: [AtomicU64; WT_SITE_COUNT] = atomics!(WT_SITE_COUNT);
pub(crate) static WT_FLUSH_TOTAL_US: [AtomicU64; WT_SITE_COUNT] = atomics!(WT_SITE_COUNT);
pub(crate) static WT_FLUSH_MAX_US: [AtomicU64; WT_SITE_COUNT] = atomics!(WT_SITE_COUNT);
pub(crate) static WT_FLUSH_DECLARES_TOTAL: [AtomicU64; WT_SITE_COUNT] = atomics!(WT_SITE_COUNT);
pub(crate) static WT_FLUSH_DECLARES_MAX: [AtomicU64; WT_SITE_COUNT] = atomics!(WT_SITE_COUNT);

// ----- v3 ctrl_lock metrics: acquire_wait + hold per site -----

const CL_SITE_COUNT: usize = 6;
const CL_SITE_NAMES: [&str; CL_SITE_COUNT] = [
    "new_session",
    "new_transport_unicast",
    "send_interest",
    "send_declare",
    "send_close",
    "other",
];
pub(crate) static CL_COUNT: [AtomicU64; CL_SITE_COUNT] = atomics!(CL_SITE_COUNT);
pub(crate) static CL_ACQUIRE_WAIT_TOTAL_US: [AtomicU64; CL_SITE_COUNT] = atomics!(CL_SITE_COUNT);
pub(crate) static CL_ACQUIRE_WAIT_MAX_US: [AtomicU64; CL_SITE_COUNT] = atomics!(CL_SITE_COUNT);
pub(crate) static CL_ACQUIRE_WAIT_HIST: [[AtomicU64; HIST_BUCKETS]; CL_SITE_COUNT] =
    hist_atomics_2d!(CL_SITE_COUNT);
pub(crate) static CL_HOLD_TOTAL_US: [AtomicU64; CL_SITE_COUNT] = atomics!(CL_SITE_COUNT);
pub(crate) static CL_HOLD_MAX_US: [AtomicU64; CL_SITE_COUNT] = atomics!(CL_SITE_COUNT);

// ----- v3 repropagate_* per-step metrics -----

const RP_KIND_COUNT: usize = 5;
const RP_KIND_NAMES: [&str; RP_KIND_COUNT] = [
    "interests",
    "subscribers",
    "queryables",
    "tokens",
    "disable_all_routes",
];
pub(crate) static RP_COUNT: [AtomicU64; RP_KIND_COUNT] = atomics!(RP_KIND_COUNT);
pub(crate) static RP_TOTAL_US: [AtomicU64; RP_KIND_COUNT] = atomics!(RP_KIND_COUNT);
pub(crate) static RP_MAX_US: [AtomicU64; RP_KIND_COUNT] = atomics!(RP_KIND_COUNT);

// ----- v3.6 repropagate_subscribers internal step metrics -----

const RPS_STEP_COUNT: usize = 4;
const RPS_STEP_NAMES: [&str; RPS_STEP_COUNT] = [
    "contains_check",
    "should_notify_compute",
    "insert_simple_resource",
    "decl_key_and_send",
];
pub(crate) static RPS_COUNT: [AtomicU64; RPS_STEP_COUNT] = atomics!(RPS_STEP_COUNT);
pub(crate) static RPS_TOTAL_US: [AtomicU64; RPS_STEP_COUNT] = atomics!(RPS_STEP_COUNT);
pub(crate) static RPS_MAX_US: [AtomicU64; RPS_STEP_COUNT] = atomics!(RPS_STEP_COUNT);

// ----- v3.6 per-declare flush time at the gateway peer_init flush loop -----
pub(crate) static FLUSH_DECLARE_COUNT: AtomicU64 = AtomicU64::new(0);
pub(crate) static FLUSH_DECLARE_TOTAL_US: AtomicU64 = AtomicU64::new(0);
pub(crate) static FLUSH_DECLARE_MAX_US: AtomicU64 = AtomicU64::new(0);

// ----- PR 1: new_transport_unicast wall-clock (caller-visible latency) -----
// Acceptance gate for PR 1 — should drop from ~5s to <50ms once the flush
// loop is spawned off the calling thread.
pub(crate) static NTU_WALLCLOCK_COUNT: AtomicU64 = AtomicU64::new(0);
pub(crate) static NTU_WALLCLOCK_TOTAL_US: AtomicU64 = AtomicU64::new(0);
pub(crate) static NTU_WALLCLOCK_MAX_US: AtomicU64 = AtomicU64::new(0);

// ----- route_data read-lock metrics (unchanged, semantics now: hold = whole fn body) -----

pub(crate) static RT_WAIT_COUNT: AtomicU64 = AtomicU64::new(0);
pub(crate) static RT_WAIT_TOTAL_US: AtomicU64 = AtomicU64::new(0);
pub(crate) static RT_WAIT_MAX_US: AtomicU64 = AtomicU64::new(0);
pub(crate) static RT_WAIT_HIST: [AtomicU64; HIST_BUCKETS] =
    [const { AtomicU64::new(0) }; HIST_BUCKETS];
pub(crate) static RT_HOLD_TOTAL_US: AtomicU64 = AtomicU64::new(0);
pub(crate) static RT_HOLD_MAX_US: AtomicU64 = AtomicU64::new(0);

static DUMPER_INIT: Once = Once::new();

#[derive(Clone, Copy)]
pub(crate) enum InvalidationSource {
    DeclareFinal,
    PeerInit,
    Other,
}

#[derive(Clone, Copy)]
pub(crate) enum WTableSite {
    PeerInit = 0,
    DeclareFinal = 1,
    RegisterExpr = 2,
    InterestFinal = 3,
    SendClose = 4,
}

#[derive(Clone, Copy)]
pub(crate) enum CtrlLockSite {
    NewSession = 0,
    NewTransportUnicast = 1,
    SendInterest = 2,
    SendDeclare = 3,
    SendClose = 4,
    Other = 5,
}

#[derive(Clone, Copy)]
pub(crate) enum RepropagateKind {
    Interests = 0,
    Subscribers = 1,
    Queryables = 2,
    Tokens = 3,
    DisableAllRoutes = 4,
}

#[derive(Clone, Copy)]
pub(crate) enum RepropagateSubsStep {
    ContainsCheck = 0,
    ShouldNotifyCompute = 1,
    InsertSimpleResource = 2,
    DeclKeyAndSend = 3,
}

fn ensure_dumper_started() {
    DUMPER_INIT.call_once(|| {
        let pid = std::process::id();
        let start = Instant::now();
        let _ = thread::Builder::new()
            .name("zenoh-routing-diag".into())
            .spawn(move || loop {
                thread::sleep(Duration::from_secs(1));
                let elapsed_ms = start.elapsed().as_millis();

                let inv_final = DISABLE_ALL_ROUTES_DECLARE_FINAL.swap(0, Ordering::Relaxed);
                let inv_peer = DISABLE_ALL_ROUTES_PEER_INIT.swap(0, Ordering::Relaxed);
                let inv_other = DISABLE_ALL_ROUTES_OTHER.swap(0, Ordering::Relaxed);
                let compute_count = COMPUTE_DATA_ROUTE_COUNT.swap(0, Ordering::Relaxed);
                let compute_total = COMPUTE_DATA_ROUTE_TOTAL_US.swap(0, Ordering::Relaxed);
                let compute_max = COMPUTE_DATA_ROUTE_MAX_US.swap(0, Ordering::Relaxed);
                let cache_hit = ROUTE_CACHE_HIT.swap(0, Ordering::Relaxed);
                let cache_miss = ROUTE_CACHE_MISS.swap(0, Ordering::Relaxed);

                if inv_final
                    | inv_peer
                    | inv_other
                    | compute_count
                    | cache_hit
                    | cache_miss
                    != 0
                {
                    let avg_us = if compute_count > 0 {
                        compute_total / compute_count
                    } else {
                        0
                    };
                    println!(
                        "metric name=routing_diag pid={pid} elapsed_ms={elapsed_ms} \
                         disable_all_routes_declare_final={inv_final} \
                         disable_all_routes_peer_init={inv_peer} \
                         disable_all_routes_other={inv_other} \
                         compute_data_route_count={compute_count} \
                         compute_data_route_avg_us={avg_us} \
                         compute_data_route_max_us={compute_max} \
                         route_cache_hit={cache_hit} \
                         route_cache_miss={cache_miss}"
                    );
                }

                for i in 0..WT_SITE_COUNT {
                    let count = WT_COUNT[i].swap(0, Ordering::Relaxed);
                    if count == 0 {
                        // also drain flush counters even if no acquire happened
                        let f_count = WT_FLUSH_COUNT[i].swap(0, Ordering::Relaxed);
                        if f_count > 0 {
                            let f_total = WT_FLUSH_TOTAL_US[i].swap(0, Ordering::Relaxed);
                            let f_max = WT_FLUSH_MAX_US[i].swap(0, Ordering::Relaxed);
                            let d_total = WT_FLUSH_DECLARES_TOTAL[i].swap(0, Ordering::Relaxed);
                            let d_max = WT_FLUSH_DECLARES_MAX[i].swap(0, Ordering::Relaxed);
                            let f_avg = f_total / f_count;
                            let d_avg = d_total / f_count;
                            println!(
                                "metric name=wtables_flush_diag pid={pid} elapsed_ms={elapsed_ms} \
                                 site={} count={f_count} \
                                 elapsed_avg_us={f_avg} elapsed_max_us={f_max} \
                                 declares_avg={d_avg} declares_max={d_max}",
                                WT_SITE_NAMES[i]
                            );
                        }
                        continue;
                    }
                    let aw_total = WT_ACQUIRE_WAIT_TOTAL_US[i].swap(0, Ordering::Relaxed);
                    let aw_max = WT_ACQUIRE_WAIT_MAX_US[i].swap(0, Ordering::Relaxed);
                    let (aw_hist, aw_hist_total) = hist_drain(&WT_ACQUIRE_WAIT_HIST[i]);
                    let aw_p50 = hist_percentile(&aw_hist, aw_hist_total, 0.50);
                    let aw_p90 = hist_percentile(&aw_hist, aw_hist_total, 0.90);
                    let aw_p99 = hist_percentile(&aw_hist, aw_hist_total, 0.99);
                    let aw_p999 = hist_percentile(&aw_hist, aw_hist_total, 0.999);
                    let h_total = WT_HOLD_TOTAL_US[i].swap(0, Ordering::Relaxed);
                    let h_max = WT_HOLD_MAX_US[i].swap(0, Ordering::Relaxed);
                    let aw_avg = aw_total / count;
                    let h_avg = h_total / count;
                    println!(
                        "metric name=wtables_diag pid={pid} elapsed_ms={elapsed_ms} \
                         site={} count={count} \
                         acquire_wait_avg_us={aw_avg} acquire_wait_max_us={aw_max} \
                         acquire_wait_p50_us={aw_p50} acquire_wait_p90_us={aw_p90} \
                         acquire_wait_p99_us={aw_p99} acquire_wait_p999_us={aw_p999} \
                         hold_avg_us={h_avg} hold_max_us={h_max}",
                        WT_SITE_NAMES[i]
                    );
                    let f_count = WT_FLUSH_COUNT[i].swap(0, Ordering::Relaxed);
                    if f_count > 0 {
                        let f_total = WT_FLUSH_TOTAL_US[i].swap(0, Ordering::Relaxed);
                        let f_max = WT_FLUSH_MAX_US[i].swap(0, Ordering::Relaxed);
                        let d_total = WT_FLUSH_DECLARES_TOTAL[i].swap(0, Ordering::Relaxed);
                        let d_max = WT_FLUSH_DECLARES_MAX[i].swap(0, Ordering::Relaxed);
                        let f_avg = f_total / f_count;
                        let d_avg = d_total / f_count;
                        println!(
                            "metric name=wtables_flush_diag pid={pid} elapsed_ms={elapsed_ms} \
                             site={} count={f_count} \
                             elapsed_avg_us={f_avg} elapsed_max_us={f_max} \
                             declares_avg={d_avg} declares_max={d_max}",
                            WT_SITE_NAMES[i]
                        );
                    }
                }

                for i in 0..CL_SITE_COUNT {
                    let count = CL_COUNT[i].swap(0, Ordering::Relaxed);
                    if count == 0 {
                        continue;
                    }
                    let aw_total = CL_ACQUIRE_WAIT_TOTAL_US[i].swap(0, Ordering::Relaxed);
                    let aw_max = CL_ACQUIRE_WAIT_MAX_US[i].swap(0, Ordering::Relaxed);
                    let (aw_hist, aw_hist_total) = hist_drain(&CL_ACQUIRE_WAIT_HIST[i]);
                    let aw_p50 = hist_percentile(&aw_hist, aw_hist_total, 0.50);
                    let aw_p90 = hist_percentile(&aw_hist, aw_hist_total, 0.90);
                    let aw_p99 = hist_percentile(&aw_hist, aw_hist_total, 0.99);
                    let aw_p999 = hist_percentile(&aw_hist, aw_hist_total, 0.999);
                    let h_total = CL_HOLD_TOTAL_US[i].swap(0, Ordering::Relaxed);
                    let h_max = CL_HOLD_MAX_US[i].swap(0, Ordering::Relaxed);
                    let aw_avg = aw_total / count;
                    let h_avg = h_total / count;
                    println!(
                        "metric name=ctrl_lock_diag pid={pid} elapsed_ms={elapsed_ms} \
                         site={} count={count} \
                         acquire_wait_avg_us={aw_avg} acquire_wait_max_us={aw_max} \
                         acquire_wait_p50_us={aw_p50} acquire_wait_p90_us={aw_p90} \
                         acquire_wait_p99_us={aw_p99} acquire_wait_p999_us={aw_p999} \
                         hold_avg_us={h_avg} hold_max_us={h_max}",
                        CL_SITE_NAMES[i]
                    );
                }

                for i in 0..RP_KIND_COUNT {
                    let count = RP_COUNT[i].swap(0, Ordering::Relaxed);
                    if count == 0 {
                        continue;
                    }
                    let total = RP_TOTAL_US[i].swap(0, Ordering::Relaxed);
                    let mx = RP_MAX_US[i].swap(0, Ordering::Relaxed);
                    let avg = total / count;
                    println!(
                        "metric name=repropagate_diag pid={pid} elapsed_ms={elapsed_ms} \
                         kind={} count={count} avg_us={avg} max_us={mx}",
                        RP_KIND_NAMES[i]
                    );
                }

                for i in 0..RPS_STEP_COUNT {
                    let count = RPS_COUNT[i].swap(0, Ordering::Relaxed);
                    if count == 0 {
                        continue;
                    }
                    let total = RPS_TOTAL_US[i].swap(0, Ordering::Relaxed);
                    let mx = RPS_MAX_US[i].swap(0, Ordering::Relaxed);
                    let avg = total / count;
                    println!(
                        "metric name=repropagate_subs_step_diag pid={pid} elapsed_ms={elapsed_ms} \
                         step={} count={count} avg_us={avg} max_us={mx}",
                        RPS_STEP_NAMES[i]
                    );
                }

                let fd_count = FLUSH_DECLARE_COUNT.swap(0, Ordering::Relaxed);
                if fd_count > 0 {
                    let fd_total = FLUSH_DECLARE_TOTAL_US.swap(0, Ordering::Relaxed);
                    let fd_max = FLUSH_DECLARE_MAX_US.swap(0, Ordering::Relaxed);
                    let fd_avg = fd_total / fd_count;
                    println!(
                        "metric name=flush_declare_diag pid={pid} elapsed_ms={elapsed_ms} \
                         count={fd_count} avg_us={fd_avg} max_us={fd_max}"
                    );
                }

                let ntu_count = NTU_WALLCLOCK_COUNT.swap(0, Ordering::Relaxed);
                if ntu_count > 0 {
                    let ntu_total = NTU_WALLCLOCK_TOTAL_US.swap(0, Ordering::Relaxed);
                    let ntu_max = NTU_WALLCLOCK_MAX_US.swap(0, Ordering::Relaxed);
                    let ntu_avg = ntu_total / ntu_count;
                    println!(
                        "metric name=new_transport_unicast_diag pid={pid} elapsed_ms={elapsed_ms} \
                         count={ntu_count} avg_us={ntu_avg} max_us={ntu_max}"
                    );
                }

                let rt_count = RT_WAIT_COUNT.swap(0, Ordering::Relaxed);
                let rt_wait_total = RT_WAIT_TOTAL_US.swap(0, Ordering::Relaxed);
                let rt_wait_max = RT_WAIT_MAX_US.swap(0, Ordering::Relaxed);
                let (rt_hist, rt_hist_total) = hist_drain(&RT_WAIT_HIST);
                let rt_hold_total = RT_HOLD_TOTAL_US.swap(0, Ordering::Relaxed);
                let rt_hold_max = RT_HOLD_MAX_US.swap(0, Ordering::Relaxed);
                if rt_count > 0 {
                    let wait_avg = rt_wait_total / rt_count;
                    let hold_avg = rt_hold_total / rt_count;
                    let wait_p50 = hist_percentile(&rt_hist, rt_hist_total, 0.50);
                    let wait_p90 = hist_percentile(&rt_hist, rt_hist_total, 0.90);
                    let wait_p99 = hist_percentile(&rt_hist, rt_hist_total, 0.99);
                    let wait_p999 = hist_percentile(&rt_hist, rt_hist_total, 0.999);
                    println!(
                        "metric name=rtables_diag pid={pid} elapsed_ms={elapsed_ms} \
                         count={rt_count} \
                         wait_avg_us={wait_avg} wait_max_us={rt_wait_max} \
                         wait_p50_us={wait_p50} wait_p90_us={wait_p90} \
                         wait_p99_us={wait_p99} wait_p999_us={wait_p999} \
                         hold_avg_us={hold_avg} hold_max_us={rt_hold_max}"
                    );
                }
            });
    });
}

#[inline]
fn record_max(atomic: &AtomicU64, new_value: u64) {
    let mut current = atomic.load(Ordering::Relaxed);
    while new_value > current {
        match atomic.compare_exchange_weak(
            current,
            new_value,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(prev) => current = prev,
        }
    }
}

#[inline]
pub(crate) fn record_disable_all_routes(source: InvalidationSource) {
    ensure_dumper_started();
    let counter = match source {
        InvalidationSource::DeclareFinal => &DISABLE_ALL_ROUTES_DECLARE_FINAL,
        InvalidationSource::PeerInit => &DISABLE_ALL_ROUTES_PEER_INIT,
        InvalidationSource::Other => &DISABLE_ALL_ROUTES_OTHER,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub(crate) fn record_compute_data_route(elapsed_us: u64) {
    ensure_dumper_started();
    COMPUTE_DATA_ROUTE_COUNT.fetch_add(1, Ordering::Relaxed);
    COMPUTE_DATA_ROUTE_TOTAL_US.fetch_add(elapsed_us, Ordering::Relaxed);
    record_max(&COMPUTE_DATA_ROUTE_MAX_US, elapsed_us);
}

#[inline]
pub(crate) fn record_route_cache_hit() {
    ensure_dumper_started();
    ROUTE_CACHE_HIT.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub(crate) fn record_route_cache_miss() {
    ensure_dumper_started();
    ROUTE_CACHE_MISS.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub(crate) fn record_repropagate(kind: RepropagateKind, elapsed_us: u64) {
    ensure_dumper_started();
    let i = kind as usize;
    RP_COUNT[i].fetch_add(1, Ordering::Relaxed);
    RP_TOTAL_US[i].fetch_add(elapsed_us, Ordering::Relaxed);
    record_max(&RP_MAX_US[i], elapsed_us);
}

#[inline]
pub(crate) fn record_repropagate_subs_step(step: RepropagateSubsStep, elapsed_us: u64) {
    ensure_dumper_started();
    let i = step as usize;
    RPS_COUNT[i].fetch_add(1, Ordering::Relaxed);
    RPS_TOTAL_US[i].fetch_add(elapsed_us, Ordering::Relaxed);
    record_max(&RPS_MAX_US[i], elapsed_us);
}

#[inline]
pub(crate) fn record_flush_declare(elapsed_us: u64) {
    ensure_dumper_started();
    FLUSH_DECLARE_COUNT.fetch_add(1, Ordering::Relaxed);
    FLUSH_DECLARE_TOTAL_US.fetch_add(elapsed_us, Ordering::Relaxed);
    record_max(&FLUSH_DECLARE_MAX_US, elapsed_us);
}

/// PR 1 acceptance gate: time spent in `gateway::new_transport_unicast`
/// from entry to return. With PR 1's spawned flush, this should drop to
/// ~lock-hold time (was ~5s with the synchronous flush).
#[inline]
pub(crate) fn record_new_transport_unicast(elapsed_us: u64) {
    ensure_dumper_started();
    NTU_WALLCLOCK_COUNT.fetch_add(1, Ordering::Relaxed);
    NTU_WALLCLOCK_TOTAL_US.fetch_add(elapsed_us, Ordering::Relaxed);
    record_max(&NTU_WALLCLOCK_MAX_US, elapsed_us);
}

#[inline]
pub(crate) fn record_wtables_flush(site: WTableSite, declares: u64, elapsed_us: u64) {
    ensure_dumper_started();
    let i = site as usize;
    WT_FLUSH_COUNT[i].fetch_add(1, Ordering::Relaxed);
    WT_FLUSH_TOTAL_US[i].fetch_add(elapsed_us, Ordering::Relaxed);
    record_max(&WT_FLUSH_MAX_US[i], elapsed_us);
    WT_FLUSH_DECLARES_TOTAL[i].fetch_add(declares, Ordering::Relaxed);
    record_max(&WT_FLUSH_DECLARES_MAX[i], declares);
}

/// Times wtables write-lock acquire wait + hold for a single site.
///
/// Usage pattern that gives strictly correct hold measurement (declaration
/// order matters because Drop runs LIFO):
///
/// ```ignore
/// let pre_acq = Instant::now();
/// let mut wtables = zwrite!(self.tables.tables);
/// let _wt_timer = WTableTimer::new(WTableSite::Foo, pre_acq);
/// // ... work ...
/// // end of scope: wtables drops first (lock released), then _wt_timer
/// // (records hold from acquire to release)
/// ```
///
/// If the site explicitly drops `wtables` before end of scope (because it
/// flushes deferred declares), call `wt_timer.release()` immediately before
/// the explicit drop so the hold metric is recorded before the lock is
/// released:
///
/// ```ignore
/// let pre_acq = Instant::now();
/// let mut wtables = zwrite!(self.tables.tables);
/// let mut wt_timer = WTableTimer::new(WTableSite::Foo, pre_acq);
/// // ... work ...
/// wt_timer.release();
/// drop(wtables);
/// ```
pub(crate) struct WTableTimer {
    site: WTableSite,
    acquired_at: Instant,
    released: bool,
}

impl WTableTimer {
    /// Construct **after** the lock has been acquired. `pre_acquire` is the
    /// timestamp captured *before* `zwrite!`; the difference between Now
    /// and `pre_acquire` is the acquire-wait time.
    #[inline]
    pub(crate) fn new(site: WTableSite, pre_acquire: Instant) -> Self {
        ensure_dumper_started();
        let now = Instant::now();
        let wait_us = now.duration_since(pre_acquire).as_micros() as u64;
        let i = site as usize;
        WT_COUNT[i].fetch_add(1, Ordering::Relaxed);
        WT_ACQUIRE_WAIT_TOTAL_US[i].fetch_add(wait_us, Ordering::Relaxed);
        record_max(&WT_ACQUIRE_WAIT_MAX_US[i], wait_us);
        hist_record(&WT_ACQUIRE_WAIT_HIST[i], wait_us);
        Self {
            site,
            acquired_at: now,
            released: false,
        }
    }

    #[inline]
    pub(crate) fn release(&mut self) {
        if self.released {
            return;
        }
        let hold_us = self.acquired_at.elapsed().as_micros() as u64;
        let i = self.site as usize;
        WT_HOLD_TOTAL_US[i].fetch_add(hold_us, Ordering::Relaxed);
        record_max(&WT_HOLD_MAX_US[i], hold_us);
        self.released = true;
    }
}

impl Drop for WTableTimer {
    fn drop(&mut self) {
        self.release();
    }
}

/// Times ctrl_lock acquire wait + hold for a single site. Same usage pattern
/// as `WTableTimer`: capture `Instant::now()` before `zlock!`, then
/// construct after acquire.
pub(crate) struct CtrlLockTimer {
    site: CtrlLockSite,
    acquired_at: Instant,
    released: bool,
}

impl CtrlLockTimer {
    #[inline]
    pub(crate) fn new(site: CtrlLockSite, pre_acquire: Instant) -> Self {
        ensure_dumper_started();
        let now = Instant::now();
        let wait_us = now.duration_since(pre_acquire).as_micros() as u64;
        let i = site as usize;
        CL_COUNT[i].fetch_add(1, Ordering::Relaxed);
        CL_ACQUIRE_WAIT_TOTAL_US[i].fetch_add(wait_us, Ordering::Relaxed);
        record_max(&CL_ACQUIRE_WAIT_MAX_US[i], wait_us);
        hist_record(&CL_ACQUIRE_WAIT_HIST[i], wait_us);
        Self {
            site,
            acquired_at: now,
            released: false,
        }
    }

    #[inline]
    pub(crate) fn release(&mut self) {
        if self.released {
            return;
        }
        let hold_us = self.acquired_at.elapsed().as_micros() as u64;
        let i = self.site as usize;
        CL_HOLD_TOTAL_US[i].fetch_add(hold_us, Ordering::Relaxed);
        record_max(&CL_HOLD_MAX_US[i], hold_us);
        self.released = true;
    }
}

impl Drop for CtrlLockTimer {
    fn drop(&mut self) {
        self.release();
    }
}

/// RAII guard that records the elapsed time of `compute_data_route` calls,
/// covering all early-return paths.
pub(crate) struct ComputeDataRouteTimer {
    start: Instant,
}

impl ComputeDataRouteTimer {
    #[inline]
    pub(crate) fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl Drop for ComputeDataRouteTimer {
    fn drop(&mut self) {
        record_compute_data_route(self.start.elapsed().as_micros() as u64);
    }
}

/// Records `route_data` read-lock wait + total fn duration. Construct
/// *before* `zread!` and call `acquired()` after lock acquired; then drop
/// at end of scope.
///
/// Note: the "hold" measurement covers from `acquired()` to Drop, which
/// extends past the explicit lock release. Use this as an end-to-end
/// publisher-side latency, not a strict lock-hold time.
pub(crate) struct RTableTimer {
    start: Instant,
    acquired_at: Option<Instant>,
}

impl RTableTimer {
    #[inline]
    pub(crate) fn start() -> Self {
        ensure_dumper_started();
        Self {
            start: Instant::now(),
            acquired_at: None,
        }
    }

    #[inline]
    pub(crate) fn acquired(&mut self) {
        let now = Instant::now();
        let wait = now.duration_since(self.start).as_micros() as u64;
        RT_WAIT_COUNT.fetch_add(1, Ordering::Relaxed);
        RT_WAIT_TOTAL_US.fetch_add(wait, Ordering::Relaxed);
        record_max(&RT_WAIT_MAX_US, wait);
        hist_record(&RT_WAIT_HIST, wait);
        self.acquired_at = Some(now);
    }
}

impl Drop for RTableTimer {
    fn drop(&mut self) {
        if let Some(acq) = self.acquired_at {
            let hold = acq.elapsed().as_micros() as u64;
            RT_HOLD_TOTAL_US.fetch_add(hold, Ordering::Relaxed);
            record_max(&RT_HOLD_MAX_US, hold);
        }
    }
}

/// Per-call breakdown printed once on every `Face::send_close()` invocation.
#[derive(Default)]
pub(crate) struct CloseFacePhases {
    pub terminate_us: u64,
    pub finalize_pending_queries_us: u64,
    pub ctrl_lock_acquire_us: u64,
    pub finalize_pending_interests_us: u64,
    pub wtables_acquire_us: u64,
    pub close_face_body_us: u64,
    pub send_declares_us: u64,
}

impl CloseFacePhases {
    pub(crate) fn emit(&self, face_id: u64) {
        let total_us = self.terminate_us
            + self.finalize_pending_queries_us
            + self.ctrl_lock_acquire_us
            + self.finalize_pending_interests_us
            + self.wtables_acquire_us
            + self.close_face_body_us
            + self.send_declares_us;
        println!(
            "metric name=close_diag face_id={face_id} \
             terminate_us={} \
             finalize_pending_queries_us={} \
             ctrl_lock_acquire_us={} \
             finalize_pending_interests_us={} \
             wtables_acquire_us={} \
             close_face_body_us={} \
             send_declares_us={} \
             total_us={total_us}",
            self.terminate_us,
            self.finalize_pending_queries_us,
            self.ctrl_lock_acquire_us,
            self.finalize_pending_interests_us,
            self.wtables_acquire_us,
            self.close_face_body_us,
            self.send_declares_us,
        );
    }
}
