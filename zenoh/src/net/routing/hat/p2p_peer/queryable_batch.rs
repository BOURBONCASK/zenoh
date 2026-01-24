//
// Copyright (c) 2023 ZettaScale Technology
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

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::net::routing::dispatcher::resource::Resource;

/// Type of propagation operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropagationType {
    /// Remove then insert (double propagate for undeclare with remaining queryables)
    ForgetThenDeclare,
    /// Only forget (undeclare with no remaining queryables)
    ForgetOnly,
}

/// Entry in the batch queue
#[derive(Clone)]
pub struct BatchEntry {
    pub resource: Arc<Resource>,
    pub operation: PropagationType,
    pub enqueued_at: Instant,
}

impl std::fmt::Debug for BatchEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchEntry")
            .field("resource_expr", &self.resource.expr())
            .field("operation", &self.operation)
            .field("enqueued_at", &self.enqueued_at)
            .finish()
    }
}

/// Adaptive batching configuration
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// High-load flush interval (during startup storms)
    pub high_load_interval: Duration,
    /// Normal flush interval (during steady state)
    pub normal_interval: Duration,
    /// Threshold to switch to high-load mode (events per second)
    pub high_load_threshold: usize,
    /// Maximum batch size before forced flush
    pub max_batch_size: usize,
    /// Time window for calculating event rate
    pub rate_window: Duration,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            high_load_interval: Duration::from_millis(100),
            normal_interval: Duration::from_millis(10),
            high_load_threshold: 50, // 50 events per second
            max_batch_size: 100,
            rate_window: Duration::from_secs(1),
        }
    }
}

/// Queryable propagation batch queue
pub struct QueryableBatchQueue {
    /// Pending operations by resource expression
    pending: HashMap<String, BatchEntry>,
    /// Configuration
    config: BatchConfig,
    /// Last flush time
    last_flush: Instant,
    /// Event timestamps for rate calculation
    recent_events: Vec<Instant>,
    /// Total events processed (for stats)
    total_enqueued: usize,
    /// Total flushes (for stats)
    total_flushes: usize,
}

impl QueryableBatchQueue {
    pub fn new(config: BatchConfig) -> Self {
        Self {
            pending: HashMap::new(),
            config,
            last_flush: Instant::now(),
            recent_events: Vec::new(),
            total_enqueued: 0,
            total_flushes: 0,
        }
    }

    pub fn with_default_config() -> Self {
        Self::new(BatchConfig::default())
    }

    /// Enqueue a queryable propagation operation
    pub fn enqueue(&mut self, resource: Arc<Resource>, operation: PropagationType) {
        let now = Instant::now();
        let res_expr = resource.expr().to_string();

        // Update or insert entry
        self.pending.insert(
            res_expr.clone(),
            BatchEntry {
                resource,
                operation,
                enqueued_at: now,
            },
        );

        // Track event for rate calculation
        self.recent_events.push(now);
        self.total_enqueued += 1;

        // Clean old events outside the rate window
        let cutoff = now.checked_sub(self.config.rate_window).unwrap_or(now);
        self.recent_events.retain(|&t| t >= cutoff);

        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                "BATCH_ENQUEUE res={} operation={:?} pending={} rate={}/s",
                res_expr,
                operation,
                self.pending.len(),
                self.calculate_rate()
            );
        }
    }

    /// Check if batch should be flushed
    pub fn should_flush(&self) -> bool {
        if self.pending.is_empty() {
            return false;
        }

        let now = Instant::now();
        let elapsed = now.duration_since(self.last_flush);

        // Force flush if max batch size reached
        if self.pending.len() >= self.config.max_batch_size {
            if tracing::enabled!(tracing::Level::DEBUG) {
                tracing::debug!(
                    "BATCH_SHOULD_FLUSH reason=max_size pending={} max={}",
                    self.pending.len(),
                    self.config.max_batch_size
                );
            }
            return true;
        }

        // Check if flush interval exceeded
        let interval = if self.is_high_load() {
            self.config.high_load_interval
        } else {
            self.config.normal_interval
        };

        if elapsed >= interval {
            if tracing::enabled!(tracing::Level::DEBUG) {
                tracing::debug!(
                    "BATCH_SHOULD_FLUSH reason=interval elapsed={:?} threshold={:?} high_load={} pending={}",
                    elapsed,
                    interval,
                    self.is_high_load(),
                    self.pending.len()
                );
            }
            return true;
        }

        false
    }

    /// Calculate current event rate (events per second)
    fn calculate_rate(&self) -> usize {
        if self.recent_events.is_empty() {
            return 0;
        }

        let now = Instant::now();
        let cutoff = now.checked_sub(self.config.rate_window).unwrap_or(now);
        let count = self.recent_events.iter().filter(|&&t| t >= cutoff).count();

        // Calculate events per second
        count
    }

    /// Check if we're in high-load mode
    fn is_high_load(&self) -> bool {
        self.calculate_rate() > self.config.high_load_threshold
    }

    /// Get current flush interval based on load
    pub fn current_flush_interval(&self) -> Duration {
        if self.is_high_load() {
            self.config.high_load_interval
        } else {
            self.config.normal_interval
        }
    }

    /// Take all pending entries for flushing
    pub fn take_pending(&mut self) -> Vec<BatchEntry> {
        let now = Instant::now();
        self.last_flush = now;
        self.total_flushes += 1;

        let pending: Vec<BatchEntry> = self.pending.drain().map(|(_, entry)| entry).collect();

        if tracing::enabled!(tracing::Level::INFO) {
            let rate = self.calculate_rate();
            let high_load = self.is_high_load();
            tracing::info!(
                "BATCH_FLUSH entries={} rate={}/s high_load={} total_enqueued={} total_flushes={}",
                pending.len(),
                rate,
                high_load,
                self.total_enqueued,
                self.total_flushes
            );
        }

        pending
    }

    /// Get statistics
    pub fn stats(&self) -> BatchStats {
        BatchStats {
            pending: self.pending.len(),
            rate: self.calculate_rate(),
            high_load: self.is_high_load(),
            total_enqueued: self.total_enqueued,
            total_flushes: self.total_flushes,
            current_interval: self.current_flush_interval(),
        }
    }
}

/// Batch queue statistics
#[derive(Debug, Clone)]
pub struct BatchStats {
    pub pending: usize,
    pub rate: usize,
    pub high_load: bool,
    pub total_enqueued: usize,
    pub total_flushes: usize,
    pub current_interval: Duration,
}
