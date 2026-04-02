//! Dispatch strategies
//!
//! This module defines strategies for dispatching data.

use chrono::Duration;
use std::time::Duration as StdDuration;

/// Trigger for batched dispatch
#[derive(Debug, Clone)]
pub enum DispatchTrigger {
    /// Trigger when batch size reaches threshold
    BatchSize,
    
    /// Trigger after a certain period of no activity
    Holdoff { holdoff: Duration },
    
    /// Trigger at fixed intervals
    Interval { interval: StdDuration },
    
    /// Trigger based on cron schedule
    Cron { cron: String },
}

/// Strategy for dispatching data
#[derive(Debug, Clone)]
pub enum DispatchStrategy {
    /// Dispatch data immediately
    Realtime,
    
    /// Collect data in batches before dispatching
    Batched {
        max_batch: usize,
        trigger: DispatchTrigger,
    },
}
