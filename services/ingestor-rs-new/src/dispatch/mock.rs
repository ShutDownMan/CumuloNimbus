//! Mock dispatcher for testing
//! 
//! This module provides a mock dispatcher for testing.

use std::sync::{Arc, Mutex};
use async_trait::async_trait;

use crate::domain::{DataSeries, NumericDataPoint};
use crate::error::IngestorResult;
use super::Dispatcher;

/// Mock dispatcher for testing
pub struct MockDispatcher {
    dispatched: Arc<Mutex<Vec<DataSeries<NumericDataPoint>>>>,
}

impl MockDispatcher {
    /// Create a new mock dispatcher
    pub fn new() -> Self {
        Self {
            dispatched: Arc::new(Mutex::new(Vec::new())),
        }
    }
    
    /// Get the dispatched data series
    pub fn get_dispatched(&self) -> Vec<DataSeries<NumericDataPoint>> {
        self.dispatched.lock().unwrap().clone()
    }
}

#[async_trait]
impl Dispatcher for MockDispatcher {
    async fn dispatch(&self, dataseries: &DataSeries<NumericDataPoint>) -> IngestorResult<()> {
        // Store a copy of the data series
        self.dispatched.lock().unwrap().push(dataseries.clone());
        Ok(())
    }
}

impl Default for MockDispatcher {
    fn default() -> Self {
        Self::new()
    }
}
