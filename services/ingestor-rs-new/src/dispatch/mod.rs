//! Data dispatch functionality
//! 
//! This module is responsible for dispatching data to external systems.

mod service_bus;
mod strategy;

#[cfg(test)]
mod mock;

// Re-export public APIs
pub use service_bus::ServiceBusDispatcher;
pub use strategy::{DispatchStrategy, DispatchTrigger};
pub use mock::MockDispatcher;

use async_trait::async_trait;
use std::sync::Arc;

use crate::domain::{DataSeries, NumericDataPoint};
use crate::error::IngestorResult;

/// Trait for dispatching data to external systems
#[async_trait]
pub trait Dispatcher: Send + Sync {
    /// Dispatch a data series to external systems
    async fn dispatch(&self, dataseries: &DataSeries<NumericDataPoint>) -> IngestorResult<()>;
}
