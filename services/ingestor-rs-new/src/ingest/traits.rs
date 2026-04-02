//! Data ingestor traits
//! 
//! This module defines traits for data ingestion.

use tokio::task::JoinHandle;
use crate::error::IngestorResult;

/// Registration for a running ingestor
pub struct IngestorRegistration {
    /// Handle to the ingestor task
    pub handler: JoinHandle<()>,
}

/// Trait for data ingestors
pub trait DataIngestor {
    /// Start the ingestor
    async fn start(&mut self) -> IngestorResult<IngestorRegistration>;
    
    /// Stop the ingestor
    async fn stop(&mut self) -> IngestorResult<()>;
}
