//! Data ingestion functionality
//! 
//! This module is responsible for ingesting data from various sources.

mod mqtt;
mod traits;

// Re-export public APIs
pub use mqtt::{MqttIngestor, MqttConfig};
pub use traits::{DataIngestor, IngestorRegistration};
