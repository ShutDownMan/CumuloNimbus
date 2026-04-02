use std::fmt;

/// Custom error types for the ingestor service
#[derive(Debug)]
pub enum IngestorError {
    /// Database connection or query errors
    Database(String),
    
    /// MQTT connection or subscription errors
    Mqtt(String),
    
    /// Message processing errors
    Processing(String),
    
    /// Service bus communication errors
    ServiceBus(String),
    
    /// Utility errors
    Utility(String),
    
    /// Configuration errors
    Config(String),
}

impl fmt::Display for IngestorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IngestorError::Database(msg) => write!(f, "Database error: {}", msg),
            IngestorError::Mqtt(msg) => write!(f, "MQTT error: {}", msg),
            IngestorError::Processing(msg) => write!(f, "Processing error: {}", msg),
            IngestorError::ServiceBus(msg) => write!(f, "Service bus error: {}", msg),
            IngestorError::Utility(msg) => write!(f, "Utility error: {}", msg),
            IngestorError::Config(msg) => write!(f, "Configuration error: {}", msg),
        }
    }
}

impl std::error::Error for IngestorError {}

// Convenience Result type for ingestor operations
pub type IngestorResult<T> = Result<T, IngestorError>;

// Conversion from sqlx errors
impl From<sqlx::Error> for IngestorError {
    fn from(err: sqlx::Error) -> Self {
        IngestorError::Database(err.to_string())
    }
}

// Conversion from rumqttc errors
impl From<rumqttc::ClientError> for IngestorError {
    fn from(err: rumqttc::ClientError) -> Self {
        IngestorError::Mqtt(err.to_string())
    }
}

// Conversion from anyhow errors (for transition period)
impl From<anyhow::Error> for IngestorError {
    fn from(err: anyhow::Error) -> Self {
        IngestorError::Utility(err.to_string())
    }
}
