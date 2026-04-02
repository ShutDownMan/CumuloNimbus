//! Data point models
//! 
//! This module defines the data points that make up data series.

use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Unique identifier for a data point
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataPointId(pub Uuid);

impl DataPointId {
    pub fn new() -> Self {
        DataPointId(Uuid::new_v4())
    }
}

/// Base trait for a data point
pub trait DataPoint {
    /// The data type of this point
    type Value;
    
    /// Get the timestamp of this data point
    fn timestamp(&self) -> DateTime<Utc>;
    
    /// Get the value of this data point
    fn value(&self) -> &Self::Value;
    
    /// Get the ID of this data point
    fn id(&self) -> &DataPointId;
}

/// A numeric data point
#[derive(Debug, Clone)]
pub struct NumericDataPoint {
    id: DataPointId,
    timestamp: DateTime<Utc>,
    value: f64,
}

impl NumericDataPoint {
    pub fn new(timestamp: DateTime<Utc>, value: f64) -> Self {
        Self {
            id: DataPointId::new(),
            timestamp,
            value,
        }
    }
    
    pub fn with_id(id: DataPointId, timestamp: DateTime<Utc>, value: f64) -> Self {
        Self {
            id, 
            timestamp,
            value,
        }
    }
}

impl DataPoint for NumericDataPoint {
    type Value = f64;
    
    fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }
    
    fn value(&self) -> &Self::Value {
        &self.value
    }
    
    fn id(&self) -> &DataPointId {
        &self.id
    }
}

/// A text data point
#[derive(Debug, Clone)]
pub struct TextDataPoint {
    id: DataPointId,
    timestamp: DateTime<Utc>,
    value: String,
}

impl TextDataPoint {
    pub fn new(timestamp: DateTime<Utc>, value: String) -> Self {
        Self {
            id: DataPointId::new(),
            timestamp,
            value,
        }
    }
    
    pub fn with_id(id: DataPointId, timestamp: DateTime<Utc>, value: String) -> Self {
        Self {
            id,
            timestamp,
            value,
        }
    }
}

impl DataPoint for TextDataPoint {
    type Value = String;
    
    fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }
    
    fn value(&self) -> &Self::Value {
        &self.value
    }
    
    fn id(&self) -> &DataPointId {
        &self.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_numeric_data_point() {
        let now = Utc::now();
        let point = NumericDataPoint::new(now, 42.5);
        
        assert_eq!(point.timestamp(), now);
        assert_eq!(*point.value(), 42.5);
    }
    
    #[test]
    fn test_text_data_point() {
        let now = Utc::now();
        let point = TextDataPoint::new(now, "test".to_string());
        
        assert_eq!(point.timestamp(), now);
        assert_eq!(*point.value(), "test".to_string());
    }
}
