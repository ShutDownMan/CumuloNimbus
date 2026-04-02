//! Data series models
//! 
//! This module defines the data series that represent time-series data.

use std::fmt;
use uuid::Uuid;
use super::data_point::{DataPoint, NumericDataPoint, TextDataPoint};

/// The type of data stored in a data series
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Numeric,
    Text,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Numeric => write!(f, "numeric"),
            DataType::Text => write!(f, "text"),
        }
    }
}

/// Metadata for a data series
#[derive(Debug, Clone)]
pub struct DataSeriesMetadata {
    pub id: String,
    pub name: String,
    pub description: String,
    pub data_type: DataType,
}

impl DataSeriesMetadata {
    pub fn new(name: String, description: String, data_type: DataType) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            name,
            description,
            data_type,
        }
    }
    
    pub fn with_id(id: String, name: String, description: String, data_type: DataType) -> Self {
        Self {
            id,
            name,
            description,
            data_type,
        }
    }
}

/// A series of data points
#[derive(Debug, Clone)]
pub struct DataSeries<P> 
where
    P: DataPoint,
{
    pub metadata: DataSeriesMetadata,
    pub values: Vec<P>,
}

impl<P> DataSeries<P> 
where
    P: DataPoint,
{
    pub fn new(metadata: DataSeriesMetadata) -> Self {
        Self {
            metadata,
            values: Vec::new(),
        }
    }
    
    pub fn add_point(&mut self, point: P) {
        self.values.push(point);
    }
    
    pub fn len(&self) -> usize {
        self.values.len()
    }
    
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    
    pub fn clear(&mut self) {
        self.values.clear();
    }
}

/// Type aliases for common data series types
pub type NumericDataSeries = DataSeries<NumericDataPoint>;
pub type TextDataSeries = DataSeries<TextDataPoint>;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    
    #[test]
    fn test_data_series() {
        let metadata = DataSeriesMetadata::new(
            "Temperature".to_string(),
            "Temperature readings".to_string(),
            DataType::Numeric,
        );
        
        let mut series = NumericDataSeries::new(metadata);
        assert!(series.is_empty());
        
        let now = Utc::now();
        series.add_point(NumericDataPoint::new(now, 22.5));
        assert_eq!(series.len(), 1);
        
        series.clear();
        assert!(series.is_empty());
    }
}
