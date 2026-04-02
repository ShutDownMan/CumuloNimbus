//! Domain models for the ingestor service
//! 
//! This module contains the core domain models that represent
//! the business entities in the ingestor service.

mod data_point;
mod data_series;
mod sensor;

// Re-export domain models
pub use data_point::{DataPoint, NumericDataPoint, TextDataPoint, DataPointId};
pub use data_series::{DataSeries, DataSeriesMetadata, DataType};
pub use sensor::{Sensor, SensorId, SensorType, SensorReading};
