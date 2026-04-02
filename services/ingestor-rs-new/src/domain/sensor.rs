//! Sensor models
//!
//! This module defines sensor-related domain models.

use uuid::Uuid;
use chrono::{DateTime, Utc};

/// Unique identifier for a sensor
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SensorId(pub String);

impl SensorId {
    pub fn new<S: Into<String>>(id: S) -> Self {
        SensorId(id.into())
    }
    
    pub fn random() -> Self {
        SensorId(Uuid::new_v4().to_string())
    }
}

/// Types of sensors
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SensorType {
    Temperature,
    Humidity,
    Pressure,
    WindSpeed,
    WindDirection,
    Rainfall,
    SoilMoisture,
    LightLevel,
    Custom(String),
}

/// Sensor entity
#[derive(Debug, Clone)]
pub struct Sensor {
    pub id: SensorId,
    pub name: String,
    pub sensor_type: SensorType,
    pub station_id: String,
    pub location: Option<String>,
}

impl Sensor {
    pub fn new<S: Into<String>>(
        id: SensorId,
        name: S,
        sensor_type: SensorType,
        station_id: S,
        location: Option<S>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            sensor_type,
            station_id: station_id.into(),
            location: location.map(Into::into),
        }
    }
}

/// A reading from a sensor
#[derive(Debug, Clone)]
pub struct SensorReading {
    pub sensor_id: SensorId,
    pub timestamp: DateTime<Utc>,
    pub value: f64,
    pub unit: String,
}

impl SensorReading {
    pub fn new(sensor_id: SensorId, value: f64, unit: String) -> Self {
        Self {
            sensor_id,
            timestamp: Utc::now(),
            value,
            unit,
        }
    }
    
    pub fn with_timestamp(sensor_id: SensorId, value: f64, unit: String, timestamp: DateTime<Utc>) -> Self {
        Self {
            sensor_id,
            timestamp,
            value,
            unit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_sensor_id() {
        let id = SensorId::new("temp-1");
        assert_eq!(id.0, "temp-1");
        
        let random_id = SensorId::random();
        assert!(!random_id.0.is_empty());
    }
    
    #[test]
    fn test_sensor() {
        let sensor = Sensor::new(
            SensorId::new("temp-1"),
            "Temperature Sensor 1",
            SensorType::Temperature,
            "station-1",
            Some("Building A"),
        );
        
        assert_eq!(sensor.id.0, "temp-1");
        assert_eq!(sensor.name, "Temperature Sensor 1");
        assert!(matches!(sensor.sensor_type, SensorType::Temperature));
        assert_eq!(sensor.station_id, "station-1");
        assert_eq!(sensor.location, Some("Building A".to_string()));
    }
    
    #[test]
    fn test_sensor_reading() {
        let sensor_id = SensorId::new("temp-1");
        let reading = SensorReading::new(sensor_id.clone(), 22.5, "°C".to_string());
        
        assert_eq!(reading.sensor_id, sensor_id);
        assert_eq!(reading.value, 22.5);
        assert_eq!(reading.unit, "°C");
    }
}
