use std::time::Duration;
use crate::error::{IngestorError, IngestorResult};

/// Base configuration for the ingestor service
#[derive(Debug, Clone)]
pub struct Config {
    pub database: DatabaseConfig,
    pub mqtt: MqttConfig,
    pub housekeeper: HousekeeperConfig,
    pub dispatch: DispatchConfig,
}

/// Database configuration
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub url: String,
}

/// MQTT configuration
#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub client_id: String,
    pub host: String,
    pub port: u16,
    pub keep_alive: Duration,
    pub topics: Vec<String>,
}

/// Housekeeper configuration
#[derive(Debug, Clone)]
pub struct HousekeeperConfig {
    pub work_interval: Duration,
    pub idle_interval: Duration,
    pub patience_falloff_rate: f64,
    pub patience_recovery_rate: f64,
    pub patience_min_threshold: f64,
}

/// Dispatcher configuration
#[derive(Debug, Clone)]
pub struct DispatchConfig {
    pub strategy: DispatchStrategy,
    pub temporary_storage_duration: Option<chrono::Duration>,
}

/// Dispatch strategies
#[derive(Debug, Clone)]
pub enum DispatchStrategy {
    Realtime,
    Batched { 
        max_batch: usize,
        trigger: DispatchTrigger,
    },
}

/// Dispatch triggers
#[derive(Debug, Clone)]
pub enum DispatchTrigger {
    BatchSize,
    Holdoff { holdoff: chrono::Duration },
    Interval { interval: Duration },
    Cron { cron: String },
}

impl Config {
    /// Load configuration from environment variables or defaults
    pub fn from_env() -> IngestorResult<Self> {
        // Load database config
        let db_url = std::env::var("INGESTOR_DB_URL").unwrap_or_else(|_| "sqlite://./db/ingestor.db".to_string());
        
        // Load MQTT config
        let mqtt_host = std::env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".to_string());
        let mqtt_port = std::env::var("MQTT_PORT")
            .map(|p| p.parse::<u16>().unwrap_or(1883))
            .unwrap_or(1883);
        let mqtt_client_id = std::env::var("MQTT_CLIENT_ID").unwrap_or_else(|_| "ingestor-rs".to_string());
        let mqtt_keep_alive = std::env::var("MQTT_KEEP_ALIVE")
            .map(|s| s.parse::<u64>().unwrap_or(60))
            .unwrap_or(60);
        let mqtt_topics = std::env::var("MQTT_TOPICS")
            .map(|s| s.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_else(|_| vec!["agrometeo/stations/#".to_string()]);
        
        // Load housekeeper config
        let hk_work_interval = std::env::var("HK_WORK_INTERVAL")
            .map(|s| s.parse::<u64>().unwrap_or(2500))
            .unwrap_or(2500);
        let hk_idle_interval = std::env::var("HK_IDLE_INTERVAL")
            .map(|s| s.parse::<u64>().unwrap_or(15000))
            .unwrap_or(15000);
        let hk_patience_falloff = std::env::var("HK_PATIENCE_FALLOFF")
            .map(|s| s.parse::<f64>().unwrap_or(0.5))
            .unwrap_or(0.5);
        let hk_patience_recovery = std::env::var("HK_PATIENCE_RECOVERY")
            .map(|s| s.parse::<f64>().unwrap_or(0.1))
            .unwrap_or(0.1);
        let hk_patience_min = std::env::var("HK_PATIENCE_MIN")
            .map(|s| s.parse::<f64>().unwrap_or(0.01))
            .unwrap_or(0.01);
            
        // Load dispatch config
        let dispatch_strategy = match std::env::var("DISPATCH_STRATEGY").unwrap_or_else(|_| "batched".to_string()).as_str() {
            "realtime" => DispatchStrategy::Realtime,
            _ => {
                let max_batch = std::env::var("DISPATCH_MAX_BATCH")
                    .map(|s| s.parse::<usize>().unwrap_or(1000))
                    .unwrap_or(1000);
                let interval_seconds = std::env::var("DISPATCH_INTERVAL_SECONDS")
                    .map(|s| s.parse::<u64>().unwrap_or(1))
                    .unwrap_or(1);
                DispatchStrategy::Batched {
                    max_batch,
                    trigger: DispatchTrigger::Interval { interval: Duration::from_secs(interval_seconds) },
                }
            }
        };
        
        let temp_storage = std::env::var("TEMP_STORAGE_MINUTES")
            .map(|s| s.parse::<i64>().ok().map(chrono::Duration::minutes))
            .unwrap_or(Some(chrono::Duration::minutes(5)));
            
        Ok(Config {
            database: DatabaseConfig {
                url: db_url,
            },
            mqtt: MqttConfig {
                client_id: mqtt_client_id,
                host: mqtt_host,
                port: mqtt_port,
                keep_alive: Duration::from_secs(mqtt_keep_alive),
                topics: mqtt_topics,
            },
            housekeeper: HousekeeperConfig {
                work_interval: Duration::from_millis(hk_work_interval),
                idle_interval: Duration::from_millis(hk_idle_interval),
                patience_falloff_rate: hk_patience_falloff,
                patience_recovery_rate: hk_patience_recovery,
                patience_min_threshold: hk_patience_min,
            },
            dispatch: DispatchConfig {
                strategy: dispatch_strategy,
                temporary_storage_duration: temp_storage,
            },
        })
    }
}
