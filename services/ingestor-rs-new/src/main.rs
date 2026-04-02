use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

mod config;
mod domain;
mod dispatch;
mod error;
mod housekeeping;
mod ingest;
mod storage;

use config::Config;
use dispatch::{Dispatcher, ServiceBusDispatcher};
use error::IngestorResult;
use housekeeping::Housekeeper;
use ingest::{DataIngestor, MqttIngestor, MqttConfig};
use storage::{SqliteStorage, Storage};

/// Main entry point for the ingestor service
#[tokio::main]
async fn main() -> IngestorResult<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_env("LOG_LEVEL"))
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_line_number(true)
        .init();

    info!("Starting Ingestor Service");

    // Load configuration
    let config = Config::from_env()?;
    info!("Configuration loaded");

    // Initialize database
    let storage = SqliteStorage::connect(&config.database.url, "./migrations").await?;
    storage.initialize().await?;
    info!("Database initialized");

    // Initialize service bus
    let service_bus = Arc::new(
        intercom::ServiceBus::new().await
            .map_err(|e| error::IngestorError::ServiceBus(format!("Failed to initialize service bus: {}", e)))?
    );
    info!("Service bus initialized");

    // Initialize dispatcher
    let dispatcher = Arc::new(ServiceBusDispatcher::new(
        storage.get_pool(),
        service_bus.clone(),
        tokio::runtime::Handle::current(),
        config.dispatch.clone(),
    ));
    info!("Dispatcher initialized");

    // Initialize housekeeper
    let housekeeper = Housekeeper::new(
        storage.get_pool(),
        config.housekeeper.clone(),
    );
    info!("Housekeeper initialized");

    // Start the housekeeper in the background
    let housekeeper_handle = tokio::spawn(async move {
        if let Err(e) = housekeeper.start().await {
            error!("Housekeeper error: {}", e);
        }
    });

    // Initialize and start the MQTT ingestor
    let mqtt_config = MqttConfig {
        connection: config.mqtt,
        topic_mapping: std::collections::HashMap::new(), // This would be populated from config in a real app
    };
    
    let mut mqtt_ingestor = MqttIngestor::new(mqtt_config, dispatcher.clone());
    let registration = mqtt_ingestor.start().await?;
    info!("MQTT ingestor started");

    // Wait for the MQTT ingestor to complete
    match registration.handler.await {
        Ok(_) => info!("MQTT ingestor stopped normally"),
        Err(e) => error!("MQTT ingestor error: {}", e),
    }

    // Cancel the housekeeper
    housekeeper_handle.abort();

    info!("Ingestor service stopping");
    Ok(())
}
