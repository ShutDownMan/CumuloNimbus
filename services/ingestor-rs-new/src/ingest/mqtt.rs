//! MQTT-based data ingestor
//! 
//! This module implements data ingestion from MQTT brokers.

use std::collections::HashMap;
use std::sync::Arc;

use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use sha1::{Sha1, Digest};

use crate::error::{IngestorError, IngestorResult};
use crate::dispatch::Dispatcher;
use crate::domain::{DataSeries, DataSeriesMetadata, DataType, NumericDataPoint};
use crate::config;

use super::traits::{DataIngestor, IngestorRegistration};

/// Configuration for the MQTT ingestor
#[derive(Clone, Debug)]
pub struct MqttConfig {
    /// MQTT broker connection options
    pub connection: config::MqttConfig,
    
    /// Mapping from sensor topics to dataseries IDs
    pub topic_mapping: HashMap<String, String>,
}

/// MQTT-based data ingestor
pub struct MqttIngestor {
    config: MqttConfig,
    dispatcher: Arc<Dispatcher>,
    mqtt_client: Option<AsyncClient>,
    mqtt_eventloop: Option<EventLoop>,
    collector: Option<JoinHandle<()>>,
    message_sender: mpsc::Sender<IngestMessage>,
    message_receiver: Arc<Mutex<mpsc::Receiver<IngestMessage>>>,
}

#[derive(Debug)]
struct IngestMessage {
    topic: String,
    payload: Vec<u8>,
}

impl MqttIngestor {
    /// Create a new MQTT ingestor
    pub fn new(config: MqttConfig, dispatcher: Arc<Dispatcher>) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        
        Self {
            config,
            dispatcher,
            mqtt_client: None,
            mqtt_eventloop: None,
            collector: None,
            message_sender: sender,
            message_receiver: Arc::new(Mutex::new(receiver)),
        }
    }
    
    /// Initialize the MQTT connection
    async fn initialize_connection(&mut self) -> IngestorResult<()> {
        let mqtt_options = self.create_mqtt_options()?;
        
        let (client, eventloop) = AsyncClient::new(mqtt_options, 10);
        self.mqtt_client = Some(client);
        self.mqtt_eventloop = Some(eventloop);
        
        Ok(())
    }
    
    fn create_mqtt_options(&self) -> IngestorResult<MqttOptions> {
        let mut mqtt_options = MqttOptions::new(
            &self.config.connection.client_id,
            &self.config.connection.host,
            self.config.connection.port,
        );
        
        mqtt_options.set_keep_alive(self.config.connection.keep_alive);
        
        Ok(mqtt_options)
    }
    
    /// Generate a UUID from a string seed using SHA1
    fn generate_uuid_from_seed(seed: &str) -> Uuid {
        // Create a SHA-1 hash from the seed
        let mut hasher = Sha1::new();
        hasher.update(seed.as_bytes());
        let hash = hasher.finalize();
        
        // Convert the hash to a string and take the first 32 characters
        let hash: String = format!("{:x}", hash);
        let hash = &hash[0..32];
        
        // Generate a UUID from the hash
        Uuid::parse_str(hash).expect("Valid UUID from hash")
    }
    
    /// Process incoming MQTT messages
    async fn process_messages(&self, receiver: Arc<Mutex<mpsc::Receiver<IngestMessage>>>) -> IngestorResult<()> {
        let mut receiver = receiver.lock().await;
        
        while let Some(message) = receiver.recv().await {
            self.handle_message(message).await?;
        }
        
        Ok(())
    }
    
    /// Handle a single MQTT message
    async fn handle_message(&self, message: IngestMessage) -> IngestorResult<()> {
        let topic = message.topic;
        
        // Extract data series ID from topic based on mapping
        let dataseries_id = match self.config.topic_mapping.get(&topic) {
            Some(id) => id.clone(),
            None => {
                // If no explicit mapping, generate an ID from the topic
                Self::generate_uuid_from_seed(&topic).to_string()
            }
        };
        
        // Try to parse payload as float
        let payload_str = String::from_utf8_lossy(&message.payload);
        let value = payload_str.trim().parse::<f64>().map_err(|e| {
            IngestorError::Processing(format!("Failed to parse payload as number: {}", e))
        })?;
        
        // Create data series with a single point
        let metadata = DataSeriesMetadata::with_id(
            dataseries_id,
            format!("MQTT Data: {}", topic),
            format!("Data from MQTT topic: {}", topic),
            DataType::Numeric,
        );
        
        let mut dataseries = DataSeries::new(metadata);
        let datapoint = NumericDataPoint::new(chrono::Utc::now(), value);
        dataseries.add_point(datapoint);
        
        // Dispatch the data series
        self.dispatcher.dispatch(&dataseries).await.map_err(|e| {
            IngestorError::Processing(format!("Failed to dispatch data: {}", e))
        })?;
        
        Ok(())
    }
}

impl DataIngestor for MqttIngestor {
    async fn start(&mut self) -> IngestorResult<IngestorRegistration> {
        // Initialize the MQTT connection
        self.initialize_connection().await?;
        
        let client = self.mqtt_client.as_ref()
            .ok_or_else(|| IngestorError::Mqtt("MQTT client not initialized".to_string()))?;
        
        // Subscribe to topics
        for topic in &self.config.connection.topics {
            info!("Subscribing to MQTT topic: {}", topic);
            client.subscribe(topic, QoS::AtMostOnce).await
                .map_err(|e| IngestorError::Mqtt(format!("Failed to subscribe: {}", e)))?;
        }
        
        // Start the message handler task
        let receiver = self.message_receiver.clone();
        let message_handler = tokio::spawn(async move {
            let ingestor = Arc::new(Self {
                config: self.config.clone(),
                dispatcher: self.dispatcher.clone(),
                mqtt_client: None,
                mqtt_eventloop: None,
                collector: None,
                message_sender: self.message_sender.clone(),
                message_receiver: receiver.clone(),
            });
            
            if let Err(e) = ingestor.process_messages(receiver).await {
                error!("Error processing messages: {}", e);
            }
        });
        
        // Start the MQTT event loop
        let mut eventloop = self.mqtt_eventloop.take()
            .ok_or_else(|| IngestorError::Mqtt("MQTT eventloop not initialized".to_string()))?;
        
        let sender = self.message_sender.clone();
        let collector = tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(event) => {
                        if let rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish)) = event {
                            let message = IngestMessage {
                                topic: publish.topic,
                                payload: publish.payload.to_vec(),
                            };
                            
                            if let Err(e) = sender.send(message).await {
                                error!("Failed to send message to handler: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("MQTT connection error: {}", e);
                        // TODO: Add reconnection logic
                        break;
                    }
                }
            }
        });
        
        self.collector = Some(collector);
        
        Ok(IngestorRegistration {
            handler: message_handler,
        })
    }
    
    async fn stop(&mut self) -> IngestorResult<()> {
        if let Some(client) = &self.mqtt_client {
            // Unsubscribe from all topics
            for topic in &self.config.connection.topics {
                if let Err(e) = client.unsubscribe(topic).await {
                    warn!("Failed to unsubscribe from {}: {}", topic, e);
                }
            }
            
            // Disconnect from broker
            if let Err(e) = client.disconnect().await {
                warn!("Failed to disconnect from MQTT broker: {}", e);
            }
        }
        
        // Cancel all tasks
        if let Some(collector) = self.collector.take() {
            collector.abort();
        }
        
        self.mqtt_client = None;
        self.mqtt_eventloop = None;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::MockDispatcher;
    use std::time::Duration;
    
    #[test]
    fn test_generate_uuid_from_seed() {
        let seed = "test/topic/1";
        let uuid1 = MqttIngestor::generate_uuid_from_seed(seed);
        let uuid2 = MqttIngestor::generate_uuid_from_seed(seed);
        
        // Same seed should produce the same UUID
        assert_eq!(uuid1, uuid2);
        
        let other_seed = "test/topic/2";
        let uuid3 = MqttIngestor::generate_uuid_from_seed(other_seed);
        
        // Different seeds should produce different UUIDs
        assert_ne!(uuid1, uuid3);
    }
    
    #[tokio::test]
    async fn test_create_mqtt_options() {
        let config = MqttConfig {
            connection: config::MqttConfig {
                client_id: "test-client".to_string(),
                host: "localhost".to_string(),
                port: 1883,
                keep_alive: Duration::from_secs(60),
                topics: vec!["test/topic/#".to_string()],
            },
            topic_mapping: HashMap::new(),
        };
        
        let dispatcher = Arc::new(MockDispatcher::new());
        let ingestor = MqttIngestor::new(config, dispatcher);
        
        let options = ingestor.create_mqtt_options().unwrap();
        assert_eq!(options.connect_options().client_id, "test-client");
    }
}
